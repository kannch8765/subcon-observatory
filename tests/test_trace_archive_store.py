"""Trace archive step 2: --archive writes, re-reads and verifies; --restore-day gives the rows back as psql CSV."""
import datetime, io, json, os, sqlite3, stat, subprocess, sys, zlib

import pytest

from observatory import trace_archive as ta
from tests.test_export import FAKE_PSQL
from tests.test_trace_archive import request, row

NOW = datetime.datetime(2026, 11, 8, tzinfo=datetime.timezone.utc)
SHARED = {"role": "user", "content": "long transcript 🐾 " * 50}


def fixture_rows():
    return [row("a", request(SHARED, {"role": "user", "content": "lone \\ud83d surrogate"}), source=""),
            row("b", request(SHARED, {"role": "assistant", "content": "reply"}), created_at="2026-08-08 11:00:00+09"),
            row("c", request(SHARED), created_at="2026-08-09 01:00:00+09", agent_tags='["x"]'),
            row("d", "not json, kept whole", created_at="2026-08-09 02:00:00+09"),
            row("bad", request({"role": "user", "content": "ok"}), created_at="2026-08-09 03:00:00+09",
                response_json='{"x": "\udcff"}'),
            row("new", request(SHARED), created_at="2026-11-01 09:00:00+09")]  # inside the 30-day window


def setup(tmp_path, rows):
    fixtures = tmp_path / "letta.json"
    fixtures.write_text(json.dumps({"provider_traces": rows}))
    directory = tmp_path / "archive"
    directory.mkdir(mode=0o700)
    return {"LETTA_PSQL": str(FAKE_PSQL), "LETTA_PG_ARGS": str(fixtures)}, str(directory)


def run(config, directory, **kw):
    out = io.StringIO()
    code = ta.run_archive(config, directory, now=NOW, out=out, min_free_gb=kw.pop("min_free_gb", 0), **kw)
    return code, out.getvalue()


def archive_db(directory):
    return sqlite3.connect(os.path.join(directory, ta.ARCHIVE_DB))


def last_totals(directory):
    return json.loads(archive_db(directory).execute("select totals from runs order by run desc limit 1").fetchone()[0])


def test_archive_writes_verifies_and_stores_each_message_once(tmp_path):
    config, directory = setup(tmp_path, fixture_rows())
    code, out = run(config, directory)
    assert code == 0 and "2026-08-08" in out and "2026-11-01" not in out
    t = last_totals(directory)
    assert (t["rows"], t["new"], t["verified"], t["undecodable"], t["reread_failed"], t["conflict"]) == (5, 4, 4, 1, 0, 0)
    db = archive_db(directory)
    assert sorted(i for (i,) in db.execute("select id from traces where verified_at is not null")) == ["a", "b", "c", "d"]
    assert db.execute("select status from traces where id = 'd'").fetchone() == ("whole",)
    # SYSTEM, SHARED, the lone-surrogate message and the reply: each stored once across 3 split rows
    assert db.execute("select count(*) from messages").fetchone() == (4,)
    assert db.execute("select value from meta where key = 'format'").fetchone() == (ta.ARCHIVE_FORMAT,)


def test_second_run_adds_nothing_and_counts_already(tmp_path):
    config, directory = setup(tmp_path, fixture_rows())
    run(config, directory)
    size = os.path.getsize(os.path.join(directory, ta.ARCHIVE_DB))
    run(config, directory)
    t = last_totals(directory)
    assert (t["new"], t["already"], t["verified"], t["added_bytes"]) == (0, 4, 0, 0)
    assert os.path.getsize(os.path.join(directory, ta.ARCHIVE_DB)) == size


def test_changed_postgres_row_is_a_conflict_keeps_the_archived_copy_but_unverifies_it(tmp_path):
    rows = fixture_rows()
    config, directory = setup(tmp_path, rows)
    run(config, directory)
    before = archive_db(directory).execute("select record_sha256 from traces where id = 'b'").fetchone()
    rows[1]["response_json"] = '{"choices": ["changed"]}'
    (tmp_path / "letta.json").write_text(json.dumps({"provider_traces": rows}))
    for _ in range(2):  # stays a conflict, and unverified, for as long as the two differ
        code, _ = run(config, directory)
        assert code == 4 and last_totals(directory)["conflict"] == 1
        # The old copy no longer holds what Postgres holds: it must not count as verified (nor be restored)
        assert archive_db(directory).execute("select record_sha256, verified_at from traces where id = 'b'").fetchone() == (*before, None)
    reader = sqlite3.connect(f"file:{os.path.join(directory, ta.ARCHIVE_DB)}?mode=ro", uri=True)
    assert [r["id"] for r in ta.restore_rows(reader, "2026-08-08")] == ["a"]


def test_reread_catches_a_damaged_archive_and_leaves_the_row_unverified(tmp_path):
    rows = fixture_rows()
    config, directory = setup(tmp_path, rows)
    run(config, directory)
    db = archive_db(directory)
    shared = ta.message_hash(ta.canonical(SHARED))
    with db:
        db.execute("update messages set z = ? where hash = ?", (zlib.compress(b'{"role":"user"}'), shared))
        db.execute("update traces set verified_at = null where id = 'c'")  # as if a run stopped before marking it
    reader = sqlite3.connect(f"file:{os.path.join(directory, ta.ARCHIVE_DB)}?mode=ro", uri=True)
    assert not ta.verify_archived(reader, rows[2])
    code, _ = run(config, directory)
    t = last_totals(directory)
    assert code == 4
    # a, b and c all hold SHARED: c (unverified) and a, b (verified by the first run) fail the re-read; d doesn't
    assert (t["reread_failed"], t["already"], t["verified"]) == (3, 1, 0)
    assert [i for (i,) in db.execute("select id from traces where verified_at is not null")] == ["d"]


def test_already_verified_rows_are_reread_on_every_run(tmp_path):
    # A verified mark is not trusted forever: damage after a fully verified run is caught by the next run
    config, directory = setup(tmp_path, fixture_rows())
    assert run(config, directory)[0] == 0
    db = archive_db(directory)
    first = dict(db.execute("select id, verified_at from traces"))
    reply = ta.message_hash(ta.canonical({"role": "assistant", "content": "reply"}))  # only in row b
    with db:
        db.execute("update messages set z = ? where hash = ?", (zlib.compress(b'{"role":"assistant"}'), reply))
    code, _ = run(config, directory)
    t = last_totals(directory)
    assert code == 4 and (t["reread_failed"], t["already"], t["verified"]) == (1, 3, 0)
    marks = dict(db.execute("select id, verified_at from traces"))
    assert marks["b"] is None
    assert {i: m for i, m in marks.items() if i != "b"} == {i: m for i, m in first.items() if i != "b"}  # kept as they were


def test_verified_row_that_postgres_can_no_longer_give_exactly_loses_its_mark(tmp_path):
    rows = fixture_rows()
    config, directory = setup(tmp_path, rows)
    assert run(config, directory)[0] == 0  # "bad" is undecodable but was never archived: report-only, exit 0
    rows[1]["response_json"] = '{"x": "\udcff"}'  # b, verified above, now holds a byte that isn't UTF-8
    (tmp_path / "letta.json").write_text(json.dumps({"provider_traces": rows}))
    code, _ = run(config, directory)
    t = last_totals(directory)
    assert code == 4 and (t["undecodable"], t["reread_failed"], t["already"]) == (1, 1, 3)
    db = archive_db(directory)
    assert db.execute("select verified_at from traces where id = 'b'").fetchone() == (None,)
    assert db.execute("select count(*) from traces where id = 'b'").fetchone() == (1,)  # the archived bytes stay
    reader = sqlite3.connect(f"file:{os.path.join(directory, ta.ARCHIVE_DB)}?mode=ro", uri=True)
    assert [r["id"] for r in ta.restore_rows(reader, "2026-08-08")] == ["a"]


@pytest.mark.parametrize("column, value", [("day", "2026-08-09"), ("created_at", "2026-08-08 11:00:01+09"),
                                           ("status", "whole")])
def test_lookup_columns_must_match_the_record(tmp_path, column, value):
    # restore finds rows by day; a re-run must not certify a copy filed under the wrong day (or time, or status)
    config, directory = setup(tmp_path, fixture_rows())
    run(config, directory)
    with archive_db(directory) as db:
        db.execute(f"update traces set {column} = ? where id = 'b'", (value,))
    path = os.path.join(directory, ta.ARCHIVE_DB)
    with pytest.raises(ValueError, match="lookup columns"):  # restore refuses it while it still has its mark
        list(ta.restore_rows(sqlite3.connect(f"file:{path}?mode=ro", uri=True), "2026-08-09" if column == "day" else "2026-08-08"))
    code, _ = run(config, directory)
    t = last_totals(directory)
    assert code == 4 and (t["reread_failed"], t["already"]) == (1, 3)
    assert archive_db(directory).execute("select verified_at from traces where id = 'b'").fetchone() == (None,)
    reader = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    assert [r["id"] for d in ("2026-08-08", "2026-08-09") for r in ta.restore_rows(reader, d)] == ["a", "c", "d"]


def test_damaged_record_and_missing_trace_fail_verification(tmp_path):
    rows = fixture_rows()
    config, directory = setup(tmp_path, rows)
    run(config, directory)
    db = archive_db(directory)
    with db:
        db.execute("update traces set record_z = ? where id = 'a'", (b"not zlib",))
    reader = sqlite3.connect(f"file:{os.path.join(directory, ta.ARCHIVE_DB)}?mode=ro", uri=True)
    assert not ta.verify_archived(reader, rows[0])
    assert not ta.verify_archived(reader, row("never-archived", request(SHARED)))


def test_archive_directory_and_files_are_private(tmp_path):
    config, directory = setup(tmp_path, fixture_rows())
    run(config, directory)
    for name in (ta.ARCHIVE_DB, ta.ARCHIVE_LOCK):
        assert stat.S_IMODE(os.stat(os.path.join(directory, name)).st_mode) & 0o077 == 0
    os.chmod(directory, 0o750)
    with pytest.raises(RuntimeError, match="mode 700"):
        run(config, directory)
    created = tmp_path / "fresh"
    ta.open_archive(str(created))[2].close()
    assert stat.S_IMODE(os.stat(created).st_mode) == 0o700


def test_a_second_run_cannot_take_the_lock(tmp_path):
    config, directory = setup(tmp_path, fixture_rows())
    db, path, lock = ta.open_archive(directory)
    try:
        with pytest.raises(RuntimeError, match="lock"):
            run(config, directory)
    finally:
        lock.close()
    assert run(config, directory)[0] == 0


def test_low_disk_stops_before_writing(tmp_path):
    config, directory = setup(tmp_path, fixture_rows())
    code, out = run(config, directory, min_free_gb=1e9)
    assert code == 3 and "stopped before 2026-08-08" in out
    assert archive_db(directory).execute("select count(*) from traces").fetchone() == (0,)


def test_restore_gives_back_the_same_rows(tmp_path):
    rows = fixture_rows()
    config, directory = setup(tmp_path, rows)
    run(config, directory)
    reader = sqlite3.connect(f"file:{os.path.join(directory, ta.ARCHIVE_DB)}?mode=ro", uri=True)
    restored = list(ta.restore_rows(reader, "2026-08-08")) + list(ta.restore_rows(reader, "2026-08-09"))
    originals = {r["id"]: r for r in rows}
    assert [r["id"] for r in restored] == ["a", "b", "c", "d"]  # "bad" was never archived
    for got in restored:
        want = originals[got["id"]]
        assert set(got) == set(want)
        assert {k: v for k, v in got.items() if k != "request_json"} == {k: v for k, v in want.items() if k != "request_json"}
        got["request_json"].encode("utf-8")  # valid UTF-8: the lone surrogate is an escape again, not a raw code point
        if want["id"] == "d":
            assert got["request_json"] == want["request_json"]
        else:
            assert ta.parse_exact(got["request_json"]) == ta.parse_exact(want["request_json"])
    assert "\\ud83d" in restored[0]["request_json"]


def test_restore_refuses_damaged_bytes(tmp_path):
    # After a delete there is no Postgres row to compare with: the hashes are what stops a damaged restore.
    config, directory = setup(tmp_path, fixture_rows())
    run(config, directory)
    path = os.path.join(directory, ta.ARCHIVE_DB)
    shared = ta.message_hash(ta.canonical(SHARED))
    good = archive_db(directory).execute("select z from messages where hash = ?", (shared,)).fetchone()[0]
    with archive_db(directory) as db:
        db.execute("update messages set z = ? where hash = ?", (zlib.compress(b'{"role":"user","content":"x"}'), shared))
    with pytest.raises(ValueError, match="message doesn't match"):
        list(ta.restore_rows(sqlite3.connect(f"file:{path}?mode=ro", uri=True), "2026-08-09"))
    with archive_db(directory) as db:
        db.execute("update messages set z = ? where hash = ?", (good, shared))
        db.execute("update traces set record_z = ? where id = 'd'", (zlib.compress(b'{"columns":{}}'),))
    with pytest.raises(ValueError, match="record doesn't match"):
        list(ta.restore_rows(sqlite3.connect(f"file:{path}?mode=ro", uri=True), "2026-08-09"))


def test_restore_skips_unverified_rows(tmp_path):
    config, directory = setup(tmp_path, fixture_rows())
    run(config, directory)
    with archive_db(directory) as db:
        db.execute("update traces set verified_at = null where id = 'b'")
    reader = sqlite3.connect(f"file:{os.path.join(directory, ta.ARCHIVE_DB)}?mode=ro", uri=True)
    assert [r["id"] for r in ta.restore_rows(reader, "2026-08-08")] == ["a"]


def test_csv_keeps_null_and_empty_apart_and_is_private(tmp_path):
    path = tmp_path / "day.csv"
    rows = [{"id": "a", "source": None, "note": ""}, {"id": "b", "source": 'say "hi"\nbye', "note": "x"}]
    assert ta.write_csv(iter(rows), str(path)) == (2, ["id", "source", "note"])
    assert path.read_text() == '"id","source","note"\n"a",,""\n"b","say ""hi""\nbye","x"\n'
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    with pytest.raises(FileExistsError):
        ta.write_csv(iter(rows), str(path))


def test_restore_of_a_day_without_verified_rows_writes_nothing(tmp_path):
    path = tmp_path / "day.csv"
    assert ta.write_csv(iter([]), str(path)) == (0, [])
    assert not path.exists()
    config, directory = setup(tmp_path, fixture_rows())
    run(config, directory)
    out = subprocess.run([sys.executable, "-m", "observatory.trace_archive", "--restore-day", "2026-08-10",
                          "--archive-dir", directory, "--out", str(path)], capture_output=True, text=True, check=True,
                         cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    assert out.stdout == "no verified rows for 2026-08-10; nothing written\n" and not path.exists()


def test_failed_csv_write_leaves_no_file(tmp_path):
    path = tmp_path / "day.csv"
    with pytest.raises(RuntimeError):
        ta.write_csv(iter([{"id": "a"}, {"other": "b"}]), str(path))
    assert not path.exists()


def test_json_text_escapes_only_lone_surrogates():
    value = json.loads('{"a": "\\ud83d", "b": "\\ud83d\\udc3e", "c": "é"}')
    text = ta.json_text(value)
    assert text == '{"a":"\\ud83d","b":"🐾","c":"é"}' and json.loads(text) == value
