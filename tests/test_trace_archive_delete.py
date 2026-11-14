"""Trace archive step 3: --delete removes old rows only while they are verified in the archive, analysed by telemetry,
rebuilt exactly from the archive's bytes, and unchanged since that check."""
import io, json, os, sqlite3, sys, zlib

import pytest

from observatory import trace_archive as ta
from observatory.remote._sanitize import h
from tests.test_trace_archive import request, row
from tests.test_trace_archive_store import NOW, SHARED, archive_db, fixture_rows, run, setup


def telemetry(tmp_path, ids):
    tdir = tmp_path / "telemetry"
    tdir.mkdir(exist_ok=True)
    db = sqlite3.connect(tdir / "telemetry.db")
    db.execute("create table if not exists letta_traces (trace text primary key)")
    db.executemany("insert or ignore into letta_traces values (?)", [(h(i),) for i in ids])
    db.commit()
    db.close()
    return str(tdir)


def archived(tmp_path, rows=None, analysed=("a", "b", "c", "d", "bad", "new")):
    """Postgres fixtures + a verified archive (a, b, c, d; `bad` is undecodable, `new` inside the window) + telemetry."""
    rows = rows if rows is not None else fixture_rows()
    config, directory = setup(tmp_path, rows)
    assert run(config, directory)[0] == 0
    return {**config, "TELEMETRY_DIR": telemetry(tmp_path, analysed)}, directory, rows


def delete(config, directory, **kw):
    out = io.StringIO()
    code = ta.run_delete(config, directory, now=NOW, out=out, **kw)
    return code, out.getvalue()


def postgres_ids(tmp_path):
    return sorted(r["id"] for r in json.loads((tmp_path / "letta.json").read_text())["provider_traces"])


def queries(tmp_path, start):
    log = tmp_path / "letta.json.log"
    return [q for q in log.read_text().splitlines() if q.startswith(start)] if log.exists() else []


def totals(directory):
    return json.loads(archive_db(directory).execute("select totals from runs order by run desc limit 1").fetchone()[0])


def write_rows(tmp_path, rows):
    (tmp_path / "letta.json").write_text(json.dumps({"provider_traces": rows}))


def test_dry_run_checks_everything_and_deletes_nothing(tmp_path):
    config, directory, _ = archived(tmp_path)
    code, out = delete(config, directory, dry_run=True)
    assert code == 0 and "dry run: nothing deleted" in out
    t = totals(directory)
    assert (t["would_delete"], t["deleted"], t["not_archived"]) == (4, 0, 1)  # `bad` was never archived
    assert postgres_ids(tmp_path) == ["a", "b", "bad", "c", "d", "new"]
    assert not queries(tmp_path, "delete") and not queries(tmp_path, "vacuum")
    assert archive_db(directory).execute("select count(*) from deletions").fetchone() == (0,)


def test_delete_removes_exactly_the_archived_analysed_old_rows_then_vacuums(tmp_path):
    config, directory, _ = archived(tmp_path)
    code, out = delete(config, directory)
    assert code == 0 and "VACUUM done" in out
    assert postgres_ids(tmp_path) == ["bad", "new"]  # unarchivable and inside-the-window rows stay
    t = totals(directory)
    assert (t["deleted"], t["not_archived"], t["mismatch"], t["changed"], t["vacuum"]) == (4, 1, 0, 0, "done")
    db = archive_db(directory)
    run_id = db.execute("select max(run) from runs").fetchone()[0]
    assert sorted(db.execute("select id, day, run, outcome, requested_at <= resolved_at from deletions")) == [
        ("a", "2026-08-08", run_id, "deleted", 1), ("b", "2026-08-08", run_id, "deleted", 1),
        ("c", "2026-08-09", run_id, "deleted", 1), ("d", "2026-08-09", run_id, "deleted", 1)]
    assert db.execute("select finished_at is not null from runs where run = ?", (run_id,)).fetchone() == (1,)
    assert len(queries(tmp_path, "vacuum")) == 1
    # The DELETE statement itself re-checks each row's md5 and the cutoff
    sql = queries(tmp_path, "delete")[0]
    assert "md5(t::text) = v.md5" in sql and f"t.created_at < '{ta.cutoff_time(30, NOW)}'" in sql


def test_second_run_finds_nothing_left_and_does_not_vacuum(tmp_path):
    config, directory, _ = archived(tmp_path)
    delete(config, directory)
    code, _ = delete(config, directory)
    t = totals(directory)
    assert code == 0 and (t["rows"], t["deleted"], t["not_archived"], t["vacuum"]) == (1, 0, 1, "not_run")
    assert len(queries(tmp_path, "vacuum")) == 1


def test_deleted_rows_still_restore_from_the_archive(tmp_path):
    config, directory, rows = archived(tmp_path)
    delete(config, directory)
    reader = sqlite3.connect(f"file:{os.path.join(directory, ta.ARCHIVE_DB)}?mode=ro", uri=True)
    restored = {r["id"]: r for r in ta.restore_rows(reader, "2026-08-08")}
    for original in rows[:2]:
        got = restored[original["id"]]
        assert {k: v for k, v in got.items() if k != "request_json"} == {k: v for k, v in original.items() if k != "request_json"}
        assert ta.parse_exact(got["request_json"]) == ta.parse_exact(original["request_json"])


def test_rows_telemetry_has_not_analysed_stay(tmp_path):
    config, directory, _ = archived(tmp_path, analysed=("a", "b", "d"))
    code, _ = delete(config, directory)
    assert code == 0 and postgres_ids(tmp_path) == ["bad", "c", "new"]
    assert totals(directory)["not_analysed"] == 1


def test_missing_telemetry_deletes_nothing(tmp_path):
    config, directory, _ = archived(tmp_path)
    for broken in ({k: v for k, v in config.items() if k != "TELEMETRY_DIR"}, {**config, "TELEMETRY_DIR": str(tmp_path / "nope")}):
        with pytest.raises(RuntimeError, match="TELEMETRY_DIR"):
            delete(broken, directory)
    assert postgres_ids(tmp_path) == ["a", "b", "bad", "c", "d", "new"]


def test_unverified_rows_stay(tmp_path):
    config, directory, _ = archived(tmp_path)
    with archive_db(directory) as db:
        db.execute("update traces set verified_at = null where id = 'b'")
    code, _ = delete(config, directory)
    assert code == 0 and "b" in postgres_ids(tmp_path) and totals(directory)["not_archived"] == 2


def test_damaged_archive_row_stays_and_exits_4(tmp_path):
    config, directory, _ = archived(tmp_path)
    with archive_db(directory) as db:
        db.execute("update traces set record_z = ? where id = 'a'", (zlib.compress(b'{"columns": {}}'),))
    code, out = delete(config, directory)
    assert code == 4 and "run --archive" in out
    assert postgres_ids(tmp_path) == ["a", "bad", "new"] and totals(directory)["mismatch"] == 1


def test_damaged_message_blob_keeps_every_row_that_uses_it(tmp_path):
    config, directory, _ = archived(tmp_path)
    shared = ta.message_hash(ta.canonical(SHARED))  # in a, b and c; d is kept whole, without messages
    with archive_db(directory) as db:
        db.execute("update messages set z = ? where hash = ?", (zlib.compress(b'{"role":"user","content":"x"}'), shared))
    code, _ = delete(config, directory)
    assert code == 4 and postgres_ids(tmp_path) == ["a", "b", "bad", "c", "new"]
    assert totals(directory)["mismatch"] == 3


@pytest.mark.parametrize("before, after", [("true", "1"), ("false", "0"), ("true", "1.0")])
def test_a_boolean_that_became_a_number_is_a_mismatch_and_the_row_stays(tmp_path, before, after):
    rows = fixture_rows()
    rows[0]["request_json"] = request(SHARED, {"role": "user", "content": "x", "meta": {"flags": [{"option": json.loads(before)}]}})
    config, directory, rows = archived(tmp_path, rows)
    rows[0]["request_json"] = rows[0]["request_json"].replace(f'"option": {before}', f'"option": {after}')
    assert f'"option": {after}' in rows[0]["request_json"]
    write_rows(tmp_path, rows)
    code, _ = delete(config, directory)
    assert code == 4 and "a" in postgres_ids(tmp_path) and totals(directory)["mismatch"] == 1


def test_reordered_keys_are_a_mismatch_too(tmp_path):
    rows = fixture_rows()
    config, directory, rows = archived(tmp_path, rows)
    request_value = json.loads(rows[0]["request_json"])
    rows[0]["request_json"] = json.dumps(dict(reversed(list(request_value.items()))), ensure_ascii=False)
    write_rows(tmp_path, rows)
    code, _ = delete(config, directory)
    assert code == 4 and "a" in postgres_ids(tmp_path) and totals(directory)["mismatch"] == 1


def test_json_equal_is_exact():
    from decimal import Decimal
    p = ta.parse_exact
    for a, b in [("true", "1"), ("false", "0"), ("[true]", "[1]"), ('{"a": false}', '{"a": 0}'), ("1", '"1"'),
                 ("null", "0"), ("null", "false"), ('{"a": 1, "b": 2}', '{"b": 2, "a": 1}'), ("[1, 2]", "[2, 1]"),
                 ("[1]", "[1, 1]"), ("1.5", "1.50001"), ('{"a": {}}', '{"a": []}')]:
        assert not ta.json_equal(p(a), p(b)), (a, b)
    for a, b in [("1", "1.0"), ("1e2", "100"), ("1.50", "1.5"), ('{"a": [true, null, "x"]}', '{"a":[true,null,"x"]}'),
                 ("0.1234567890123456789012345", "0.1234567890123456789012345")]:
        assert ta.json_equal(p(a), p(b)), (a, b)
    assert ta.json_equal(Decimal("1"), 1) and not ta.json_equal(True, Decimal("1"))


def test_postgres_row_changed_after_archiving_stays(tmp_path):
    config, directory, rows = archived(tmp_path)
    rows[1]["response_json"] = '{"choices": ["changed"]}'
    write_rows(tmp_path, rows)
    code, _ = delete(config, directory)
    assert code == 4 and "b" in postgres_ids(tmp_path) and totals(directory)["mismatch"] == 1


def test_row_changed_between_check_and_delete_stays(tmp_path, monkeypatch):
    config, directory, rows = archived(tmp_path)
    real = ta._delete_batch

    def change_c_first(config, batch, cutoff):  # after c passed its checks, before its DELETE
        if any(trace_id == "c" for trace_id, _, _ in batch):
            current = json.loads((tmp_path / "letta.json").read_text())["provider_traces"]
            for r in current:
                if r["id"] == "c":
                    r["agent_tags"] = '["changed"]'
            write_rows(tmp_path, current)
        return real(config, batch, cutoff)

    monkeypatch.setattr(ta, "_delete_batch", change_c_first)
    code, _ = delete(config, directory)
    assert code == 4 and postgres_ids(tmp_path) == ["bad", "c", "new"]
    t = totals(directory)
    assert (t["deleted"], t["changed"]) == (3, 1)
    assert archive_db(directory).execute("select outcome from deletions where id = 'c'").fetchall() == [("stayed",)]


def test_unsafe_ids_are_never_put_into_the_statement(tmp_path):
    rows = [*fixture_rows(), row("q'1", request(SHARED), created_at="2026-08-08 12:00:00+09"),
            row("q2\n", request(SHARED), created_at="2026-08-08 13:00:00+09")]  # `$` alone would let this one through
    config, directory, _ = archived(tmp_path, rows, analysed=("a", "b", "c", "d", "q'1", "q2\n"))
    code, out = delete(config, directory)
    assert code == 4 and totals(directory)["unsafe_id"] == 2  # a timer must notice
    assert "unsafe_id rows stay" in out and all("q'1" not in q and "q2" not in q for q in queries(tmp_path, "delete"))
    assert postgres_ids(tmp_path) == ["bad", "new", "q'1", "q2\n"]


def test_id_and_md5_shapes_are_matched_whole():
    for bad in ("q1\n", "a b", "x'y", "", "a" * 201):
        assert not ta._SAFE_ID.fullmatch(bad)
    assert ta._SAFE_ID.fullmatch("provider_trace-0b1c2d3e-aaaa-4bbb-8ccc-0123456789ab")
    assert not ta._MD5.fullmatch("0" * 32 + "\n") and ta._MD5.fullmatch("0" * 32)


def test_the_delete_statement_itself_keeps_rows_inside_the_window(tmp_path):
    config, directory, _ = archived(tmp_path)
    md5 = {r["id"]: d for r, d in ta._stream(config, "where created_at < '2030-01-01'", with_md5=True)}
    deleted = ta._delete_batch(config, [("new", md5["new"], "2026-11-01"), ("a", md5["a"], "2026-08-08")],
                               ta.cutoff_time(30, NOW))
    assert deleted == {"a"} and "new" in postgres_ids(tmp_path)
    assert ta._delete_batch(config, [("b", "0" * 32, "2026-08-08")], ta.cutoff_time(30, NOW)) == set()  # wrong md5
    assert "b" in postgres_ids(tmp_path)


def test_a_failed_vacuum_still_records_and_reports_the_deletes(tmp_path, monkeypatch):
    config, directory, _ = archived(tmp_path)

    def broken(config):
        raise ta.subprocess.CalledProcessError(1, ["psql"])

    monkeypatch.setattr(ta, "_vacuum", broken)
    code, out = delete(config, directory)
    assert code == 4 and "VACUUM failed" in out and "total" in out
    assert postgres_ids(tmp_path) == ["bad", "new"]
    t = totals(directory)
    assert (t["deleted"], t["vacuum"]) == (4, "failed")
    assert archive_db(directory).execute("select count(*) from deletions where outcome = 'deleted'").fetchone() == (4,)


def test_a_vacuum_the_server_skips_with_a_warning_is_not_done(tmp_path):
    config, directory, rows = archived(tmp_path)
    (tmp_path / "letta.json").write_text(json.dumps({"provider_traces": rows, "__vacuum_warning": True}))
    code, out = delete(config, directory)
    assert code == 4 and totals(directory)["vacuum"] == "warning" and "server warning" in out
    assert "permission denied" not in out  # the server's own text is not passed on
    assert postgres_ids(tmp_path) == ["bad", "new"]


def test_a_crash_right_after_the_delete_leaves_the_rows_marked_requested(tmp_path, monkeypatch):
    config, directory, _ = archived(tmp_path)
    real = ta._delete_batch

    def crash_after(config, batch, cutoff):
        real(config, batch, cutoff)
        raise KeyboardInterrupt

    monkeypatch.setattr(ta, "_delete_batch", crash_after)
    with pytest.raises(KeyboardInterrupt):
        delete(config, directory)
    assert postgres_ids(tmp_path) == ["bad", "c", "d", "new"]  # the first day's batch went
    db = archive_db(directory)
    crashed = db.execute("select max(run) from runs").fetchone()[0]
    assert sorted(db.execute("select id, run, outcome from deletions")) == [("a", crashed, None), ("b", crashed, None)]
    assert db.execute("select finished_at from runs where run = ?", (crashed,)).fetchone() == (None,)

    # A dry run reports them and records nothing
    monkeypatch.setattr(ta, "_delete_batch", real)
    code, out = delete(config, directory, dry_run=True)
    assert "left 2 deletions unanswered; Postgres now: gone 2, still there 0 (dry run" in out
    assert sorted(db.execute("select id, outcome from deletions")) == [("a", None), ("b", None)]

    # The rerun asks Postgres, records the outcome on the earlier attempt's own rows, and carries on
    code, out = delete(config, directory)
    assert code == 0 and "left 2 deletions unanswered; Postgres now: gone 2, still there 0 (recorded)" in out
    assert sorted(db.execute("select id, run, outcome, resolved_at is not null from deletions where run = ?", (crashed,))) == [
        ("a", crashed, "gone", 1), ("b", crashed, "gone", 1)]
    assert totals(directory)["earlier"] == {"unresolved": 2, "gone": 2, "present": 0}
    assert postgres_ids(tmp_path) == ["bad", "new"]
    assert db.execute("select count(*) from deletions where outcome is null").fetchone() == (0,)


def test_a_stop_before_the_delete_reached_postgres_leaves_present_rows_that_are_deleted_on_the_rerun(tmp_path, monkeypatch):
    config, directory, _ = archived(tmp_path)
    real = ta._delete_batch

    def stop_before(config, batch, cutoff):
        raise KeyboardInterrupt

    monkeypatch.setattr(ta, "_delete_batch", stop_before)
    with pytest.raises(KeyboardInterrupt):
        delete(config, directory)
    db = archive_db(directory)
    crashed = db.execute("select max(run) from runs").fetchone()[0]
    monkeypatch.setattr(ta, "_delete_batch", real)
    code, out = delete(config, directory)
    assert code == 0 and "gone 0, still there 2 (recorded)" in out and postgres_ids(tmp_path) == ["bad", "new"]
    rerun = db.execute("select max(run) from runs").fetchone()[0]
    assert sorted(db.execute("select id, run, outcome from deletions where id in ('a', 'b')")) == [
        ("a", crashed, "present"), ("a", rerun, "deleted"), ("b", crashed, "present"), ("b", rerun, "deleted")]


def test_if_postgres_cannot_say_what_happened_the_requests_stay_unresolved(tmp_path, monkeypatch):
    config, directory, _ = archived(tmp_path)
    real = ta._delete_batch

    def crash_after(config, batch, cutoff):
        real(config, batch, cutoff)
        raise KeyboardInterrupt

    monkeypatch.setattr(ta, "_delete_batch", crash_after)
    with pytest.raises(KeyboardInterrupt):
        delete(config, directory)

    def unreachable(config, ids):
        raise ta.subprocess.CalledProcessError(2, ["psql"])

    monkeypatch.setattr(ta, "_delete_batch", real)
    monkeypatch.setattr(ta, "_present_ids", unreachable)
    with pytest.raises(ta.subprocess.CalledProcessError):
        delete(config, directory)
    assert archive_db(directory).execute("select count(*) from deletions where outcome is null").fetchone() == (2,)
    assert postgres_ids(tmp_path) == ["bad", "c", "d", "new"]  # nothing new was deleted


def test_max_rows_defers_the_rest_oldest_first(tmp_path):
    config, directory, _ = archived(tmp_path)
    code, _ = delete(config, directory, max_rows=2)
    assert code == 0 and postgres_ids(tmp_path) == ["bad", "c", "d", "new"]
    assert (totals(directory)["deleted"], totals(directory)["deferred"]) == (2, 2)


def test_batches_are_separate_statements(tmp_path):
    config, directory, _ = archived(tmp_path)
    delete(config, directory, batch_rows=1)
    assert len(queries(tmp_path, "delete")) == 4 and postgres_ids(tmp_path) == ["bad", "new"]


def test_no_vacuum(tmp_path):
    config, directory, _ = archived(tmp_path)
    code, out = delete(config, directory, vacuum=False)
    assert code == 0 and not queries(tmp_path, "vacuum") and "VACUUM done" not in out


def test_date_range_limits_what_is_deleted(tmp_path):
    config, directory, _ = archived(tmp_path)
    delete(config, directory, date_from="2026-08-09", date_to="2026-08-09")
    assert postgres_ids(tmp_path) == ["a", "b", "bad", "new"]


def test_refuses_short_retention_bad_limits_missing_archive_and_a_held_lock(tmp_path):
    config, directory, _ = archived(tmp_path)
    with pytest.raises(ValueError, match="at least the last 30 days"):
        delete(config, directory, older_than_days=29)
    for kw in ({"batch_rows": 0}, {"max_rows": 0}):
        with pytest.raises(ValueError):
            delete(config, directory, **kw)
    with pytest.raises(RuntimeError, match="--archive first"):
        delete(config, str(tmp_path / "empty"))
    _, _, lock = ta.open_archive(directory)
    try:
        with pytest.raises(RuntimeError, match="lock"):
            delete(config, directory)
    finally:
        lock.close()
    assert postgres_ids(tmp_path) == ["a", "b", "bad", "c", "d", "new"]


def test_cli_guards(tmp_path, monkeypatch):
    config, directory, _ = archived(tmp_path)
    cfg = tmp_path / "obs.env"
    cfg.write_text("".join(f"{k}={v}\n" for k, v in config.items()))
    for argv in (["--delete", "--config", str(cfg)],                                          # no --archive-dir
                 ["--delete", "--config", str(cfg), "--archive-dir", directory, "--older-than-days", "7"],
                 ["--archive", "--config", str(cfg), "--archive-dir", directory, "--dry-run"]):
        monkeypatch.setattr(sys, "argv", ["trace_archive", *argv])
        with pytest.raises(SystemExit) as exc:
            ta.main()
        assert exc.value.code == 2
    assert postgres_ids(tmp_path) == ["a", "b", "bad", "c", "d", "new"]
    monkeypatch.setattr(sys, "argv", ["trace_archive", "--delete", "--config", str(cfg), "--archive-dir", directory,
                                      "--dry-run"])
    with pytest.raises(SystemExit) as exc:
        ta.main()
    assert exc.value.code == 0 and postgres_ids(tmp_path) == ["a", "b", "bad", "c", "d", "new"]
    assert archive_db(directory).execute("select args from runs order by run desc limit 1").fetchone()[0].startswith(
        "delete --dry-run where created_at <")
