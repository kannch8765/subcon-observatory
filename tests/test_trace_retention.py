"""Trace archive step 4: the daily run archives, then deletes, whole days that left the window, and always ends with an
encrypted snapshot of the archive and a status file."""
import datetime, hashlib, io, json, os, re, shutil, sqlite3, stat, subprocess, sys, types, zlib

import pytest

from observatory import trace_archive as ta
from observatory import trace_retention as tr
from tests.test_trace_archive import request, row
from tests.test_trace_archive_delete import postgres_ids, telemetry, write_rows
from tests.test_trace_archive_store import NOW, SHARED, archive_db, fixture_rows, run as archive_run, setup

RECIPIENT = "age1" + "q" * 58  # the fake age takes any recipient; main() checks the shape


@pytest.fixture
def fake_age(tmp_path, monkeypatch):
    """A stand-in for age: writes the age header, the recipient with a random nonce (real age picks a new file key each
    time, so two encryptions of the same plaintext differ), and the plaintext. Exits 1 when FAKE_AGE_FAIL is set; with
    FAKE_AGE_GARBAGE it writes something that isn't an age file and still exits 0."""
    script = tmp_path / "fake_age.py"
    script.write_text(f"#!{sys.executable}\n" + """import os, sys
if os.environ.get("FAKE_AGE_FAIL"):
    sys.exit(1)
args = sys.argv[1:]
out, recipient = args[args.index("--output") + 1], args[args.index("--recipient") + 1]
if os.environ.get("FAKE_AGE_GARBAGE"):
    open(out, "wb").write(b"not an age file")
    sys.exit(0)
with open(out, "wb") as f:
    f.write(b"age-encryption.org/v1\\n" + recipient.encode() + b" " + os.urandom(8).hex().encode() + b"\\n"
            + open(args[-1], "rb").read())
""")
    script.chmod(0o700)
    monkeypatch.setattr(tr, "AGE", str(script))
    return script


def daily(tmp_path, rows=None, analysed=("a", "b", "c", "d", "bad", "new", "e", "f")):
    """Postgres fixtures, an empty archive directory and telemetry: the state before a night's run."""
    config, directory = setup(tmp_path, rows if rows is not None else fixture_rows())
    config = {**config, "TELEMETRY_DIR": telemetry(tmp_path, analysed), "TELEMETRY_TZ": "Asia/Tokyo"}
    offsite = tmp_path / "offsite"
    offsite.mkdir(mode=0o700)
    return config, directory, str(offsite), str(tmp_path / "telemetry" / "trace_retention.json")


def night(config, directory, offsite, status, now=NOW, **kw):
    out = io.StringIO()
    code = tr.run(config, directory, offsite_dir=offsite, recipient=kw.pop("recipient", RECIPIENT), status_path=status,
                  min_free_gb=0, now=now, out=out, **kw)
    return code, out.getvalue()


def sha256(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def published(offsite):
    """(manifest, path of the snapshot file it names)."""
    manifest = json.loads(open(os.path.join(offsite, tr.MANIFEST)).read())
    return manifest, os.path.join(offsite, manifest["file"])


def pair_usable(offsite):
    """What the pull checks: the manifest names a file that is there, with the size and sha256 it says."""
    manifest, snap = published(offsite)
    return os.path.isfile(snap) and (os.path.getsize(snap), sha256(snap)) == (manifest["bytes"], manifest["sha256"])


def kept(offsite, before):
    """The last pair is exactly as it was and usable, and nothing else is left behind."""
    return (published(offsite)[0] == before and pair_usable(offsite)
            and sorted(os.listdir(offsite)) == sorted([before["file"], tr.MANIFEST]))


def test_last_day_is_the_whole_day_before_the_cutoff_in_the_configured_zone():
    now = datetime.datetime(2026, 11, 7, 20, 0, tzinfo=datetime.timezone.utc)  # 2026-11-08 05:00 in Tokyo
    assert tr.last_day({"TELEMETRY_TZ": "Asia/Tokyo"}, 30, now) == "2026-10-08"
    assert tr.last_day({"TELEMETRY_TZ": "UTC"}, 30, now) == "2026-10-07"
    for zone in ("Asia/Tokyo", "UTC", "America/Los_Angeles"):  # the last day ends by the cutoff, in its own zone
        tz = __import__("zoneinfo").ZoneInfo(zone)
        end = datetime.datetime.fromisoformat(tr.last_day({"TELEMETRY_TZ": zone}, 30, now)).replace(tzinfo=tz)
        assert end + datetime.timedelta(days=1) <= now - datetime.timedelta(days=30)


def test_night_archives_deletes_and_snapshots(tmp_path, fake_age):
    config, directory, offsite, status = daily(tmp_path)
    code, out = night(config, directory, offsite, status)
    assert code == 0 and out.endswith("== exit 0\n") and "VACUUM done" in out
    assert postgres_ids(tmp_path) == ["bad", "new"]  # unarchivable and inside-the-window rows stay
    s = json.loads(open(status).read())
    assert (s["last_day"], s["archive"]["exit"], s["delete"]["exit"], s["exit"], s["error"]) == ("2026-10-08", 0, 0, 0, None)
    assert (s["archive"]["totals"]["verified"], s["delete"]["totals"]["deleted"]) == (4, 4)
    assert stat.S_IMODE(os.stat(status).st_mode) == 0o640
    # The snapshot: complete, private, described by its manifest; no plaintext left behind
    manifest, snap = published(offsite)
    assert tr._GENERATION.match(manifest["file"]) and manifest["file"].split("-")[-1][:12] == manifest["sha256"][:12]
    assert sorted(os.listdir(offsite)) == sorted([manifest["file"], tr.MANIFEST]) and pair_usable(offsite)
    assert stat.S_IMODE(os.stat(snap).st_mode) == 0o600
    assert (manifest["verified_rows"], manifest["verified_days"], manifest["recipient"], manifest["checked_messages"]) == (
        4, ["2026-08-08", "2026-08-09"], RECIPIENT, 4)
    plain = open(snap, "rb").read().split(b"\n", 2)[2]  # the fake's header lines, then the plaintext copy
    assert hashlib.sha256(plain).hexdigest() == manifest["plaintext_sha256"] == s["snapshot"]["plaintext_sha256"]
    # The copy is a consistent archive that knows about the deletes
    copy = tmp_path / "copy.sqlite3"
    copy.write_bytes(plain)
    db = sqlite3.connect(copy)
    assert db.execute("select count(*) from deletions where outcome = 'deleted'").fetchone() == (4,)


def test_whole_days_only_a_row_older_than_the_cutoff_waits_for_its_own_night(tmp_path, fake_age):
    rows = [row("e", request(SHARED), created_at="2026-10-08 23:00:00+09"),
            row("f", request(SHARED), created_at="2026-10-09 01:00:00+09")]  # older than the cutoff, on the next day
    config, directory, offsite, status = daily(tmp_path, rows)
    cutoff = datetime.datetime.fromisoformat(ta.cutoff_time(30, NOW))
    assert datetime.datetime.fromisoformat("2026-10-09T01:00:00+09:00") < cutoff
    assert night(config, directory, offsite, status)[0] == 0
    assert postgres_ids(tmp_path) == ["f"]
    assert night(config, directory, offsite, status, now=NOW + datetime.timedelta(days=1))[0] == 0
    assert postgres_ids(tmp_path) == []
    assert json.loads(open(status).read())["last_day"] == "2026-10-09"


def test_a_second_run_the_same_night_finds_nothing_and_still_snapshots(tmp_path, fake_age):
    config, directory, offsite, status = daily(tmp_path)
    night(config, directory, offsite, status)
    first = published(offsite)[0]
    code, _ = night(config, directory, offsite, status)
    s = json.loads(open(status).read())
    assert code == 0 and (s["archive"]["totals"]["new"], s["delete"]["totals"]["deleted"]) == (0, 0)
    assert s["snapshot"]["verified_rows"] == first["verified_rows"] == 4


def test_each_night_publishes_a_new_generation_and_keeps_only_the_one_before(tmp_path, fake_age):
    config, directory, offsite, status = daily(tmp_path)
    files = []
    for _ in range(3):
        assert night(config, directory, offsite, status)[0] == 0
        files.append(published(offsite)[0]["file"])
    assert len(set(files)) == 3 and pair_usable(offsite)
    # the newest (named by the manifest) and the one before it, for a pull that started before the switch
    assert sorted(os.listdir(offsite)) == sorted(files[1:] + [tr.MANIFEST])


def test_dry_run_archives_for_real_and_deletes_nothing(tmp_path, fake_age):
    config, directory, offsite, status = daily(tmp_path)
    code, out = night(config, directory, offsite, status, dry_run=True)
    s = json.loads(open(status).read())
    assert code == 0 and "dry run: nothing deleted" in out and s["dry_run"] is True
    assert postgres_ids(tmp_path) == ["a", "b", "bad", "c", "d", "new"]
    assert (s["archive"]["totals"]["verified"], s["delete"]["totals"]["would_delete"]) == (4, 4)


def test_archive_problem_skips_the_delete_but_still_snapshots(tmp_path, fake_age):
    rows = fixture_rows()
    config, directory, offsite, status = daily(tmp_path, rows)
    archive_run(config, directory)
    rows[1]["response_json"] = '{"choices": ["changed"]}'  # Postgres changed after the archive: a conflict
    write_rows(tmp_path, rows)
    code, out = night(config, directory, offsite, status)
    s = json.loads(open(status).read())
    assert code == 4 and "== delete skipped: --archive exited 4" in out
    assert (s["archive"]["exit"], s["archive"]["totals"]["conflict"], s["delete"]) == (4, 1, None)
    assert postgres_ids(tmp_path) == ["a", "b", "bad", "c", "d", "new"]
    assert s["snapshot"]["verified_rows"] == 3  # the conflict row lost its verified_at


def test_archive_out_of_space_exits_3_and_deletes_nothing(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)
    monkeypatch.setattr(ta, "run_delete", lambda *a, **k: pytest.fail("--delete must not run"))
    out = io.StringIO()
    code = tr.run(config, directory, offsite_dir=offsite, recipient=RECIPIENT, status_path=status, min_free_gb=1e9,
                  now=NOW, out=out)
    s = json.loads(open(status).read())
    assert code == 3 and s["archive"]["exit"] == 3 and s["delete"] is None
    assert s["snapshot"] == {"error": "RuntimeError"}  # no room for the snapshot either; 3 still wins
    assert postgres_ids(tmp_path) == ["a", "b", "bad", "c", "d", "new"]


def test_vacuum_warning_exits_4_after_the_deletes_and_snapshots(tmp_path, fake_age):
    config, directory, offsite, status = daily(tmp_path)
    fixtures = json.loads((tmp_path / "letta.json").read_text())
    (tmp_path / "letta.json").write_text(json.dumps({**fixtures, "__vacuum_warning": True}))
    code, _ = night(config, directory, offsite, status)
    s = json.loads(open(status).read())
    assert code == 4 and (s["delete"]["exit"], s["delete"]["totals"]["vacuum"]) == (4, "warning")
    assert postgres_ids(tmp_path) == ["bad", "new"]
    assert s["snapshot"]["verified_rows"] == 4


def test_error_mid_run_still_snapshots_and_records_only_its_class(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)

    def boom(*a, **k):
        raise RuntimeError("detail that must not be printed")

    monkeypatch.setattr(ta, "run_delete", boom)
    out = io.StringIO()
    with pytest.raises(RuntimeError):
        tr.run(config, directory, offsite_dir=offsite, recipient=RECIPIENT, status_path=status, min_free_gb=0, now=NOW,
               out=out)
    s = json.loads(open(status).read())
    assert (s["error"], s["exit"], s["delete"]) == ("RuntimeError", 1, None)
    assert s["snapshot"]["verified_rows"] == 4 and "detail" not in out.getvalue() + open(status).read()


def test_main_prints_the_class_and_writes_the_traceback_to_a_private_file(tmp_path, fake_age, monkeypatch, capsys):
    config, directory, offsite, status = daily(tmp_path)
    config_file = tmp_path / "observatory.env"
    config_file.write_text("".join(f"{k}={v}\n" for k, v in config.items()))

    def boom(*a, **k):
        raise ValueError("detail that must not be printed")

    monkeypatch.setattr(ta, "run_archive", boom)
    monkeypatch.setattr(sys, "argv", ["trace_retention", "--config", str(config_file), "--archive-dir", directory,
                                      "--status", status])
    umask = os.umask(0o022)  # main() sets its own; give the rest of the test session ours back
    try:
        with pytest.raises(SystemExit) as exit_:
            tr.main()
    finally:
        os.umask(umask)
    printed = capsys.readouterr()
    assert exit_.value.code == 1 and "== failed: ValueError" in printed.out
    assert "detail" not in printed.out + printed.err
    log = os.path.join(directory, tr.ERROR_LOG)
    assert "detail that must not be printed" in open(log).read() and stat.S_IMODE(os.stat(log).st_mode) == 0o600


def test_snapshot_failure_exits_5_and_keeps_the_last_snapshot(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)
    night(config, directory, offsite, status)
    before = published(offsite)[0]
    monkeypatch.setenv("FAKE_AGE_FAIL", "1")
    code, out = night(config, directory, offsite, status)
    s = json.loads(open(status).read())
    assert code == 5 and s["snapshot"] == {"error": "CalledProcessError"} and "snapshot failed" in out
    assert kept(offsite, before)  # no plaintext or half snapshot left


def test_output_that_is_not_an_age_file_never_replaces_the_last_snapshot(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)
    night(config, directory, offsite, status)
    before = published(offsite)[0]
    monkeypatch.setenv("FAKE_AGE_GARBAGE", "1")
    code, _ = night(config, directory, offsite, status)
    assert code == 5 and json.loads(open(status).read())["snapshot"] == {"error": "RuntimeError"}
    assert kept(offsite, before)


def test_snapshot_refuses_a_directory_open_to_others(tmp_path, fake_age):
    config, directory, offsite, status = daily(tmp_path)
    os.chmod(offsite, 0o755)
    code, _ = night(config, directory, offsite, status)
    assert code == 5 and os.listdir(offsite) == []
    assert postgres_ids(tmp_path) == ["bad", "new"]  # the deletes stand


def test_snapshot_refuses_while_another_run_holds_the_archive_lock(tmp_path, fake_age):
    config, directory, offsite, status = daily(tmp_path)
    archive_run(config, directory)
    db, _, lock = ta.open_archive(directory)  # another archive run holds the lock
    try:
        with pytest.raises(RuntimeError, match="lock"):
            tr.snapshot(directory, offsite, RECIPIENT)
    finally:
        db.close()
        lock.close()
    assert os.listdir(offsite) == []


def test_a_missing_archive_is_never_snapshotted(tmp_path, fake_age):
    _, directory, offsite, _ = daily(tmp_path)
    with pytest.raises(RuntimeError, match="no archive"):
        tr.snapshot(directory, offsite, RECIPIENT, min_free_gb=0)
    assert os.listdir(directory) == [] and os.listdir(offsite) == []  # no empty archive created either


def test_fewer_verified_rows_never_replace_the_last_snapshot(tmp_path, fake_age):
    config, directory, offsite, status = daily(tmp_path)
    night(config, directory, offsite, status)
    before = published(offsite)[0]
    db = archive_db(directory)
    with db:
        db.execute("update traces set verified_at = null where id = 'a'")  # e.g. a damaged or older archive
    code, _ = night(config, directory, offsite, status)
    assert code == 5 and json.loads(open(status).read())["snapshot"] == {"error": "RuntimeError"}
    assert kept(offsite, before)


@pytest.mark.parametrize("damage", ["record_not_zlib", "record_changed", "message_changed", "message_missing"])
def test_a_damaged_record_or_message_of_a_deleted_row_never_replaces_the_last_snapshot(tmp_path, fake_age, damage):
    """The archive pass re-reads only rows still in Postgres: a deleted row's record or messages can go bad while its
    verified_at stays, and quick_check and the verified count can't see it. The snapshot's own check does."""
    config, directory, offsite, status = daily(tmp_path)
    night(config, directory, offsite, status)
    assert postgres_ids(tmp_path) == ["bad", "new"]  # a, b, c, d live only in the archive now
    before = published(offsite)[0]
    db = archive_db(directory)
    digest = json.loads(zlib.decompress(db.execute("select record_z from traces where id = 'b'").fetchone()[0]))[
        "message_hashes"][-1]  # b's own reply
    with db:
        if damage == "record_not_zlib":
            db.execute("update traces set record_z = ? where id = 'b'", (b"not zlib",))
        elif damage == "record_changed":
            db.execute("update traces set record_z = ? where id = 'b'", (zlib.compress(b'{"columns": {}}'),))
        elif damage == "message_changed":
            db.execute("update messages set z = ? where hash = ?", (zlib.compress(b'{"role":"x"}'), digest))
        else:
            db.execute("delete from messages where hash = ?", (digest,))
    assert db.execute("select count(*) from traces where verified_at is not null").fetchone() == (4,)
    assert db.execute("pragma quick_check").fetchone() == ("ok",)
    code, _ = night(config, directory, offsite, status)
    assert code == 5 and json.loads(open(status).read())["snapshot"]["error"] in ("ValueError", "KeyError")
    assert kept(offsite, before)


def test_a_manifest_that_cant_be_written_leaves_the_last_pair_usable(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)
    night(config, directory, offsite, status)
    before = published(offsite)[0]
    real = tr._write_json

    def manifest_fails(path, value):
        if os.path.basename(path) == tr.MANIFEST:
            open(path + ".tmp", "w").write("half")  # as a write that stopped part-way
            raise OSError("disk full")
        real(path, value)

    monkeypatch.setattr(tr, "_write_json", manifest_fails)
    code, _ = night(config, directory, offsite, status)
    assert code == 5 and json.loads(open(status).read())["snapshot"] == {"error": "OSError"}
    assert kept(offsite, before)  # the new generation and the half manifest are gone


def test_a_run_that_published_while_we_waited_for_the_lock_keeps_its_file(tmp_path, fake_age, monkeypatch):
    """Run A starts with a manifest naming G0; run B takes the archive lock first and publishes G1; then A gets the
    lock and fails before it publishes. G1, which the live manifest names, must still be there."""
    config, directory, offsite, status = daily(tmp_path)
    night(config, directory, offsite, status)
    g0 = published(offsite)[0]["file"]
    real_open, raced = ta.open_archive, []

    def open_after_run_b(directory):
        if not raced:
            raced.append(True)
            tr.snapshot(directory, offsite, RECIPIENT, min_free_gb=0)  # run B, all the way, before A gets the lock
            monkeypatch.setenv("FAKE_AGE_FAIL", "1")  # A then fails before it publishes
        return real_open(directory)

    monkeypatch.setattr(ta, "open_archive", open_after_run_b)
    with pytest.raises(subprocess.CalledProcessError):
        tr.snapshot(directory, offsite, RECIPIENT, min_free_gb=0)  # run A
    g1 = published(offsite)[0]["file"]
    assert raced and g1 != g0 and pair_usable(offsite)
    assert sorted(os.listdir(offsite)) == sorted([g1, tr.MANIFEST])  # A removed G0 (not current), never G1


def test_a_copy_that_fails_quick_check_never_replaces_the_last_snapshot(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)
    night(config, directory, offsite, status)
    before = published(offsite)[0]

    class Damaged(sqlite3.Connection):
        def execute(self, sql, *args):
            return super().execute("select 'page 2: btree damaged'" if sql == "pragma quick_check" else sql, *args)

    monkeypatch.setattr(tr, "sqlite3", types.SimpleNamespace(
        connect=lambda *a, **k: sqlite3.connect(*a, factory=Damaged, **k)))
    code, _ = night(config, directory, offsite, status)
    assert code == 5 and kept(offsite, before)


def test_an_error_while_copying_removes_the_plaintext(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)

    def broken(path):
        assert os.path.exists(path)  # the plaintext copy is there at this point
        raise OSError("disk trouble")

    monkeypatch.setattr(tr, "_sha256", broken)
    code, _ = night(config, directory, offsite, status)
    assert code == 5 and json.loads(open(status).read())["snapshot"] == {"error": "OSError"}
    assert os.listdir(offsite) == []


def test_a_bad_zone_still_snapshots_and_writes_the_status(tmp_path, fake_age):
    config, directory, offsite, status = daily(tmp_path)
    archive_run(config, directory)
    with pytest.raises(Exception) as raised:
        night({**config, "TELEMETRY_TZ": "Nowhere/Nope"}, directory, offsite, status)
    s = json.loads(open(status).read())
    assert (s["error"], s["exit"], s["last_day"], s["archive"]) == (type(raised.value).__name__, 1, None, None)
    assert s["snapshot"]["verified_rows"] == 4
    assert postgres_ids(tmp_path) == ["a", "b", "bad", "c", "d", "new"]


def test_a_delete_that_stops_after_deleting_keeps_its_counts(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)

    def vacuum_crash(config):
        raise RuntimeError("stopped")

    monkeypatch.setattr(ta, "_vacuum", vacuum_crash)
    with pytest.raises(RuntimeError):
        night(config, directory, offsite, status)
    s = json.loads(open(status).read())
    assert postgres_ids(tmp_path) == ["bad", "new"]
    assert (s["delete"]["exit"], s["delete"]["totals"]["deleted"], s["delete"]["totals"]["vacuum"]) == (None, 4, "pending")
    assert s["snapshot"]["verified_rows"] == 4


def test_a_failing_lookup_after_a_crashed_delete_still_snapshots_and_keeps_the_first_error(tmp_path, fake_age,
                                                                                            monkeypatch):
    config, directory, offsite, status = daily(tmp_path)
    real, calls = tr._last_run, []

    def lookup(directory):  # works for the archive's counts, fails in the error path
        calls.append(directory)
        if len(calls) > 1:
            raise sqlite3.OperationalError("database is locked")
        return real(directory)

    def vacuum_crash(config):
        raise RuntimeError("stopped")

    monkeypatch.setattr(tr, "_last_run", lookup)
    monkeypatch.setattr(ta, "_vacuum", vacuum_crash)
    with pytest.raises(RuntimeError, match="stopped"):
        night(config, directory, offsite, status)
    s = json.loads(open(status).read())
    assert len(calls) == 2 and postgres_ids(tmp_path) == ["bad", "new"]
    assert (s["error"], s["exit"], s["delete"], s["snapshot"]["verified_rows"]) == ("RuntimeError", 1, None, 4)


def test_a_status_that_cant_be_written_fails_the_run_after_the_snapshot(tmp_path, fake_age):
    config, directory, offsite, _ = daily(tmp_path)
    with pytest.raises(FileNotFoundError):
        night(config, directory, offsite, str(tmp_path / "no-such-dir" / "status.json"))
    assert json.loads(open(os.path.join(offsite, tr.MANIFEST)).read())["verified_rows"] == 4


@pytest.mark.skipif(not (shutil.which("age") and shutil.which("age-keygen")), reason="needs age and age-keygen")
def test_real_age_snapshot_decrypts_to_the_manifest_plaintext(tmp_path):
    config, directory, offsite, status = daily(tmp_path)
    key = tmp_path / "key.txt"
    subprocess.run(["age-keygen", "-o", str(key)], check=True, capture_output=True)
    recipient = subprocess.run(["age-keygen", "-y", str(key)], check=True, capture_output=True, text=True).stdout.strip()
    assert tr._RECIPIENT.match(recipient)
    assert night(config, directory, offsite, status, recipient=recipient)[0] == 0
    manifest = json.loads(open(os.path.join(offsite, tr.MANIFEST)).read())
    plain = tmp_path / "decrypted.sqlite3"
    subprocess.run(["age", "--decrypt", "--identity", str(key), "--output", str(plain), published(offsite)[1]],
                   check=True, capture_output=True)
    assert sha256(plain) == manifest["plaintext_sha256"]
    assert sqlite3.connect(plain).execute("select count(*) from traces where verified_at is not null").fetchone() == (4,)


@pytest.mark.parametrize("extra", [["--offsite-dir", "x"], ["--offsite-dir", "x", "--age-recipient", "age1nope"],
                                   ["--older-than-days", "29"]])
def test_main_refuses_bad_arguments(tmp_path, monkeypatch, extra):
    monkeypatch.setattr(sys, "argv", ["trace_retention", "--config", "c", "--archive-dir", str(tmp_path), *extra])
    with pytest.raises(SystemExit) as exit_:
        tr.main()
    assert exit_.value.code == 2


DOCS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")


def documented_units():
    """(doc, ini block) for every ```ini block in docs/."""
    for name in sorted(os.listdir(DOCS)):
        for block in re.findall(r"```ini\n(.*?)```", open(os.path.join(DOCS, name)).read(), re.S):
            yield name, block


def test_documented_units_have_no_trailing_comments():
    """systemd takes only whole-line comments: `MemoryMax=1500M  # ...` makes the value invalid and the setting is
    ignored, while systemd-analyze verify still exits 0."""
    bad = [(name, line) for name, block in documented_units() for line in block.splitlines()
           if re.match(r"^[A-Za-z]+=", line) and re.search(r"\s[#;]", line)]
    assert bad == []


@pytest.mark.skipif(not shutil.which("systemd-analyze"), reason="needs systemd-analyze")
def test_the_documented_retention_unit_verifies_without_any_diagnostic(tmp_path):
    block = next(b for _, b in documented_units() if "# subcon-trace-retention.service" in b)
    service, timer = block.split("# subcon-trace-retention.timer\n")
    fill = {"<pinned observatory checkout>": os.path.dirname(DOCS), "<config>": str(tmp_path / "observatory.env"),
            "<archive dir>": str(tmp_path / "archive"), "<offsite dir>": str(tmp_path / "offsite"),
            "<TELEMETRY_DIR>": str(tmp_path / "telemetry"), "<owner's age1... key>": RECIPIENT}
    for name, body in (("subcon-trace-retention.service", service), ("subcon-trace-retention.timer", timer)):
        for placeholder, value in fill.items():
            body = body.replace(placeholder, value)
        assert "<" not in body
        (tmp_path / name).write_text(body)
    result = subprocess.run(["systemd-analyze", "verify", str(tmp_path / "subcon-trace-retention.service"),
                             str(tmp_path / "subcon-trace-retention.timer")], capture_output=True, text=True)
    assert (result.returncode, result.stdout + result.stderr) == (0, "")  # the exit code alone hides ignored settings


def test_the_status_carries_the_last_good_real_run_and_the_first_run(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)
    night(config, directory, offsite, status, dry_run=True)  # a dry run is never a good retention run
    first = json.loads(open(status).read())
    assert first["last_ok_at"] is None and first["since"] == first["started_at"]
    night(config, directory, offsite, status)
    good = json.loads(open(status).read())
    assert good["last_ok_at"] == good["finished_at"] and good["since"] == first["since"]
    monkeypatch.setenv("FAKE_AGE_FAIL", "1")  # the next night fails (snapshot, exit 5)
    assert night(config, directory, offsite, status, now=NOW + datetime.timedelta(days=1))[0] == 5
    failed = json.loads(open(status).read())
    assert (failed["last_ok_at"], failed["since"]) == (good["finished_at"], first["since"])
    assert tr.last_ok(failed, datetime.datetime.now(datetime.timezone.utc)) == good["finished_at"]


def test_a_status_from_before_these_fields_still_counts_as_the_last_good_run(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)
    old = {"started_at": "2026-11-07T11:32:48+00:00", "finished_at": "2026-11-07T11:36:18+00:00", "exit": 0,
           "dry_run": False}  # as written by the first release of trace_retention
    open(status, "w").write(json.dumps(old))
    monkeypatch.setenv("FAKE_AGE_FAIL", "1")
    assert night(config, directory, offsite, status)[0] == 5
    s = json.loads(open(status).read())
    assert (s["last_ok_at"], s["since"]) == (old["finished_at"], old["started_at"])


def test_an_unreadable_status_counts_from_when_it_was_written(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)
    open(status, "w").write("{not json")
    os.utime(status, (1_700_000_000, 1_700_000_000))
    monkeypatch.setenv("FAKE_AGE_FAIL", "1")
    night(config, directory, offsite, status)
    s = json.loads(open(status).read())
    assert s["last_ok_at"] is None
    assert s["since"] == datetime.datetime.fromtimestamp(1_700_000_000, datetime.timezone.utc).isoformat(timespec="seconds")


def test_a_dry_run_after_a_good_night_keeps_the_last_good_run(tmp_path, fake_age):
    config, directory, offsite, status = daily(tmp_path)
    night(config, directory, offsite, status)
    good = json.loads(open(status).read())
    night(config, directory, offsite, status, dry_run=True)
    dry = json.loads(open(status).read())
    assert dry["dry_run"] is True and dry["exit"] == 0 and dry["last_ok_at"] == good["finished_at"]


def test_a_carried_value_that_isnt_a_timestamp_is_dropped(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)
    open(status, "w").write(json.dumps({"since": "2026-11-01T00:00:00+00:00", "last_ok_at": "yesterday-ish", "exit": 5}))
    monkeypatch.setenv("FAKE_AGE_FAIL", "1")
    night(config, directory, offsite, status)
    s = json.loads(open(status).read())
    assert (s["last_ok_at"], s["since"]) == (None, "2026-11-01T00:00:00+00:00")


def real_now():
    return datetime.datetime.now(datetime.timezone.utc)  # status files hold wall-clock times (ta._now())


def seed(status, text, hours_old):
    open(status, "w").write(text)
    written = (real_now() - datetime.timedelta(hours=hours_old)).timestamp()
    os.utime(status, (written, written))


def retention_age(status):
    from observatory import health
    return health.trace_retention_age_h(os.path.dirname(status), real_now())


def test_an_invalid_carried_since_never_restarts_the_clock_over_failed_nights(tmp_path, fake_age, monkeypatch):
    config, directory, offsite, status = daily(tmp_path)
    old = (real_now() - datetime.timedelta(hours=72)).isoformat()
    seed(status, json.dumps({"since": "broken", "started_at": old, "finished_at": old, "exit": 5, "dry_run": False}), 72)
    assert retention_age(status) >= 72
    monkeypatch.setenv("FAKE_AGE_FAIL", "1")
    for n in range(3):  # three failed nights in a row: still warning, from the same start
        assert night(config, directory, offsite, status, now=NOW + datetime.timedelta(days=n))[0] == 5
        assert retention_age(status) >= 72
        assert tr.timestamp(json.loads(open(status).read())["since"]) == tr.timestamp(old)


@pytest.mark.parametrize("text", ["[1, 2]", '"just a string"', "{not json", "[" * 100_000])
def test_a_status_that_isnt_a_readable_object_keeps_its_age_over_failed_nights(tmp_path, fake_age, monkeypatch, text):
    config, directory, offsite, status = daily(tmp_path)
    seed(status, text, 72)
    assert retention_age(status) >= 72
    monkeypatch.setenv("FAKE_AGE_FAIL", "1")
    for n in range(2):
        assert night(config, directory, offsite, status, now=NOW + datetime.timedelta(days=n))[0] == 5
        assert retention_age(status) >= 72


def publish(status, clock, monkeypatch, **fields):
    """trace_retention's status publication with the merge's clock set to `clock`."""
    monkeypatch.setattr(tr, "_clock", lambda: clock)
    record = {"started_at": (clock - datetime.timedelta(minutes=4)).isoformat(), "finished_at": clock.isoformat(),
              "exit": 0, "dry_run": False, **fields}
    tr._publish_status(status, record)
    return json.loads(open(status).read())


def age_at(status, clock):
    from observatory import health
    return health.trace_retention_age_h(os.path.dirname(status), clock)


T = datetime.datetime(2026, 11, 20, 4, 44, tzinfo=datetime.timezone.utc)


def test_the_merge_keeps_a_concurrent_runs_newer_success(tmp_path, monkeypatch):
    status = str(tmp_path / "trace_retention.json")
    publish(status, T - datetime.timedelta(minutes=1), monkeypatch)  # B: finished after A, published first
    s = publish(status, T, monkeypatch, finished_at=(T - datetime.timedelta(minutes=5)).isoformat())  # A, good
    assert s["last_ok_at"] == (T - datetime.timedelta(minutes=1)).isoformat()
    s = publish(status, T + datetime.timedelta(minutes=1), monkeypatch, exit=5)  # a late failure keeps it too
    assert s["last_ok_at"] == (T - datetime.timedelta(minutes=1)).isoformat()


def test_a_future_dated_success_never_hides_failures_once_the_clock_is_right(tmp_path, monkeypatch):
    status = str(tmp_path / "trace_retention.json")
    publish(status, T + datetime.timedelta(days=30), monkeypatch)  # recorded while the clock read 30 days ahead
    s = publish(status, T, monkeypatch)  # the clock is right again: a correctly dated success
    assert s["last_ok_at"] == T.isoformat()
    for night in range(1, 5):  # then four failed nights
        s = publish(status, T + datetime.timedelta(days=night), monkeypatch, exit=5)
        assert s["last_ok_at"] == T.isoformat()
    assert age_at(status, T + datetime.timedelta(hours=47.9)) == pytest.approx(47.9)
    from observatory import health
    warn, _ = health.thresholds({})["trace_retention_last_ok_age_h"]
    assert health.level(age_at(status, T + datetime.timedelta(hours=48)), warn, None) == "warn"  # the boundary
    assert age_at(status, T + datetime.timedelta(hours=96)) == pytest.approx(96)


def test_a_future_dated_carried_success_falls_back_to_the_start(tmp_path, monkeypatch):
    status = str(tmp_path / "trace_retention.json")
    open(status, "w").write(json.dumps({"since": (T - datetime.timedelta(hours=72)).isoformat(),
                                        "last_ok_at": (T + datetime.timedelta(days=30)).isoformat()}))
    assert age_at(status, T) == pytest.approx(72)  # the reader ignores it
    s = publish(status, T, monkeypatch, exit=5)  # and the writer doesn't carry it on
    assert s["last_ok_at"] is None and age_at(status, T) == pytest.approx(72)


def test_the_clock_skew_allowance_is_ten_minutes(tmp_path):
    status = str(tmp_path / "trace_retention.json")
    for ahead, counted in ((9, True), (11, False)):
        open(status, "w").write(json.dumps({"since": (T - datetime.timedelta(hours=72)).isoformat(),
                                            "last_ok_at": (T + datetime.timedelta(minutes=ahead)).isoformat()}))
        assert (age_at(status, T) == 0) is counted


def test_a_success_published_while_another_run_writes_its_status_is_never_lost(tmp_path, fake_age, monkeypatch):
    """Run A failed its snapshot and is publishing its status; run B succeeds and publishes in between. B's success
    must survive: the status read, merge and write happen under one lock."""
    import threading
    config, directory, offsite, status = daily(tmp_path)
    old = (real_now() - datetime.timedelta(hours=72)).isoformat()
    seed(status, json.dumps({"since": old, "last_ok_at": old, "exit": 5, "dry_run": False}), 72)
    bad_offsite = tmp_path / "offsite-open"
    bad_offsite.mkdir()
    os.chmod(bad_offsite, 0o755)  # A's snapshot refuses a directory open to others: exit 5 (chmod: umask-proof)
    real_read, b = tr.read_status, []

    def run_b():
        b.append(night(config, directory, offsite, status, now=NOW + datetime.timedelta(minutes=1)))

    def read_then_let_b_publish(path):
        got = real_read(path)
        if not b and threading.current_thread() is threading.main_thread():
            worker = threading.Thread(target=run_b)
            worker.start()
            worker.join(timeout=5)  # without the lock, B finishes and publishes here, before A writes
            read_then_let_b_publish.worker = worker
        return got

    monkeypatch.setattr(tr, "read_status", read_then_let_b_publish)
    assert night(config, directory, str(bad_offsite), status)[0] == 5  # run A
    read_then_let_b_publish.worker.join(timeout=30)
    assert b and b[0][0] == 0  # B succeeded
    s = json.loads(open(status).read())
    assert s["exit"] == 0 and tr.timestamp(s["last_ok_at"]) > real_now() - datetime.timedelta(minutes=5)
    assert retention_age(status) < 1


def test_a_future_dated_since_falls_back_to_a_real_start(tmp_path, monkeypatch):
    status = str(tmp_path / "trace_retention.json")
    open(status, "w").write(json.dumps({"since": (T + datetime.timedelta(days=30)).isoformat(),
                                        "started_at": (T - datetime.timedelta(hours=72)).isoformat(), "exit": 5}))
    assert age_at(status, T) == pytest.approx(72)
    s = publish(status, T, monkeypatch, exit=5)
    assert tr.timestamp(s["since"]) == T - datetime.timedelta(hours=72)


def test_a_run_over_a_file_with_no_usable_clock_restarts_it_once_then_warns(tmp_path, monkeypatch):
    status = str(tmp_path / "trace_retention.json")
    ahead = T + datetime.timedelta(days=30)
    open(status, "w").write(json.dumps({"since": ahead.isoformat(), "last_ok_at": ahead.isoformat(),
                                        "started_at": ahead.isoformat(), "exit": 0, "dry_run": False}))
    os.utime(status, (ahead.timestamp(), ahead.timestamp()))
    s = publish(status, T, monkeypatch, exit=5)  # the clock is right again; this run fails
    assert s["last_ok_at"] is None and s["since"] == (T - datetime.timedelta(minutes=4)).isoformat()
    for night in range(1, 3):  # and the next nights fail too: the clock keeps running from that run
        s = publish(status, T + datetime.timedelta(days=night), monkeypatch, exit=5)
        assert s["since"] == (T - datetime.timedelta(minutes=4)).isoformat()
    from observatory import health
    assert health.level(age_at(status, T + datetime.timedelta(hours=48)), 48, None) == "warn"
