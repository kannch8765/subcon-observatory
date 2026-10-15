"""Collector end to end against the fake deployment from test_export: ingest, idempotence, rollup, health, no leaks."""
import csv, datetime, json, sqlite3

from observatory import collect, health, telemetry
from tests.test_export import SECRETS, deployment  # noqa: F401  (fixture)

NOW = datetime.datetime(2026, 10, 15, 12, 0, tzinfo=datetime.timezone.utc)


def config_for(deployment, tmp_path):
    return {**deployment, "TELEMETRY_DIR": str(tmp_path / "telemetry"), "TELEMETRY_TZ": "Asia/Tokyo", "DISK_PATHS": str(tmp_path)}


def count(db, table):
    return db.execute(f'select count(*) from "{table}"').fetchone()[0]


def test_collect_end_to_end(deployment, tmp_path):
    config = config_for(deployment, tmp_path)
    ok, counts, errors = collect.run(config, since="2026-10-01", now=NOW)
    assert ok and not errors, errors
    assert counts["store/batches"] == 1 and counts["letta/steps"] == 1 and counts["letta_traces/traces"] == 2
    tdir = tmp_path / "telemetry"
    db = sqlite3.connect(tdir / "telemetry.db")
    assert db.execute("select retry_reasons from batches").fetchone()[0] == "turn_failed"

    rollup = dict(db.execute("select metric, value from daily_rollup where day = '2026-10-15' and scope = 'all'").fetchall())
    assert (rollup["attempts"], rollup["retryable"], rollup["retry_reason:turn_failed"]) == (1, 1, 1)
    assert rollup["retryable_share"] == 1.0 and rollup["steps"] == 1 and rollup["failed_step_share"] == 1.0
    assert rollup["backlog_lines"] == 2 and rollup["backlog_sessions"] == 0  # 5 transcript lines, cursor at index 2

    report = json.loads((tdir / "health.json").read_text())
    levels = {c["name"]: c["level"] for c in report["checks"]}
    assert report["schema"] == "subcon_health_v1" and report["overall"] == "alert"
    assert levels["batch_retryable_share_24h"] == "alert" and levels["letta_failed_step_share_24h"] == "alert"
    assert levels["stuck_active_sessions"] == "ok"
    digest = json.loads((tdir / "digest.json").read_text())
    assert digest["day"] == "2026-10-14" and digest["overall"] == "alert"  # digest = yesterday

    # A second run is idempotent for upsert/log tables and adds one backlog sample
    ok, _, _ = collect.run(config, now=NOW + datetime.timedelta(hours=1))
    assert ok
    assert count(db, "batches") == 1 and count(db, "letta_steps") == 1 and count(db, "outcomes") == 1
    assert count(db, "backlog_samples") == 2 and count(db, "collect_runs") == 2
    assert not (tdir / "work").exists()

    for path in tdir.iterdir():
        if path.is_file():
            data = path.read_bytes()
            for secret in SECRETS:
                assert secret.encode() not in data, f"{secret!r} leaked into {path.name}"


def test_plan_never_reads_all_traces_without_since(deployment, tmp_path):
    config = config_for(deployment, tmp_path)
    names = [name for name, _ in collect.plan(config, None)]
    assert "letta_traces" not in names and "opencode" in names
    assert ("letta_traces", "2026-08-01") in collect.plan(config, "2026-08-01")
    assert ("letta", "2026-08-01") in collect.plan(config, "2026-08-01")


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_log_identity_survives_a_new_exporter_column(tmp_path):
    db = telemetry.connect(tmp_path / "t.db")
    row = {"batch_prefix": "batch", "bid": "012345678901", "status": "completed", "finalized_at": "2026-10-15T00:00:00Z"}
    write_csv(tmp_path / "a.csv", [row])
    write_csv(tmp_path / "b.csv", [{**row, "retry_reasons": ""}])
    telemetry.ingest_csv(db, "store/batches", tmp_path / "a.csv", "s1")
    telemetry.ingest_csv(db, "store/batches", tmp_path / "b.csv", "s2")
    assert count(db, "batches") == 1
    assert db.execute("select bid from batches").fetchone()[0] == "012345678901"  # hashed ids keep leading zeros


def test_stuck_session_needs_an_hour_over_the_gap(tmp_path):
    db = telemetry.connect(tmp_path / "t.db")
    sample = {"session": "abc", "last_processed_index": 9, "transcript_found": 1, "transcript_msgs": 100,
              "transcript_mtime": "2026-10-15T11:00:00+00:00"}
    write_csv(tmp_path / "s.csv", [sample])
    telemetry.ingest_csv(db, "backlog/backlog", tmp_path / "s.csv", "2026-10-15T11:30:00+00:00")
    stuck = lambda: health.values(db, NOW, [])["stuck_active_sessions"]
    assert stuck() == 0  # only 30 min of history
    telemetry.ingest_csv(db, "backlog/backlog", tmp_path / "s.csv", "2026-10-15T10:00:00+00:00")
    assert stuck() == 1


def test_threshold_overrides_and_levels():
    assert health.level(None, 1, 2) == "ok" and health.level(1.5, 1, 2) == "warn" and health.level(2, 1, 2) == "alert"
    assert health.thresholds({"HEALTH_DISK_USED_SHARE_ALERT": "0.95"})["disk_used_share"] == (0.80, 0.95)


def test_rollup_survives_a_column_an_older_snapshot_lacks(tmp_path):
    from observatory import rollup
    import zoneinfo
    db = telemetry.connect(tmp_path / "t.db")
    write_csv(tmp_path / "b.csv", [{"batch_prefix": "batch", "bid": "b1", "status": "retryable_failure", "finalized_at": "2026-10-15T00:00:00Z"}])
    telemetry.ingest_csv(db, "store/batches", tmp_path / "b.csv", "s1")  # no retry_reasons column at all
    metrics = rollup.compute(db, zoneinfo.ZoneInfo("Asia/Tokyo"), ["2026-10-15"])
    assert metrics[("2026-10-15", "all")]["retryable"] == 1


def failing_export(config, only, local=False, snapshot_dir=None):
    snapshot_dir.mkdir(parents=True)
    (snapshot_dir / "snapshot.json").write_text(json.dumps({"exporters": {only[0]: {"error": "exporter boom"}}}))
    return snapshot_dir


def test_failed_exporters_never_publish_green_health(deployment, tmp_path, monkeypatch):
    config = config_for(deployment, tmp_path)
    monkeypatch.setattr(collect, "export", failing_export)
    ok, _, errors = collect.run(config, now=NOW)
    assert not ok and set(errors) >= {"store", "letta"}
    report = json.loads((tmp_path / "telemetry" / "health.json").read_text())
    levels = {c["name"]: c["level"] for c in report["checks"]}
    assert report["overall"] == "alert" and report["last_success_at"] is None
    assert levels["collector_failed_exporters"] == "alert" and levels["collector_last_success_age_h"] == "alert"


def test_partial_failures_age_out_the_last_success(deployment, tmp_path, monkeypatch):
    config = config_for(deployment, tmp_path)
    assert collect.run(config, since="2026-10-01", now=NOW)[0]
    monkeypatch.setattr(collect, "export", failing_export)
    collect.run(config, now=NOW + datetime.timedelta(hours=3))
    report = json.loads((tmp_path / "telemetry" / "health.json").read_text())
    age = next(c for c in report["checks"] if c["name"] == "collector_last_success_age_h")
    assert report["last_success_at"] == NOW.isoformat() and age["level"] == "warn" and age["value"] == 3.0


def test_backfill_recomputes_historical_days(deployment, tmp_path):
    config = config_for(deployment, tmp_path)
    later = NOW + datetime.timedelta(days=5)  # the fixture's 10-15 rows are now outside the default 3-day window
    assert collect.run(config, since="2026-10-01", now=later)[0]
    db = sqlite3.connect(tmp_path / "telemetry" / "telemetry.db")
    day = dict(db.execute("select metric, value from daily_rollup where day = '2026-10-15' and scope = 'all'").fetchall())
    assert day["attempts"] == 1 and day["steps"] == 1


def test_shared_block_keeps_every_agent_link(tmp_path):
    db = telemetry.connect(tmp_path / "t.db")
    write_csv(tmp_path / "blocks.csv", [{"block": "blk", "agent": "active", "label": "persona", "chars": 6000, "limit": 5000},
                                        {"block": "blk", "agent": "zz-old", "label": "persona", "chars": 6000, "limit": 9000}])
    write_csv(tmp_path / "steps.csv", [{"step": "s1", "agent": "active", "status": "COMPLETED", "created_at": "2026-10-15T11:00:00+00:00"}])
    telemetry.ingest_csv(db, "letta/blocks", tmp_path / "blocks.csv", "s1")
    telemetry.ingest_csv(db, "letta/steps", tmp_path / "steps.csv", "s1")
    assert count(db, "letta_blocks") == 2
    assert health.values(db, NOW, [])["blocks_over_limit"] == 1


def test_plan_does_not_touch_an_existing_database(deployment, tmp_path):
    config = config_for(deployment, tmp_path)
    assert collect.run(config, since="2026-10-01", now=NOW)[0]
    tdir = tmp_path / "telemetry"
    db = sqlite3.connect(tdir / "telemetry.db")
    db.execute("pragma journal_mode=delete")  # so a stray write-mode connect would visibly flip it back to wal
    db.close()
    before = {p.name: p.read_bytes() for p in tdir.iterdir() if p.is_file()}
    collect.plan(config, None)
    after = {p.name: p.read_bytes() for p in tdir.iterdir() if p.is_file()}
    assert after == before
