"""Daily chart report: built once a day by the collector, aggregates only, never fails a collect run."""
import datetime, json, os, sqlite3, zoneinfo

from observatory import charts, collect, health, telemetry
from tests.test_collect import NOW, config_for
from tests.test_export import SECRETS, deployment  # noqa: F401  (fixture)

TZ = zoneinfo.ZoneInfo("Asia/Tokyo")


def rollup_db(tmp_path, values):
    db = telemetry.connect(tmp_path / "t.db")
    db.executemany("insert into daily_rollup values (?, ?, ?, ?)", values)
    return db


def test_collect_writes_yesterdays_report_once(deployment, tmp_path):
    config = config_for(deployment, tmp_path)
    ok, _, errors = collect.run(config, since="2026-10-01", now=NOW)  # 21:00 JST
    assert ok and "charts" not in errors, errors
    root = tmp_path / "telemetry" / "charts"
    day = root / "2026-10-14"  # yesterday in Tokyo
    assert (day / "report.html").exists() and os.readlink(root / "latest") == "2026-10-14"
    data = json.loads((day / "metrics.json").read_text())
    assert data["schema"] == "subcon_charts_v1" and data["days"][-1] == "2026-10-14" and len(data["days"]) == 90
    db = sqlite3.connect(tmp_path / "telemetry" / "telemetry.db")
    sizes = dict(db.execute("select metric, value from daily_rollup where day = '2026-10-15' and metric like 'pg_bytes:%'").fetchall())
    assert sizes == {"pg_bytes:provider_traces": 5100000000, "pg_bytes:_database": 5600000000}

    before = (day / "report.html").stat().st_mtime_ns
    assert collect.run(config, now=NOW + datetime.timedelta(hours=1))[0]
    assert (day / "report.html").stat().st_mtime_ns == before  # once a day, not every hour

    for path in root.rglob("*"):
        if path.is_file():
            text = path.read_bytes()
            for secret in SECRETS:
                assert secret.encode() not in text, f"{secret!r} leaked into {path}"


def test_no_report_before_the_settle_hour(deployment, tmp_path):
    config = config_for(deployment, tmp_path)
    early = datetime.datetime(2026, 10, 15, 3, 0, tzinfo=TZ)
    assert collect.run(config, now=early)[0]
    assert not (tmp_path / "telemetry" / "charts").exists()


def test_chart_failure_never_fails_the_run(deployment, tmp_path, monkeypatch):
    config = config_for(deployment, tmp_path)

    def boom(*args, **kwargs):
        raise ValueError("chart boom")

    monkeypatch.setattr(charts, "ensure", boom)
    ok, _, errors = collect.run(config, since="2026-10-01", now=NOW)
    assert ok and errors == {"charts": "ValueError: chart boom"}
    assert json.loads((tmp_path / "telemetry" / "health.json").read_text())["checks"]


def test_build_series_tiles_zones_and_deploys(tmp_path):
    day = "2026-10-18"
    values = [(day, "all", "attempts", 10), (day, "all", "completed", 6), (day, "all", "retryable", 4),
              (day, "all", "retryable_share", 0.4), (day, "all", "steps", 20), (day, "all", "failed_steps", 5)]
    values += [(day, "all", f"retry_reason:r{i}", i) for i in range(1, 6)]
    values += [("2026-10-10", "all", "pg_bytes:provider_traces", 1e9), (day, "all", "pg_bytes:provider_traces", 1.7e9),
               (day, "model:x", "steps", 20), (day, "batch:batch", "attempts", 10)]
    db = rollup_db(tmp_path, values)
    db.execute('create table deploy_reflog (_row text, checkout_idx, sha, "at", action)')
    db.execute("insert into deploy_reflog values ('a', 0, 'abc1234', '2026-10-17T15:30:00Z', 'reset')")  # 10-18 00:30 JST
    page, data = charts.build(db, TZ, day, health.thresholds({}))

    reasons = dict(charts.series_for(charts.CHARTS[2], data["series"]["all"], 90))
    assert list(reasons) == ["r5", "r4", "r3", "other"] and reasons["other"][-1] == 3  # top 3 + Other (r1 + r2)
    ok_steps = dict(charts.series_for(charts.CHARTS[4], data["series"]["all"], 90))["Succeeded"]
    assert ok_steps[-1] == 15
    assert data["summary"]["last_7d"]["retryable_share"] == 0.4
    assert data["summary"]["provider_traces_bytes_per_day_7d"] == 0.7e9 / 8  # 10-10 -> 10-18
    assert set(data["series"]) == {"all", "model:x"}  # per-batch-prefix scopes stay out
    assert data["deploys"] == [{"day": day, "checkout": 0, "sha": "abc1234", "action": "reset"}]
    assert "alert ≥ 25.0%" in page and "warn ≥ 10.0%" in page and "deploy abc1234" in page
    assert "No data in this window yet" in page  # e.g. opencode cost
    assert "<script src" not in page and "http" not in page.split("<body>")[1]  # self-contained


def test_ensure_prunes_old_days_and_keeps_latest(tmp_path):
    db = rollup_db(tmp_path, [("2026-10-18", "all", "attempts", 1)])
    root = tmp_path / "charts"
    for old in ("2026-08-01", "2026-10-01"):
        (root / old).mkdir(parents=True)
    assert charts.ensure(db, TZ, "2026-10-18", root, {}, keep_days=30)
    assert not charts.ensure(db, TZ, "2026-10-18", root, {}, keep_days=30)
    assert sorted(p.name for p in root.iterdir()) == ["2026-10-01", "2026-10-18", "latest"]


def test_failed_collection_waits_for_a_successful_retry(deployment, tmp_path, monkeypatch):
    """Haru #2 review: a failed first eligible run must not seal an incomplete report for the day."""
    from tests.test_collect import failing_export
    config = config_for(deployment, tmp_path)
    first = datetime.datetime(2026, 10, 16, 6, 17, tzinfo=TZ)
    real_export = collect.export
    monkeypatch.setattr(collect, "export", failing_export)
    assert not collect.run(config, now=first)[0]
    report = tmp_path / "telemetry" / "charts" / "2026-10-15" / "metrics.json"
    assert not report.exists()
    monkeypatch.setattr(collect, "export", real_export)
    assert collect.run(config, since="2026-10-01", now=first + datetime.timedelta(hours=1))[0]
    data = json.loads(report.read_text())
    assert data["summary"]["last_7d"]["attempts"] == 1 and data["series"]["all"]["attempts"][-1] == 1


def test_latest_heals_after_a_crash_between_rename_and_link(tmp_path, monkeypatch):
    """Haru #2 review: publication must converge on retry, also on a first-ever publication."""
    db = rollup_db(tmp_path, [("2026-10-17", "all", "attempts", 1), ("2026-10-18", "all", "attempts", 2)])
    root = tmp_path / "charts"
    real_replace = os.replace

    def broken_replace(src, dst):
        if str(dst).endswith("latest"):
            raise OSError("injected")
        real_replace(src, dst)

    monkeypatch.setattr(charts.os, "replace", broken_replace)
    try:
        charts.ensure(db, TZ, "2026-10-17", root, {})
    except OSError:
        pass
    assert (root / "2026-10-17" / "report.html").exists() and not (root / "latest").exists()  # first publication broke
    monkeypatch.setattr(charts.os, "replace", real_replace)
    assert not charts.ensure(db, TZ, "2026-10-17", root, {})  # not rewritten, but published
    assert os.readlink(root / "latest") == "2026-10-17"

    monkeypatch.setattr(charts.os, "replace", broken_replace)
    try:
        charts.ensure(db, TZ, "2026-10-18", root, {})
    except OSError:
        pass
    assert os.readlink(root / "latest") == "2026-10-17"
    monkeypatch.setattr(charts.os, "replace", real_replace)
    charts.ensure(db, TZ, "2026-10-18", root, {})
    assert os.readlink(root / "latest") == "2026-10-18"
    assert not list(root.glob(".*.tmp"))


def test_bad_chart_config_stays_inside_the_chart_boundary(deployment, tmp_path):
    for value in ("", "six"):
        config = {**config_for(deployment, tmp_path / value), "CHARTS_AFTER_HOUR": value}
        ok, _, errors = collect.run(config, since="2026-10-01", now=NOW)
        assert ok, errors
        assert value == "" or errors["charts"].startswith("ValueError")  # empty = default hour
