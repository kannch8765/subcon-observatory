"""Healthcheck for the nightly trace retention: hours since the last real run that succeeded, from its status file."""
import datetime, json, os

from observatory import collect, health
from tests.test_collect import NOW, config_for
from tests.test_export import deployment  # noqa: F401  (fixture)


def write_status(tdir, **fields):
    tdir.mkdir(parents=True, exist_ok=True)
    path = tdir / health.TRACE_RETENTION_STATUS
    path.write_text(json.dumps(fields))
    return path


def ago(hours):
    return (NOW - datetime.timedelta(hours=hours)).isoformat()


def level(value):
    return health.level(value, *health.thresholds({})["trace_retention_last_ok_age_h"])


def test_no_status_file_means_retention_doesnt_run_here(tmp_path):
    assert health.trace_retention_age_h(str(tmp_path), NOW) is None and level(None) == "ok"
    assert health.trace_retention_age_h(None, NOW) is None


def test_age_of_the_last_good_real_run_warns_after_two_failed_nights(tmp_path):
    write_status(tmp_path, since=ago(200), last_ok_at=ago(47.5), finished_at=ago(1), exit=5, dry_run=False)
    assert health.trace_retention_age_h(str(tmp_path), NOW) == 47.5 and level(47.5) == "ok"
    write_status(tmp_path, since=ago(200), last_ok_at=ago(48.5), finished_at=ago(1), exit=4, dry_run=False)
    assert level(health.trace_retention_age_h(str(tmp_path), NOW)) == "warn"
    assert health.thresholds({})["trace_retention_last_ok_age_h"] == (48, None)  # never an alert by default


def test_a_good_run_is_its_own_last_good_run(tmp_path):
    write_status(tmp_path, since=ago(200), last_ok_at=ago(100), finished_at=ago(2), exit=0, dry_run=False)
    assert health.trace_retention_age_h(str(tmp_path), NOW) == 2


def test_a_dry_run_is_not_a_good_retention_run(tmp_path):
    write_status(tmp_path, since=ago(60), last_ok_at=None, finished_at=ago(1), exit=0, dry_run=True)
    assert health.trace_retention_age_h(str(tmp_path), NOW) == 60 and level(60) == "warn"  # never succeeded: from `since`


def test_a_dry_run_after_a_good_night_still_reports_that_night(tmp_path):
    write_status(tmp_path, since=ago(300), last_ok_at=ago(20), finished_at=ago(1), exit=0, dry_run=True)
    assert health.trace_retention_age_h(str(tmp_path), NOW) == 20


def test_a_status_file_that_goes_away_mid_check_is_not_an_error(tmp_path, monkeypatch):
    write_status(tmp_path, since=ago(10))
    monkeypatch.setattr(health.os.path, "getmtime", lambda path: (_ for _ in ()).throw(FileNotFoundError(path)))
    assert health.trace_retention_age_h(str(tmp_path), NOW) is None


def test_a_status_from_before_the_carried_fields_still_works(tmp_path):
    write_status(tmp_path, started_at=ago(10.1), finished_at=ago(10), exit=0, dry_run=False)
    assert health.trace_retention_age_h(str(tmp_path), NOW) == 10
    write_status(tmp_path, started_at=ago(70), finished_at=ago(69.9), exit=1, dry_run=False)
    assert level(health.trace_retention_age_h(str(tmp_path), NOW)) == "warn"


def test_an_unreadable_status_counts_from_when_it_was_written(tmp_path):
    path = write_status(tmp_path)
    path.write_text("[1, 2")
    written = (NOW - datetime.timedelta(hours=50)).timestamp()
    os.utime(path, (written, written))
    assert round(health.trace_retention_age_h(str(tmp_path), NOW), 6) == 50


def test_the_collector_publishes_the_check(deployment, tmp_path):
    config = config_for(deployment, tmp_path)
    write_status(tmp_path / "telemetry", since=ago(300), last_ok_at=ago(72), finished_at=ago(1), exit=5, dry_run=False)
    assert collect.run(config, since="2026-10-01", now=NOW)[0]
    report = json.loads((tmp_path / "telemetry" / "health.json").read_text())
    c = next(c for c in report["checks"] if c["name"] == "trace_retention_last_ok_age_h")
    assert (c["level"], c["value"], c["warn"], c["alert"]) == ("warn", 72.0, 48, None)


def test_a_status_nested_too_deep_never_stops_the_collector(deployment, tmp_path):
    config = config_for(deployment, tmp_path)
    path = write_status(tmp_path / "telemetry")
    path.write_text("[" * 100_000)
    written = (NOW - datetime.timedelta(hours=60)).timestamp()
    os.utime(path, (written, written))
    assert collect.run(config, since="2026-10-01", now=NOW)[0]
    report = json.loads((tmp_path / "telemetry" / "health.json").read_text())
    c = next(c for c in report["checks"] if c["name"] == "trace_retention_last_ok_age_h")
    assert (c["level"], round(c["value"])) == ("warn", 60)


def test_an_invalid_since_falls_back_to_a_valid_start(tmp_path):
    write_status(tmp_path, since="broken", started_at=ago(72), finished_at=ago(71.9), exit=5, dry_run=False)
    assert health.trace_retention_age_h(str(tmp_path), NOW) == 72


def future_status(tdir, days_ahead=30):
    """A status written while the clock ran `days_ahead` days fast: JSON timestamps and the file's mtime alike."""
    ahead = NOW + datetime.timedelta(days=days_ahead)
    path = write_status(tdir, since=ahead.isoformat(), started_at=ahead.isoformat(), finished_at=ahead.isoformat(),
                        last_ok_at=ahead.isoformat(), exit=0, dry_run=False)
    os.utime(path, (ahead.timestamp(), ahead.timestamp()))
    return path


def test_a_file_with_no_usable_clock_is_untrusted_not_zero(tmp_path):
    future_status(tmp_path)
    assert health.trace_retention_age_h(str(tmp_path), NOW) == health.UNTRUSTED_CLOCK


def test_future_json_with_a_real_mtime_counts_from_the_mtime(tmp_path):
    path = future_status(tmp_path)
    written = (NOW - datetime.timedelta(hours=50)).timestamp()
    os.utime(path, (written, written))
    assert round(health.trace_retention_age_h(str(tmp_path), NOW), 6) == 50


def test_the_collector_warns_on_an_untrusted_clock_at_every_check_while_nothing_rewrites_it(deployment, tmp_path):
    """The clock was corrected after the status was written and no run has written one since (the timer stopped, or
    the run can't write): every check must stay visibly warn, never report 0 h."""
    config = config_for(deployment, tmp_path)
    future_status(tmp_path / "telemetry")
    assert collect.run(config, since="2026-10-01", now=NOW)[0]
    for later in (datetime.timedelta(0), datetime.timedelta(hours=48), datetime.timedelta(hours=96),
                  datetime.timedelta(days=29)):
        assert collect.run(config, now=NOW + later)[0]
        report = json.loads((tmp_path / "telemetry" / "health.json").read_text())
        c = next(c for c in report["checks"] if c["name"] == "trace_retention_last_ok_age_h")
        assert (c["level"], c["value"]) == ("warn", None), later
        assert report["info"]["trace_retention_clock"] == "untrusted"


def test_a_trusted_clock_reports_no_clock_info(deployment, tmp_path):
    config = config_for(deployment, tmp_path)
    write_status(tmp_path / "telemetry", since=ago(10), last_ok_at=ago(5), finished_at=ago(5), exit=0, dry_run=False)
    assert collect.run(config, since="2026-10-01", now=NOW)[0]
    report = json.loads((tmp_path / "telemetry" / "health.json").read_text())
    assert "trace_retention_clock" not in report["info"]
