"""On-server collector: run the exporters locally, merge their (sanitized) CSVs into telemetry.db,
recompute the daily rollup and write health.json + digest.json. Meant for a systemd timer.

    python3 -E -s -m observatory.collect --config /etc/subcon-observatory.env --plan    # what it would do, writes nothing
    python3 -E -s -m observatory.collect --config /etc/subcon-observatory.env           # one collect run
    python3 -E -s -m observatory.collect --config ... --since 2026-08-01                # backfill (incl. letta_traces)

TELEMETRY_DIR is the only place it writes. Letta exports are incremental: each run re-reads from one day before the
newest ingested row (or --since). Once a day (first fully successful run at or after CHARTS_AFTER_HOUR local, default 6) it writes
TELEMETRY_DIR/charts/<yesterday>/{report.html,metrics.json}; a chart failure is logged but never fails the run.
"""
import argparse, datetime, fcntl, json, os, pathlib, shutil, sys, zoneinfo

from observatory import charts, health, rollup, telemetry
from observatory.export import EXPORTERS, export, load_config

ALWAYS = ["store", "whispers", "backlog", "recall_log", "deploy", "letta"]
ROLLUP_DAYS = 3


def plan(config, since, db=None):
    """Exporters to run, each with the LETTA_SINCE it gets (None = full history).

    With `db` (the run's own connection) the incremental watermark is resolved. Without it, as for `--plan`, the rule
    is described instead: even a read-only open of a WAL database creates -wal/-shm files, and --plan writes nothing."""
    def bound(table):
        if since:
            return since
        if db is None:
            return f"<newest {table}.created_at - 1 day>"
        return telemetry.since_day(telemetry.max_value(db, table, "created_at"))

    steps = [(name, bound("letta_steps") if name == "letta" else None) for name in ALWAYS]
    if config.get("COLLECT_TRACES", "1") != "0":
        traces_since = bound("letta_traces")
        if traces_since:  # never an unbounded provider_traces read: the first one needs --since
            steps.append(("letta_traces", traces_since))
    if config.get("OPENCODE_CSV_GLOBS"):
        steps.append(("opencode", None))
    return steps


def atomic_json(path, value):
    tmp = path.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True))
    os.chmod(tmp, 0o644)
    os.replace(tmp, path)


def daily_charts(db, now, tz, tdir, config, force=False):
    """Yesterday's chart report, once the day's late rows have settled. Returns an error string or None.
    Only called after a fully successful collection, so a report is never sealed with missing data."""
    try:
        if now.astimezone(tz).hour < int(config.get("CHARTS_AFTER_HOUR") or 6) and not force:
            return None
        charts.ensure(db, tz, rollup.recent_days(now, tz, 2)[1], tdir / "charts", health.thresholds(config),
                      keep_days=int(config.get("CHARTS_KEEP_DAYS", charts.KEEP_DAYS)), force=force)
    except Exception as error:  # the report is a nice-to-have; telemetry and health must still land
        return f"{type(error).__name__}: {error}"
    return None


def run(config, since=None, now=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    tz = zoneinfo.ZoneInfo(config["TELEMETRY_TZ"]) if config.get("TELEMETRY_TZ") else now.astimezone().tzinfo
    tdir = pathlib.Path(config["TELEMETRY_DIR"])
    tdir.mkdir(parents=True, exist_ok=True)
    with open(tdir / "collect.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)  # an overlapping run fails fast instead of piling up
        started = now.isoformat()
        db = telemetry.connect(tdir / "telemetry.db")
        steps = plan(config, since, db)
        counts, redacted, errors, ok = {}, {}, {}, False
        work = tdir / "work"
        shutil.rmtree(work, ignore_errors=True)
        try:
            for name, letta_since in steps:
                cfg = {**config, "LETTA_SINCE": letta_since or ""}
                snap = export(cfg, [name], local=True, snapshot_dir=work / name)
                summary = json.loads((snap / "snapshot.json").read_text())["exporters"][name]
                if "error" in summary:
                    errors[name] = summary["error"]
                    continue
                redacted.update({f"{name}/{t}": v["redacted_cells"] for t, v in summary.items() if v["redacted_cells"]})
                counts.update(telemetry.ingest_snapshot(db, snap, started))
                db.commit()
            # A backfill recomputes every day it may have touched, not just the latest few
            days = rollup.days_since(now, tz, since) if since else rollup.recent_days(now, tz, ROLLUP_DAYS)
            rollup.write(db, tz, days)
            db.commit()
            last_ok = db.execute("select max(started_at) from collect_runs where ok = 1").fetchone()[0]
            report = health.check(db, now, config, collector_errors=sorted(errors),
                                  last_success_at=started if not errors else last_ok)
            report["collector"] = {"errors": sorted(errors), "redacted_cells": sum(redacted.values())}
            atomic_json(tdir / "health.json", report)
            yesterday = rollup.recent_days(now, tz, 2)[1]
            atomic_json(tdir / "digest.json", {**rollup.digest(db, yesterday), "generated_at": started, "overall": report["overall"]})
            ok = not errors
            # After a failed collection, wait for a successful retry: the report is written once per day
            chart_error = daily_charts(db, now, tz, tdir, config, force=bool(since)) if ok else None
            if chart_error:
                errors["charts"] = chart_error
        except Exception as error:
            errors["collect"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            db.execute("insert or replace into collect_runs values (?, ?, ?, ?, ?, ?, ?, ?)",
                       (started, datetime.datetime.now(datetime.timezone.utc).isoformat(), int(ok),
                        dict(steps).get("letta"), dict(steps).get("letta_traces"), json.dumps(counts), json.dumps(redacted),
                        json.dumps(errors) if errors else None))
            db.commit()
            db.close()
            shutil.rmtree(work, ignore_errors=True)
    return ok, counts, errors


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", required=True)
    parser.add_argument("--plan", action="store_true", help="print what would run; write nothing")
    parser.add_argument("--since", help="ISO date: backfill Letta (and letta_traces) from here")
    args = parser.parse_args()
    config = load_config(args.config)
    if "TELEMETRY_DIR" not in config:
        parser.error("config needs TELEMETRY_DIR")
    unknown = [n for n in ALWAYS if n not in EXPORTERS]
    assert not unknown, unknown
    if args.plan:
        print(f"TELEMETRY_DIR={config['TELEMETRY_DIR']}")
        for name, letta_since in plan(config, args.since):
            note = " (skipped until a --since backfill has ingested traces)" if name == "letta_traces" and not args.since else ""
            print(f"EXPORT {name}" + (f" since={letta_since}{note}" if name.startswith("letta") else ""))
        print("RESULT=PLAN_OK")
        return
    ok, counts, errors = run(config, args.since)
    print(json.dumps({"ok": ok, "rows": counts, "errors": errors}, sort_keys=True))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
