"""Healthcheck over the telemetry store: each check -> ok / warn / alert with its value and thresholds.
Output is numbers and fixed names only, so a bot can render it without seeing any content."""
import datetime, shutil

from observatory.rollup import RESOLVED_INTENT, backlog_gap, pct, rows
from observatory.telemetry import parse_ts

LEVELS = ["ok", "warn", "alert"]
# name -> (warn, alert); `None` = that level is not used. Override with HEALTH_<NAME>_WARN / _ALERT in config.
THRESHOLDS = {
    "batch_retryable_share_24h": (0.10, 0.25),
    "stuck_active_sessions": (None, 1),
    "unresolved_intent_age_h": (1, 24),
    "whisper_ready_p90_s_24h": (120, 300),
    "letta_failed_step_share_24h": (0.05, 0.20),
    "disk_used_share": (0.80, 0.90),
    "blocks_over_limit": (1, None),
    # A run whose exporters failed must never look green, and repeated partial failures must age out
    "collector_failed_exporters": (None, 1),
    "collector_last_success_age_h": (2, 26),
}
STUCK_GAP_LINES = 50
NONE_IS_ALERT = {"collector_last_success_age_h"}  # "never succeeded" is the worst case, not "no data"


def thresholds(config):
    out = {}
    for name, (warn, alert) in THRESHOLDS.items():
        get = lambda level, default: float(config[k]) if (k := f"HEALTH_{name.upper()}_{level}") in config else default
        out[name] = (get("WARN", warn), get("ALERT", alert))
    return out


def level(value, warn, alert):
    if value is None:
        return "ok"
    if alert is not None and value >= alert:
        return "alert"
    if warn is not None and value >= warn:
        return "warn"
    return "ok"


def within(ts, since):
    t = parse_ts(ts)
    return bool(t and t >= since)


def values(db, now, disk_paths):
    day_ago = now - datetime.timedelta(days=1)
    out = {}

    attempts = [r for r in rows(db, "select * from batches where status != 'pending'") if within(r["finalized_at"], day_ago)]
    out["batch_retryable_share_24h"] = (sum(r["status"] != "completed" for r in attempts) / len(attempts)) if attempts else None

    # A session is stuck when it is still active (transcript touched in 24 h) and its cursor gap has stayed over the
    # limit for at least an hour: in the newest sample and in the newest sample that is >= 1 h older.
    samples = sorted({r["sampled_at"] for r in rows(db, "select distinct sampled_at from backlog_samples")})
    stuck = None
    if samples:
        latest = samples[-1]
        hour_before = [s for s in samples if parse_ts(s) <= parse_ts(latest) - datetime.timedelta(hours=1)]
        gaps = lambda sampled_at: {r["session"]: (backlog_gap(r), r["transcript_mtime"])
                                   for r in rows(db, "select * from backlog_samples where sampled_at = ?", (sampled_at,))}
        now_gaps, old_gaps = gaps(latest), gaps(hour_before[-1]) if hour_before else {}
        stuck = sum(1 for s, (gap, mtime) in now_gaps.items()
                    if gap > STUCK_GAP_LINES and within(mtime, day_ago) and old_gaps.get(s, (0, None))[0] > STUCK_GAP_LINES)
    out["stuck_active_sessions"] = stuck

    latest_outcome = {}
    for r in sorted(rows(db, "select * from intent_outcomes"), key=lambda r: r["recorded_at"] or ""):
        latest_outcome[r["iid"]] = r["outcome"]
    ages = [(now - t).total_seconds() / 3600 for r in rows(db, "select * from intents")
            if latest_outcome.get(r["iid"]) not in RESOLVED_INTENT and (t := parse_ts(r["captured_at"]))]
    out["unresolved_intent_age_h"] = max(ages) if ages else None
    out["unresolved_intents"] = len(ages)

    ready = [r["ready_latency_ms"] for r in rows(db, "select * from whispers") if within(r["created_at"], day_ago)]
    p90 = pct(ready, 0.9)
    out["whisper_ready_p90_s_24h"] = p90 / 1000 if p90 is not None else None

    steps_all = rows(db, "select * from letta_steps")
    steps = [r for r in steps_all if within(r["created_at"], day_ago)]
    out["letta_failed_step_share_24h"] = (sum(r["status"] == "FAILED" for r in steps) / len(steps)) if steps else None

    shares = []
    for path in disk_paths:
        try:
            usage = shutil.disk_usage(path)
            shares.append(usage.used / usage.total)
        except OSError:
            pass
    out["disk_used_share"] = max(shares) if shares else None

    # Only agents that actually ran in the last 24 h: Letta keeps blocks of every old and sibling agent forever
    active = {r["agent"] for r in steps_all if within(r["created_at"], day_ago)}
    blocks = [b for b in rows(db, "select * from letta_blocks") if b["agent"] in active]
    out["blocks_over_limit"] = sum(1 for b in blocks if isinstance(b["chars"], int) and isinstance(b["limit"], int) and b["chars"] > b["limit"]) if blocks else None
    return out


def check(db, now, config, collector_errors=(), last_success_at=None):
    disk_paths = [p for p in (config.get("DISK_PATHS") or "/").split(",") if p]
    measured = values(db, now, disk_paths)
    measured["collector_failed_exporters"] = len(collector_errors)
    last_ok = parse_ts(last_success_at)
    measured["collector_last_success_age_h"] = (now - last_ok).total_seconds() / 3600 if last_ok else None
    checks = []
    for name, (warn, alert) in thresholds(config).items():
        value = measured.get(name)
        lvl = "alert" if value is None and name in NONE_IS_ALERT else level(value, warn, alert)
        checks.append({"name": name, "level": lvl, "value": round(value, 4) if isinstance(value, float) else value,
                       "warn": warn, "alert": alert})
    overall = max((c["level"] for c in checks), key=LEVELS.index, default="ok")
    return {"schema": "subcon_health_v1", "generated_at": now.isoformat(), "last_success_at": last_success_at,
            "overall": overall, "checks": checks,
            "info": {"unresolved_intents": measured["unresolved_intents"]}}
