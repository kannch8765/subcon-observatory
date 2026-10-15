"""Daily rollup: one (day, scope, metric) -> value row per metric, recomputed for recent days on every collect run.
Long format, so adding a metric never needs a migration. Days are in the configured local timezone."""
import collections, datetime, sqlite3

from observatory.telemetry import parse_ts

RESOLVED_INTENT = {"accepted", "duplicate", "permanently_rejected"}


class Row(dict):
    """A column an older snapshot didn't export reads as None instead of failing the whole metric."""

    def __missing__(self, key):
        return None


def rows(db, sql, params=()):
    try:
        cur = db.execute(sql, params)
    except sqlite3.OperationalError as error:
        if str(error).startswith("no such table"):  # not ingested yet; anything else is a real bug
            return []
        raise
    names = [d[0] for d in cur.description]
    return [Row(zip(names, r)) for r in cur]


def pct(values, q):
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    return values[min(len(values) - 1, int(q * len(values)))]


def num(value):
    return value if isinstance(value, (int, float)) else 0


class Acc:
    """metrics[(day, scope)][metric] with helpers for counts, sums and percentile samples."""

    def __init__(self, tz, days):
        self.tz, self.days = tz, set(days)
        self.counts = collections.defaultdict(lambda: collections.defaultdict(float))
        self.samples = collections.defaultdict(lambda: collections.defaultdict(list))

    def day(self, ts):
        t = parse_ts(ts)
        d = t.astimezone(self.tz).date().isoformat() if t else None
        return d if d in self.days else None

    def add(self, day, scopes, metric, value=1):
        for scope in scopes:
            self.counts[(day, scope)][metric] += value

    def sample(self, day, scopes, metric, value):
        if value is not None:
            for scope in scopes:
                self.samples[(day, scope)][metric].append(value)

    def result(self):
        out = {}
        for key, metrics in self.counts.items():
            out.setdefault(key, {}).update(metrics)
        for key, metrics in self.samples.items():
            for metric, values in metrics.items():
                out.setdefault(key, {})[f"{metric}_p50"] = pct(values, 0.5)
                out.setdefault(key, {})[f"{metric}_p90"] = pct(values, 0.9)
        for metrics in out.values():  # derived shares
            for share, part, whole in (("retryable_share", "retryable", "attempts"),
                                       ("failed_step_share", "failed_steps", "steps"),
                                       ("cache_hit_share", "cached_tokens", "prompt_tokens"),
                                       ("opencode_cache_hit_share", "opencode_cache_read_tokens", "opencode_prompt_tokens")):
                if metrics.get(whole):
                    metrics[share] = metrics.get(part, 0) / metrics[whole]
        return out


def compute(db, tz, days):
    acc = Acc(tz, days)
    for r in rows(db, "select * from batches where status != 'pending'"):
        d = acc.day(r["finalized_at"])
        if not d:
            continue
        scopes = ["all", f"batch:{r['batch_prefix']}"]
        acc.add(d, scopes, "attempts")
        acc.add(d, scopes, "completed" if r["status"] == "completed" else "retryable")
        for reason in (r["retry_reasons"] or "").split(">"):
            if reason:
                acc.add(d, scopes, f"retry_reason:{reason}")
    for r in rows(db, "select * from letta_steps"):
        d = acc.day(r["created_at"])
        if not d:
            continue
        scopes = ["all", f"model:{r['model']}"]
        acc.add(d, scopes, "steps")
        if r["status"] == "FAILED":
            acc.add(d, scopes, "failed_steps")
        for metric, column in (("prompt_tokens", "prompt_tokens"), ("cached_tokens", "cached_input_tokens"),
                               ("reasoning_tokens", "reasoning_tokens"), ("output_tokens", "completion_tokens")):
            acc.add(d, scopes, metric, num(r[column]))
        acc.sample(d, scopes, "llm_ms", r["llm_request_ms"])
    for r in rows(db, "select * from letta_runs"):
        if d := acc.day(r["created_at"]):
            acc.add(d, ["all"], "runs")
    for r in rows(db, "select * from whispers"):
        if d := acc.day(r["created_at"]):
            acc.add(d, ["all", f"whisper:{r['source'] or 'async'}"], "whispers")
            acc.sample(d, ["all", f"whisper:{r['source'] or 'async'}"], "whisper_ready_ms", r["ready_latency_ms"])
    for r in rows(db, "select * from intents"):
        if d := acc.day(r["captured_at"]):
            acc.add(d, ["all"], "intents_captured")
    for r in rows(db, "select * from intent_outcomes"):
        if (d := acc.day(r["recorded_at"])) and r["outcome"] in RESOLVED_INTENT:
            acc.add(d, ["all"], "intents_resolved")
    for r in rows(db, "select * from recall_events where event in ('start', 'failure')"):
        if d := acc.day(r["ts"]):
            acc.add(d, ["all"], "recall_calls" if r["event"] == "start" else "recall_failures")
    for r in rows(db, "select * from opencode_requests"):
        d = acc.day(r["started_at"])
        if not d:
            continue
        prompt = num(r["input_tokens"]) + num(r["cache_read_tokens"])
        acc.add(d, ["all"], "opencode_requests")
        acc.add(d, ["all"], "opencode_cost", num(r["cost"]))
        acc.add(d, ["all"], "opencode_prompt_tokens", prompt)
        acc.add(d, ["all"], "opencode_cache_read_tokens", num(r["cache_read_tokens"]))
        if prompt and num(r["input_tokens"]) / prompt > 0.5:
            acc.add(d, ["all"], "opencode_miss_cost", num(r["cost"]))
        if r["outcome"] == "failed":
            acc.add(d, ["all"], f"opencode_failed:{r['status_code']}")
    out = acc.result()
    # Backlog: the last sample taken on each day
    last_sample = {}
    for r in rows(db, "select distinct sampled_at from backlog_samples"):
        if d := acc.day(r["sampled_at"]):
            last_sample[d] = max(last_sample.get(d, ""), r["sampled_at"])
    for d, sampled_at in last_sample.items():
        gaps = [backlog_gap(r) for r in rows(db, "select * from backlog_samples where sampled_at = ?", (sampled_at,))]
        metrics = out.setdefault((d, "all"), {})
        metrics["backlog_sessions"] = sum(1 for g in gaps if g > 2)
        metrics["backlog_lines"] = sum(gaps)
    return out


def backlog_gap(row):
    if not row.get("transcript_found") or row.get("transcript_msgs") is None or row.get("last_processed_index") is None:
        return 0
    return max(0, row["transcript_msgs"] - (row["last_processed_index"] + 1))


def recent_days(now, tz, n):
    today = now.astimezone(tz).date()
    return [(today - datetime.timedelta(days=i)).isoformat() for i in range(n)]


def days_since(now, tz, since):
    """Every local day from `since` (ISO date) through today, newest first."""
    first = datetime.date.fromisoformat(since[:10])
    today = now.astimezone(tz).date()
    return [(today - datetime.timedelta(days=i)).isoformat() for i in range((today - first).days + 1)]


def write(db, tz, days):
    metrics = compute(db, tz, days)
    db.execute(f"delete from daily_rollup where day in ({', '.join('?' for _ in days)})", days)
    db.executemany("insert into daily_rollup (day, scope, metric, value) values (?, ?, ?, ?)",
                   [(d, s, m, v) for (d, s), ms in metrics.items() for m, v in ms.items() if v is not None])
    return sum(len(ms) for ms in metrics.values())


DIGEST_METRICS = ["attempts", "retryable", "retryable_share", "backlog_sessions", "backlog_lines", "intents_captured",
                  "intents_resolved", "whispers", "whisper_ready_ms_p50", "whisper_ready_ms_p90", "steps", "failed_steps",
                  "cache_hit_share", "recall_calls", "recall_failures", "opencode_requests", "opencode_cost",
                  "opencode_miss_cost"]


def digest(db, day):
    """Yesterday's headline numbers (scope `all`) for the morning card."""
    values = {r["metric"]: r["value"] for r in rows(db, "select metric, value from daily_rollup where day = ? and scope = 'all'", (day,))}
    return {"day": day, "metrics": {m: values[m] for m in DIGEST_METRICS if m in values},
            "retry_reasons": {m.split(":", 1)[1]: v for m, v in values.items() if m.startswith("retry_reason:")}}
