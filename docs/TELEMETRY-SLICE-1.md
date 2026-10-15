# Telemetry slice 1: on-server collector, daily rollup, healthcheck

Status: **collector implemented, not installed** (2026-10-15). Owner decisions below are folded in.

## Goal

Replace "Claude SSHes in and exports a snapshot" with a job on the server that
keeps sanitized metrics **permanently**, summarizes them per day, and says
green / yellow / red. Raw Letta `provider_traces` can only be trimmed once this
has been running and verified.

Out of scope for slice 1: in-process emitters inside Subcon (slice 2), Grafana /
live pages (later), any raw-trace deletion. Alert delivery is a separate PR in the
review bot (below).

## Shape

```
systemd timer (hourly at :17)
  └─ observatory.collect                      runs ON the server, stdlib only
       1. run the existing exporters with --local      (same allowlist sanitizer)
       2. ingest their CSVs into telemetry.db          (upsert by hashed id)
       3. recompute daily_rollup for the last 3 days   (late rows are absorbed)
       4. write health.json + exit code
```

Nothing new crosses the privacy line: `collect` only reads the CSVs the
exporters already produce, and those are covered by `tests/test_export.py`.
The observatory keeps working from a laptop over SSH exactly as now.

## Storage: `telemetry.db` (SQLite, WAL)

Separate from the memory store and from Letta's database. Location:
`TELEMETRY_DIR` (the only path the job may write).

| table | key | source | notes |
|---|---|---|---|
| `letta_steps` | step | letta/steps | incremental by `created_at` (`LETTA_SINCE` = last ingested − 1 day) |
| `letta_runs` | run | letta/runs | joins opencode `conversation` |
| `letta_messages` | message | letta/messages | tool status / error templates only |
| `letta_traces` | trace | letta_traces/traces | usage + finish reason; hourly incremental keeps it cheap |
| `batches` | (bid, attempt, status) | store/batches | adds `retry_reasons` once the exporter reads it |
| `outcomes`, `intents`, `intent_outcomes` | natural keys | store/* | |
| `backlog_samples` | (sampled_at, session) | backlog | **time series**: one row per session per run |
| `whispers` | whisper | whispers | `ready_latency_ms` |
| `recall_events` | (ts, event, run) | recall_log | only useful once the recall log is persistent (below) |
| `opencode_requests` | rid | opencode | filled when CSVs are dropped into `OPENCODE_CSV_DIR` |
| `daily_rollup` | (day, scope, metric) | computed | long format: adding a metric needs no migration |
| `collect_runs` | started_at | the job itself | duration, rows per table, redactions, errors |

Size: the full Letta history since August is ~12 MB as CSV, so about
100 MB/year. Telemetry rows are kept indefinitely.

## Daily rollup metrics

Per day (server local time) × scope (`all`, `model:<id>`, `lane:async|sync`):

- **Batches:** attempts, completed, retryable, share retryable, counts per `retry_reasons` value.
- **Backlog:** sessions with gap > 2, lines past cursor (end-of-day sample).
- **Intents:** captured, resolved, unresolved > 1 h, median capture→outcome lag.
- **Whispers:** count, `ready_latency_ms` p50 / p90.
- **Letta:** runs, steps, failed steps by `error_type`, LLM ms p50 / p90,
  prompt / cached / reasoning / output tokens, cache-hit share, tool calls by name.
- **Cost (when opencode CSVs exist):** requests, cost, cost on cache misses, `400` count.
- **Recall:** calls, timeouts, latency p50 / p90 (after the log move).

## Healthcheck (`health.json`)

Each check → `ok` / `warn` / `alert` + value + threshold. Overall = worst.

Implemented in `observatory/health.py`; tested in `tests/test_collect.py`.

| check | warn | alert |
|---|---|---|
| retryable share of batch attempts, last 24 h | > 10 % | > 25 % |
| active session (transcript touched < 24 h) with cursor gap > 50 lines for > 1 h | — | any |
| remember intent without outcome | > 1 h | > 24 h |
| whisper `ready_latency_ms` p90, last 24 h | > 120 s | > 300 s |
| Letta failed-step share, last 24 h | > 5 % | > 20 % |
| disk used on the Letta / telemetry volume | > 80 % | > 90 % |
| Letta block over its char limit (agents active in 24 h only) | any | — |

| collector: any exporter failed in this run | — | any |
| collector: age of the last fully successful run (`last_success_at`) | > 2 h | > 26 h, or never |

A run with failed exporters still writes a fresh `health.json`, but it can never be green, and repeated partial
failures age out through `last_success_at`. The collector can't report its own death, so the bot also alerts once if
`health.json` itself is older than 2 h.

Thresholds live in config so they can be tuned without a release.

## Running it safely

Recommended: a root oneshot service that can **read everything but write one
directory**, instead of granting a new user ACLs on Kohaku's home:

```ini
[Service]
Type=oneshot
ExecStart=/usr/bin/python3 -E -s -m observatory.collect --config <config>
WorkingDirectory=<pinned observatory checkout>
Nice=19
IOSchedulingClass=idle
ProtectSystem=strict
ReadWritePaths=<TELEMETRY_DIR>
ProtectHome=read-only
PrivateTmp=no                      # must see the runtime's PrivateTmp logs until they move
NoNewPrivileges=yes
CapabilityBoundingSet=CAP_DAC_READ_SEARCH
IPAddressDeny=any
IPAddressAllow=localhost           # Letta Postgres only
MemoryMax=512M
RuntimeMaxSec=20min
```

- `-E -s` instead of `-I`: `-I` would also drop the pinned checkout from `sys.path`, so `-m` could not import the package.
- A dedicated non-root user was checked and needs permission changes (the Postgres client sits in a group-restricted
  directory, and Letta needs a read-only role), so the sandboxed root unit is the smaller change.
- Letta access stays read-only per transaction (`default_transaction_read_only=on`), as in the exporters.
- The checkout is pinned to a reviewed commit; upgrades go through the same `--plan` / apply deploy pattern.
- Alternative if you'd rather not run as root: a `subcon-telemetry` user + read ACLs + a read-only Postgres role. More moving parts.

## Alert delivery: the Subcon review bot

Owner decision (2026-10-15): alerts reach a human through the existing Subcon review Telegram bot, not Kohaku's chat.
The bot reads `health.json` / `digest.json` (read-only) in the loop it already runs; the collector still writes only
`TELEMETRY_DIR`.

| what | when |
|---|---|
| state change (ok → warn → alert, and recovered) | on change only; never repeats the same state |
| alert | immediately, except quiet hours (~00:30–08:00): held until 08:00 |
| warn | no ping; shown in the morning digest |
| daily digest | ~09:00, yesterday's `digest.json` + current health |
| `/health` | on demand |
| collector stale (`health.json` > 2 h old) | one alert |

The bot is first moved out of `claude-telegram` into its own repo (pure move), then this lands there with rich-text cards.

## Recall log: make it persistent (Subcon-side change)

The recall runtime and the live worker log to `os.tmpdir()`, which is the
runtime's PrivateTmp, so every restart wipes them.

- Change: honour `SUBCON_LOG_DIR` (default unchanged), and set it in the
  runtime's environment to a directory already in its `ReadWritePaths`.
- Caveat: a service environment change only takes effect on the next runtime
  restart. Ship the code now and set the variable at the next planned restart;
  do not restart Kohaku just for this.
- Also fixes tests writing to the real `/tmp` log (resolve the path at call
  time, not at import).

## Rollout

1. ✅ `observatory.collect` + tests (fake deployment, like the exporters). Also replayed against the existing local
   snapshots: per-day cost ($1.47 / $2.15 / $2.00), cache-hit share (88–91 %), backlog (21,202 lines) and the
   10-11 / 10-12 retry spike (73 % / 87 % retryable) match the hand analysis.
2. **Owner approves the server plan** (unit, paths, pinned commit). `--plan` prints what it would read/write.
3. One-time backfill: `--since 2026-08-01`, including `letta_traces` (expected minutes, at idle priority). A run with
   `--since` recomputes the rollup for every day from that date, not just the latest three.
4. Enable the hourly timer. Compare the first rollup days against today's hand analysis
   (e.g. 10-12..10-15 cost $1.90/day, cache-hit share 89.6 %).
5. After ~1 week green: trace retention gets its own plan (keep raw N days, `VACUUM`), with owner approval.

## Decisions (owner, 2026-10-15)

- Sandboxed root oneshot + hourly timer.
- Alerts via the Subcon review bot with the rules above.
- Thresholds above as a start.
