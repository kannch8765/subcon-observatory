# subcon-observatory

Read-only, sanitized observability for a [Subcon](https://github.com/kannch8765/claude-subconscious-wip) / Letta deployment.

It answers ops questions like "are batches failing?", "how long do whispers take?", and "did that deploy help?" without ever reading memory or chat content.

## Safety rules

- **Read-only.** Exporters only read files on the server. Nothing is installed or written there: each exporter streams over `ssh host python3 -` and returns a tar stream.
- **Typed columns only.** Every exported column has a type (`hash`, `int`, `ts`, `enum`, `ident`, `len`, `shape`, `bool`). A value that doesn't fit its type becomes `<redacted>`, and redactions are counted in each `manifest.json`. Free text has no column type, so it can't leave the server.
- **Hashed ids.** Memory, session, batch and attempt ids are exported as 12-hex sha256 prefixes.
- **No data or deployment details in git.** Snapshots live outside the repo. Real hosts and paths go in `config.local.env` (gitignored).
- **Tested.** `tests/test_export.py` runs every exporter against a fake deployment full of secret text and fails if any of it reaches the snapshot.

## Exporters

| exporter | source | tables |
|---|---|---|
| `store` | relationship-memory JSONL store | memories, batches, outcomes, entity_outcomes, evidence, reinforcements, intents, intent_outcomes |
| `whispers` | whisper queue files | whispers (latency, size, delivery) |
| `backlog` | Claude-P session state + transcripts | backlog (cursor vs. transcript length per session) |
| `recall_log` | recall runtime logs | recall_events (whitelisted event templates) |
| `deploy` | Subcon git checkouts | heads, reflog (sha, time, action verb) |
| `letta` | Letta Postgres (read-only transactions) | steps (+ metrics), runs (+ metrics), messages (role, tool, sizes, ids-per-tool-return), blocks, block_history |
| `letta_traces` | Letta `provider_traces` (opt-in, can be GBs) | traces (usage, finish reason, message mix per LLM call), tool_calls (argument shapes + whitelisted enum options) |
| `opencode` | opencode gateway request-log CSVs downloaded from the console (read locally; there is no request-log API) | requests (timing, status/error code, tokens incl. cache read/write, cost; `conversation` joins to letta runs) |

## Usage

```sh
cp config.example.env config.local.env   # fill in host and paths
uv sync
uv run pytest                              # secret-leak tests
uv run python -m observatory.export        # -> $SNAPSHOT_ROOT/<UTC timestamp>/<exporter>/*.csv
uv run python -m observatory.export --only store,backlog
uv run python -m observatory.export --only letta_traces --since 2026-10-01   # bound the big one
uv run python -m observatory.export --only opencode    # after downloading request logs from the opencode console
```

### On the server: telemetry store + health

`observatory.collect` runs the same exporters locally on the server (no SSH), merges their sanitized CSVs into a SQLite
`telemetry.db`, recomputes a long-format `daily_rollup`, and writes `health.json` (ok / warn / alert per check) and
`digest.json`. It writes only `TELEMETRY_DIR`. Design and unit hardening: [docs/TELEMETRY-SLICE-1.md](docs/TELEMETRY-SLICE-1.md).

```
python3 -E -s -m observatory.collect --config <file> --plan                 # what it would do; writes nothing
python3 -E -s -m observatory.collect --config <file> --since 2026-08-01     # first run / backfill (incl. letta_traces)
python3 -E -s -m observatory.collect --config <file>                        # hourly, incremental
```

Each snapshot has a `snapshot.json` (row and redaction counts per table, plus the observatory commit) and one directory per exporter.

## Roadmap

- `analyze/`: standard metrics and charts, plus a diff against the previous snapshot
- Swap the SSH export for a dedicated telemetry store once it exists
