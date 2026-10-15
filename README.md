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

## Usage

```sh
cp config.example.env config.local.env   # fill in host and paths
uv sync
uv run pytest                              # secret-leak tests
uv run python -m observatory.export        # -> $SNAPSHOT_ROOT/<UTC timestamp>/<exporter>/*.csv
uv run python -m observatory.export --only store,backlog
```

Each snapshot has a `snapshot.json` (row and redaction counts per table, plus the observatory commit) and one directory per exporter.

## Roadmap

- Letta Postgres exporters (step metrics, tool-call shapes, core-memory block history)
- `analyze/`: standard metrics and charts, plus a diff against the previous snapshot
- Swap the SSH export for a dedicated telemetry store once it exists
