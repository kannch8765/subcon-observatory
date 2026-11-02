# Trace archive

Letta's `provider_traces` keeps the raw request and response of every LLM call. It is the biggest table in the Letta
database and grows by about 100 MB a day. The owner's rule for removing data: **delete only what is also kept somewhere
else, or what has already been analysed.** This archive is the "kept somewhere else" half, without loss.

## Why it compresses

Every request carries the whole conversation so far. On one deployment, two days of traces held 343 MB of messages but
only 17 MB of distinct messages (20.6×). System prompts and tool definitions repeat as well, but they are small next to
the conversation. The telemetry collector already copies each trace's metrics (sizes, tokens, tool calls), not its content.

## Format

- **Messages:** each distinct message once, keyed by the sha256 of its canonical JSON
  (`separators=(",", ":")`, `ensure_ascii=False`, lone surrogates kept with `surrogatepass`).
- **Trace record:** every other column verbatim, the request without `messages`, the request's key order, and the
  ordered list of message hashes.
- **Fallback:** a request that isn't a JSON object with a `messages` list is kept as its original text.

Requests are compared as **JSON values**, not bytes. Whitespace and number spelling may differ; keys, their order,
strings and numbers' values don't. A request with a duplicated object key is kept as text, since parsing would drop one.

## A row may leave Postgres only if

1. it is older than the retention window (default 30 days),
2. its record and messages are in the archive, and
3. rebuilding it from the archive gives the same row: every other column the same text, the request the same JSON value.

A row that can't be read exactly (bytes that aren't UTF-8) or that fails to rebuild is never deleted, and `--plan` counts it.

## Steps

| Step | What | Writes |
|---|---|---|
| 1 | `--plan`: per day, rows, raw size, new distinct messages, estimated archive size, rows that rebuild | nothing |
| 2 | `--archive`: write the archive (outside the Letta database, readable by root only), re-read and verify each row | archive only |
| 3 | `--delete`: delete verified rows in small batches, re-checking each against Postgres first, then `VACUUM` | Postgres |
| 4 | daily timer: steps 2 and 3 for the day that just left the window | both |

Step 1 is in this version. Each later step is its own change and needs the owner's go. After step 3, plain `VACUUM` lets
new traces reuse the space, so the table stops growing; giving the space back to the disk needs `VACUUM FULL`, which locks
the table for the rewrite and is a separate decision.

## Privacy

The archive still holds conversation content, so it lives outside the Letta database with root-only permissions, and no
part of it leaves the server. `--plan` prints counts and sizes only.

## Running

```bash
python3 -E -s -m observatory.trace_archive --config <file> --plan
python3 -E -s -m observatory.trace_archive --config <file> --plan --from 2026-08-08 --to 2026-08-08   # one day
```

It uses the same `LETTA_PSQL` / `LETTA_PG_ARGS` / `LETTA_PG_LIBDIR` settings as the exporters, in a read-only
transaction. Run it at low priority (`nice -n 19 ionice -c3`); a full plan parses every old request once.
