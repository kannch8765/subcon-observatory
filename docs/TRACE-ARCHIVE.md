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
strings and numbers' values don't. Numbers are compared exactly (as decimals), so a value a float can't hold, like
`1e400` or a 20-digit fraction, is never "rebuilt" as a nearby float. A request that can't be split and rebuilt exactly
is kept as text instead: a duplicated object key (parsing would drop one), such a number, `NaN`/`Infinity`, or nesting
too deep to parse.

## A row may leave Postgres only if

1. it is older than the retention window (default 30 days),
2. it has been analysed: the telemetry collector's `letta_traces` already has its metrics (the trace id's 12-hex hash),
3. its record and messages are in the archive, and
4. rebuilding it from the archive gives the same row: every other column the same text, the request the same JSON value.

So the routine order is **analyse → archive → verify → delete**. A row that can't be read exactly (bytes that aren't
UTF-8), that fails to rebuild, or that the collector hasn't seen is never deleted; `--plan` and `--archive` count them.

## Steps

| Step | What | Writes |
|---|---|---|
| 1 | `--plan`: per day, rows, raw size, new distinct messages, estimated archive size (each blob zlib-compressed on its own; no framing, checksums or file system overhead, so not a storage guarantee), rows that rebuild | nothing |
| 2 | `--archive`: write the archive (outside the Letta database, readable by root only), re-read and verify each row; `--restore-day` writes verified rows back out as CSV | archive only |
| 3 | `--delete`: delete verified rows that telemetry has analysed, in small batches, re-checking each against Postgres first, then `VACUUM` | Postgres |
| 4 | daily timer: steps 2 and 3 for the day that just left the window | both |

Steps 1 and 2 are in this version. Each later step is its own change and needs the owner's go. After step 3, plain `VACUUM` lets
new traces reuse the space, so the table stops growing; giving the space back to the disk needs `VACUUM FULL`, which locks
the table for the rewrite and is a separate decision.

## Archive file (step 2)

One SQLite file, `provider_traces.sqlite3`, in a directory that must belong to the user running the tool and be mode
700 (it's created that way; a directory open to group/other is refused). The file and its lock are created 600.

| table | holds |
|---|---|
| `messages` | `hash` (sha256 of the canonical bytes), `z` (those bytes, zlib) |
| `traces` | `id`, `day` (server time zone), `created_at`, `status` (`split` / `whole`), `record_sha256`, `record_z`, `archived_at`, `verified_at` |
| `meta` | `format` = 1 |
| `runs` | when each `--archive` run started and finished, its range, and its counts |

`--archive` works one day at a time, oldest first, each day in its own read-only Postgres transaction (open while that
day is read and archived, so one snapshot per day, never one for the whole range):

1. Read the day's rows and split them; a row that doesn't rebuild in memory is counted and left out.
2. Write new messages and records in one SQLite transaction per batch (about 64 MB of rows), so a crash leaves no half
   row.
3. Re-read the batch through a second, read-only connection: every message and record must match its hash, and the
   rebuilt row must equal the Postgres row. Only then is `verified_at` set.
4. Run again any time: a row already verified with the same record is re-read the same way and counts as `already`; if
   it no longer rebuilds (a damaged archive), it counts as `reread_failed` and loses its `verified_at`. If the Postgres
   row has changed since it was archived, it's a `conflict`: the archived record is kept as it is but loses its
   `verified_at` (it no longer holds what Postgres holds), and it stays unverified while the two differ.

`verified_at` means "rebuilt the Postgres row at the last check", not a standing guarantee: step 3 re-checks every row
against both the archive and Postgres right before deleting it. A Letta schema change (a column added or renamed) changes
every record, so all old rows would show as `conflict`; that needs its own handling before step 3/4 runs unattended.

It holds an exclusive lock for the whole run and stops before a day when the archive's file system has less than
`--min-free-gb` (default 2) free. Exit codes: 0 all good; 3 stopped for space; 4 at least one `conflict` or
`reread_failed` (look before any delete). Rows that can't be archived at all (`undecodable`, `verify_failed`) are only
counted: they never leave Postgres.

`--restore-day` rebuilds the day's verified rows and writes them as psql CSV (every value quoted, NULL as an empty
unquoted field, file created 600, never overwritten) and prints the `\copy provider_traces (...) from ... with (format
csv, header match)` that loads it. Hashes are checked on the way out: after a delete there's no Postgres row left to
compare with. A day without verified rows writes no file. The restored request is the same JSON value, not the same bytes (whitespace and number spelling may
differ, as above); lone surrogates from `\ud83d`-style escapes are written as escapes again, so the file is valid UTF-8.

## Privacy

The archive still holds conversation content, so it lives outside the Letta database with root-only permissions, and no
part of it leaves the server. `--plan` and `--archive` print counts and sizes only; `--restore-day` writes content
to a root-only file on the server and prints only a row count.

## Running

```bash
python3 -E -s -m observatory.trace_archive --config <file> --plan
python3 -E -s -m observatory.trace_archive --config <file> --plan --from 2026-08-08 --to 2026-08-08   # one day
python3 -E -s -m observatory.trace_archive --config <file> --archive --archive-dir /var/lib/subcon-trace-archive
python3 -E -s -m observatory.trace_archive --restore-day 2026-08-08 --archive-dir /var/lib/subcon-trace-archive --out <file>
```

It uses the same `LETTA_PSQL` / `LETTA_PG_ARGS` / `LETTA_PG_LIBDIR` settings as the exporters, in a read-only
transaction. Run it at low priority (`nice -n 19 ionice -c3`); a full plan or first archive parses every old request
once.
