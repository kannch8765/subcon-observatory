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

Requests are compared as **JSON values**, not bytes (`json_equal`, the one comparison used when splitting, verifying
and deleting). Whitespace and number spelling may differ (`1`, `1.0` and `1e0` are the same number); keys, their order,
strings, numbers' values and types don't: `true` is never `1`, `null` is never `0`, `{}` is never `[]`. Python's own
`==` can't be used for this (it says `True == 1` and ignores key order). Numbers are compared exactly (as decimals), so a value a float can't hold, like
`1e400` or a 20-digit fraction, is never "rebuilt" as a nearby float. A request that can't be split and rebuilt exactly
is kept as text instead: a duplicated object key (parsing would drop one), such a number, `NaN`/`Infinity`, or nesting
too deep to parse.

## A row may leave Postgres only if

1. it is older than the retention window (default 30 days),
2. it has been analysed: the telemetry collector's `letta_traces` already has its metrics (the trace id's 12-hex hash),
3. its record and messages are in the archive, and
4. rebuilding it from the archive gives the same row: every other column the same text, the request the same JSON value.

So the routine order is **analyse → archive → verify → delete**. A row that can't be read exactly (bytes that aren't
UTF-8), that fails to rebuild, or that the collector hasn't seen is never deleted; `--plan`, `--archive` and `--delete`
count them.

## Steps

| Step | What | Writes |
|---|---|---|
| 1 | `--plan`: per day, rows, raw size, new distinct messages, estimated archive size (each blob zlib-compressed on its own; no framing, checksums or file system overhead, so not a storage guarantee), rows that rebuild | nothing |
| 2 | `--archive`: write the archive (outside the Letta database, readable by root only), re-read and verify each row; `--restore-day` writes verified rows back out as CSV | archive only |
| 3 | `--delete`: delete verified rows that telemetry has analysed, in small batches, re-checking each against Postgres first, then `VACUUM` | Postgres |
| 4 | `observatory.trace_retention`, daily timer: steps 2 and 3 for the whole day that just left the window, then an encrypted snapshot of the archive for its off-site copy | both |

After step 3, plain `VACUUM` lets new traces reuse the space, so the table stops growing; giving the space back to the
disk needs `VACUUM FULL`, which locks the table for the rewrite and is a separate decision.

## Archive file (step 2)

One SQLite file, `provider_traces.sqlite3`, in a directory that must belong to the user running the tool and be mode
700 (it's created that way; a directory open to group/other is refused). The file and its lock are created 600.

| table | holds |
|---|---|
| `messages` | `hash` (sha256 of the canonical bytes), `z` (those bytes, zlib) |
| `traces` | `id`, `day` (server time zone), `created_at`, `status` (`split` / `whole`), `record_sha256`, `record_z`, `archived_at`, `verified_at` |
| `meta` | `format` = 1 |
| `runs` | when each `--archive` / `--delete` run started and finished, its range, and its counts |
| `deletions` | one row per row and `--delete` run that asked for it: `id`, `day`, `run`, `requested_at`, `outcome`, `resolved_at`. `outcome` is `deleted` / `stayed` (Postgres's answer), `gone` / `present` (what a later run found for a request that never got an answer), or empty while unanswered (added in step 3; older archives get the table on their next run) |

`--archive` works one day at a time, oldest first, each day in its own read-only Postgres transaction (open while that
day is read and archived, so one snapshot per day, never one for the whole range):

1. Read the day's rows and split them; a row that doesn't rebuild in memory is counted and left out.
2. Write new messages and records in one SQLite transaction per batch (about 64 MB of rows), so a crash leaves no half
   row.
3. Re-read the batch through a second, read-only connection: every message and record must match its hash, the row's
   unhashed lookup columns (`id`, `day`, `created_at`, `status`, which restore finds rows by) must agree with the hashed
   record, and the rebuilt row must equal the Postgres row. Only then is `verified_at` set.
4. Run again any time: a row already verified with the same record is re-read the same way and counts as `already`; if
   it no longer rebuilds (a damaged archive), it counts as `reread_failed` and loses its `verified_at`. If the Postgres
   row has changed since it was archived, it's a `conflict`: the archived record is kept as it is but loses its
   `verified_at` (it no longer holds what Postgres holds), and it stays unverified while the two differ. If Postgres now
   holds a verified row in a form that can't be archived (`undecodable` / `verify_failed`), the old copy is stale in the
   same way: it loses its `verified_at` and counts as `reread_failed`. A failed row is not repaired: it stays unverified
   until someone looks.

`verified_at` means "rebuilt the Postgres row at the last check", not a standing guarantee: step 3 re-checks every row
against both the archive and Postgres right before deleting it. A Letta schema change (a column added or renamed) changes
every record, so all old rows would show as `conflict`; that needs its own handling before step 3/4 runs unattended.

It holds an exclusive lock for the whole run and stops before a day when the archive's file system has less than
`--min-free-gb` (default 2) free. Exit codes: 0 all good; 3 stopped for space; 4 at least one `conflict` or
`reread_failed` (look before any delete). Rows that were never archived and can't be (`undecodable`, `verify_failed`)
are only counted: they never leave Postgres.

`--restore-day` rebuilds the day's verified rows and writes them as psql CSV (every value quoted, NULL as an empty
unquoted field, file created 600, never overwritten) and prints the `\copy provider_traces (...) from ... with (format
csv, header match)` that loads it. Hashes are checked on the way out: after a delete there's no Postgres row left to
compare with, and so are the lookup columns. A day without verified rows writes no file. The restored request is the
same JSON value, not the same bytes (whitespace and number spelling may differ, as above); lone surrogates from `\ud83d`-style escapes are written as escapes again, so the file is valid UTF-8.

## Delete (step 3)

`--delete` is the only mode that writes to Postgres. It refuses `--older-than-days` under 30, needs the archive (run
`--archive` first) and `TELEMETRY_DIR` in the config, and takes the archive's lock, so it never runs next to an
`--archive` run. Day by day, oldest first:

1. Read the day's rows in one read-only transaction, each with Postgres's own `md5(row::text)`.
2. Check each row again, right now, against all four conditions above: it is older than the cutoff (the query), it is
   verified in the archive, its hashed id is in telemetry's `letta_traces` (`observatory.remote._sanitize.h`, the
   collector's own hash), and the archive's bytes still rebuild it (`verify_archived`, the same check `--archive`
   makes, with every hash checked).
3. After that transaction has ended, delete the rows that passed, `--batch-rows` (default 50) per statement, each
   statement its own short transaction:
   `delete from provider_traces t using (values (id, md5), ...) as v(id, md5) where t.id = v.id and md5(t::text) =
   v.md5 and t.created_at < <cutoff> returning t.id`. A row that changed after its check no longer has that md5 and
   stays (`changed`). Ids go into the statement as literals, so only ids of a plain shape (letters, digits, `_.:-`)
   are ever sent; any other row stays (`unsafe_id`).
4. Each batch is written to the archive's `deletions` table as requested (no `outcome`) before its DELETE, and gets
   its outcome after Postgres answers: `deleted`, or `stayed` (changed since its check). A stop in between leaves
   unanswered requests, never unrecorded deletes.

Every run starts by resolving the requests earlier runs left unanswered: it asks Postgres which of those ids are still
there (ids only) and records `gone` or `present` on the earlier attempt's own rows, so that attempt stays in the
journal. A `present` row is then checked like any other and may be deleted in this run (a new journal row). The report
says how many there were and what Postgres said; a dry run reports them and records nothing. If Postgres can't be
asked, the run stops before deleting anything and the requests stay unanswered.

At the end, if anything was deleted, plain `VACUUM (ANALYZE) provider_traces`: it doesn't lock out reads or writes, and
new traces reuse the space. The run's counts are recorded and printed before the VACUUM, so a VACUUM failure can't
hide what was deleted. `--dry-run` makes every check and deletes nothing (it still records the run); `--max-rows N` deletes at most N rows
(the oldest first) and counts the rest as `deferred`; `--no-vacuum` skips the VACUUM. Per day it prints rows, deleted
(or would_delete), not_archived, not_analysed, mismatch (the archive no longer rebuilds the row), changed, unsafe_id and
deferred. Exit codes: 0 all good; 4 at least one `mismatch`, `changed` or `unsafe_id` row (they stay; `--archive`
shows what differs), or the VACUUM failed or ended with a server warning (the deletes stand; without the right
privilege PG skips the table with a WARNING and still succeeds; the server's text isn't passed on). The VACUUM session
runs `set client_min_messages = warning` first, so neither a role default nor a connection string's own `options` set
to `error` can hide the warning, and any stderr at all counts (whatever language the server writes it in, and psql's
own messages too). A row that fails its re-check keeps its `verified_at` here: the next `--archive` run marks it.

**After a delete the archive holds the only copy of those rows.** Keep a second copy of the archive file somewhere
else before the first real delete (where is the owner's decision: it holds conversation content), and after each one.

Deleted rows stay restorable: `--restore-day` reads only the archive. Letta only writes `provider_traces` while it runs;
it reads them back only in its debug API (a step's trace), which answers "not found" for deleted rows.

## Daily retention (step 4)

`observatory.trace_retention` is what the timer runs each night:

1. **One cutoff, whole days.** The last day is the day `older-than-days + 1` days ago in `TELEMETRY_TZ` (the server's
   zone when unset), and both steps get it as `--to` and the same cutoff. So they see the same rows, no day is split
   between two nights, and rows stay 30 to 31 days. Rows newer than the cutoff stay whatever zone Postgres uses for
   days: the cutoff is in every query too.
2. **`--archive`** through that day. Normally that's one day (older ones are gone); after a break it catches up.
3. **`--delete`** through that day, only if `--archive` exited 0. It still re-checks every row itself (see step 3).
4. **Snapshot**, always, whatever happened before (rows may already be gone): a consistent copy of the archive
   (SQLite's backup API, under the archive's lock, so no run writes meanwhile), encrypted with
   [age](https://age-encryption.org) to the owner's public key into `--offsite-dir`, one file per run
   (`provider_traces.<UTC time>-<sha256 prefix>.sqlite3.age`). The server never holds the private key.
   - **The manifest is the pointer.** `provider_traces.sqlite3.age.json` names the current file, with its size and
     sha256, the plaintext's size and sha256, and the verified rows, days and messages it holds. It's written last,
     in one rename. Until then the last file and manifest are untouched, so any failure (an exception or a kill) leaves
     the last pair as it was; whatever the failed run wrote is removed (by the next run after a kill).
   - The file before the current one stays until the next run, for a pull that started before the switch; older ones
     are removed at the start of a run, to make room.
   - **A new snapshot is published only if** the copy passes SQLite's `quick_check`; every verified trace's record, and
     then every message those records need (each distinct message once), still decompresses and matches its hash, as
     `--restore-day` reads them (the archive pass re-reads only rows still in Postgres, so rows already deleted are
     checked nowhere else); it holds at least as many verified rows as the last one (the archive only ever adds rows);
     and age's output starts with age's header. A missing archive is never snapshotted.
   - The plaintext copy exists only while it's checked and encrypted; a hard kill (timeout, OOM) can leave it in the
     root-only directory until the next run removes it.
   - The directory must belong to the running user and be mode 700, like the archive's.
5. **Status**: `--status` writes the run's exits, both steps' counts (their `runs` rows), the manifest or the
   snapshot's error class, and an unexpected error's class, as JSON (mode 640, for the health check), on every path.
   A delete that stops after its runs row keeps its counts there with `exit` null.

It prints counts, sizes and hashes only. An unexpected error prints its class name; the traceback goes to
`retention-error.log` in the archive directory (root-only; don't print it without looking: a traceback may quote
anything). `--dry-run` archives for real and runs `--delete --dry-run`.

Exit codes: 0 all good; 3 `--archive` stopped for disk space (nothing deleted); 4 `--archive` or `--delete` reported
rows that need a look, or the VACUUM failed or warned (their own output says which); 5 the snapshot failed (the deletes
stand, the last snapshot is older); 1 an unexpected error. Any of them fails the unit. A `conflict` or `reread_failed`
row (exit 4) holds every night's delete until someone looks, on purpose: the health check is to warn about it.

**Off-site copy.** The owner pulls the manifest and the file it names to their own machine about weekly, checks the
file's size and sha256 against the manifest, and keeps the previous copy until the new one checks out; a decrypt check
now and then compares the plaintext sha256. Between pulls, the newest rows' only copy is the archive on the server (their metrics are in telemetry).

**Unit and timer** (`/etc/systemd/system/`), the collector's sandbox plus what the step 3 runs showed. systemd takes
only whole-line comments: a `# ...` after a value becomes part of it, and the setting is ignored with a warning
(`systemd-analyze verify` still exits 0), so check its output, not only its exit code:

```ini
# subcon-trace-retention.service
[Unit]
Description=Subcon trace retention: archive, then delete, provider_traces older than 30 days; encrypted snapshot
After=letta-selfhost-postgres.service

[Service]
Type=oneshot
WorkingDirectory=<pinned observatory checkout>
ExecStart=/usr/bin/python3 -E -s -m observatory.trace_retention --config <config> --archive-dir <archive dir> \
    --offsite-dir <offsite dir> --age-recipient <owner's age1... key> --status <TELEMETRY_DIR>/trace_retention.json
UMask=0077
Nice=19
IOSchedulingClass=idle
ProtectSystem=strict
ProtectHome=read-only
# SQLite's spill files; psql reaches Postgres over TCP
PrivateTmp=yes
# telemetry.db is in WAL mode: even a read-only open creates its -shm/-wal next to it, so the directory is writable
# and the database file itself is not
ReadWritePaths=<archive dir> <offsite dir> <TELEMETRY_DIR>
ReadOnlyPaths=<TELEMETRY_DIR>/telemetry.db
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
NoNewPrivileges=yes
RestrictSUIDSGID=yes
LockPersonality=yes
CapabilityBoundingSet=CAP_DAC_READ_SEARCH
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
IPAddressDeny=any
# Letta Postgres only
IPAddressAllow=localhost
# a first full --archive peaked at 993 MB; a normal night needs far less
MemoryMax=1500M
# a normal night takes minutes; this bounds a catch-up after a break
TimeoutStartSec=2h

# subcon-trace-retention.timer
[Unit]
Description=Nightly Subcon trace retention

[Timer]
OnCalendar=*-*-* 04:40:00 Asia/Tokyo
AccuracySec=1min
Persistent=true

[Install]
WantedBy=timers.target
```

- 04:40 is inside the alert quiet hours and away from the collector's :17.
- The archive and off-site directories must exist (mode 700) before the unit starts: systemd refuses a missing
  `ReadWritePaths`. All three directories belong to root: the bounding set has no `CAP_DAC_OVERRIDE`, so root writes
  only where it's the owner.
- The checkout is pinned to a reviewed commit, as the collector's.

## Privacy

The archive still holds conversation content, so it lives outside the Letta database with root-only permissions. The
only part that leaves the server is the snapshot, encrypted to the owner's key on the server and pulled by the owner.
`--plan`, `--archive`, `--delete` and `trace_retention` print counts, sizes and hashes only; `--restore-day` writes
content to a root-only file on the server and prints only a row count.

## Running

```bash
python3 -E -s -m observatory.trace_archive --config <file> --plan
python3 -E -s -m observatory.trace_archive --config <file> --plan --from 2026-08-08 --to 2026-08-08   # one day
python3 -E -s -m observatory.trace_archive --config <file> --archive --archive-dir /var/lib/subcon-trace-archive
python3 -E -s -m observatory.trace_archive --restore-day 2026-08-08 --archive-dir /var/lib/subcon-trace-archive --out <file>
python3 -E -s -m observatory.trace_archive --config <file> --delete --archive-dir /var/lib/subcon-trace-archive --dry-run
python3 -E -s -m observatory.trace_archive --config <file> --delete --archive-dir /var/lib/subcon-trace-archive --max-rows 100
python3 -E -s -m observatory.trace_retention --config <file> --archive-dir /var/lib/subcon-trace-archive \
    --offsite-dir /var/lib/subcon-trace-offsite --age-recipient age1... --status <TELEMETRY_DIR>/trace_retention.json --dry-run
```

It uses the same `LETTA_PSQL` / `LETTA_PG_ARGS` / `LETTA_PG_LIBDIR` settings as the exporters, in a read-only
transaction; only `--delete`'s DELETE and VACUUM statements run in a session that may write. Run it at low priority
(`nice -n 19 ionice -c3`); a full plan, a first archive or a full delete parses every old request once.
