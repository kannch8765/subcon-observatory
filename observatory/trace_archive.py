"""Lossless, deduplicated archive of Letta `provider_traces`, so old raw rows can leave Postgres without losing anything.

    python3 -E -s -m observatory.trace_archive --config <file> --plan                          # rows older than 30 days
    python3 -E -s -m observatory.trace_archive --config <file> --plan --from 2026-08-08 --to 2026-08-09
    python3 -E -s -m observatory.trace_archive --config <file> --archive --archive-dir <dir>   # write + verify
    python3 -E -s -m observatory.trace_archive --restore-day 2026-08-08 --archive-dir <dir> --out <file.csv>
    python3 -E -s -m observatory.trace_archive --config <file> --delete --archive-dir <dir> --dry-run   # what would go

Every LLM request repeats the whole conversation so far, so the same messages are stored again on every step.
The archive keeps each distinct message once (keyed by sha256) and, per trace, a record of every other column plus
the list of message hashes. A trace counts as archivable only if it can be rebuilt from that record to the same
JSON value it had (see docs/TRACE-ARCHIVE.md).

--plan reads Postgres read-only, writes nothing anywhere, and prints per day how many rows would be archived, how
much they take raw, how much the archive would add, and whether every row rebuilds. --archive writes the archive (a
SQLite file in a directory only its owner can enter), re-reads every row from disk and marks it verified only if it
rebuilds the Postgres row. --restore-day writes verified rows back out as CSV that psql's \\copy loads. --delete is
the only mode that writes to Postgres: it deletes old rows that are verified in the archive and analysed by the
telemetry collector, each re-checked against both right before its delete. JSON is parsed here, not in Postgres:
broken surrogates in stored payloads make PG's json functions fail.
"""
import argparse, csv, datetime, decimal, fcntl, hashlib, io, json, os, re, shlex, shutil, sqlite3, stat, subprocess, sys, zlib
from dataclasses import dataclass, field

from observatory.export import load_config
from observatory.remote._sanitize import h as trace_hash  # the id hash the collector stores in letta_traces.trace

csv.field_size_limit(1 << 31)

DEFAULT_OLDER_THAN_DAYS = 30
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# psql reads bytes that aren't UTF-8 as surrogate escapes; a field containing one can't be archived exactly.
_ESCAPED_BYTE = re.compile("[\udc80-\udcff]")
# Python's csv reader can't tell NULL from "" (or from any sentinel text), so NULL-ness travels separately: the
# query adds one 0/1 digit per column in this extra column, computed by Postgres itself.
NULL_MASK = "__observatory_null_mask"
# --delete reads each row with Postgres's own md5 of its text form and deletes it only while it still has that md5.
ROW_MD5 = "__observatory_row_md5"
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


class DuplicateKey(ValueError):
    pass


def _no_duplicate_keys(pairs):
    keys = [k for k, _ in pairs]
    if len(keys) != len(set(keys)):
        raise DuplicateKey("duplicate object key")  # json.loads would silently keep only the last one
    return dict(pairs)


def parse_json(text):
    return json.loads(text, object_pairs_hook=_no_duplicate_keys)


def _no_constant(name):
    raise ValueError(f"non-standard JSON constant {name}")


def parse_exact(text):
    """The JSON value with numbers exact: floats as Decimal (1e400 or a 20-digit fraction keep their value), and no
    NaN/Infinity, which aren't JSON. Used to check that a split request rebuilds to the same value."""
    return json.loads(text, object_pairs_hook=_no_duplicate_keys, parse_float=decimal.Decimal, parse_constant=_no_constant)


def canonical(value):
    """Bytes a message is stored and hashed as. surrogatepass keeps lone surrogates from \\ud83d-style escapes."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8", "surrogatepass")


def message_hash(blob):
    return hashlib.sha256(blob).hexdigest()


@dataclass
class Split:
    status: str                      # "split" (messages deduplicated), "whole" (request kept verbatim), "undecodable"
    record: dict = field(default_factory=dict)
    blobs: list = field(default_factory=list)   # [(hash, canonical bytes)] in message order


def split_trace(row):
    """One provider_traces row (column -> text or None) -> its archive record and message blobs."""
    if any(isinstance(v, str) and _ESCAPED_BYTE.search(v) for v in row.values()):
        return Split("undecodable")
    columns = {k: v for k, v in row.items() if k != "request_json"}
    raw = row.get("request_json")
    whole = Split("whole", {"columns": columns, "request_text": raw})
    # Anything that can't be split and rebuilt exactly is kept verbatim: not JSON, a duplicated key, a number a float
    # can't hold (1e400 -> inf), or nesting too deep to parse (RecursionError must not stop the scan of other rows).
    try:
        request = parse_json(raw) if raw is not None else None
        messages = request.get("messages") if isinstance(request, dict) else None
        if not isinstance(messages, list):
            return whole
        blobs = [(message_hash(b), b) for b in map(canonical, messages)]
        rest = {k: v for k, v in request.items() if k != "messages"}
        split = Split("split", {"columns": columns, "request_rest": rest, "request_keys": list(request),
                                "message_hashes": [h for h, _ in blobs]}, blobs)
        rebuilt = rebuild(split.record, dict(blobs).__getitem__)["request_json"]
        if parse_exact(canonical(rebuilt).decode("utf-8", "surrogatepass")) != parse_exact(raw):
            return whole
        return split
    except (ValueError, RecursionError):
        return whole


def rebuild(record, lookup):
    """Archive record + hash -> canonical bytes lookup -> the original row (request_json as text or as a JSON value)."""
    row = dict(record["columns"])
    if "request_text" in record:
        row["request_json"] = record["request_text"]
        return row
    messages = [json.loads(lookup(h).decode("utf-8", "surrogatepass")) for h in record["message_hashes"]]
    request = {}
    for key in record["request_keys"]:  # original key order, messages back in its place
        request[key] = messages if key == "messages" else record["request_rest"][key]
    row["request_json"] = request
    return row


def verify(row, split, lookup=None):
    """True when the record rebuilds the row: other columns identical text, request the same JSON value with exact
    numbers. `lookup` gives message bytes by hash (default: the split's own blobs; the archive passes its reader).
    Never raises: a row that can't be checked is simply not verified."""
    if split.status == "undecodable":
        return False
    try:
        rebuilt = rebuild(split.record, lookup or dict(split.blobs).__getitem__)
        if {k: v for k, v in rebuilt.items() if k != "request_json"} != {k: v for k, v in row.items() if k != "request_json"}:
            return False
        if split.status == "whole":
            return rebuilt["request_json"] == row.get("request_json")
        return parse_exact(canonical(rebuilt["request_json"]).decode("utf-8", "surrogatepass")) == parse_exact(row["request_json"])
    except (ValueError, RecursionError, KeyError):
        return False


def decode_nulls(row, columns):
    """Apply the per-column NULL mask: a 1 makes the value None, a 0 keeps the text exactly (even "")."""
    mask = row.pop(NULL_MASK)
    if len(mask) != len(columns) or set(mask) - {"0", "1"} or list(row) != columns:
        raise RuntimeError("NULL mask doesn't match the columns")
    return {c: (None if bit == "1" else row[c]) for c, bit in zip(columns, mask)}


def _psql(config):
    env = {**os.environ, "PGOPTIONS": "-c default_transaction_read_only=on -c jit=off"}
    if config.get("LETTA_PG_LIBDIR"):
        env["LD_LIBRARY_PATH"] = config["LETTA_PG_LIBDIR"]
    return [config["LETTA_PSQL"], *shlex.split(config["LETTA_PG_ARGS"]), "-X", "-q"], env


def trace_columns(config):
    """provider_traces column names in table order (validated identifiers, so they can be quoted into the query)."""
    cmd, env = _psql(config)
    out = subprocess.run([*cmd, "-A", "-t", "-c", "select column_name from information_schema.columns "
                          "where table_name = 'provider_traces' order by ordinal_position"],
                         env=env, capture_output=True, text=True, check=True).stdout
    columns = [line for line in out.splitlines() if line]
    if not columns or any(not _IDENT.match(c) for c in columns) or NULL_MASK in columns or ROW_MD5 in columns:
        raise RuntimeError(f"unexpected provider_traces columns: {columns!r}")
    return columns


def _stream(config, where, with_md5=False):
    """(row, md5 or None) per provider_traces row: row as a dict, NULL as None (from the mask, never from the text);
    md5 is Postgres's md5 of the row's text form, read in the same statement."""
    columns = trace_columns(config)
    mask = "concat(" + ", ".join(f'("{c}" is null)::int' for c in columns) + ")"
    select = ", ".join(f'"{c}"' for c in columns)
    extra = f", md5(provider_traces::text) as {ROW_MD5}" if with_md5 else ""
    query = f'select {select}, {mask} as {NULL_MASK}{extra} from provider_traces {where} order by created_at, id'
    cmd, env = _psql(config)
    proc = subprocess.Popen([*cmd, "-c", f"\\copy ({query}) to stdout with (format csv, header, force_quote *)"],
                            stdout=subprocess.PIPE, env=env)
    text = io.TextIOWrapper(proc.stdout, encoding="utf-8", errors="surrogateescape", newline="")
    for row in csv.DictReader(text):
        digest = row.pop(ROW_MD5) if with_md5 else None
        yield decode_nulls(row, columns), digest
    if proc.wait() != 0:
        raise RuntimeError(f"psql exited with {proc.returncode}")


def stream_rows(config, where):
    """provider_traces rows as dicts, NULL as None (from the mask, never from the text)."""
    yield from (row for row, _ in _stream(config, where))


def cutoff_time(older_than_days, now=None):
    """The retention cutoff as ISO text: rows created before it are 'older than N days'."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    return (now - datetime.timedelta(days=older_than_days)).isoformat()


def where_clause(older_than_days, date_from=None, date_to=None, now=None):
    """created_at bounds. Dates are validated, never interpolated raw; --to is inclusive."""
    parts = [f"created_at < '{cutoff_time(older_than_days, now)}'"]
    for value, op, shift in ((date_from, ">=", 0), (date_to, "<", 1)):
        if value:
            if not _DATE.match(value):
                raise ValueError("--from / --to must be YYYY-MM-DD")
            day = datetime.date.fromisoformat(value) + datetime.timedelta(days=shift)
            parts.append(f"created_at {op} '{day.isoformat()}'")
    return "where " + " and ".join(parts)


def _size(value):
    return len(value.encode("utf-8", "surrogateescape")) if isinstance(value, str) else 0


def plan(rows, seen=None):
    """Per-day dry run: (days {day: stats}, totals). `seen` = hashes already archived (none yet in this version).
    archive_bytes is an estimate: the sum of independently zlib-compressed blobs, not a measured archive size."""
    seen = set(seen or ())
    days = {}
    for row in rows:
        day = (row.get("created_at") or "")[:10] or "unknown"
        stats = days.setdefault(day, {"rows": 0, "raw_bytes": 0, "new_message_bytes": 0, "archive_bytes": 0,
                                      "split": 0, "whole": 0, "undecodable": 0, "verify_failed": 0})
        stats["rows"] += 1
        stats["raw_bytes"] += sum(_size(v) for v in row.values())
        split = split_trace(row)
        stats[split.status] += 1
        if split.status == "undecodable":
            continue  # counted once, as undecodable; verify_failed is only for rows that could be read
        if not verify(row, split):
            stats["verify_failed"] += 1
            continue
        for h, blob in split.blobs:
            if h not in seen:
                seen.add(h)
                stats["new_message_bytes"] += len(blob)
                stats["archive_bytes"] += len(zlib.compress(blob, 6))
        stats["archive_bytes"] += len(zlib.compress(canonical(split.record), 6))
    totals = {key: sum(day[key] for day in days.values()) for key in next(iter(days.values()), {})}
    return days, totals


def _mb(n):
    return f"{n / 1e6:9.1f}"


def report(days, totals, out=sys.stdout):
    out.write("archive_MB is an estimate: each message and record zlib-compressed on its own, without the real archive's\n"
              "framing, checksums or file system overhead. Not a storage guarantee.\n")
    out.write("day          rows    raw_MB  new_msg_MB  archive_MB  split  whole  undecodable  verify_failed\n")
    for day in sorted(days):
        d = days[day]
        out.write(f"{day:<10} {d['rows']:6d} {_mb(d['raw_bytes'])} {_mb(d['new_message_bytes'])}   {_mb(d['archive_bytes'])}"
                  f"  {d['split']:5d}  {d['whole']:5d}  {d['undecodable']:11d}  {d['verify_failed']:13d}\n")
    if totals:
        ratio = totals["raw_bytes"] / totals["archive_bytes"] if totals["archive_bytes"] else 0
        out.write(f"{'total':<10} {totals['rows']:6d} {_mb(totals['raw_bytes'])} {_mb(totals['new_message_bytes'])}   "
                  f"{_mb(totals['archive_bytes'])}  {totals['split']:5d}  {totals['whole']:5d}  {totals['undecodable']:11d}  "
                  f"{totals['verify_failed']:13d}\n")
        out.write(f"raw / archive = {ratio:.1f}x; rows that can't be archived (undecodable + verify_failed) are never deleted\n")
    else:
        out.write("no rows in range\n")


# --- step 2: the archive -------------------------------------------------------------------------------------------

ARCHIVE_DB = "provider_traces.sqlite3"
ARCHIVE_LOCK = "archive.lock"
ARCHIVE_FORMAT = "1"  # canonical JSON as in canonical(), each blob zlib level 6, record sha256 over the canonical bytes
BATCH_BYTES = 64 << 20  # raw row bytes held in memory between commit + re-read
DEFAULT_MIN_FREE_GB = 2.0
ARCHIVE_SCHEMA = """
create table if not exists meta (key text primary key, value text not null);
create table if not exists messages (hash text primary key, z blob not null);
create table if not exists traces (id text primary key, day text not null, created_at text, status text not null,
    record_sha256 text not null, record_z blob not null, archived_at text not null, verified_at text);
create index if not exists traces_day on traces (day, created_at, id);
create table if not exists runs (run integer primary key, started_at text not null, finished_at text, args text not null,
    totals text);
create table if not exists deletions (id text primary key, day text not null, run integer not null,
    requested_at text not null, deleted_at text);
"""
ARCHIVE_STATS = ("rows", "new", "already", "verified", "conflict", "undecodable", "verify_failed", "reread_failed",
                 "added_bytes")


def _now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def _private_file(path):
    os.close(os.open(path, os.O_WRONLY | os.O_CREAT, 0o600))
    if stat.S_IMODE(os.stat(path).st_mode) & 0o077:
        raise RuntimeError(f"{os.path.basename(path)} must not be readable by group/other")


def open_archive(directory):
    """(connection, path, lock) for the archive in `directory`. The archive holds conversation content, so the
    directory must belong to this user and be closed to group/other (it's created that way). The lock is exclusive and
    held until the caller closes it: two runs never write at once."""
    os.makedirs(directory, mode=0o700, exist_ok=True)
    st = os.stat(directory)
    if st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) & 0o077:
        raise RuntimeError(f"archive directory must belong to uid {os.getuid()} with mode 700")
    lock_path, path = os.path.join(directory, ARCHIVE_LOCK), os.path.join(directory, ARCHIVE_DB)
    _private_file(lock_path)
    lock = open(lock_path, "a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        raise RuntimeError("another archive run holds the lock") from None
    _private_file(path)  # SQLite gives its journal the database file's mode
    db = sqlite3.connect(path)
    db.executescript(ARCHIVE_SCHEMA)
    with db:
        db.execute("insert or ignore into meta values ('format', ?)", (ARCHIVE_FORMAT,))
    if db.execute("select value from meta where key = 'format'").fetchone()[0] != ARCHIVE_FORMAT:
        raise RuntimeError("archive was written in another format")
    return db, path, lock


def load_message(reader, digest):
    """Message bytes from the archive; raises KeyError if missing, ValueError if they don't match their hash."""
    got = reader.execute("select z from messages where hash = ?", (digest,)).fetchone()
    if got is None:
        raise KeyError(digest)
    try:
        blob = zlib.decompress(got[0])
    except zlib.error as exc:
        raise ValueError("message blob doesn't decompress") from exc
    if message_hash(blob) != digest:
        raise ValueError("message doesn't match its hash")
    return blob


def trace_day(created_at):
    """A trace's day key in the archive: the date part of its created_at text (server time zone)."""
    return (created_at or "")[:10] or "unknown"


def load_record(reader, trace_id):
    """(status, record) from the archive; raises KeyError if missing, ValueError if it doesn't match its hash or if the
    unhashed lookup columns (id, day, created_at, status) disagree with the hashed record: restore finds rows by them."""
    got = reader.execute("select status, record_sha256, record_z, day, created_at from traces where id = ?",
                         (trace_id,)).fetchone()
    if got is None:
        raise KeyError(trace_id)
    status, digest, z, day, created_at = got
    try:
        blob = zlib.decompress(z)
    except zlib.error as exc:
        raise ValueError("record blob doesn't decompress") from exc
    if hashlib.sha256(blob).hexdigest() != digest:
        raise ValueError("record doesn't match its hash")
    record = json.loads(blob.decode("utf-8", "surrogatepass"))
    columns = record["columns"]
    implied = (trace_id, trace_day(columns.get("created_at")), columns.get("created_at"),
               "whole" if "request_text" in record else "split")
    if (columns.get("id"), day, created_at, status) != implied:
        raise ValueError("lookup columns don't match the record")
    return status, record


def verify_archived(reader, row):
    """True when what is on disk rebuilds the Postgres row (the same check as verify, from the archive's bytes)."""
    try:
        status, record = load_record(reader, row["id"])
    except (KeyError, ValueError, RecursionError):
        return False
    return verify(row, Split(status, record), lambda digest: load_message(reader, digest))


def _flush(db, path, batch, days):
    """Write a batch in one transaction, then re-read the whole batch through a second, read-only connection: a row is
    verified only while it rebuilds. That includes rows an earlier run verified (`already`), so a damaged archive is
    caught on the next run instead of being trusted forever; a row that fails loses its mark. A row already archived
    with a different record (the Postgres row changed) is a conflict: its record stays as it is, but it loses its
    verified mark, since it no longer holds what Postgres holds. A row Postgres now holds in a form that can't be archived
    (`rejected`: undecodable or verify_failed) is only counted as such, unless an earlier run verified it: then that copy
    is stale too, loses its mark, and counts as `reread_failed`."""
    reread, unmark, archived_at = [], [], _now()
    with db:
        for day, row, split, rejected in batch:
            if rejected:
                # an id with escaped bytes can't be in the archive (and SQLite can't take it as a parameter)
                old = None if _ESCAPED_BYTE.search(row["id"]) else db.execute(
                    "select verified_at from traces where id = ?", (row["id"],)).fetchone()
                if old and old[0]:
                    unmark.append((row["id"],))
                days[day]["reread_failed" if old and old[0] else rejected] += 1
                continue
            record = canonical(split.record)
            digest = hashlib.sha256(record).hexdigest()
            old = db.execute("select record_sha256, verified_at from traces where id = ?", (row["id"],)).fetchone()
            if old and old[0] != digest:
                days[day]["conflict"] += 1
                unmark.append((row["id"],))
                continue
            if not old:  # (an unverified old row, e.g. from an interrupted run, is only re-checked below)
                for message, blob in split.blobs:  # most messages repeat: look before compressing
                    if not db.execute("select 1 from messages where hash = ?", (message,)).fetchone():
                        z = zlib.compress(blob, 6)
                        db.execute("insert into messages values (?, ?)", (message, z))
                        days[day]["added_bytes"] += len(z)
                z = zlib.compress(record, 6)
                db.execute("insert into traces values (?, ?, ?, ?, ?, ?, ?, null)",
                           (row["id"], day, row.get("created_at"), split.status, digest, z, archived_at))
                days[day]["added_bytes"] += len(z)
                days[day]["new"] += 1
            reread.append((day, row, bool(old and old[1])))
        db.executemany("update traces set verified_at = null where id = ?", unmark)
    reader = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        checked = [(day, row["id"], was, verify_archived(reader, row)) for day, row, was in reread]
    finally:
        reader.close()
    with db:  # a row verified before keeps its first verified_at
        db.executemany("update traces set verified_at = ? where id = ?",
                       [(_now(), trace_id) for _, trace_id, was, ok in checked if ok and not was])
        db.executemany("update traces set verified_at = null where id = ?",
                       [(trace_id,) for _, trace_id, _, ok in checked if not ok])
    for day, _, was, ok in checked:
        days[day]["reread_failed" if not ok else "already" if was else "verified"] += 1


def archive_rows(db, path, rows, batch_bytes=BATCH_BYTES):
    """Archive the rows that rebuild in memory, in batches of about `batch_bytes`; per-day stats. Rows that can't be
    read exactly or don't rebuild are left out (they can never be deleted) and are counted in `_flush`, which also takes
    the mark off an earlier verified copy of them; only their id is kept in memory."""
    days, batch, size = {}, [], 0
    for row in rows:
        day = trace_day(row.get("created_at"))
        stats = days.setdefault(day, dict.fromkeys(ARCHIVE_STATS, 0))
        stats["rows"] += 1
        split = split_trace(row)
        rejected = "undecodable" if split.status == "undecodable" else None if verify(row, split) else "verify_failed"
        if rejected:
            batch.append((day, {"id": row["id"]}, None, rejected))
            continue
        batch.append((day, row, split, None))
        size += sum(_size(v) for v in row.values())
        if size >= batch_bytes:
            _flush(db, path, batch, days)
            batch, size = [], 0
    if batch:
        _flush(db, path, batch, days)
    return days


def day_counts(config, where):
    """[(day, rows)] in range, oldest first. Selects no content, so each day can then be read in its own read-only
    transaction (open while that day is archived) instead of one for the whole range."""
    query = (f"select to_char(created_at, 'YYYY-MM-DD') as day, count(*) as rows from provider_traces {where} "
             "group by 1 order by 1")
    cmd, env = _psql(config)
    out = subprocess.run([*cmd, "-c", f"\\copy ({query}) to stdout with (format csv, header)"],
                         env=env, capture_output=True, text=True, check=True).stdout
    return [(r["day"], int(r["rows"])) for r in csv.DictReader(io.StringIO(out))]


def run_archive(config, directory, older_than_days=DEFAULT_OLDER_THAN_DAYS, date_from=None, date_to=None,
                min_free_gb=DEFAULT_MIN_FREE_GB, now=None, out=sys.stdout):
    """Archive every day in range, oldest first; stops before a day if free space is under `min_free_gb`.
    Returns 3 when it stopped for space, else 4 when any row is a conflict or failed its re-read (so a timer doesn't
    report success), else 0. Rows that can't be archived at all (undecodable, verify_failed) are only counted."""
    now = now or datetime.datetime.now(datetime.timezone.utc)  # one cutoff for the whole run
    where = where_clause(older_than_days, date_from, date_to, now=now)
    db, path, lock = open_archive(directory)
    try:
        with db:
            run = db.execute("insert into runs (started_at, args) values (?, ?)", (_now(), where)).lastrowid
        days, stopped = {}, None
        for day, _ in day_counts(config, where):
            if shutil.disk_usage(directory).free < min_free_gb * 1e9:
                stopped = f"stopped before {day}: less than {min_free_gb:g} GB free"
                break
            days.update(archive_rows(db, path, stream_rows(config, where_clause(older_than_days, day, day, now=now))))
        totals = {key: sum(d[key] for d in days.values()) for key in ARCHIVE_STATS}
        with db:
            db.execute("update runs set finished_at = ?, totals = ? where run = ?",
                       (_now(), json.dumps({**totals, "stopped": stopped}), run))
        archive_report(days, totals, os.path.getsize(path), stopped, out)
        if stopped:
            return 3
        return 4 if totals["conflict"] or totals["reread_failed"] else 0
    finally:
        db.close()
        lock.close()


def archive_report(days, totals, archive_size, stopped=None, out=sys.stdout):
    out.write("day          rows    new  already  verified  conflict  undecodable  verify_failed  reread_failed  added_MB\n")
    for day in sorted(days):
        d = days[day]
        out.write(f"{day:<10} {d['rows']:6d} {d['new']:6d} {d['already']:8d} {d['verified']:9d} {d['conflict']:9d}"
                  f" {d['undecodable']:12d} {d['verify_failed']:14d} {d['reread_failed']:14d} {_mb(d['added_bytes'])}\n")
    t = totals
    out.write(f"{'total':<10} {t['rows']:6d} {t['new']:6d} {t['already']:8d} {t['verified']:9d} {t['conflict']:9d}"
              f" {t['undecodable']:12d} {t['verify_failed']:14d} {t['reread_failed']:14d} {_mb(t['added_bytes'])}\n")
    out.write(f"archive file: {archive_size / 1e6:.1f} MB. Only verified rows (verified + already) may ever be deleted.\n")
    if stopped:
        out.write(stopped + "\n")


_LONE_SURROGATE = re.compile("[\ud800-\udfff]")


def json_text(value):
    """A rebuilt request as JSON text Postgres accepts: valid UTF-8, with lone surrogates (kept from \\ud83d-style
    escapes in the original) written as escapes again. The same JSON value as the original, not the same bytes."""
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return _LONE_SURROGATE.sub(lambda m: f"\\u{ord(m.group()):04x}", text)


def restore_rows(reader, day):
    """The verified archived rows of one day, oldest first, as Postgres would take them back: every column text or
    None, request_json as JSON text."""
    ids = reader.execute("select id from traces where day = ? and verified_at is not null order by created_at, id",
                         (day,)).fetchall()
    for (trace_id,) in ids:
        status, record = load_record(reader, trace_id)
        row = rebuild(record, lambda digest: load_message(reader, digest))
        if status == "split":
            row["request_json"] = json_text(row["request_json"])
        yield row


def write_csv(rows, path):
    """Rows as psql CSV: every value quoted, NULL as an empty unquoted field (so "" stays an empty string), UTF-8.
    The file is created 600 and never overwritten; a failed write, or no rows at all, leaves no file. Returns (rows,
    column order)."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    columns, n = [], 0
    try:
        with open(fd, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NOTNULL, lineterminator="\n")
            for row in rows:
                if not columns:
                    columns = list(row)
                    writer.writerow(columns)
                if list(row) != columns:
                    raise RuntimeError("rows with different columns")
                writer.writerow(row.values())
                n += 1
    except BaseException:
        os.unlink(path)
        raise
    if not n:  # no header either: an empty file would only invite a `\copy` with no columns
        os.unlink(path)
    return n, columns


# --- step 3: delete what the archive holds and telemetry has analysed ----------------------------------------------

MIN_DELETE_DAYS = 30
DEFAULT_BATCH_ROWS = 50
DELETE_STATS = ("rows", "deleted", "would_delete", "not_archived", "not_analysed", "mismatch", "changed", "unsafe_id",
                "deferred")
# Ids and digests go into the DELETE statement as literals, so only these shapes are ever sent; any other id is skipped.
_SAFE_ID = re.compile(r"[A-Za-z0-9_.:-]{1,200}")  # used with fullmatch: no trailing newline slips through
_MD5 = re.compile(r"[0-9a-f]{32}")


def analysed_traces(config):
    """The hashed trace ids in telemetry's letta_traces: the 'analysed' half of the delete rule. Read-only. A missing
    telemetry database is an error, never 'nothing analysed'."""
    path = os.path.join(config.get("TELEMETRY_DIR") or "", "telemetry.db")
    if not config.get("TELEMETRY_DIR") or not os.path.isfile(path):
        raise RuntimeError("--delete needs TELEMETRY_DIR with the collector's telemetry.db in the config")
    tel = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return {t for (t,) in tel.execute("select trace from letta_traces") if t}
    finally:
        tel.close()


def _psql_write(config):
    """psql for the one session that may write. Only the read-only default is dropped, so it prints rows (and their
    md5) under the same server settings as the reads."""
    cmd, env = _psql(config)
    env["PGOPTIONS"] = "-c jit=off"
    return cmd, env


def _delete_batch(config, batch, cutoff):
    """Delete, in one statement (its own transaction), each (id, md5) row that still has the md5 it had when it was
    checked and is older than the cutoff. Returns the ids Postgres deleted: a row changed since its check stays."""
    values = ", ".join(f"('{trace_id}', '{digest}')" for trace_id, digest, _ in batch)  # shapes checked by the caller
    sql = (f"delete from provider_traces t using (values {values}) as v(id, md5) "
           f"where t.id = v.id and md5(t::text) = v.md5 and t.created_at < '{cutoff}' returning t.id")
    cmd, env = _psql_write(config)
    out = subprocess.run([*cmd, "-v", "ON_ERROR_STOP=1", "-A", "-t", "-c", sql], env=env, capture_output=True,
                         text=True, check=True).stdout
    asked = {trace_id for trace_id, _, _ in batch}
    return {line for line in out.splitlines() if line in asked}


def _vacuum(config):
    """Plain VACUUM: new rows reuse the space; the file doesn't shrink (that's VACUUM FULL, which locks the table)."""
    cmd, env = _psql_write(config)
    subprocess.run([*cmd, "-v", "ON_ERROR_STOP=1", "-c", "vacuum (analyze) provider_traces"], env=env,
                   capture_output=True, text=True, check=True)


def _check_day(config, db, reader, analysed, where, days, budget):
    """Read one day's rows (one read-only transaction, each row with its md5) and check each: verified in the archive,
    analysed by telemetry, rebuilt exactly from the archive's bytes. Returns ([(id, md5, day)] that passed, budget left
    for --max-rows); counts the rest in `days`."""
    eligible = []
    for row, digest in _stream(config, where, with_md5=True):
        day = trace_day(row.get("created_at"))
        stats = days.setdefault(day, dict.fromkeys(DELETE_STATS, 0))
        stats["rows"] += 1
        trace_id = row["id"]
        if not _SAFE_ID.fullmatch(trace_id) or not _MD5.fullmatch(digest or ""):
            stats["unsafe_id"] += 1
            continue
        got = db.execute("select verified_at from traces where id = ?", (trace_id,)).fetchone()
        if not got or not got[0]:
            stats["not_archived"] += 1
        elif trace_hash(trace_id) not in analysed:
            stats["not_analysed"] += 1
        elif not verify_archived(reader, row):
            stats["mismatch"] += 1
        elif budget is not None and budget <= 0:
            stats["deferred"] += 1
        else:
            eligible.append((trace_id, digest, day))
            budget = None if budget is None else budget - 1
    return eligible, budget


def run_delete(config, directory, older_than_days=DEFAULT_OLDER_THAN_DAYS, date_from=None, date_to=None,
               batch_rows=DEFAULT_BATCH_ROWS, max_rows=None, dry_run=False, vacuum=True, now=None, out=sys.stdout):
    """Delete old rows that are in the archive and analysed, day by day, oldest first. Right before its delete, each
    row is read again from Postgres and must be verified in the archive, have its hashed id in telemetry, and still be
    rebuilt exactly from the archive's bytes. It's then deleted only while it has the md5 it was read with. Deleted ids
    are recorded in the archive's `deletions` table; at the end, plain VACUUM if anything went. --dry-run does every
    check and deletes nothing. Returns 4 when any row failed its re-check (mismatch), changed before its delete or
    has an id that can't go into the statement (unsafe_id), or when the VACUUM failed; else 0."""
    if older_than_days < MIN_DELETE_DAYS:
        raise ValueError(f"--delete keeps at least the last {MIN_DELETE_DAYS} days")
    if batch_rows < 1 or (max_rows is not None and max_rows < 1):
        raise ValueError("--batch-rows and --max-rows must be at least 1")
    if not os.path.isfile(os.path.join(directory, ARCHIVE_DB)):
        raise RuntimeError("no archive in that directory: --archive first")
    now = now or datetime.datetime.now(datetime.timezone.utc)  # one cutoff for the whole run
    where, cutoff = where_clause(older_than_days, date_from, date_to, now=now), cutoff_time(older_than_days, now)
    analysed = analysed_traces(config)
    db, path, lock = open_archive(directory)
    try:
        with db:
            run = db.execute("insert into runs (started_at, args) values (?, ?)",
                             (_now(), f"delete{' --dry-run' if dry_run else ''} {where}")).lastrowid
        days, budget = {}, max_rows
        for day, _ in day_counts(config, where):
            # Check the whole day in its read-only transaction, then delete in batches after it has ended. The
            # archive's reader is closed before `deletions` is written, as in _flush.
            reader = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            try:
                eligible, budget = _check_day(config, db, reader, analysed,
                                              where_clause(older_than_days, day, day, now=now), days, budget)
            finally:
                reader.close()
            for start in range(0, len(eligible), batch_rows):
                batch = eligible[start:start + batch_rows]
                if dry_run:
                    for _, _, d in batch:
                        days[d]["would_delete"] += 1
                    continue
                # Requested first, so a crash right after the DELETE leaves rows marked requested (deleted_at null),
                # never unrecorded; Postgres says which of them went.
                with db:
                    db.executemany("insert or replace into deletions values (?, ?, ?, ?, null)",
                                   [(t, d, run, _now()) for t, _, d in batch])
                deleted = _delete_batch(config, batch, cutoff)
                with db:
                    db.executemany("update deletions set deleted_at = ? where id = ?",
                                   [(_now(), t) for t, _, _ in batch if t in deleted])
                    db.executemany("delete from deletions where id = ? and deleted_at is null",
                                   [(t,) for t, _, _ in batch if t not in deleted])
                for t, _, d in batch:
                    days[d]["deleted" if t in deleted else "changed"] += 1
        totals = {key: sum(d[key] for d in days.values()) for key in DELETE_STATS}
        # The deletes are done: record and report them before VACUUM, so a VACUUM failure can't hide what went.
        record = {**totals, "vacuum": "pending" if vacuum and totals["deleted"] else "skipped"}
        with db:
            db.execute("update runs set finished_at = ?, totals = ? where run = ?", (_now(), json.dumps(record), run))
        delete_report(days, totals, dry_run, out)
        if record["vacuum"] == "pending":
            try:
                _vacuum(config)
                record["vacuum"] = "done"
            except subprocess.CalledProcessError:
                record["vacuum"] = "failed"
            with db:
                db.execute("update runs set totals = ? where run = ?", (json.dumps(record), run))
            out.write("VACUUM done: new rows reuse the space; giving it back to the disk needs VACUUM FULL (not done here).\n"
                      if record["vacuum"] == "done" else "VACUUM failed (the deletes stand): run it again by hand.\n")
        bad = totals["mismatch"] or totals["changed"] or totals["unsafe_id"] or record["vacuum"] == "failed"
        return 4 if bad else 0
    finally:
        db.close()
        lock.close()


def delete_report(days, totals, dry_run, out=sys.stdout):
    out.write("day          rows  deleted  would_delete  not_archived  not_analysed  mismatch  changed  unsafe_id  deferred\n")
    for day in sorted(days):
        d = days[day]
        out.write(f"{day:<10} {d['rows']:6d} {d['deleted']:8d} {d['would_delete']:13d} {d['not_archived']:13d}"
                  f" {d['not_analysed']:13d} {d['mismatch']:9d} {d['changed']:8d} {d['unsafe_id']:10d} {d['deferred']:9d}\n")
    t = totals
    out.write(f"{'total':<10} {t['rows']:6d} {t['deleted']:8d} {t['would_delete']:13d} {t['not_archived']:13d}"
              f" {t['not_analysed']:13d} {t['mismatch']:9d} {t['changed']:8d} {t['unsafe_id']:10d} {t['deferred']:9d}\n")
    if dry_run:
        out.write("dry run: nothing deleted.\n")
    if t["mismatch"] or t["changed"]:
        out.write("mismatch / changed rows stay in Postgres: run --archive to see what differs.\n")
    if t["unsafe_id"]:
        out.write("unsafe_id rows stay in Postgres: their ids can't go into the statement; look at them by hand.\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true", help="dry run: read only, write nothing")
    mode.add_argument("--archive", action="store_true", help="write and verify the archive (needs --archive-dir)")
    mode.add_argument("--restore-day", metavar="YYYY-MM-DD", help="write that day's verified rows to --out as CSV")
    mode.add_argument("--delete", action="store_true",
                      help="delete old rows that are verified in the archive and analysed by telemetry (writes Postgres)")
    parser.add_argument("--config", help="observatory config (Letta psql settings, TELEMETRY_DIR); needed by --plan, "
                                         "--archive and --delete")
    parser.add_argument("--archive-dir", help="archive directory: owned by the running user, mode 700")
    parser.add_argument("--out", help="CSV file for --restore-day; must not exist yet")
    parser.add_argument("--older-than-days", type=int, default=DEFAULT_OLDER_THAN_DAYS)
    parser.add_argument("--from", dest="date_from", help="YYYY-MM-DD, server time zone")
    parser.add_argument("--to", dest="date_to", help="YYYY-MM-DD inclusive, server time zone")
    parser.add_argument("--min-free-gb", type=float, default=DEFAULT_MIN_FREE_GB,
                        help="--archive stops before a day when the archive's file system has less free space")
    parser.add_argument("--dry-run", action="store_true", help="--delete: every check, no delete")
    parser.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS, help="--delete: rows per DELETE statement")
    parser.add_argument("--max-rows", type=int, help="--delete: delete at most this many rows in this run")
    parser.add_argument("--no-vacuum", action="store_true", help="--delete: skip the plain VACUUM at the end")
    args = parser.parse_args()
    if args.older_than_days < 1:
        parser.error("--older-than-days must be at least 1")
    if (args.plan or args.archive or args.delete) and not args.config:
        parser.error("--plan, --archive and --delete need --config")
    if (args.archive or args.restore_day or args.delete) and not args.archive_dir:
        parser.error("--archive, --restore-day and --delete need --archive-dir")
    if args.delete and args.older_than_days < MIN_DELETE_DAYS:
        parser.error(f"--delete keeps at least the last {MIN_DELETE_DAYS} days (--older-than-days >= {MIN_DELETE_DAYS})")
    if (args.dry_run or args.max_rows is not None or args.no_vacuum) and not args.delete:
        parser.error("--dry-run, --max-rows and --no-vacuum only go with --delete")
    if args.restore_day:
        if not args.out or not _DATE.match(args.restore_day):
            parser.error("--restore-day needs a YYYY-MM-DD day and --out")
        reader = sqlite3.connect(f"file:{os.path.join(args.archive_dir, ARCHIVE_DB)}?mode=ro", uri=True)
        n, columns = write_csv(restore_rows(reader, args.restore_day), args.out)
        if not n:
            print(f"no verified rows for {args.restore_day}; nothing written")
            return
        print(f"{n} rows -> {args.out}. Load with:\n"
              f"  \\copy provider_traces ({', '.join(columns)}) from '{args.out}' with (format csv, header match)")
        return
    os.umask(0o077)
    config = load_config(args.config)
    if args.archive:
        sys.exit(run_archive(config, args.archive_dir, args.older_than_days, args.date_from, args.date_to,
                             args.min_free_gb))
    if args.delete:
        sys.exit(run_delete(config, args.archive_dir, args.older_than_days, args.date_from, args.date_to,
                            args.batch_rows, args.max_rows, args.dry_run, not args.no_vacuum))
    days, totals = plan(stream_rows(config, where_clause(args.older_than_days, args.date_from, args.date_to)))
    report(days, totals)


if __name__ == "__main__":
    main()
