"""Lossless, deduplicated archive of Letta `provider_traces`, so old raw rows can leave Postgres without losing anything.

    python3 -E -s -m observatory.trace_archive --config <file> --plan                          # rows older than 30 days
    python3 -E -s -m observatory.trace_archive --config <file> --plan --from 2026-08-08 --to 2026-08-09
    python3 -E -s -m observatory.trace_archive --config <file> --archive --archive-dir <dir>   # write + verify
    python3 -E -s -m observatory.trace_archive --restore-day 2026-08-08 --archive-dir <dir> --out <file.csv>

Every LLM request repeats the whole conversation so far, so the same messages are stored again on every step.
The archive keeps each distinct message once (keyed by sha256) and, per trace, a record of every other column plus
the list of message hashes. A trace counts as archivable only if it can be rebuilt from that record to the same
JSON value it had (see docs/TRACE-ARCHIVE.md).

--plan reads Postgres read-only, writes nothing anywhere, and prints per day how many rows would be archived, how
much they take raw, how much the archive would add, and whether every row rebuilds. --archive writes the archive (a
SQLite file in a directory only its owner can enter), re-reads every row from disk and marks it verified only if it
rebuilds the Postgres row. --restore-day writes verified rows back out as CSV that psql's \\copy loads. Nothing here
deletes from or writes to Postgres. JSON is parsed here, not in Postgres: broken surrogates in stored payloads make
PG's json functions fail.
"""
import argparse, csv, datetime, decimal, fcntl, hashlib, io, json, os, re, shlex, shutil, sqlite3, stat, subprocess, sys, zlib
from dataclasses import dataclass, field

from observatory.export import load_config

csv.field_size_limit(1 << 31)

DEFAULT_OLDER_THAN_DAYS = 30
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# psql reads bytes that aren't UTF-8 as surrogate escapes; a field containing one can't be archived exactly.
_ESCAPED_BYTE = re.compile("[\udc80-\udcff]")
# Python's csv reader can't tell NULL from "" (or from any sentinel text), so NULL-ness travels separately: the
# query adds one 0/1 digit per column in this extra column, computed by Postgres itself.
NULL_MASK = "__observatory_null_mask"
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
    if not columns or any(not _IDENT.match(c) for c in columns) or NULL_MASK in columns:
        raise RuntimeError(f"unexpected provider_traces columns: {columns!r}")
    return columns


def stream_rows(config, where):
    """provider_traces rows as dicts, NULL as None (from the mask, never from the text)."""
    columns = trace_columns(config)
    mask = "concat(" + ", ".join(f'("{c}" is null)::int' for c in columns) + ")"
    select = ", ".join(f'"{c}"' for c in columns)
    query = f'select {select}, {mask} as {NULL_MASK} from provider_traces {where} order by created_at, id'
    cmd, env = _psql(config)
    proc = subprocess.Popen([*cmd, "-c", f"\\copy ({query}) to stdout with (format csv, header, force_quote *)"],
                            stdout=subprocess.PIPE, env=env)
    text = io.TextIOWrapper(proc.stdout, encoding="utf-8", errors="surrogateescape", newline="")
    yield from (decode_nulls(row, columns) for row in csv.DictReader(text))
    if proc.wait() != 0:
        raise RuntimeError(f"psql exited with {proc.returncode}")


def where_clause(older_than_days, date_from=None, date_to=None, now=None):
    """created_at bounds. Dates are validated, never interpolated raw; --to is inclusive."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    cutoff = (now - datetime.timedelta(days=older_than_days)).isoformat()
    parts = [f"created_at < '{cutoff}'"]
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


def load_record(reader, trace_id):
    """(status, record) from the archive; raises KeyError if missing, ValueError if it doesn't match its hash."""
    got = reader.execute("select status, record_sha256, record_z from traces where id = ?", (trace_id,)).fetchone()
    if got is None:
        raise KeyError(trace_id)
    status, digest, z = got
    try:
        blob = zlib.decompress(z)
    except zlib.error as exc:
        raise ValueError("record blob doesn't decompress") from exc
    if hashlib.sha256(blob).hexdigest() != digest:
        raise ValueError("record doesn't match its hash")
    return status, json.loads(blob.decode("utf-8", "surrogatepass"))


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
    verified mark, since it no longer holds what Postgres holds."""
    reread, conflicts, archived_at = [], [], _now()
    with db:
        for day, row, split in batch:
            record = canonical(split.record)
            digest = hashlib.sha256(record).hexdigest()
            old = db.execute("select record_sha256, verified_at from traces where id = ?", (row["id"],)).fetchone()
            if old and old[0] != digest:
                days[day]["conflict"] += 1
                conflicts.append((row["id"],))
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
        db.executemany("update traces set verified_at = null where id = ?", conflicts)
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
    read exactly or don't rebuild are counted and left out: they can never be deleted."""
    days, batch, size = {}, [], 0
    for row in rows:
        day = (row.get("created_at") or "")[:10] or "unknown"
        stats = days.setdefault(day, dict.fromkeys(ARCHIVE_STATS, 0))
        stats["rows"] += 1
        split = split_trace(row)
        if split.status == "undecodable":
            stats["undecodable"] += 1
            continue
        if not verify(row, split):
            stats["verify_failed"] += 1
            continue
        batch.append((day, row, split))
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


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true", help="dry run: read only, write nothing")
    mode.add_argument("--archive", action="store_true", help="write and verify the archive (needs --archive-dir)")
    mode.add_argument("--restore-day", metavar="YYYY-MM-DD", help="write that day's verified rows to --out as CSV")
    parser.add_argument("--config", help="observatory config (Letta psql settings); needed by --plan and --archive")
    parser.add_argument("--archive-dir", help="archive directory: owned by the running user, mode 700")
    parser.add_argument("--out", help="CSV file for --restore-day; must not exist yet")
    parser.add_argument("--older-than-days", type=int, default=DEFAULT_OLDER_THAN_DAYS)
    parser.add_argument("--from", dest="date_from", help="YYYY-MM-DD, server time zone")
    parser.add_argument("--to", dest="date_to", help="YYYY-MM-DD inclusive, server time zone")
    parser.add_argument("--min-free-gb", type=float, default=DEFAULT_MIN_FREE_GB,
                        help="--archive stops before a day when the archive's file system has less free space")
    args = parser.parse_args()
    if args.older_than_days < 1:
        parser.error("--older-than-days must be at least 1")
    if (args.plan or args.archive) and not args.config:
        parser.error("--plan and --archive need --config")
    if (args.archive or args.restore_day) and not args.archive_dir:
        parser.error("--archive and --restore-day need --archive-dir")
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
    days, totals = plan(stream_rows(config, where_clause(args.older_than_days, args.date_from, args.date_to)))
    report(days, totals)


if __name__ == "__main__":
    main()
