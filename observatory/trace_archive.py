"""Lossless, deduplicated archive of Letta `provider_traces`, so old raw rows can leave Postgres without losing anything.

    python3 -E -s -m observatory.trace_archive --config <file> --plan                          # rows older than 30 days
    python3 -E -s -m observatory.trace_archive --config <file> --plan --from 2026-08-08 --to 2026-08-09

Every LLM request repeats the whole conversation so far, so the same messages are stored again on every step.
The archive keeps each distinct message once (keyed by sha256) and, per trace, a record of every other column plus
the list of message hashes. A trace counts as archivable only if it can be rebuilt from that record to the same
JSON value it had (see docs/TRACE-ARCHIVE.md).

This version only has --plan: it reads Postgres read-only, writes nothing anywhere, and prints per day how many rows
would be archived, how much they take raw, how much the archive would add, and whether every row rebuilds.
JSON is parsed here, not in Postgres: broken surrogates in stored payloads make PG's json functions fail.
"""
import argparse, csv, datetime, decimal, hashlib, io, json, os, re, shlex, subprocess, sys, zlib
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


def verify(row, split):
    """True when the record rebuilds the row: other columns identical text, request the same JSON value with exact
    numbers. Never raises: a row that can't be checked is simply not verified."""
    if split.status == "undecodable":
        return False
    try:
        rebuilt = rebuild(split.record, dict(split.blobs).__getitem__)
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


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", required=True)
    parser.add_argument("--plan", action="store_true", help="dry run: read only, write nothing (the only mode so far)")
    parser.add_argument("--older-than-days", type=int, default=DEFAULT_OLDER_THAN_DAYS)
    parser.add_argument("--from", dest="date_from", help="YYYY-MM-DD, server time zone")
    parser.add_argument("--to", dest="date_to", help="YYYY-MM-DD inclusive, server time zone")
    args = parser.parse_args()
    if not args.plan:
        parser.error("only --plan exists in this version; archiving and deleting come later")
    if args.older_than_days < 1:
        parser.error("--older-than-days must be at least 1")
    config = load_config(args.config)
    days, totals = plan(stream_rows(config, where_clause(args.older_than_days, args.date_from, args.date_to)))
    report(days, totals)


if __name__ == "__main__":
    main()
