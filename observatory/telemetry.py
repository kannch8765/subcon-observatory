"""SQLite telemetry store fed by exporter snapshots.

Tables mirror the exporter CSVs (already sanitized), so nothing here can see more than a snapshot does.
Columns are added automatically when an exporter grows one. Every table has a `_row` primary key:
- upsert: the latest row per natural key wins (Letta steps, runs, memories, ...)
- log:    append-only rows deduped by content (batches, outcomes, recall events, ...)
- sample: point-in-time state keyed by the collect run (backlog per session per run = a time series)
"""
import csv, datetime, hashlib, pathlib, re, sqlite3

# exporter/table -> (db table, mode, key columns, columns left out of a log row's identity)
TABLES = {
    "letta/steps": ("letta_steps", "upsert", ["step"], []),
    "letta/runs": ("letta_runs", "upsert", ["run"], []),
    "letta/messages": ("letta_messages", "upsert", ["message"], []),
    "letta/blocks": ("letta_blocks", "upsert", ["block"], []),
    "letta/block_history": ("letta_block_history", "log", [], []),
    "letta_traces/traces": ("letta_traces", "upsert", ["trace"], []),
    "letta_traces/tool_calls": ("letta_tool_calls", "upsert", ["trace", "ord"], []),
    "store/memories": ("memories", "upsert", ["mid"], []),
    "store/batches": ("batches", "log", [], []),
    "store/outcomes": ("outcomes", "log", [], []),
    "store/entity_outcomes": ("entity_outcomes", "log", [], []),
    "store/intents": ("intents", "upsert", ["iid"], []),
    "store/intent_outcomes": ("intent_outcomes", "log", [], []),
    "store/evidence": ("evidence", "log", [], []),
    "store/reinforcements": ("reinforcements", "log", [], []),
    "backlog/backlog": ("backlog_samples", "sample", ["session"], []),
    "whispers/whispers": ("whispers", "upsert", ["root_idx", "created_at", "source", "batch_prefix", "text_chars"], []),
    "recall_log/recall_events": ("recall_events", "log", [], ["file_idx"]),  # file order changes as logs rotate
    "deploy/heads": ("deploy_heads", "sample", ["checkout_idx"], []),
    "deploy/reflog": ("deploy_reflog", "log", [], ["checkout_idx"]),
    "opencode/requests": ("opencode_requests", "upsert", ["rid"], []),
}
_NUM = re.compile(r"^-?\d+(\.\d+)?$")
# Hashed ids stay text: an all-digit hash must keep its leading zeros to join across tables
ID_COLUMNS = {"step", "run", "agent", "conversation", "message", "block", "trace", "mid", "bid", "attempt", "iid", "eid",
              "session", "rid", "sha"}


def connect(path):
    db = sqlite3.connect(path, timeout=30)
    db.execute("pragma journal_mode=wal")
    db.execute("""create table if not exists collect_runs (started_at text primary key, finished_at text, ok integer,
                  letta_since text, traces_since text, rows text, redacted text, error text)""")
    db.execute("""create table if not exists daily_rollup (day text, scope text, metric text, value real,
                  primary key (day, scope, metric))""")
    return db


def _value(column, text):
    if text in (None, ""):
        return None
    if column not in ID_COLUMNS and _NUM.match(text):
        return float(text) if "." in text else int(text)
    return text


def _ensure(db, table, columns):
    db.execute(f'create table if not exists "{table}" (_row text primary key, _first_seen text, _last_seen text)')
    have = {r[1] for r in db.execute(f'pragma table_info("{table}")')}
    for c in columns:
        if c not in have:
            db.execute(f'alter table "{table}" add column "{c}"')


def _row_key(mode, row, keys, exclude, sampled_at):
    if mode == "log":
        # Only non-empty cells, so a column added to an exporter later doesn't change old rows' identity
        cells = sorted((k, v) for k, v in row.items() if v not in (None, "") and k not in exclude)
        return hashlib.sha256(repr(cells).encode()).hexdigest()[:24]
    ident = "\x1f".join(str(row.get(k) or "") for k in keys)
    return f"{sampled_at}\x1f{ident}" if mode == "sample" else ident


def ingest_csv(db, name, path, sampled_at):
    """Merge one exporter CSV into its table. Returns the number of rows read."""
    table, mode, keys, exclude = TABLES[name]
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        columns = [c for c in reader.fieldnames or [] if c]
        _ensure(db, table, columns + (["sampled_at"] if mode == "sample" else []))
        cols = columns + (["sampled_at"] if mode == "sample" else [])
        quoted = ", ".join(f'"{c}"' for c in cols)
        marks = ", ".join("?" for _ in cols)
        update = "_last_seen = excluded._last_seen" + ("" if mode == "log" else "".join(f', "{c}" = excluded."{c}"' for c in cols))
        sql = (f'insert into "{table}" (_row, _first_seen, _last_seen, {quoted}) values (?, ?, ?, {marks}) '
               f"on conflict(_row) do update set {update}")
        n = 0
        for row in reader:
            values = [_value(c, row.get(c)) for c in columns] + ([sampled_at] if mode == "sample" else [])
            db.execute(sql, [_row_key(mode, row, keys, exclude, sampled_at), sampled_at, sampled_at, *values])
            n += 1
    return n


def ingest_snapshot(db, snap, sampled_at):
    """Ingest every known table of a snapshot directory; unknown CSVs are skipped."""
    counts = {}
    for path in sorted(pathlib.Path(snap).glob("*/*.csv")):
        name = f"{path.parent.name}/{path.stem}"
        if name in TABLES:
            counts[name] = ingest_csv(db, name, path, sampled_at)
    return counts


def max_value(db, table, column):
    try:
        return db.execute(f'select max("{column}") from "{table}"').fetchone()[0]
    except sqlite3.OperationalError:  # table not created yet
        return None


def since_day(latest, overlap_days=1):
    """ISO date `overlap_days` before the newest ingested timestamp (re-reading a day absorbs late rows), or None."""
    if not latest:
        return None
    day = parse_ts(latest)
    return (day - datetime.timedelta(days=overlap_days)).date().isoformat() if day else None


def parse_ts(value):
    """Exporter timestamps (ISO with `Z`, `+00:00`, `+09`, or naive = UTC) -> aware datetime, else None."""
    if not isinstance(value, str) or not value:
        return None
    try:
        ts = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=datetime.timezone.utc)
