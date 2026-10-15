# Read-only psql access to the Letta database, shared by the letta* exporters.
# Config: LETTA_PSQL (psql binary), LETTA_PG_ARGS (e.g. "-h 127.0.0.1 -p 5432 -U letta -d letta"),
#         LETTA_PG_LIBDIR (optional LD_LIBRARY_PATH for a relocated psql), LETTA_SINCE (optional ISO date)
import shlex, subprocess

csv.field_size_limit(1 << 31)


def letta_since(column):
    """`where <column> >= '<LETTA_SINCE>'`, or "" when unset. The date is validated, never interpolated raw."""
    since = CONFIG.get("LETTA_SINCE") or ""
    if not since:
        return ""
    if not _TS.match(since):
        raise ValueError("LETTA_SINCE must be an ISO date or timestamp")
    return f"where {column} >= '{since}'"


def psql_rows(query):
    """Yield dict rows of a read-only `\\copy (query) to stdout with csv header`."""
    env = {**os.environ, "PGOPTIONS": "-c default_transaction_read_only=on -c jit=off"}
    if CONFIG.get("LETTA_PG_LIBDIR"):
        env["LD_LIBRARY_PATH"] = CONFIG["LETTA_PG_LIBDIR"]
    cmd = [CONFIG["LETTA_PSQL"], *shlex.split(CONFIG["LETTA_PG_ARGS"]), "-X", "-q",
           "-c", f"\\copy ({query}) to stdout with csv header"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, env=env, text=True, encoding="utf-8", errors="replace")
    yield from csv.DictReader(proc.stdout)
    if proc.wait() != 0:
        raise RuntimeError(f"psql exited with {proc.returncode}")


def ns_to_ms(value):
    return int(value) // 1_000_000 if value not in (None, "") and str(value).lstrip("-").isdigit() else None
