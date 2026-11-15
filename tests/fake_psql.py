#!/usr/bin/env python3
"""Stand-in for psql in tests: `fake_psql.py <fixtures.json> -X -q -c "\\copy (select ... from <table> ...) ..."`.
Prints the fixture rows of the first table named in the query as CSV with a header. Also takes --delete's two writes:
`delete from provider_traces t using (values ...) ... returning t.id` (rewrites the fixtures file, prints the deleted
ids), `vacuum ...` (no-op; with "__vacuum_warning" in the fixtures it warns on stderr like PG does when it skips a
table: `true` writes PG's English text, a string is written as given; held back when the session's
client_min_messages is "error", see below) and `select id from provider_traces where id in (...)` (`-A -t`: the ids
that exist). Several -c run in turn as psql does; only `set <name> = <value>` may come before the last one. Every
command is appended to <fixtures.json>.log, one per line."""
import hashlib, json, os, re, sys

fixtures_path = sys.argv[1]
fixtures = json.load(open(fixtures_path))
sys.stdout.reconfigure(encoding="utf-8", errors="surrogateescape")  # a "\udcff" fixture goes out as the byte 0xff
commands = [sys.argv[i + 1] for i, arg in enumerate(sys.argv[:-1]) if arg == "-c"]  # each -c in turn, one session
session = {}
for command in commands[:-1]:
    setting = re.fullmatch(r"set (\w+) = (\w+)", command)
    if not setting:
        sys.exit(f"fake_psql: only a `set` may come before the last -c, got {command[:80]!r}")
    session[setting.group(1)] = setting.group(2)
query = commands[-1]
with open(fixtures_path + ".log", "a") as log:
    log.write("".join(command.replace("\n", " ") + "\n" for command in commands))


def row_md5(row):
    """Stands in for Postgres's md5(t::text): any change to any column changes it."""
    return hashlib.md5(json.dumps(row).encode()).hexdigest()


if "from information_schema.columns" in query:  # `-A -t` column-name listing, one per line
    table = re.search(r"table_name = '(\w+)'", query).group(1)
    print("\n".join(fixtures[table][0]))
    sys.exit(0)
if query.startswith("vacuum"):
    warning = fixtures.get("__vacuum_warning")
    if warning is True:
        warning = 'WARNING:  permission denied to vacuum "provider_traces", skipping it'
    # client_min_messages, strongest first: a SET in this session; the connection's startup options, where a
    # connection string's own `options` (fixture "__dsn_client_min_messages") replaces PGOPTIONS; the role default
    # (fixture "__role_client_min_messages")
    pgoptions = re.findall(r"-c client_min_messages=(\w+)", os.environ.get("PGOPTIONS", ""))
    level = (session.get("client_min_messages") or fixtures.get("__dsn_client_min_messages")
             or (pgoptions[-1] if pgoptions else None) or fixtures.get("__role_client_min_messages", "notice"))
    if warning and level != "error":  # for client messages only error ranks above warning (log is below notice)
        sys.stderr.write(warning + "\n")
    sys.exit(0)
if query.startswith("select id from provider_traces where id in ("):
    wanted = set(re.findall(r"'([^']*)'", query))
    print("\n".join(r["id"] for r in fixtures["provider_traces"] if r["id"] in wanted))
    sys.exit(0)
if query.startswith("delete from provider_traces t using (values "):
    values = re.search(r"using \(values (.*)\) as v\(id, md5\) where ", query).group(1)
    pairs = re.findall(r"\('([^']*)', '([^']*)'\)", values)
    cutoff = re.search(r"t\.created_at < '([^']+)'", query)
    check_md5 = "md5(t::text) = v.md5" in query  # only what the statement asks for is enforced
    rows, deleted = fixtures["provider_traces"], []
    for trace_id, digest in pairs:
        for r in rows:
            if r["id"] == trace_id and (not check_md5 or row_md5(r) == digest) and (
                    not cutoff or (r.get("created_at") is not None and r["created_at"] < cutoff.group(1))):
                deleted.append(r)
    fixtures["provider_traces"] = [r for r in rows if r not in deleted]
    with open(fixtures_path, "w") as f:
        json.dump(fixtures, f)
    print("\n".join(r["id"] for r in deleted))
    sys.exit(0)
tables = [t for t in re.findall(r"\bfrom (\w+)", query) if t in fixtures]
if not tables:
    sys.exit(f"fake_psql: no fixture for query {query[:80]!r}")
rows = fixtures[tables[0]]
# created_at bounds from where_clause (text comparison is enough for the fixtures' "YYYY-MM-DD hh:mm..+09" values)
for op, bound in re.findall(r"created_at (<|>=) '([^']+)'", query):
    rows = [r for r in rows if r.get("created_at") is not None and (r["created_at"] < bound) == (op == "<")]
if "to_char(created_at, 'YYYY-MM-DD') as day, count(*) as rows" in query:  # day_counts
    days = {}
    for r in rows:
        days[r["created_at"][:10]] = days.get(r["created_at"][:10], 0) + 1
    sys.stdout.write("day,rows\n" + "".join(f"{d},{n}\n" for d, n in sorted(days.items())))
    sys.exit(0)
if not rows:
    sys.exit(0)  # real psql would still print the header; csv.DictReader yields no rows either way
null = re.search(r"null '([^']*)'", query)  # psql's `null '<string>'` option (default for CSV: empty)
null_text = null.group(1) if null else ""
digests = [row_md5(r) for r in rows]  # of the stored row, before the extra columns
mask = re.search(r"\) as (__\w+_null_mask)\b", query)  # the per-column NULL mask: concat((c is null)::int, ...)
if mask:
    rows = [{**r, mask.group(1): "".join("1" if v is None else "0" for v in r.values())} for r in rows]
md5 = re.search(r"md5\(provider_traces::text\) as (\w+)", query)
if md5:
    rows = [{**r, md5.group(1): d} for r, d in zip(rows, digests)]


def field(value):
    """Like psql CSV with force_quote *: every non-NULL value quoted, NULL written unquoted as the null string."""
    return null_text if value is None else '"' + str(value).replace('"', '""') + '"'


sys.stdout.write(",".join(rows[0]) + "\n")
for r in rows:
    sys.stdout.write(",".join(field(v) for v in r.values()) + "\n")
