#!/usr/bin/env python3
"""Stand-in for psql in tests: `fake_psql.py <fixtures.json> -X -q -c "\\copy (select ... from <table> ...) ..."`.
Prints the fixture rows of the first table named in the query as CSV with a header. Also takes --delete's two writes:
`delete from provider_traces t using (values ...) ... returning t.id` (rewrites the fixtures file, prints the deleted
ids), `vacuum ...` (no-op; with "__vacuum_warning" in the fixtures it warns on stderr like PG does when it skips a
table) and `select id from provider_traces where id in (...)` (`-A -t`: the ids that exist). Every query is appended
to <fixtures.json>.log, one per line."""
import hashlib, json, re, sys

fixtures_path = sys.argv[1]
fixtures = json.load(open(fixtures_path))
sys.stdout.reconfigure(encoding="utf-8", errors="surrogateescape")  # a "\udcff" fixture goes out as the byte 0xff
query = sys.argv[sys.argv.index("-c") + 1]
with open(fixtures_path + ".log", "a") as log:
    log.write(query.replace("\n", " ") + "\n")


def row_md5(row):
    """Stands in for Postgres's md5(t::text): any change to any column changes it."""
    return hashlib.md5(json.dumps(row).encode()).hexdigest()


if "from information_schema.columns" in query:  # `-A -t` column-name listing, one per line
    table = re.search(r"table_name = '(\w+)'", query).group(1)
    print("\n".join(fixtures[table][0]))
    sys.exit(0)
if query.startswith("vacuum"):
    if fixtures.get("__vacuum_warning"):
        sys.stderr.write('WARNING:  permission denied to vacuum "provider_traces", skipping it\n')
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
