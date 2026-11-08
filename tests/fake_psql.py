#!/usr/bin/env python3
"""Stand-in for psql in tests: `fake_psql.py <fixtures.json> -X -q -c "\\copy (select ... from <table> ...) ..."`.
Prints the fixture rows of the first table named in the query as CSV with a header."""
import json, re, sys

fixtures = json.load(open(sys.argv[1]))
sys.stdout.reconfigure(encoding="utf-8", errors="surrogateescape")  # a "\udcff" fixture goes out as the byte 0xff
query = sys.argv[sys.argv.index("-c") + 1]
if "from information_schema.columns" in query:  # `-A -t` column-name listing, one per line
    table = re.search(r"table_name = '(\w+)'", query).group(1)
    print("\n".join(fixtures[table][0]))
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
mask = re.search(r"\) as (__\w+_null_mask) from", query)  # the per-column NULL mask: concat((c is null)::int, ...)
if mask:
    rows = [{**r, mask.group(1): "".join("1" if v is None else "0" for v in r.values())} for r in rows]


def field(value):
    """Like psql CSV with force_quote *: every non-NULL value quoted, NULL written unquoted as the null string."""
    return null_text if value is None else '"' + str(value).replace('"', '""') + '"'


sys.stdout.write(",".join(rows[0]) + "\n")
for r in rows:
    sys.stdout.write(",".join(field(v) for v in r.values()) + "\n")
