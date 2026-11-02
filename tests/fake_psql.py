#!/usr/bin/env python3
"""Stand-in for psql in tests: `fake_psql.py <fixtures.json> -X -q -c "\\copy (select ... from <table> ...) ..."`.
Prints the fixture rows of the first table named in the query as CSV with a header."""
import csv, json, re, sys

fixtures = json.load(open(sys.argv[1]))
query = sys.argv[sys.argv.index("-c") + 1]
tables = [t for t in re.findall(r"\bfrom (\w+)", query) if t in fixtures]
if not tables:
    sys.exit(f"fake_psql: no fixture for query {query[:80]!r}")
rows = fixtures[tables[0]]
null = re.search(r"null '([^']*)'", query)  # like psql's `null '<marker>'` option: None is written as the marker
if null:
    rows = [{k: null.group(1) if v is None else v for k, v in r.items()} for r in rows]
writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0]))
writer.writeheader()
writer.writerows(rows)
