# Sanitizing table writer, shared by every remote exporter.
#
# Remote exporters are stdlib-only. The runner concatenates this file + one exporter and pipes the
# result into `python3 -` on the server, so nothing gets installed there. The output is a tar.gz
# stream on stdout. Every column is typed, and a value that doesn't fit its type is redacted (and
# counted) instead of exported. Free text has no column type, so it cannot leave the server.
import csv, datetime, hashlib, io, json, os, re, sys, tarfile, time

CONFIG = globals().get("CONFIG", {})

_TS = re.compile(r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?(Z|[+-]\d{2}(:?\d{2})?)?$")
_ENUM = re.compile(r"^[a-z0-9_.:-]{1,48}$")
_IDENT = re.compile(r"^[A-Za-z0-9_.:/-]{1,96}$")  # tool/model/error names: `Bash`, `mcp__a__b`, `org/Model-1`; never whitespace
_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,47}$")
REDACTED = "<redacted>"


def h(value):
    """Stable 12-hex id hash; compatible with the 2026-10-14 snapshots (unsalted sha256)."""
    return hashlib.sha256(str(value).encode()).hexdigest()[:12] if value not in (None, "") else ""


def _length(value):
    if value is None:
        return ""
    return len(value) if isinstance(value, (str, list, dict)) else -1


class Table:
    """CSV table whose columns are (name, type).
    Types: hash, int, float, ts, enum, ident, idents (list -> `a>b`), len, shape, opts, bool."""

    def __init__(self, name, columns):
        self.name, self.columns = name, columns
        self.buf = io.StringIO()
        self.writer = csv.writer(self.buf)
        self.writer.writerow([c for c, _ in columns])
        self.rows = 0
        self.redacted = 0

    def _cell(self, kind, value):
        if value is None or value == "":
            return ""
        if kind == "hash":
            return h(value)
        if kind == "len":
            return _length(value)
        if kind == "bool":
            if isinstance(value, str):  # Postgres CSV booleans
                value = value.lower() in ("t", "true", "1")
            return int(bool(value))
        if kind in ("int", "float"):
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, (int, float)):
                return value if kind == "float" else int(value)
            if isinstance(value, str) and re.fullmatch(r"-?\d+(\.\d+)?", value):
                return value
        elif kind == "ts":
            if isinstance(value, (int, float)):
                return datetime.datetime.fromtimestamp(value, datetime.timezone.utc).isoformat()
            if isinstance(value, str) and _TS.match(value):
                return value
        elif kind == "enum":
            if isinstance(value, str) and _ENUM.match(value):
                return value
        elif kind == "ident":
            if isinstance(value, str) and _IDENT.match(value):
                return value
        elif kind == "idents":
            if isinstance(value, list) and all(isinstance(v, str) and _IDENT.match(v) for v in value):
                return ">".join(value)
        elif kind == "shape":
            # {key: length}; keys must look like identifiers, values are never exported
            if isinstance(value, dict) and all(isinstance(k, str) and _KEY.match(k) for k in value):
                return json.dumps({k: _length(v) for k, v in value.items()}, sort_keys=True)
        elif kind == "opts":
            # {key: option}; options must be numbers, booleans, enums or lists of enums
            def ok(v):
                if isinstance(v, (bool, int, float)):
                    return True
                if isinstance(v, str):
                    return bool(_ENUM.match(v))
                return isinstance(v, list) and all(isinstance(x, str) and _ENUM.match(x) for x in v)
            if isinstance(value, dict) and all(isinstance(k, str) and _KEY.match(k) and ok(v) for k, v in value.items()):
                return json.dumps(value, sort_keys=True)
        else:
            raise ValueError(f"unknown column type {kind!r}")
        self.redacted += 1
        return REDACTED

    def add(self, *values):
        if len(values) != len(self.columns):
            raise ValueError(f"{self.name}: expected {len(self.columns)} values, got {len(values)}")
        self.writer.writerow([self._cell(kind, v) for (_, kind), v in zip(self.columns, values)])
        self.rows += 1


def read_jsonl(path):
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            try:
                yield json.loads(line)
            except Exception:
                pass


def log(*args):
    print(*args, file=sys.stderr)


def emit(exporter, tables, notes=None):
    """Write all tables + a manifest as a tar.gz stream to stdout."""
    manifest = {
        "exporter": exporter,
        "exported_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "tables": {t.name: {"rows": t.rows, "redacted_cells": t.redacted} for t in tables},
        "notes": notes or {},
    }
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        files = [(f"{t.name}.csv", t.buf.getvalue()) for t in tables]
        files.append(("manifest.json", json.dumps(manifest, indent=2)))
        for name, text in files:
            data = text.encode()
            info = tarfile.TarInfo(f"{exporter}/{name}")
            info.size, info.mtime = len(data), int(time.time())
            tar.addfile(info, io.BytesIO(data))
    sys.stdout.buffer.write(out.getvalue())
    sys.stdout.flush()
    for t in tables:
        log(f"  {exporter}/{t.name}: {t.rows} rows, {t.redacted} redacted cells")
