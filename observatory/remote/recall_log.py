# Recall runtime log: whitelisted event templates only (ids hashed, numbers and tool names kept).
# Config: RECALL_LOG_GLOBS (comma-separated glob patterns)
import glob

files = sorted({f for pat in CONFIG["RECALL_LOG_GLOBS"].split(",") if pat for f in glob.glob(pat)})
PATTERNS = [
    ("start", re.compile(r"^Starting recall (\S+) in isolated conversation")),
    ("prefetch", re.compile(r"^Recall (\S+) prefetched evidence: relationship=(\d+), transcript_hits=(\d+)")),
    ("trusted_tool", re.compile(r"^Recall (\S+) trusted tool execute: ([a-z_]+)$")),
    ("stream_tool", re.compile(r"^Recall (\S+) stream tool: ([a-z_]+)$")),
    ("terminal", re.compile(r"^Recall (\S+) terminal delivery received")),
    ("failure", re.compile(r"^Recall (\S+) model/transport failure")),
    ("drain_error", re.compile(r"^Recall (\S+) drain after terminal")),
    ("cleanup_fail", re.compile(r"^Recall conversation cleanup (?:returned|failed)()")),
]
events = Table("recall_events", [("file_idx", "int"), ("ts", "ts"), ("event", "enum"), ("rid", "hash"), ("a", "enum"), ("b", "enum")])
for i, path in enumerate(files):
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m0 = re.match(r"^\[([^\]]+)\] (.*)$", line.rstrip("\n"))
            if not m0:
                continue
            ts, msg = m0.groups()
            for name, rx in PATTERNS:
                m = rx.match(msg)
                if m:
                    g = m.groups()
                    events.add(i, ts, name, g[0] if g else None, g[1] if len(g) > 1 else None, g[2] if len(g) > 2 else None)
                    break
emit("recall_log", [events], {"n_files": len(files)})
