# Whisper queue files: latency, sizes and delivery state; never the whisper text.
# Config: WHISPER_DIRS (comma-separated)
import glob, os

roots = [d for d in CONFIG["WHISPER_DIRS"].split(",") if d]
whispers = Table("whispers", [("root_idx", "int"), ("created_at", "ts"), ("source", "enum"), ("ready_latency_ms", "int"),
                              ("text_chars", "len"), ("delivered", "bool"), ("mtime", "ts"), ("batch_prefix", "enum"),
                              ("has_memory_id", "bool")])
for i, d in enumerate(roots):
    for p in glob.glob(os.path.join(d, "*", "*")):
        try:
            with open(p) as f:
                r = json.load(f)
        except Exception:
            continue
        whispers.add(i, r.get("created_at"), r.get("source"), r.get("ready_latency_ms"), r.get("text") or "",
                     p.endswith(".delivered"), os.path.getmtime(p), (r.get("batch_id") or "").rsplit("_", 1)[0] or None,
                     bool(r.get("memory_id")))
emit("whispers", [whispers], {"n_roots": len(roots), "roots_found": sum(os.path.isdir(d) for d in roots)})
