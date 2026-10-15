# Per-session processing backlog: transcript line count vs the Subcon cursor. Numbers only.
# Config: CLAUDE_HOME (home dir of the Claude-P user; holds .claude/projects and .letta/claude)
import glob, os

H = CONFIG["CLAUDE_HOME"]
state_dir = os.path.join(H, ".letta", "claude")
transcripts = {os.path.basename(p)[:-6]: p for p in glob.glob(os.path.join(H, ".claude", "projects", "*", "*.jsonl"))}

backlog = Table("backlog", [("session", "hash"), ("last_processed_index", "int"), ("started_at", "ts"), ("n_delivered", "len"),
                            ("n_written", "len"), ("retry_through", "int"), ("retry_marked_at", "ts"), ("transcript_found", "bool"),
                            ("transcript_msgs", "int"), ("transcript_mtime", "ts"), ("state_mtime", "ts")])
for p in glob.glob(os.path.join(state_dir, "session-*.json")):
    if p.endswith(".conversation-retry.json"):
        continue
    try:
        with open(p) as f:
            s = json.load(f)
    except Exception:
        continue
    sid = s.get("sessionId") or os.path.basename(p)[8:-5]
    retry = {}
    rp = os.path.join(state_dir, f"session-{sid}.conversation-retry.json")
    if os.path.exists(rp):
        try:
            with open(rp) as f:
                retry = json.load(f)
        except Exception:
            pass
    tp = transcripts.get(sid)
    count = sum(1 for _ in read_jsonl(tp)) if tp else None
    backlog.add(sid, s.get("lastProcessedIndex"), s.get("startedAt"), s.get("deliveredMemoryIds") or [],
                s.get("sessionWrittenMemoryIds") or [], retry.get("throughIndex"), retry.get("markedAt"), bool(tp), count,
                os.path.getmtime(tp) if tp else None, os.path.getmtime(p))
emit("backlog", [backlog], {"transcripts_indexed": len(transcripts)})
