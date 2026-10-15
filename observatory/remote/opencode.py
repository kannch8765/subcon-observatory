# opencode gateway request logs: the console's per-request CSV download (there is no request-log API).
# Runs locally (the CSVs are on this machine). Overlapping downloads are deduped by request id.
# Dropped entirely: errorMessage, user agent, account/key/connection ids, geo, request/response headers,
# metadata, attempts, policies. `sessionID` (= Letta conversation id) is hashed like the letta exporter,
# so rows join to letta runs.conversation.
# Config: OPENCODE_CSV_GLOBS
import glob

csv.field_size_limit(1 << 30)  # header columns hold whole request bodies


def ms_ts(value):
    return int(value) / 1000 if isinstance(value, str) and value.isdigit() else value


requests = Table("requests", [
    ("rid", "hash"), ("started_at", "ts"), ("finished_at", "ts"), ("duration_ms", "int"), ("ttft_ms", "int"),
    ("outcome", "enum"), ("category", "enum"), ("protocol", "enum"), ("product", "enum"), ("stream", "bool"),
    ("conversation", "hash"), ("requested_model", "ident"), ("model", "ident"), ("provider", "enum"),
    ("status_code", "int"), ("error_code", "enum"), ("finish", "enum"), ("reasoning_effort", "enum"),
    ("input_tokens", "int"), ("output_tokens", "int"), ("reasoning_tokens", "int"),
    ("cache_read_tokens", "int"), ("cache_write_tokens", "int"), ("cache_write_1h_tokens", "int"),
    ("cost", "float"), ("provider_cost", "float"), ("attempt_count", "int"),
])

paths = sorted({p for g in CONFIG.get("OPENCODE_CSV_GLOBS", "").split(",") if g.strip() for p in glob.glob(g.strip())})
seen = set()
for path in paths:
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        for r in csv.DictReader(f):
            rid = r.get("requestID") or r.get("id")
            if not rid or rid in seen:
                continue
            seen.add(rid)
            requests.add(rid, ms_ts(r.get("startedAt")), ms_ts(r.get("finishedAt")), r.get("durationMs"), r.get("timeToFirstTokenMs"),
                         r.get("outcome"), r.get("category"), r.get("protocol"), r.get("product"), r.get("stream"),
                         r.get("sessionID"), r.get("requestedModel"), r.get("model"), r.get("provider"),
                         r.get("statusCode"), r.get("errorCode"), r.get("finish"), r.get("reasoningEffort"),
                         r.get("inputTokens"), r.get("outputTokens"), r.get("reasoningTokens"),
                         r.get("cacheReadTokens"), r.get("cacheWriteTokens"), r.get("cacheWrite1hTokens"),
                         r.get("cost"), r.get("providerCost"), r.get("attemptCount"))

emit("opencode", [requests], {"files": len(paths)})
