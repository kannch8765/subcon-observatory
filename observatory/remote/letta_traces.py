# Letta provider_traces: per-LLM-call usage and shape, plus per-tool-call argument shapes.
# provider_traces is large (GBs), so this is a separate exporter; use LETTA_SINCE to bound it.
# JSON is parsed here, not in Postgres: broken surrogates in stored payloads make PG's json functions fail.
# Config: see _letta.py

# Option keys whose short scalar values are kept (still validated as enums / numbers by the sanitizer)
SAFE_OPTS = {"purpose", "limit", "top_k", "k", "mode", "kind", "kinds", "status", "include_evidence", "scope", "max_results",
             "relation", "relation_type"}

traces = Table("traces", [("trace", "hash"), ("created_at", "ts"), ("step", "hash"), ("run", "hash"), ("agent", "hash"),
                          ("call_type", "ident"), ("model", "ident"), ("n_messages", "int"), ("n_sys", "int"), ("n_user", "int"),
                          ("n_assistant", "int"), ("n_tool_msgs", "int"), ("request_chars", "int"), ("n_tools_offered", "int"),
                          ("tool_choice", "ident"), ("max_completion_tokens", "int"), ("finish_reason", "ident"),
                          ("tool_calls", "idents"), ("resp_content_chars", "len"), ("resp_reasoning_chars", "len"),
                          ("prompt_tokens", "int"), ("completion_tokens", "int"), ("reasoning_tokens", "int"),
                          ("cached_tokens", "int"), ("parse_error", "enum")])
tool_calls = Table("tool_calls", [("trace", "hash"), ("step", "hash"), ("created_at", "ts"), ("model", "ident"), ("ord", "int"),
                                  ("tool", "ident"), ("arg_chars", "int"), ("arg_lens", "shape"), ("arg_opts", "opts"),
                                  ("query_words", "int"), ("parse_ok", "bool")])


def load(text):
    try:
        value = json.loads(text) if text else {}
        return (value if isinstance(value, dict) else {}), ""
    except ValueError:
        return {}, "bad_json"


for r in psql_rows(f"""select id, created_at, step_id, run_id, agent_id, call_type,
        request_json::text as req, response_json::text as resp
    from provider_traces {letta_since('created_at')} order by created_at"""):
    req, req_err = load(r["req"])
    resp, resp_err = load(r["resp"])
    msgs = req.get("messages") if isinstance(req.get("messages"), list) else []
    roles = [m.get("role") for m in msgs if isinstance(m, dict)]
    choices = resp.get("choices") if isinstance(resp.get("choices"), list) and resp.get("choices") else [{}]
    choice = choices[0] if isinstance(choices[0], dict) else {}
    msg = choice.get("message") if isinstance(choice.get("message"), dict) else {}
    calls = [c for c in (msg.get("tool_calls") if isinstance(msg.get("tool_calls"), list) else []) if isinstance(c, dict)]
    usage = resp.get("usage") if isinstance(resp.get("usage"), dict) else {}
    tool_choice = req.get("tool_choice")
    model = req.get("model")
    traces.add(r["id"], r["created_at"], r["step_id"], r["run_id"], r["agent_id"], r["call_type"], model, len(msgs),
               roles.count("system"), roles.count("user"), roles.count("assistant"), roles.count("tool"), len(r["req"] or ""),
               len(req.get("tools")) if isinstance(req.get("tools"), list) else 0,
               tool_choice.get("type") if isinstance(tool_choice, dict) else tool_choice, req.get("max_completion_tokens"),
               choice.get("finish_reason"), [(c.get("function") or {}).get("name") or "unknown" for c in calls],
               msg.get("content"), msg.get("reasoning_content"), usage.get("prompt_tokens"), usage.get("completion_tokens"),
               (usage.get("completion_tokens_details") or {}).get("reasoning_tokens"),
               (usage.get("prompt_tokens_details") or {}).get("cached_tokens"), "_".join(filter(None, [req_err, resp_err])) or None)
    for i, call in enumerate(calls):
        fn = call.get("function") or {}
        raw = fn.get("arguments") or ""
        try:
            args = json.loads(raw) if isinstance(raw, str) else raw
            parsed = isinstance(args, dict)
        except ValueError:
            args, parsed = {}, False
        args = args if isinstance(args, dict) else {}
        # drop free-text values of safe keys up front, so one bad value doesn't redact the whole cell
        opts = {k: v for k, v in args.items() if k in SAFE_OPTS and (
            isinstance(v, (bool, int, float)) or (isinstance(v, str) and _ENUM.match(v))
            or (isinstance(v, list) and all(isinstance(x, str) and _ENUM.match(x) for x in v)))}
        query = args.get("query")
        tool_calls.add(r["id"], r["step_id"], r["created_at"], model, i, fn.get("name") or "unknown",
                       len(raw) if isinstance(raw, str) else None, args, opts,
                       len(query.split()) if isinstance(query, str) else None, parsed)

emit("letta_traces", [traces, tool_calls], {"since": CONFIG.get("LETTA_SINCE") or None})
