# Letta server metadata: steps, runs, messages and core-memory blocks. Sizes, ids and enums only.
# Config: see _letta.py

steps = Table("steps", [("step", "hash"), ("created_at", "ts"), ("run", "hash"), ("agent", "hash"), ("model", "ident"),
                        ("status", "ident"), ("stop_reason", "ident"), ("error_type", "ident"), ("prompt_tokens", "int"),
                        ("completion_tokens", "int"), ("reasoning_tokens", "int"), ("cached_input_tokens", "int"),
                        ("context_window_limit", "int"), ("llm_request_ms", "int"), ("tool_execution_ms", "int"), ("step_ms", "int")])
for r in psql_rows(f"""select s.id, s.created_at, s.run_id, s.agent_id, s.model, s.status, s.stop_reason, s.error_type,
        s.prompt_tokens, s.completion_tokens, s.reasoning_tokens, s.cached_input_tokens, s.context_window_limit,
        m.llm_request_ns, m.tool_execution_ns, m.step_ns
    from steps s left join step_metrics m on m.id = s.id {letta_since('s.created_at')} order by s.created_at"""):
    steps.add(r["id"], r["created_at"], r["run_id"], r["agent_id"], r["model"], r["status"], r["stop_reason"], r["error_type"],
              r["prompt_tokens"], r["completion_tokens"], r["reasoning_tokens"], r["cached_input_tokens"], r["context_window_limit"],
              ns_to_ms(r["llm_request_ns"]), ns_to_ms(r["tool_execution_ns"]), ns_to_ms(r["step_ns"]))

runs = Table("runs", [("run", "hash"), ("created_at", "ts"), ("agent", "hash"), ("conversation", "hash"), ("status", "ident"),
                      ("stop_reason", "ident"), ("background", "bool"), ("ttft_ms", "int"), ("total_ms", "int"), ("run_ms", "int"),
                      ("num_steps", "int"), ("tools_used", "idents")])
for r in psql_rows(f"""select r.id, r.created_at, r.agent_id, r.conversation_id, r.status, r.stop_reason, r.background,
        r.ttft_ns, r.total_duration_ns, rm.run_ns, rm.num_steps, rm.tools_used::text as tools_used
    from runs r left join run_metrics rm on rm.id = r.id {letta_since('r.created_at')} order by r.created_at"""):
    try:
        tools = json.loads(r["tools_used"]) if r["tools_used"] else None
    except ValueError:
        tools = r["tools_used"]  # not a list -> redacted
    runs.add(r["id"], r["created_at"], r["agent_id"], r["conversation_id"], r["status"], r["stop_reason"], r["background"],
             ns_to_ms(r["ttft_ns"]), ns_to_ms(r["total_duration_ns"]), ns_to_ms(r["run_ns"]), r["num_steps"], tools)

# Tool returns are read only to count the memory / snippet ids they carry.
MEMORY_ID = re.compile(r"memory_id[^A-Za-z0-9]+([A-Za-z0-9_-]{8,})")
SNIPPET_ID = re.compile(r"snippet_id[^A-Za-z0-9]+([A-Za-z0-9_-]{4,})")
messages = Table("messages", [("message", "hash"), ("created_at", "ts"), ("agent", "hash"), ("conversation", "hash"), ("run", "hash"),
                              ("step", "hash"), ("role", "enum"), ("name", "ident"), ("content_chars", "int"), ("tool_calls", "idents"),
                              ("is_err", "bool"), ("sequence_id", "int"), ("n_memory_ids", "int"), ("n_snippet_ids", "int"),
                              ("tool_status", "idents"), ("tool_error", "idents")])

# Fixed error messages thrown by Subcon's client tools, exported as template names. Anything else is `other`.
# Letta wraps them in a JSON `func_response`, so they are searched for, not anchored.
TOOL_ERROR_TEMPLATES = [
    ("whisper_called_twice", re.compile(r"deliver_whisper may be called at most once per batch")),
    ("whisper_not_surfaced", re.compile(r"deliver_whisper may select only one memory and quote snippets surfaced by a prior")),
    ("whisper_bad_args", re.compile(r"deliver_whisper requires one searched memory_id and 1-3 unique snippet_ids")),
    ("whisper_no_summary", re.compile(r"historical memory whisper requires a surfaced canonical summary")),
    ("maintenance_unseen_ids", re.compile(r"\w+ may reference only memory_ids returned by a prior purpose=maintenance memory_search")),
    ("intent_mismatch", re.compile(r"Trusted assistant intent payload/store mismatch")),
]


def tool_returns(body):
    """(`status` of each tool return, error template of each failed one). Client-tool approvals report errors
    here, not in is_err. Error text never leaves: it's only matched against TOOL_ERROR_TEMPLATES."""
    try:
        value = json.loads(body)
    except ValueError:
        return None, None
    items = [item for item in (value if isinstance(value, list) else [value]) if isinstance(item, dict)]
    statuses = [item["status"] for item in items if isinstance(item.get("status"), str)]
    errors = []
    for item in items:
        if item.get("status") == "error":
            text = next((item[k] for k in ("func_response", "tool_return") if isinstance(item.get(k), str)), "")
            errors.append(next((name for name, rx in TOOL_ERROR_TEMPLATES if rx.search(text)), "other"))
    return statuses or None, errors or None


for r in psql_rows(f"""select id, created_at, agent_id, conversation_id, run_id, step_id, role, name,
        length(coalesce(text, content::text)) as content_chars,
        (select string_agg(tc->'function'->>'name', '>') from json_array_elements(
            case when json_typeof(tool_calls) = 'array' then tool_calls else '[]'::json end) tc) as tool_calls,
        is_err, sequence_id,
        case when role in ('tool', 'approval') then coalesce(tool_returns::text, content::text) end as tool_body
    from messages {letta_since('created_at')} order by created_at"""):
    body = r["tool_body"] or ""
    statuses, errors = tool_returns(body) if body else (None, None)
    messages.add(r["id"], r["created_at"], r["agent_id"], r["conversation_id"], r["run_id"], r["step_id"], r["role"], r["name"],
                 r["content_chars"], r["tool_calls"].split(">") if r["tool_calls"] else None, r["is_err"], r["sequence_id"],
                 len(set(MEMORY_ID.findall(body))) if body else None, len(set(SNIPPET_ID.findall(body))) if body else None,
                 statuses, errors)

blocks = Table("blocks", [("block", "hash"), ("agent", "hash"), ("label", "ident"), ("chars", "int"), ("limit", "int"),
                          ("created_at", "ts"), ("updated_at", "ts")])
for r in psql_rows("""select b.id, ba.agent_id, b.label, length(b.value) as chars, b."limit", b.created_at, b.updated_at
    from block b join blocks_agents ba on ba.block_id = b.id order by ba.agent_id, b.label"""):
    blocks.add(r["id"], r["agent_id"], r["label"], r["chars"], r["limit"], r["created_at"], r["updated_at"])

block_history = Table("block_history", [("block", "hash"), ("sequence_number", "int"), ("chars", "int"), ("actor_type", "ident"),
                                        ("created_at", "ts")])
try:
    for r in psql_rows("""select block_id, sequence_number, length(value) as chars, actor_type, created_at
        from block_history order by created_at"""):
        block_history.add(r["block_id"], r["sequence_number"], r["chars"], r["actor_type"], r["created_at"])
    history_note = "ok"
except RuntimeError:
    history_note = "block_history query failed (table missing in this Letta version?)"

emit("letta", [steps, runs, messages, blocks, block_history], {"since": CONFIG.get("LETTA_SINCE") or None, "block_history": history_note})
