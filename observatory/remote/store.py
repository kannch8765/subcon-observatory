# Subcon relationship-memory store: hashed ids, enums, lengths, timestamps.
# Config: STORE_DIR
import os

D = CONFIG["STORE_DIR"]
P = lambda name: os.path.join(D, name)
prefix = lambda b: (b or "").rsplit("_", 1)[0] or None

memories = Table("memories", [("mid", "hash"), ("kind", "enum"), ("status", "enum"), ("observed_at", "ts"), ("created_at", "ts"),
                              ("summary_chars", "len"), ("payload_lens", "shape"), ("n_participants", "len"), ("n_linked", "len")])
for r in read_jsonl(P("memories.jsonl")):
    payload = r.get("payload") if isinstance(r.get("payload"), dict) else {}
    memories.add(r.get("memory_id"), r.get("kind"), r.get("status"), r.get("observed_at"), r.get("created_at"),
                 r.get("summary"), payload, r.get("participants"), r.get("linked_memory_ids"))

batches = Table("batches", [("batch_prefix", "enum"), ("bid", "hash"), ("attempt", "hash"), ("status", "enum"), ("detail", "enum"),
                            ("created_at", "ts"), ("finalized_at", "ts"), ("retry_reasons", "idents")])
for r in read_jsonl(P("batches.jsonl")):
    batches.add(prefix(r.get("batch_id")), r.get("batch_id"), r.get("attempt_id"), r.get("status"), r.get("detail"),
                r.get("created_at"), r.get("finalized_at"), r.get("retry_reasons"))

outcome_cols = [("mid", "hash"), ("batch_prefix", "enum"), ("bid", "hash"), ("attempt", "hash"), ("outcome", "enum"),
                ("rejection_code", "enum"), ("recorded_at", "ts")]
outcomes = Table("outcomes", outcome_cols)
for r in read_jsonl(P("outcomes.jsonl")):
    outcomes.add(r.get("memory_id"), prefix(r.get("batch_id")), r.get("batch_id"), r.get("attempt_id"), r.get("outcome"),
                 r.get("rejection_code"), r.get("recorded_at"))
entity_outcomes = Table("entity_outcomes", [("eid", "hash")] + outcome_cols[1:])
for r in read_jsonl(P("entity-outcomes.jsonl")):
    entity_outcomes.add(r.get("entity_id"), prefix(r.get("batch_id")), r.get("batch_id"), r.get("attempt_id"), r.get("outcome"),
                        r.get("rejection_code"), r.get("recorded_at"))

evidence = Table("evidence", [("mid", "hash"), ("captured_at", "ts"), ("role", "enum"), ("event_kind", "enum"), ("tool_name", "ident"),
                              ("quote_chars", "len")])
for r in read_jsonl(P("evidence.jsonl")):
    evidence.add(r.get("memory_id"), r.get("captured_at"), r.get("role"), r.get("event_kind"), r.get("tool_name"), r.get("quote"))

reinforcements = Table("reinforcements", [("mid", "hash"), ("batch_prefix", "enum"), ("n_evidence", "len"), ("recorded_at", "ts")])
for r in read_jsonl(P("reinforcements.jsonl")):
    reinforcements.add(r.get("memory_id"), prefix(r.get("batch_id")), r.get("evidence_ids"), r.get("recorded_at"))

intents = Table("intents", [("iid", "hash"), ("session", "hash"), ("tool_name", "ident"), ("memory_chars", "len"), ("feel_chars", "len"),
                            ("captured_at", "ts")])
for r in read_jsonl(P("assistant-intents.jsonl")):
    intents.add(r.get("intent_id"), r.get("session_id"), r.get("tool_name"), ((r.get("memory") or {}).get("text")),
                ((r.get("feel") or {}).get("text")), r.get("captured_at"))

intent_outcomes = Table("intent_outcomes", [("iid", "hash")] + outcome_cols[1:])
for r in read_jsonl(P("assistant-intent-outcomes.jsonl")):
    intent_outcomes.add(r.get("intent_id"), prefix(r.get("batch_id")), r.get("batch_id"), r.get("attempt_id"), r.get("outcome"),
                        r.get("rejection_code"), r.get("recorded_at"))

files = sorted(f for f in os.listdir(D) if f.endswith(".jsonl")) if os.path.isdir(D) else []
emit("store", [memories, batches, outcomes, entity_outcomes, evidence, reinforcements, intents, intent_outcomes],
     {"store_files": files})
