"""End-to-end: run every exporter locally against a fake deployment full of secret text,
then check that none of the secrets reach the snapshot and that structure survives."""
import csv, json, os, subprocess

import pytest

from observatory.export import EXPORTERS, export

SECRET = "SECRET-いちご-text"
SECRETS = [SECRET, "Kohaku said hello", "my private feeling", "/root/private-dir-name", "Very Secret Commit"]


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\nnot json at all\n")


@pytest.fixture
def deployment(tmp_path):
    store = tmp_path / "store"
    write_jsonl(store / "memories.jsonl", [{"memory_id": "m1", "kind": "preference", "status": "active", "created_at": "2026-10-01T00:00:00Z",
                                            "summary": SECRET, "payload": {"text": SECRET, "Kohaku said hello": 1}, "participants": ["a", "b"]}])
    write_jsonl(store / "batches.jsonl", [{"batch_id": "batch_abc", "attempt_id": "at1", "status": "retryable_failure",
                                           "created_at": "2026-10-15T00:00:00Z", "finalized_at": "2026-10-15T00:01:00Z"}])
    write_jsonl(store / "outcomes.jsonl", [{"memory_id": "m1", "batch_id": "batch_abc", "outcome": "rejected",
                                            "rejection_code": "Kohaku said hello", "reason": SECRET, "recorded_at": "2026-10-15T00:00:30Z"}])
    write_jsonl(store / "evidence.jsonl", [
        {"memory_id": "m1", "role": "user", "quote": SECRET, "captured_at": "2026-10-01T00:00:00Z", "tool_name": "Bash"},
        {"memory_id": "m1", "role": "assistant", "tool_name": "mcp__kohaku-relationship-memory__memory_remember_with_feeling_v2"},
        {"memory_id": "m1", "role": "assistant", "tool_name": "Kohaku said hello"},
    ])
    write_jsonl(store / "assistant-intents.jsonl", [{"intent_id": "i1", "session_id": "s1", "tool_name": "memory_remember",
                                                     "memory": {"text": SECRET}, "feel": {"text": "my private feeling"}, "captured_at": SECRET}])
    write_jsonl(store / "assistant-intent-outcomes.jsonl", [{"intent_id": "i1", "batch_id": "batch_abc", "outcome": "deferred",
                                                             "reason": SECRET, "recorded_at": "2026-10-15T00:01:00Z"}])
    whispers = tmp_path / "whispers"
    (whispers / "s1").mkdir(parents=True)
    (whispers / "s1" / "w1.json.delivered").write_text(json.dumps({"text": SECRET, "source": "sync", "ready_latency_ms": 33000}))
    home = tmp_path / "home"
    write_jsonl(home / ".claude" / "projects" / "p" / "s1.jsonl", [{"message": SECRET}] * 5)
    (home / ".letta" / "claude").mkdir(parents=True)
    (home / ".letta" / "claude" / "session-s1.json").write_text(json.dumps({"sessionId": "s1", "lastProcessedIndex": 2, "note": SECRET}))
    log = tmp_path / "recall" / "recall_runtime.log"
    log.parent.mkdir()
    log.write_text(f"[2026-10-15T00:00:00Z] Starting recall r1 in isolated conversation\n"
                   f"[2026-10-15T00:00:01Z] Recall r1 query: {SECRET}\n"
                   f"[2026-10-15T00:00:02Z] Recall r1 prefetched evidence: relationship=3, transcript_hits=7\n")
    repo = tmp_path / "checkouts" / "claude-subconscious"
    repo.mkdir(parents=True)
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    git = lambda *a: subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, env=env)
    git("init", "-q")
    git("commit", "-q", "--allow-empty", "-m", "Very Secret Commit")
    return {
        "STORE_DIR": str(store), "WHISPER_DIRS": str(whispers), "CLAUDE_HOME": str(home),
        "RECALL_LOG_GLOBS": str(log.parent / "*.log"), "DEPLOY_GIT_GLOBS": str(tmp_path / "checkouts" / "*"),
    }


def read(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def test_no_secret_leaves_the_exporters(deployment, tmp_path):
    snap = export(deployment, local=True, snapshot_dir=tmp_path / "snap")
    files = [p for p in snap.rglob("*") if p.is_file()]
    assert {p.parent.name for p in files if p.name == "manifest.json"} == set(EXPORTERS)
    for p in files:
        text = p.read_text()
        for secret in SECRETS:
            assert secret not in text, f"{secret!r} leaked into {p.relative_to(snap)}"


def test_structure_survives_sanitizing(deployment, tmp_path):
    snap = export(deployment, local=True, snapshot_dir=tmp_path / "snap")
    mem = read(snap / "store" / "memories.csv")[0]
    assert mem["kind"] == "preference" and mem["summary_chars"] == str(len(SECRET)) and mem["n_participants"] == "2"
    assert mem["payload_lens"] == "<redacted>"  # a key that isn't an identifier is itself treated as content
    batch = read(snap / "store" / "batches.csv")[0]
    assert batch["batch_prefix"] == "batch" and batch["status"] == "retryable_failure" and len(batch["attempt"]) == 12
    assert read(snap / "store" / "outcomes.csv")[0]["rejection_code"] == "<redacted>"
    assert [e["tool_name"] for e in read(snap / "store" / "evidence.csv")] == [
        "Bash", "mcp__kohaku-relationship-memory__memory_remember_with_feeling_v2", "<redacted>"]
    assert read(snap / "store" / "intent_outcomes.csv")[0]["outcome"] == "deferred"
    assert read(snap / "store" / "intents.csv")[0]["captured_at"] == "<redacted>"
    assert read(snap / "whispers" / "whispers.csv")[0]["ready_latency_ms"] == "33000"
    backlog = read(snap / "backlog" / "backlog.csv")[0]
    assert backlog["transcript_msgs"] == "5" and backlog["last_processed_index"] == "2"
    assert [e["event"] for e in read(snap / "recall_log" / "recall_events.csv")] == ["start", "prefetch"]
    assert read(snap / "deploy" / "reflog.csv")[0]["action"] == "commit"
    manifest = json.loads((snap / "store" / "manifest.json").read_text())
    assert manifest["tables"]["outcomes"]["redacted_cells"] == 1
    assert json.loads((snap / "snapshot.json").read_text())["exporters"]["store"]["memories"]["rows"] == 1
