"""Trace archive --plan: split/rebuild is lossless, rows that can't be rebuilt are flagged, dedup counts each message once."""
import io, json

import pytest

from observatory import trace_archive as ta
from tests.test_export import FAKE_PSQL

SYSTEM = {"role": "system", "content": "You are the observer."}
TOOLS = [{"type": "function", "function": {"name": "memory_search", "parameters": {"type": "object"}}}]


def request(*messages, **extra):
    return json.dumps({"model": "m", "messages": [SYSTEM, *messages], "tools": TOOLS, **extra}, ensure_ascii=False)


def row(trace_id, req, created_at="2026-08-08 10:00:00.1+09", **columns):
    return {"id": trace_id, "created_at": created_at, "request_json": req, "response_json": '{"choices": []}',
            "step_id": "step-" + trace_id, "source": None, "agent_tags": None, **columns}


def test_split_and_rebuild_is_lossless():
    req = request({"role": "user", "content": "こんにちは 🐾 \\ud83d broken surrogate"}, {"role": "tool", "content": "x" * 100},
                  temperature=0.7)
    original = row("t1", req)
    split = ta.split_trace(original)
    assert split.status == "split"
    assert len(split.record["message_hashes"]) == 3 and "messages" not in split.record["request_rest"]
    assert ta.verify(original, split)
    rebuilt = ta.rebuild(split.record, dict(split.blobs).__getitem__)
    assert list(rebuilt["request_json"]) == list(json.loads(req))  # key order kept, messages back in place
    assert rebuilt["source"] is None and rebuilt["response_json"] == original["response_json"]  # other columns verbatim


def test_unsplittable_requests_are_kept_verbatim():
    for req in ["not json at all", '{"model": "m", "model": "dup"}', '{"model": "m"}', None]:
        split = ta.split_trace(row("t", req))
        assert split.status == "whole" and split.record["request_text"] == req and not split.blobs
        assert ta.verify(row("t", req), split)


def test_undecodable_bytes_are_never_archivable():
    bad = row("t", request({"role": "user", "content": "ok"}), response_json='{"x": "\udcff"}')
    split = ta.split_trace(bad)
    assert split.status == "undecodable" and not ta.verify(bad, split)


def test_tampered_record_fails_verification():
    original = row("t", request({"role": "user", "content": "hi"}))
    split = ta.split_trace(original)
    split.record["request_rest"]["model"] = "other"
    assert not ta.verify(original, split)


def test_plan_counts_each_message_once_and_reports_per_day():
    shared = {"role": "user", "content": "long transcript " * 200}
    rows = [row("a", request(shared)),
            row("b", request(shared, {"role": "assistant", "content": "reply"})),
            row("c", request(shared), created_at="2026-08-09 01:00:00+09"),
            row("d", "broken {", created_at="2026-08-09 02:00:00+09")]
    days, totals = ta.plan(rows)
    assert set(days) == {"2026-08-08", "2026-08-09"}
    unique = {ta.canonical(m) for m in (SYSTEM, shared, {"role": "assistant", "content": "reply"})}
    assert totals["new_message_bytes"] == sum(map(len, unique))
    assert days["2026-08-09"]["new_message_bytes"] == 0  # everything in c was already seen on 08-08
    assert (totals["rows"], totals["split"], totals["whole"], totals["verify_failed"]) == (4, 3, 1, 0)
    assert totals["archive_bytes"] < totals["raw_bytes"]
    out = io.StringIO()
    ta.report(days, totals, out)
    assert "total" in out.getvalue() and "2026-08-09" in out.getvalue()


def test_where_clause_bounds_and_validation():
    import datetime
    now = datetime.datetime(2026, 11, 2, tzinfo=datetime.timezone.utc)
    where = ta.where_clause(30, "2026-08-08", "2026-08-09", now=now)
    assert where == ("where created_at < '2026-10-03T00:00:00+00:00' and created_at >= '2026-08-08'"
                     " and created_at < '2026-08-10'")
    with pytest.raises(ValueError):
        ta.where_clause(30, "2026-08-08'; drop table x; --", now=now)


def test_null_marker_decodes_to_none_and_empty_string_stays():
    assert ta.decode_nulls({"id": "t1", "source": ta.NULL_MARKER, "note": ""}) == {"id": "t1", "source": None, "note": ""}


def test_stream_rows_reads_through_psql(tmp_path):
    fixtures = tmp_path / "letta.json"
    fixtures.write_text(json.dumps({"provider_traces": [row("t1", request({"role": "user", "content": "hi"}))]}))
    config = {"LETTA_PSQL": str(FAKE_PSQL), "LETTA_PG_ARGS": str(fixtures)}
    rows = list(ta.stream_rows(config, "where true"))
    assert [r["id"] for r in rows] == ["t1"] and rows[0]["source"] is None
    days, totals = ta.plan(rows)
    assert totals["split"] == 1 and totals["verify_failed"] == 0


def test_numbers_a_float_cannot_hold_are_kept_verbatim_not_split():
    # json.loads turns 1e400 into inf (and verify used to compare inf == inf); a 20-digit fraction loses digits.
    for extra in ('1e400', '0.10000000000000000001', 'NaN', '-Infinity'):
        req = '{"model": "m", "messages": [{"role": "user", "content": "hi"}], "temperature": ' + extra + '}'
        split = ta.split_trace(row("t", req))
        assert split.status == "whole" and split.record["request_text"] == req and not split.blobs, extra
        assert ta.verify(row("t", req), split)
    inside = '{"model": "m", "messages": [{"role": "user", "score": 1e400}]}'
    assert ta.split_trace(row("t", inside)).status == "whole"


def test_equal_numbers_spelled_differently_still_split():
    req = '{"model": "m", "messages": [{"role": "user", "n": 1e5}], "temperature": 0.70}'
    split = ta.split_trace(row("t", req))
    assert split.status == "split" and ta.verify(row("t", req), split)


def test_tampered_number_fails_exact_verification():
    req = '{"model": "m", "messages": [{"role": "user", "content": "hi"}], "temperature": 0.7}'
    split = ta.split_trace(row("t", req))
    split.record["request_rest"]["temperature"] = 0.71
    assert not ta.verify(row("t", req), split)


def test_deeply_nested_request_is_kept_verbatim_and_does_not_stop_the_scan():
    deep = '{"messages":' + '[' * 100_000 + '0' + ']' * 100_000 + '}'  # past the parser's recursion limit on any Python
    rows = [row("good-1", request({"role": "user", "content": "a"})),
            row("deep", deep),
            row("good-2", request({"role": "user", "content": "b"}))]
    split = ta.split_trace(rows[1])
    assert split.status == "whole" and ta.verify(rows[1], split)
    days, totals = ta.plan(rows)
    assert (totals["rows"], totals["split"], totals["whole"], totals["verify_failed"]) == (3, 2, 1, 0)
