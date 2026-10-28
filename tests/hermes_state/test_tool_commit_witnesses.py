"""Real owned SQLite transaction outcomes and bounded canonical source metadata."""
import sqlite3

import pytest

from agent import tool_commit_rows
from agent.session_persistence import _DB_PERSISTED_MARKER
from agent.tool_commit_rows import MAX_CALLS, MAX_JSON_DEPTH, MAX_TEXT_BYTES, json_digest, text_digest
from tests.agent.canonical_commit_fixture import CALL, TOOL, CommitFixture


@pytest.fixture
def live(tmp_path):
    fixture = CommitFixture(tmp_path)
    try:
        yield fixture
    finally:
        fixture.close()


def _record_writer_errors(live, monkeypatch):
    original = live._session_db.append_messages_batch
    errors = []

    def append(*args, **kwargs):
        try:
            return original(*args, **kwargs)
        except Exception as exc:
            errors.append(exc)
            raise

    monkeypatch.setattr(live._session_db, "append_messages_batch", append)
    return errors


@pytest.mark.parametrize("fault", ["safe-value", "sqlite-error", "unknown", "sqlite-rollback",
                                  "safe-after-rollback"])
def test_witness_failure_keeps_the_original_body_transaction_truth(live, monkeypatch, fault):
    live.stage()
    ref = live.begin()
    message = live.prepare(ref)
    if fault == "sqlite-rollback":
        live.write_sql(
            ("CREATE TABLE fixture_rollback(value INTEGER)", ()),
            ("CREATE TRIGGER fixture_rollback_now BEFORE INSERT ON fixture_rollback "
             "BEGIN SELECT RAISE(ROLLBACK, 'synthetic witness rollback'); END", ()))
    errors = _record_writer_errors(live, monkeypatch)
    seen = []

    def fail_capture(db, conn, session_id, messages, requested):
        observed = {"conn": conn, "rows": messages, "session": session_id,
                    "requested": requested, "entered_in_transaction": conn.in_transaction}
        seen.append(observed)
        messages[0][tool_commit_rows.WITNESS] = {"synthetic_seed": True}
        if fault == "sqlite-rollback":
            observed["rollback_insert_started"] = True
            try:
                conn.execute("INSERT INTO fixture_rollback VALUES (1)")
            except sqlite3.IntegrityError as exc:
                observed["raised"] = exc
                observed["transaction_after_rollback"] = conn.in_transaction
                raise
            raise AssertionError("The real SQLite rollback trigger did not refuse")
        if fault == "safe-after-rollback":
            conn.rollback()
            observed["transaction_after_rollback"] = conn.in_transaction
        error_type = sqlite3.OperationalError if fault == "sqlite-error" else (
            RuntimeError if fault == "unknown" else ValueError)
        exc = error_type("synthetic witness " + fault)
        observed["raised"] = exc
        raise exc

    monkeypatch.setattr(tool_commit_rows, "_capture", fail_capture)
    success = live.flush()
    assert len(seen) == 1
    observed, = seen
    assert observed["conn"] is live._session_db._conn
    assert observed["entered_in_transaction"] is True
    assert observed["requested"] and observed["rows"][0]["role"] == "tool"
    assert tool_commit_rows.WITNESS not in observed["rows"][0]
    safe = fault == "safe-value"
    assert success is safe
    assert len(live.rows()) == (2 if safe else 1)
    if safe:
        assert errors == []
        assert message.get(_DB_PERSISTED_MARKER) is True
    else:
        error, = errors
        assert error is observed["raised"]
        assert "synthetic witness" in str(error)
        assert not message.get(_DB_PERSISTED_MARKER)
        assert "_row_id" not in message
    if fault in {"sqlite-rollback", "safe-after-rollback"}:
        assert observed["transaction_after_rollback"] is False
    if fault == "sqlite-rollback":
        assert observed["rollback_insert_started"] is True
        assert isinstance(observed["raised"], sqlite3.IntegrityError)
    assert not live.canonical_events()
    assert ref.canonical_commit.receipt is None
    assert not live._session_db._conn.in_transaction


def test_witness_rejects_lost_transaction_even_if_a_callback_returns(live, monkeypatch):
    live.stage()
    ref = live.begin()
    message = live.prepare(ref)
    errors = _record_writer_errors(live, monkeypatch)
    seen = []

    def end_transaction(db, conn, session_id, messages, requested):
        seen.append((conn, messages, conn.in_transaction))
        messages[0][tool_commit_rows.WITNESS] = {"synthetic_seed": True}
        conn.rollback()

    monkeypatch.setattr(tool_commit_rows, "_capture", end_transaction)
    assert live.flush() is False
    assert len(seen) == 1 and seen[0][2] is True
    assert seen[0][0] is live._session_db._conn
    assert tool_commit_rows.WITNESS not in seen[0][1][0]
    error, = errors
    assert isinstance(error, RuntimeError)
    assert str(error) == "Body transaction lost during canonical observation"
    assert not message.get(_DB_PERSISTED_MARKER) and "_row_id" not in message
    assert len(live.rows()) == 1
    assert not live.canonical_events()


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), 1 << 257, object(), "\ud800"])
def test_non_json_or_oversize_scalar_has_no_digest(bad):
    assert json_digest({"synthetic": bad}) is None


def test_digest_refuses_cycles_and_shared_container_aliases():
    cyclic = []
    cyclic.append(cyclic)
    shared = []
    assert json_digest(cyclic) is None
    assert json_digest([shared, shared]) is None


def test_digest_bounds_depth_nodes_and_utf8_bytes():
    nested = "leaf"
    for _ in range(MAX_JSON_DEPTH + 1):
        nested = [nested]
    assert json_digest(nested) is None
    assert json_digest([None] * (tool_commit_rows.MAX_JSON_NODES + 1)) is None
    assert text_digest("é" * MAX_TEXT_BYTES) is None
    assert json_digest({"value": "x" * (MAX_TEXT_BYTES + 1)}) is None
    assert json_digest({"ok": [1, True, None]}) is not None


def test_oversize_original_call_block_never_reserves_pending(live):
    call = {"id": CALL, "type": "function",
            "function": {"name": TOOL, "arguments": "{}"}}
    live.stage([dict(call) for _ in range(MAX_CALLS + 1)])
    assert live.owner.staged is None
    assert live.begin().canonical_commit is None
    assert not live.owner.pending


def test_128_original_call_reservations_refuse_the_next_one_without_eviction(live):
    calls = [{"id": f"fixture-call-{index}", "type": "function",
              "function": {"name": TOOL, "arguments": "{}"}}
             for index in range(MAX_CALLS)]
    live.stage(calls)
    reserved = [live.begin(call_id=call["id"]).canonical_commit for call in calls]
    assert all(pending is not None for pending in reserved)
    first = reserved[0]
    live.stage([{"id": "overflow-call", "type": "function",
                 "function": {"name": TOOL, "arguments": "{}"}}])
    assert live.begin(call_id="overflow-call").canonical_commit is None
    assert len(live.owner.pending) == MAX_CALLS
    assert live.owner.pending[0] is first
    assert all(pending.receipt is None for pending in reserved)
    assert not live.canonical_events()


def test_prepare_freezes_first_raw_projection_and_message_uid(live):
    live.stage()
    ref = live.begin()
    first = live.prepare(ref, content="first synthetic projection", raw="first synthetic raw")
    pending = ref.canonical_commit
    frozen = (pending.source_digest.copy(), pending.projection_digest.copy(), pending.message_uid)
    replacement = {"role": "tool", "name": TOOL, "tool_call_id": CALL,
                   "content": "different synthetic projection"}
    live.owner.prepare(pending, replacement, text_digest("different raw"))
    assert pending.message is first
    assert (pending.source_digest, pending.projection_digest, pending.message_uid) == frozen
    assert live.flush() is True
    receipt, = live.canonical_events()
    assert receipt["source_result_utf8"] == frozen[0]
    assert receipt["canonical_projection_utf8"] == frozen[1]
