"""Original canonical responses must retain the actual pending request and ID."""
import asyncio
import copy
import json
import threading

import pytest

from gateway import session_server_requests as requests
from gateway.native_app_result import DECLINE
from gateway.native_app_worker import require_completed_slot
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_app_call_fixture import CallCase
from tests.gateway.native_app_context_fixture import line
from tests.gateway.native_app_producer_fixture import ProducerCase
from tests.gateway.test_native_app_phases import EDITOR_NOT_CONNECTED_ERROR, RECEIPT_STORE_FULL_ERROR, bound_exchange


class ContinuingProducerCase(ProducerCase):
    """Two model requests on the same chat worker, owner, context and socket."""

    def __init__(self, monkeypatch, tmp_path):
        self.next_request = threading.Event()
        self.continue_chat = False
        super().__init__(monkeypatch, tmp_path)

    def _worker(self):
        super()._worker()
        if self.worker_error is None and self.next_request.wait(15) and self.continue_chat:
            super()._worker()


@pytest.mark.parametrize("error,phase", [
    (RECEIPT_STORE_FULL_ERROR, "receipt_store_full"),
    (EDITOR_NOT_CONNECTED_ERROR, "editor_not_connected"),
], ids=["receipt-store-full", "editor-not-connected"])
def test_preparation_refusal_saves_ordinary_error_and_next_request_works(monkeypatch, tmp_path, error, phase):
    case = ContinuingProducerCase(monkeypatch, tmp_path)
    try:
        case.begin()
        frame = {"jsonrpc": "2.0", "id": case.slot._native_request_candidate_id,
                 "error": dict(error)}

        async def original_response():
            assert case.connection is not None
            case.connection._resolve_response(frame)

        case.run(original_response())
        case.run(asyncio.wait_for(case.worker_done.wait(), 15))
        assert case.worker_error is None and case.control_error is None
        assert case.result is not None and case.message is not None
        assert json.loads(case.result) == {
            "error": phase, "isError": True,
            "message": error["message"], "replayAllowed": False,
        }
        first_call, first_slot = case.call, case.slot
        context, owner, keeper = case.call.context, case.owner, case.keeper
        assert first_call.terminal is frame and first_call.error is None
        assert first_call.protocol_phase == first_call.request.native_phase == phase
        assert first_call.completion.declined is False
        assert first_call.completion.refusal_phase == phase
        assert first_call.invocation is None and not first_call.phases
        assert first_call.result_frame is None and first_call.raw_result is None
        assert first_call.origin is None and first_call.writer_lease is None
        assert first_slot._native_pin is None and first_slot._native_active_use is None
        assert first_call.initial_proof is None and first_call.commit_proof is None
        assert case.pending.message is None and case.pending.receipt is None
        assert require_completed_slot(first_slot) is first_call
        assert not keeper.blocked and case.db._native_read_gate.lease is None
        row = case.db._read_one("SELECT content FROM messages WHERE id=?", (case.message["_row_id"],))
        assert row is not None
        stored = row[0]
        assert stored == case.message["content"]
        assert phase in stored and "declined" not in stored
        assert not any(value["event"] in ("native_call_prepared", "native_call_committed", "native_call_released")
                       for value in case.events())

        # Model the next provider request without restarting or rebinding the chat.
        case.worker_done.clear()
        case.wire_ready.clear()
        case.continue_chat = True
        case.next_request.set()
        case.run(asyncio.wait_for(case.wire_ready.wait(), 15))
        case.call = case.slot._source_constructions["app_call"]
        case.request = case.slot._native_request_object
        assert case.call is not first_call and case.call.context is context
        assert case.owner is owner and case.keeper is keeper
        assert case.call.slot._native_request_candidate_id != frame["id"]
        case.append(line(case.call_frame(requestId="01J00000000000000000000009")))
        case.run(case.notification("native_call_bound"))
        case.phase("prepare_native_call", 10)
        case.run(case.notification("native_call_prepared"))
        case.deliver_result("next request succeeded")
        case.run(case.notification("native_call_committed"))
        for number, (command, event) in enumerate((
            ("ack_native_call", "native_call_acknowledged"),
            ("complete_native_call", "native_call_completed"),
            ("release_native_call", "native_call_released")), 11):
            case.phase(command, number)
            case.run(case.notification(event))
        case.run(asyncio.wait_for(case.worker_done.wait(), 15))
        assert case.worker_error is None and case.control_error is None
        assert case.result == "next request succeeded"
        assert case.pending.receipt is not None and case.pending.publication_observed
        assert case.message is not None
        row = case.db._read_one("SELECT content FROM messages WHERE id=?", (case.message["_row_id"],))
        assert row is not None and row[0] == case.message["content"]
        assert require_completed_slot(case.slot) is case.call
        assert require_completed_slot(first_slot) is first_call
        assert not keeper.blocked and case.db._native_read_gate.lease is None
    finally:
        case.next_request.set()
        case.close()
    assert case.keeper.completed and case.offer_task.done()


@pytest.mark.parametrize("copied", [False, True], ids=["same-request-alias", "copied-request"])
@pytest.mark.parametrize("error", [DECLINE, RECEIPT_STORE_FULL_ERROR, EDITOR_NOT_CONNECTED_ERROR],
                         ids=["decline", "receipt-store-full", "editor-not-connected"])
def test_original_native_response_refuses_foreign_request_identity(monkeypatch, tmp_path, copied, error):
    case = CallCase(monkeypatch, tmp_path)

    def invalid(current):
        original = current.call.request
        original_id = current.slot._native_request_candidate_id
        foreign_id = original_id + "-foreign"
        candidate = copy.copy(original) if copied else original
        frame = {"jsonrpc": "2.0", "id": foreign_id, "error": dict(error)}
        credit = current.source._app_context_credit
        with requests._lock:
            assert foreign_id not in requests._open
            requests._open[foreign_id] = candidate
        try:
            with pytest.raises(RuntimeStoreError, match="result_request_changed") as observed:
                current.connection._resolve_response(frame)
            assert current.call.error is observed.value and current.call.traceback is not None
            assert current.call.request is original and current.original.request is original
            assert current.call.result_frame is None and current.call.decline_frame is None
            assert current.call.protocol_phase == "awaiting_prepare"
            assert current.source._app_context_credit == credit and not current.call.phases
            assert current.call.invocation is None and current.slot._native_pin is None
            assert current.slot.origin is None
            with requests._lock:
                assert requests._open[original_id] is original
                assert requests._open[foreign_id] is candidate
        finally:
            with requests._lock:
                del requests._open[foreign_id]

    try:
        bound_exchange(case, invalid)
    finally:
        case.close()
