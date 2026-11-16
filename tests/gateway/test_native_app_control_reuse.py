"""Completed private control records free capacity, not original call custody."""
import asyncio

import pytest

from gateway.native_app_context_records import RECORD_CREDIT, reserve
from gateway.native_app_phases import _reserve
from gateway.native_app_worker import require_completed_slot
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_app_context_fixture import line
from tests.gateway.native_app_producer_fixture import ProducerCase
from tests.gateway.test_native_app_result_source import ContinuingProducerCase, TIMEOUT_ERROR, expire_approval
from tests.waiting import wait_until


@pytest.mark.parametrize("first_timeout", [False, True], ids=["twenty-successes", "completed-timeout-then-nineteen"])
def test_twenty_calls_keep_original_worker_context_and_saved_results(monkeypatch, tmp_path, first_timeout):
    case = ContinuingProducerCase(monkeypatch, tmp_path)
    count = 20
    case.request_count = count
    completed = []
    source_budget = None
    try:
        case.begin()
        context, owner, keeper = case.call.context, case.owner, case.keeper
        for index in range(count):
            if index:
                case.worker_done.clear()
                case.wire_ready.clear()
                case.continue_chat = True
                case.next_request.set()
                case.run(asyncio.wait_for(case.wire_ready.wait(), 15))
                case.call = case.slot._source_constructions["app_call"]
                case.request = case.slot._native_request_object
                case.append(line(case.call_frame(requestId=f"01J000000000000000000{100 + index * 10 + 1:05d}")))
                case.run(case.notification("native_call_bound"))
            assert case.call.context is context and case.owner is owner and case.keeper is keeper
            assert case.call.worker_thread is case.worker_thread
            if first_timeout and index == 0:
                expire_approval(case)

                async def timeout_response():
                    case.connection._resolve_response({"jsonrpc": "2.0", "id": case.slot._native_request_candidate_id,
                                                       "error": dict(TIMEOUT_ERROR)})

                case.run(timeout_response())
            else:
                if index == 2:
                    first_success = completed[1] if first_timeout else completed[0]
                    replay = {**first_success.phases["prepare_native_call"].frame,
                              "requestId": "01J00000000000000000000999"}
                    with pytest.raises(RuntimeStoreError, match="native_original_invocation_replay"):
                        _reserve(case.call, case.call.reader, replay)
                invocation = f"11111111-1111-4111-8111-{index + 1:012d}"
                case.phase("prepare_native_call", 100 + index * 10 + 2,
                           invocationId=invocation, invocationSequence=index + 1)
                case.run(case.notification("native_call_prepared"))
                case.deliver_result("native result")
                case.run(case.notification("native_call_committed"))
                for offset, (command, event) in enumerate((
                    ("ack_native_call", "native_call_acknowledged"),
                    ("complete_native_call", "native_call_completed"),
                    ("release_native_call", "native_call_released")), 3):
                    case.phase(command, 100 + index * 10 + offset,
                               invocationId=invocation, invocationSequence=index + 1)
                    case.run(case.notification(event))
            case.run(asyncio.wait_for(case.worker_done.wait(), 15))
            assert case.worker_error is None and case.control_error is None
            assert case.message is not None
            row = case.db._read_one("SELECT content FROM messages WHERE id=?", (case.message["_row_id"],))
            assert row is not None and row[0] == case.message["content"]
            completed.append(case.call)
            assert require_completed_slot(case.slot) is case.call
            assert not keeper.blocked and case.db._native_read_gate.lease is None
            if not first_timeout:
                runtime = case.call.original.connection.parent.runtime
                if source_budget is None:
                    source_budget = runtime.bytes
                assert runtime.bytes == source_budget
                assert len(runtime.assistants) == 1
                assert len(runtime.bridges) == 1
                assert len(case.call.original.connection.parent.calls) == 1
        assert wait_until(lambda: case.call.reader.active is None, timeout=5)
        for call in completed:
            assert require_completed_slot(call.slot) is call
            assert call.completion.require_retained() is call
        book = case.source._app_context_requests
        assert context.request in book.values()
        assert len(book) <= 6
        for call in completed[:-1]:
            assert call not in book.values()
            assert all(phase not in book.values() for phase in call.phases.values())
        first_success = completed[1] if first_timeout else completed[0]
        with pytest.raises(RuntimeStoreError, match="native_original_phase_reentry_or_order"):
            _reserve(first_success, first_success.reader, first_success.phases["prepare_native_call"].frame)
    finally:
        case.continue_chat = False
        case.next_request.set()
        case.close()


@pytest.mark.parametrize("state", ["active", "unconfirmed", "failed", "changed-terminal"])
def test_only_retained_idle_completion_refunds_once(monkeypatch, tmp_path, state):
    case = ProducerCase(monkeypatch, tmp_path)
    try:
        case.begin()
        case.decline()
        case.run(asyncio.wait_for(case.worker_done.wait(), 15))
        call = case.call
        assert wait_until(lambda: call.reader.active is None, timeout=5)
        book = case.source._app_context_requests
        original, credit = dict(book), case.source._app_context_credit
        terminal = call.terminal
        with case.source._state_lock:
            if state == "active":
                call.reader.active = call
            elif state == "unconfirmed":
                call.completion.confirmed = False
            elif state == "failed":
                call.error = RuntimeStoreError("test-failed-completion")
            else:
                call.terminal = dict(terminal)
            try:
                reserve(case.source, book, "charged-hole", RECORD_CREDIT)
                assert all(book[key] is value for key, value in original.items())
                assert case.source._app_context_credit == credit + RECORD_CREDIT
            finally:
                call.reader.active = None
                call.completion.confirmed = True
                call.error = None
                call.terminal = terminal
            reserve(case.source, book, "next-hole", RECORD_CREDIT)
            assert call not in book.values()
            assert book["charged-hole"] is None
            assert case.source._app_context_credit == credit + RECORD_CREDIT
            reserve(case.source, book, "third-hole", RECORD_CREDIT)
            assert case.source._app_context_credit == credit + 2 * RECORD_CREDIT
            assert require_completed_slot(case.slot) is call
    finally:
        case.close()


@pytest.mark.parametrize("failed", [False, True], ids=["pending", "failed"])
def test_reservation_preserves_uncompleted_call_and_charged_holes(monkeypatch, tmp_path, failed):
    case = ContinuingProducerCase(monkeypatch, tmp_path)
    try:
        case.begin()
        assert wait_until(lambda: case.call.reader.active is None, timeout=5)
        book = case.source._app_context_requests
        original = dict(book)
        error = RuntimeStoreError("test-uncompleted-call") if failed else None
        with case.source._state_lock:
            case.call.error = error
            credit = case.source._app_context_credit
            reserve(case.source, book, "charged-hole", RECORD_CREDIT)
            reserve(case.source, book, "next-hole", RECORD_CREDIT)
            assert all(book[key] is value for key, value in original.items())
            assert book["charged-hole"] is None and book["next-hole"] is None
            assert case.source._app_context_credit == credit + 2 * RECORD_CREDIT
            case.call.error = None
        case.decline()
        case.run(asyncio.wait_for(case.worker_done.wait(), 15))
        assert case.worker_error is None
    finally:
        case.continue_chat = False
        case.next_request.set()
        case.close()
