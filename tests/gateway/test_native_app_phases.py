"""Actual source-bound control/result consumers; lower IO models, no native authority."""
import pytest

from gateway.native_app_phases import claim, validate_frame
from gateway.native_app_result import DECLINE, resolve
from hermes_owned_gateway import OwnedGatewayUnknown
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_app_call_fixture import CallCase


def phase_frame(case):
    return {**case.call_frame(), "command": "prepare_native_call",
            "requestId": "01J00000000000000000000004",
            "invocationId": "11111111-1111-4111-8111-111111111111", "invocationSequence": 1}


def bound_exchange(case, callback):
    async def on_original_loop(value):
        callback(value)
    case.exchange(modify=lambda value: setattr(value.input, "hook", lambda: value.run(on_original_loop(value))))


@pytest.mark.parametrize("change", [
    {"invocationSequence": True}, {"invocationSequence": 0}, {"invocationSequence": 2**64},
    {"invocationSequence": "1"}, {"invocationId": "caller-picked"}, {"nonce": "x" * 64},
    {"command": "native_call_prepared"}, {"proof": {}}, {"requestId": "repeat"},
])
def test_private_native_phase_rejects_invalid_protocol_values(monkeypatch, tmp_path, change):
    case = CallCase(monkeypatch, tmp_path)

    def invalid(value):
        validate_frame(phase_frame(value))
        with pytest.raises(RuntimeStoreError, match="phase_invalid"):
            validate_frame({**phase_frame(value), **change})
        assert value.call.origin is None and value.slot._native_pin is None

    try:
        bound_exchange(case, invalid)
    finally:
        case.close()


@pytest.mark.parametrize("field,value", [("nonce", "c" * 64), ("wireSha256", "c" * 64),
                                         ("contextId", "other"), ("attemptId", "other")])
def test_actual_private_phase_refuses_wrong_original_source_before_credit(monkeypatch, tmp_path, field, value):
    case = CallCase(monkeypatch, tmp_path)

    def invalid(current):
        before = current.source._app_context_credit
        with pytest.raises(RuntimeStoreError, match="source_changed"):
            claim(current.call.reader, {**phase_frame(current), field: value})
        assert current.source._app_context_credit == before and not current.call.phases
        assert current.call.origin is None and current.slot._native_pin is None

    try:
        bound_exchange(case, invalid)
    finally:
        case.close()


def test_original_canonical_socket_decline_allocates_no_native_or_sql_pin(monkeypatch, tmp_path):
    case = CallCase(monkeypatch, tmp_path)

    def decline(current):
        frame = {"jsonrpc": "2.0", "id": current.slot._native_request_candidate_id, "error": dict(DECLINE)}
        assert not resolve(object(), frame)
        assert current.call.decline_frame is None
        current.connection._resolve_response(frame)
        assert current.call.decline_frame is frame and current.call.protocol_phase == "declined"
        assert current.call.worker_event.is_set() and current.call.error is None
        assert current.call.invocation is None and not current.call.phases
        assert current.slot._native_pin is None and current.slot._native_active_use is None
        assert current.slot.origin is None

    try:
        bound_exchange(case, decline)
    finally:
        case.close()


RECEIPT_STORE_FULL_ERROR = {
    "code": -32042,
    "message": "Native receipt store full; this request did not run. You can continue chatting.",
}

EDITOR_NOT_CONNECTED_ERROR = {
    "code": -32043,
    "message": "Editor not connected. Open this project in the editor and try again.",
}


@pytest.mark.parametrize("error,phase", [
    (RECEIPT_STORE_FULL_ERROR, "receipt_store_full"),
    (EDITOR_NOT_CONNECTED_ERROR, "editor_not_connected"),
], ids=["receipt-store-full", "editor-not-connected"])
def test_original_canonical_socket_refusal_allocates_no_native_or_sql_pin(monkeypatch, tmp_path, error, phase):
    case = CallCase(monkeypatch, tmp_path)

    def refusal(current):
        frame = {"jsonrpc": "2.0", "id": current.slot._native_request_candidate_id,
                 "error": dict(error)}
        assert not resolve(object(), frame)
        assert current.call.decline_frame is None
        current.connection._resolve_response(frame)
        assert current.call.decline_frame is frame
        assert current.call.protocol_phase == current.call.request.native_phase == phase
        assert current.call.worker_event.is_set() and current.call.error is None
        assert current.call.result_frame is None and current.call.raw_result is None
        assert current.call.invocation is None and not current.call.phases
        assert current.call.origin is None and current.call.writer_lease is None
        assert current.slot._native_pin is None and current.slot._native_active_use is None
        assert current.slot.origin is None

    try:
        bound_exchange(case, refusal)
    finally:
        case.close()


@pytest.mark.parametrize("error", [RECEIPT_STORE_FULL_ERROR, EDITOR_NOT_CONNECTED_ERROR],
                         ids=["receipt-store-full", "editor-not-connected"])
@pytest.mark.parametrize("change", ["message", "decline-message", "missing-message", "error-data",
                                    "result", "extra", "jsonrpc"])
def test_preparation_refusal_rejects_changed_wire_shape(monkeypatch, tmp_path, error, change):
    case = CallCase(monkeypatch, tmp_path)

    def invalid(current):
        changes = {
            "message": {"error": {**error, "message": "different"}},
            "decline-message": {"error": {**error, "message": DECLINE["message"]}},
            "missing-message": {"error": {"code": error["code"]}},
            "error-data": {"error": {**error, "data": {}}},
            "result": {"result": "not an editor result"},
            "extra": {"extra": True},
            "jsonrpc": {"jsonrpc": "1.0"},
        }
        frame = {"jsonrpc": "2.0", "id": current.slot._native_request_candidate_id,
                 "error": dict(error), **changes[change]}
        with pytest.raises(RuntimeStoreError, match="native_original_decline_unavailable") as observed:
            current.connection._resolve_response(frame)
        assert current.call.error is observed.value and current.call.traceback is not None
        assert current.call.result_frame is None and current.call.decline_frame is None
        assert current.call.request.native_phase == "blocked"

    try:
        bound_exchange(case, invalid)
    finally:
        case.close()


@pytest.mark.parametrize("error", [RECEIPT_STORE_FULL_ERROR, EDITOR_NOT_CONNECTED_ERROR],
                         ids=["receipt-store-full", "editor-not-connected"])
@pytest.mark.parametrize("state", ["source", "prepared", "result_received", "declined", "released",
                                    "invocation", "phases", "pin"])
def test_preparation_refusal_rejects_execution_state(monkeypatch, tmp_path, error, state):
    case = CallCase(monkeypatch, tmp_path)

    def invalid(current):
        if state == "invocation":
            current.call.invocation = ("11111111-1111-4111-8111-111111111111", 1)
        elif state == "phases":
            claim(current.call.reader, phase_frame(current))
        elif state == "pin":
            current.slot._native_pin = object()
        else:
            current.call.protocol_phase = state
        frame = {"jsonrpc": "2.0", "id": current.slot._native_request_candidate_id,
                 "error": dict(error)}
        with pytest.raises(RuntimeStoreError, match="native_original_decline_unavailable") as observed:
            current.connection._resolve_response(frame)
        assert current.call.error is observed.value and current.call.traceback is not None
        assert current.call.result_frame is None and current.call.decline_frame is None
        assert current.call.request.native_phase == "blocked"

    try:
        bound_exchange(case, invalid)
    finally:
        case.close()


@pytest.mark.parametrize("result", [{"error": {"code": -32041, "message": "different"}},
                                   {"result": {"invocationId": "not prepared", "invocationSequence": 1, "rawResult": "x"}}])
def test_native_response_cannot_skip_preparation_or_masquerade_as_decline(monkeypatch, tmp_path, result):
    case = CallCase(monkeypatch, tmp_path)

    def invalid(current):
        frame = {"jsonrpc": "2.0", "id": current.slot._native_request_candidate_id, **result}
        with pytest.raises(RuntimeStoreError) as observed:
            current.connection._resolve_response(frame)
        assert current.call.error is observed.value and current.call.traceback is not None
        assert current.call.result_frame is None and current.call.decline_frame is None
        assert current.slot._native_pin is None and current.slot.origin is None

    try:
        bound_exchange(case, invalid)
    finally:
        case.close()


def test_prepare_reserves_final_book_cells_before_any_native_or_sql_action(monkeypatch, tmp_path):
    case = CallCase(monkeypatch, tmp_path)
    stop = RuntimeError("source-only fixture ends before any native preparation action")

    def reserve(current):
        book = current.source._app_context_requests
        size, credit = len(book), current.source._app_context_credit
        phase = claim(current.call.reader, phase_frame(current))
        assert len(book) == size + 4 and current.source._app_context_credit == credit + 4 * 4096
        assert set(current.call.phase_credits) == {"ack_native_call", "complete_native_call", "release_native_call"}
        for command, reserved in current.call.phase_credits.items():
            assert reserved.call is current.call and reserved.command == command
            assert reserved.book is book and book[reserved.key] is reserved and reserved.phase is None
        assert current.slot._native_pin is None and current.slot.origin is None
        phase.failed(stop)
        raise stop

    try:
        with pytest.raises(OwnedGatewayUnknown) as observed:
            bound_exchange(case, reserve)
        assert observed.value.__cause__ is stop and case.call.error is stop
        assert not any(value["event"] == "native_call_prepared" for value in case.events())
    finally:
        case.close()


def test_exhausted_final_credit_refuses_before_prepared_proof(monkeypatch, tmp_path):
    case = CallCase(monkeypatch, tmp_path)
    errors = []

    def exhausted(current):
        book = current.source._app_context_requests
        # Model competing non-native metadata in this fresh bounded source book.
        for index in range(61 - len(book)):
            book[("other-context-source-metadata", index)] = None
        try:
            claim(current.call.reader, phase_frame(current))
        except RuntimeStoreError as error:
            errors.append(error)
            assert current.call.error is error
            assert "release_native_call" not in current.call.phase_credits
            assert current.slot._native_pin is None and current.slot.origin is None
            raise
        raise AssertionError("native preparation accepted without final release credit")

    try:
        with pytest.raises(OwnedGatewayUnknown) as observed:
            bound_exchange(case, exhausted)
        assert observed.value.__cause__ is errors[0]
        assert not any(value["event"] == "native_call_prepared" for value in case.events())
    finally:
        case.close()
