"""Original canonical responses must retain the actual pending request and ID."""
import copy

import pytest

from gateway import session_server_requests as requests
from gateway.native_app_result import DECLINE
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_app_call_fixture import CallCase
from tests.gateway.test_native_app_phases import bound_exchange


@pytest.mark.parametrize("copied", [False, True], ids=["same-request-alias", "copied-request"])
def test_original_native_response_refuses_foreign_request_identity(monkeypatch, tmp_path, copied):
    case = CallCase(monkeypatch, tmp_path)

    def invalid(current):
        original = current.call.request
        original_id = current.slot._native_request_candidate_id
        foreign_id = original_id + "-foreign"
        candidate = copy.copy(original) if copied else original
        frame = {"jsonrpc": "2.0", "id": foreign_id, "error": dict(DECLINE)}
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
