"""Original request argument links over lower IO models; no App/native authority."""
import pytest

from gateway import native_source_calls as calls
from gateway import session_server_requests as requests
from gateway.native_source_issuance import require_original_app_call
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_constructor_fixture import constructor_graph, finish, pending_slot


@pytest.mark.asyncio
async def test_native_request_keeps_the_original_effective_arguments_and_selection(monkeypatch, tmp_path):
    case = await constructor_graph(monkeypatch, tmp_path)
    try:
        slot = pending_slot(case)
        source = slot.prepare_source_declaration()
        original = source.original_source_call
        request = requests.retain_native_candidate(slot)
        assert request.params == {
            "session_id": case.owner.scope["session_id"],
            "context_id": case.selection.context,
            "snapshot_sha256": case.selection.snapshot,
            "message_uid": original.message_uid,
            "call_uid": original.call_uid,
            "call_id": original.call_id,
            "tool_name": original.tool_name,
            "tool_ref": case.selection.tools[0][1],
            "arguments": slot.pending.ref.args,
        }
        assert request.params["arguments"] is original.effective_arguments
        assert request.params["arguments"] is slot.pending.ref.args
        assert calls.require_original_source_call(slot, source, request=request) is original
        with pytest.raises(RuntimeStoreError, match="native_original_app_call_issuer_missing"):
            require_original_app_call(original, request)
        assert slot._native_pin is None and request.native_active_use is None
        assert requests._open[slot._native_request_candidate_id] is request
    finally:
        await finish(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["parameters", "arguments", "context", "extra"])
async def test_native_request_changed_parameters_cannot_reuse_original_source(monkeypatch, tmp_path, change):
    case = await constructor_graph(monkeypatch, tmp_path)
    try:
        slot = pending_slot(case)
        source = slot.prepare_source_declaration()
        request = requests.retain_native_candidate(slot)
        if change == "parameters":
            request.params = dict(request.params)
        elif change == "arguments":
            request.params["arguments"] = dict(slot.pending.ref.args)
        elif change == "context":
            request.params["context_id"] = "replacement"
        else:
            request.params["native_permission"] = True
        with pytest.raises(RuntimeStoreError, match="native_original_request_arguments_changed"):
            calls.require_original_source_call(slot, source, request=request)
        assert slot._native_request_object is request
        assert requests._open[slot._native_request_candidate_id] is request
        assert slot._native_pin is None and request.native_active_use is None
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_native_request_copied_by_initializer_retains_original_input_and_failure(monkeypatch, tmp_path):
    case = await constructor_graph(monkeypatch, tmp_path)
    try:
        slot = pending_slot(case)
        source = slot.prepare_source_declaration()
        initialize = requests._Pending.__init__

        def copy_parameters(request, *arguments, **keywords):
            initialize(request, *arguments, **keywords)
            request.params = dict(request.params)

        monkeypatch.setattr(requests._Pending, "__init__", copy_parameters)
        with pytest.raises(RuntimeStoreError, match="native_original_request_arguments_changed") as caught:
            requests.retain_native_candidate(slot)
        request = slot._native_request_object
        attempt = request._source_construction
        assert attempt.keywords["params"] is not request.params
        assert attempt.keywords["params"]["arguments"] is slot.pending.ref.args
        assert attempt.error is caught.value and attempt.phase == "blocked"
        assert request.native_phase == "blocked" and source.original_source_call.request is None
        assert requests._open[slot._native_request_candidate_id] is request
        assert slot._native_pin is None and request.native_active_use is None
    finally:
        await finish(case)
