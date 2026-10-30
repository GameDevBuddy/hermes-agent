"""Actual finite source consumers with lower metadata/association failures, no native issuance."""
import asyncio
from types import SimpleNamespace

import pytest

from agent import native_receipt_origin as receipt
from gateway import native_source_calls as calls
from gateway import session_server_requests as requests
from gateway.native_source_construction import SourceConstruction, require_constructed
from gateway.native_source_runtime import SOURCE_REFERENCE_CREDIT
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_constructor_fixture import (
    CALL, SESSION, TOOL, UID, constructor_graph, finish, pending_slot,
)


@pytest.mark.asyncio
async def test_actual_source_consumers_retain_full_originals_and_still_refuse_app_call(monkeypatch, tmp_path):
    case = await constructor_graph(monkeypatch, tmp_path)
    try:
        before = case.runtime.bytes
        slot = pending_slot(case)
        assert case.runtime.bytes == before + SOURCE_REFERENCE_CREDIT
        source = slot.prepare_source_declaration()
        original = calls.require_original_source_call(slot, source)
        assert source.slot is slot and original.model_call is case.call
        assert original.keeper is case.keeper and original.task is asyncio.current_task()
        assert slot._source_declaration_phase == "ready"
        with pytest.raises(RuntimeStoreError, match="app_call_issuer_missing"):
            requests.register_native_request(slot)
        request = slot._native_request_object
        assert original.request is request and requests._open[slot._native_request_candidate_id] is request
        for value in (case.assistant, slot, original, source, request):
            require_constructed(value)
            attempt = value._source_construction
            assert attempt.value is value and attempt.phase == "ready" and attempt.error is None
            assert attempt.connection is case.selection.source_connection
            assert attempt.graph[1] is case.graph.runtime.bootstrap_task
        assert request.native_phase == "blocked" and request.native_pair_credit == 2
        assert not request.event.is_set() and slot._native_pin is None
        assert case.runtime.construction_error is None and not case.runtime.blocked
        assert not requests.resolve({"id": slot._native_request_candidate_id, "result": {"approved": True}})
        assert requests.open_requests(case.live.event_stream, SESSION) == []
        assert requests.withdraw(case.live.event_stream, SESSION, "native.invoke") == 0
    finally:
        await finish(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", ["pending-ref", "book", "bootstrap-task", "listener-task", "profile"])
async def test_slot_changed_original_during_frozen_json_stays_retained_and_blocked(monkeypatch, tmp_path, replacement):
    case = await constructor_graph(monkeypatch, tmp_path)
    try:
        freezer, observed = receipt.frozen_json, []

        def freeze(value, *args):
            slot = case.bridge.slots[UID]
            observed.append(slot)
            attempt = slot._source_construction
            assert attempt.phase == "initializing" and attempt.credit == SOURCE_REFERENCE_CREDIT
            assert case.runtime.bytes >= attempt.credit
            with pytest.raises(RuntimeStoreError, match="constructor_incomplete"):
                slot.prepare_source_declaration()
            if len(observed) == 1:
                task = SimpleNamespace(done=lambda: False, get_loop=lambda: case.runtime.loop)
                changes = {
                    "pending-ref": lambda: setattr(slot.pending, "ref", SimpleNamespace(**vars(slot.pending.ref))),
                    "book": lambda: setattr(case.bridge, "slots", dict(case.bridge.slots)),
                    "bootstrap-task": lambda: setattr(case.runtime, "bootstrap_task", task),
                    "listener-task": lambda: (setattr(case.graph.handle, "task", task),
                                               setattr(case.graph.handle.source_owner, "task", task)),
                    "profile": lambda: monkeypatch.setenv("HERMES_HOME", str(tmp_path / "changed-profile")),
                }
                changes[replacement]()
            return freezer(value, *args)

        monkeypatch.setattr(receipt, "frozen_json", freeze)
        ref = SimpleNamespace(name=TOOL, call_id=CALL, task_id=SESSION, args={"effective": 2})
        pending = case.owner.bind(ref)
        assert pending.native_dispatch_slot is None and pending.native_origin_error == "native_reservation_unavailable"
        slot = observed[0]
        attempt = slot._source_construction
        assert attempt.value is slot and isinstance(attempt.error, RuntimeStoreError) and attempt.phase == "blocked"
        assert case.runtime.construction_error is attempt.error and case.runtime.blocked
        assert attempt.arguments[-1].owner is case.owner
        with pytest.raises(RuntimeStoreError):
            require_constructed(slot)
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_failed_keeper_callback_retains_preconstructor_attempt_and_never_retries(monkeypatch, tmp_path):
    case = await constructor_graph(monkeypatch, tmp_path)
    error = asyncio.CancelledError("original keeper callback")
    try:
        slot = pending_slot(case)
        retain = case.keeper.retain_pin_attempt

        def fail(value):
            retain(value)
            raise error

        monkeypatch.setattr(case.keeper, "retain_pin_attempt", fail)
        with pytest.raises(asyncio.CancelledError) as caught:
            calls.capture_original_call(slot, case.keeper, case.selection, b"{}", b"{}")
        pair = case.graph.handle.source_owner.calls[id(slot)]
        attempt = pair[1]
        assert pair[0] is slot and type(attempt) is SourceConstruction
        assert caught.value is error and attempt.value is None and attempt.error is error
        assert case.keeper.slots[id(slot)] is slot and attempt.arguments[2] is case.keeper
        assert case.runtime.construction_error is error
        with pytest.raises(RuntimeStoreError):
            calls.capture_original_call(slot, case.keeper, case.selection, b"{}", b"{}")
        assert case.graph.handle.source_owner.calls[id(slot)] is pair
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_full_declaration_initializer_then_failure_keeps_exact_frozen_self(monkeypatch, tmp_path):
    case = await constructor_graph(monkeypatch, tmp_path)
    error, observed = RuntimeError("after original declaration initializer"), []
    original = receipt.NativeSourceDeclarationRefs.__init__

    def initialize(value, *args):
        original(value, *args)
        observed.append(value)
        raise error

    try:
        slot = pending_slot(case)
        monkeypatch.setattr(receipt.NativeSourceDeclarationRefs, "__init__", initialize)
        with pytest.raises(RuntimeError) as caught:
            slot.prepare_source_declaration()
        source, = observed
        assert caught.value is error and slot._source_declaration_refs is source
        assert slot._source_constructions["declaration"] is source
        assert source.slot is slot and source.pending is slot.pending
        assert source._source_construction.error is error
        assert case.runtime.construction_error is error
        with pytest.raises(RuntimeStoreError):
            calls.require_original_source_call(slot, source)
    finally:
        await finish(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["uuid", "event", "attach"])
async def test_original_request_first_attempt_credit_and_error_survive_callbacks(monkeypatch, tmp_path, stage):
    case = await constructor_graph(monkeypatch, tmp_path)
    error, observed = RuntimeError("original request " + stage), []
    try:
        slot = pending_slot(case)
        source = slot.prepare_source_declaration()
        event = requests.threading.Event
        attach = calls.OriginalSourceCall.attach_request

        def fail_uuid():
            attempt = slot._source_constructions["request"]
            observed.append(attempt)
            assert attempt.pair_credit == 2 and attempt.value is None
            raise error

        def check_event():
            value = slot._native_request_object
            observed.append(value)
            assert value.native_slot is slot and value.native_pair_credit == 2
            assert value.native_phase == "constructing" and not hasattr(value, "event")
            assert slot._native_request_candidate_id not in requests._open
            with pytest.raises(RuntimeStoreError, match="reentry"):
                requests.retain_native_candidate(slot)
            with pytest.raises(RuntimeStoreError, match="constructor_incomplete"):
                calls.require_original_source_call(slot, source, request=value)
            return event()

        def fail_attach(value, original_slot, request):
            observed.append(request)
            attach(value, original_slot, request)
            raise error

        if stage == "uuid":
            monkeypatch.setattr(requests.uuid, "uuid4", fail_uuid)
        if stage == "event":
            monkeypatch.setattr(requests.threading, "Event", check_event)
        if stage == "attach":
            monkeypatch.setattr(calls.OriginalSourceCall, "attach_request", fail_attach)
        if stage == "event":
            request = requests.retain_native_candidate(slot)
            assert request is observed[0] and request._source_construction.error is None
            assert request.native_phase == "invoke" and request.replay is False
        else:
            with pytest.raises(RuntimeError) as caught:
                requests.retain_native_candidate(slot)
            retained = slot._source_constructions["request"]
            attempt = retained if type(retained) is SourceConstruction else retained._source_construction
            assert caught.value is error and attempt.error is error and attempt.phase == "blocked"
            assert case.runtime.construction_error is error
            with pytest.raises(RuntimeStoreError):
                requests.retain_native_candidate(slot)
            if stage == "attach":
                assert slot._native_request_object is observed[0]
                assert observed[0].native_phase == "blocked" and source.original_source_call.request is observed[0]
                assert requests._open[slot._native_request_candidate_id] is observed[0]
    finally:
        await finish(case)


def test_ordinary_pending_and_default_response_protocol_are_unchanged():
    events = object()
    pending = requests._Pending(SESSION, ["question"], events, "clarify", {"value": 1})
    assert pending.native_slot is pending.native_phase is pending.native_active_use is None
    assert pending.native_pair_credit == 0 and pending.replay is True
    assert pending.qids == ("question",) and pending.params == {"value": 1}
    request_id = "srq-ordinary-constructor-fixture"
    requests._open[request_id] = pending
    assert requests.resolve({"id": request_id, "result": {"answers": {"question": "yes"}}})
    assert pending.event.is_set() and request_id not in requests._open
    assert pending.result is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["assistant", "call"])
async def test_after_initializer_revalidation_retains_original_before_refusing(monkeypatch, tmp_path, kind):
    case = await constructor_graph(monkeypatch, tmp_path, capture_assistant=(kind != "assistant"))
    observed = []
    try:
        cls = calls.OriginalSourceAssistant if kind == "assistant" else calls.OriginalSourceCall
        initialize = cls.__init__

        def changed(value, *args):
            initialize(value, *args)
            observed.append(value)
            if kind == "assistant":
                case.owner.staged = dict(case.witness)
            else:
                case.owner.native_source_assistant = object()

        slot = pending_slot(case) if kind == "call" else None
        monkeypatch.setattr(cls, "__init__", changed)
        with pytest.raises(RuntimeStoreError) as caught:
            if kind == "assistant":
                calls.capture_original_assistant(case.authority, SESSION, 1, case.live, case.owner,
                                                 case.message, case.witness)
            else:
                calls.capture_original_call(slot, case.keeper, case.selection, b"{}", b"{}")
        value, = observed
        attempt = value._source_construction
        assert attempt.value is value and attempt.phase == "blocked" and attempt.error is caught.value
        assert case.runtime.construction_error is caught.value
        retained = (case.runtime.assistants[id(case.witness)] if kind == "assistant"
                    else case.graph.handle.source_owner.calls[id(slot)][1])
        assert retained is value
        with pytest.raises(RuntimeStoreError):
            require_constructed(value)
    finally:
        await finish(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", ["task", "model-call"])
async def test_keeper_callback_cannot_adopt_replacement_before_call_constructor(monkeypatch, tmp_path, replacement):
    case = await constructor_graph(monkeypatch, tmp_path)
    try:
        slot = pending_slot(case)
        task, model = case.keeper.task, case.call
        retain = case.keeper.retain_pin_attempt

        def changed(value):
            retain(value)
            if replacement == "task":
                case.keeper.task = SimpleNamespace(done=lambda: False, get_loop=lambda: case.runtime.loop)
            else:
                case.assistant.calls[0] = {**model, "function": dict(model["function"])}

        monkeypatch.setattr(case.keeper, "retain_pin_attempt", changed)
        with pytest.raises(RuntimeStoreError, match="constructor_inputs_changed") as caught:
            calls.capture_original_call(slot, case.keeper, case.selection, b"{}", b"{}")
        attempt = case.graph.handle.source_owner.calls[id(slot)][1]
        assert type(attempt) is SourceConstruction and attempt.value is None
        assert attempt.inputs[7] is task and attempt.inputs[-1] is model
        assert attempt.error is caught.value and case.runtime.construction_error is caught.value
        assert case.keeper.slots[id(slot)] is slot
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_selection_callback_cannot_replace_declaration_original_task(monkeypatch, tmp_path):
    case = await constructor_graph(monkeypatch, tmp_path)
    try:
        slot = pending_slot(case)
        task = case.keeper.task
        current = type(case.selection).current

        def changed(selection):
            result = current(selection)
            case.keeper.task = SimpleNamespace(done=lambda: False, get_loop=lambda: case.runtime.loop)
            return result

        monkeypatch.setattr(type(case.selection), "current", changed)
        with pytest.raises(RuntimeStoreError, match="constructor_inputs_changed") as caught:
            slot.prepare_source_declaration()
        attempt = slot._source_constructions["declaration"]
        assert type(attempt) is SourceConstruction and attempt.value is None
        assert attempt.inputs[10] is task and attempt.error is caught.value
        assert slot._source_declaration_refs is None and slot._source_declaration_phase == "blocked"
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_uuid_callback_cannot_replace_original_request_event_stream(monkeypatch, tmp_path):
    case = await constructor_graph(monkeypatch, tmp_path)
    try:
        slot = pending_slot(case)
        source = slot.prepare_source_declaration()
        events, uuid = source.live.event_stream, requests.uuid.uuid4

        def changed():
            result = uuid()
            source.live.event_stream = SimpleNamespace(**vars(events))
            return result

        monkeypatch.setattr(requests.uuid, "uuid4", changed)
        with pytest.raises(RuntimeStoreError, match="constructor_inputs_changed") as caught:
            requests.retain_native_candidate(slot)
        attempt = slot._source_constructions["request"]
        assert type(attempt) is SourceConstruction and attempt.value is None
        assert attempt.inputs[12] is events and attempt.error is caught.value
        assert slot._native_request_object is None and slot._native_request_candidate_id not in requests._open
    finally:
        await finish(case)
