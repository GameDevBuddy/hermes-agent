"""Original before-initializer custody; fake lower failures convey no remote/native authority."""
import asyncio

import pytest

from agent import native_receipt_origin as receipt
from gateway import native_source_calls as calls
from gateway import session_server_requests as requests
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_constructor_fixture import (
    SESSION, constructor_graph, finish, original_self, pending_slot,
)


def assert_failure(value, error):
    attempt = value._source_construction
    assert attempt.value is value and attempt.error is error and attempt.phase == "blocked"
    assert attempt.credit > 0 and attempt.arguments
    return attempt


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["lock", "arguments", "call", "tool"])
async def test_slot_keeps_actual_partial_before_lower_initialization(monkeypatch, tmp_path, failure):
    case = await constructor_graph(monkeypatch, tmp_path)
    try:
        from agent.tool_commit_owner import PendingToolCommit
        from agent.tool_commit_rows import json_digest
        from types import SimpleNamespace
        from tests.gateway.native_constructor_fixture import CALL, TOOL, UID
        ref = SimpleNamespace(name=TOOL, call_id=CALL, task_id=SESSION, args={"effective": 2})
        pending = PendingToolCommit(case.owner, case.witness, UID, ref, json_digest(ref.args))
        error = RuntimeError("original slot lower failure")
        original_lock, original_freeze = receipt.threading.RLock, receipt.frozen_json
        count = 0

        def fail_lock():
            raise error

        def freeze(value, *args):
            nonlocal count
            count += 1
            if count == {"arguments": 1, "call": 2, "tool": 3}[failure]:
                raise error
            return original_freeze(value, *args)

        with monkeypatch.context() as lower:
            lower.setattr(receipt.threading, "RLock", fail_lock if failure == "lock" else original_lock)
            if failure != "lock":
                lower.setattr(receipt, "frozen_json", freeze)
            with pytest.raises(RuntimeError) as caught:
                case.bridge.reserve(pending)
        assert caught.value is error
        value = original_self(error, receipt.NativeSlot.__init__)
        assert case.bridge.slots[UID] is value
        attempt = assert_failure(value, error)
        assert attempt.arguments[-1] is pending
        with pytest.raises(RuntimeStoreError, match="reentry"):
            case.bridge.reserve(pending)
        assert case.bridge.slots[UID] is value and attempt.error is error
        with pytest.raises(RuntimeStoreError):
            value.prepare_source_declaration()
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_assistant_keeps_partial_and_first_witness_failure(monkeypatch, tmp_path):
    case = await constructor_graph(monkeypatch, tmp_path, capture_assistant=False)
    error = RuntimeError("original witness getter")

    class Witness(dict):
        def __getitem__(self, key):
            if key == "calls_digest":
                raise error
            return super().__getitem__(key)

    witness = Witness(case.witness)
    case.owner.staged = witness
    try:
        with pytest.raises(RuntimeError) as caught:
            calls.capture_original_assistant(case.authority, SESSION, 1, case.live, case.owner,
                                             case.message, witness)
        value = original_self(error, calls.OriginalSourceAssistant.__init__)
        assert caught.value is error and case.runtime.assistants[id(witness)] is value
        assert_failure(value, error)
        with pytest.raises(RuntimeStoreError):
            value.require_assistant()
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_call_keeps_partial_before_original_argument_getter(monkeypatch, tmp_path):
    error = RuntimeError("original effective argument getter")
    from tests.gateway.native_constructor_fixture import CALL, TOOL

    class Ref:
        name, call_id, task_id = TOOL, CALL, SESSION
        fail = False

        @property
        def args(self):
            if self.fail:
                raise error
            return self.original

    case = await constructor_graph(monkeypatch, tmp_path)
    try:
        ref = Ref()
        ref.original = {"effective": 2}
        slot = pending_slot(case, ref)
        ref.fail = True
        with pytest.raises(RuntimeError) as caught:
            calls.capture_original_call(slot, case.keeper, case.selection, b"{}", b"{}")
        value = original_self(error, calls.OriginalSourceCall.__init__)
        assert caught.value is error and case.graph.handle.source_owner.calls[id(slot)] == (slot, value)
        assert case.keeper.slots[id(slot)] is slot
        assert_failure(value, error)
        with pytest.raises(RuntimeStoreError):
            value.require_call()
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_request_keeps_actual_partial_before_event_constructor(monkeypatch, tmp_path):
    case = await constructor_graph(monkeypatch, tmp_path)
    error = asyncio.CancelledError("original event constructor cancellation")
    try:
        slot = pending_slot(case)
        slot.prepare_source_declaration()

        def event():
            raise error

        with monkeypatch.context() as lower:
            lower.setattr(requests.threading, "Event", event)
            with pytest.raises(asyncio.CancelledError) as caught:
                requests.retain_native_candidate(slot)
        value = original_self(error, requests._Pending.__init__)
        assert caught.value is error and slot._native_request_object is value
        assert slot._native_request_candidate_id is not None
        attempt = assert_failure(value, error)
        assert attempt.pair_credit == 2
        with pytest.raises(RuntimeStoreError, match="reentry"):
            requests.retain_native_candidate(slot)
        assert slot._native_request_object is value and attempt.error is error
        assert slot._native_request_candidate_id not in requests._open
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_declaration_failure_retains_original_and_cannot_be_used(monkeypatch, tmp_path):
    case = await constructor_graph(monkeypatch, tmp_path)
    error = RuntimeError("original declaration attach failure")
    try:
        slot = pending_slot(case)
        attach = calls.OriginalSourceCall.attach_declaration

        def fail(value, source):
            attach(value, source)
            raise error

        monkeypatch.setattr(calls.OriginalSourceCall, "attach_declaration", fail)
        with pytest.raises(RuntimeError) as caught:
            slot.prepare_source_declaration()
        source = slot._source_declaration_refs
        assert caught.value is error and source is not None
        assert slot._source_declaration_phase == "blocked"
        with pytest.raises(RuntimeStoreError):
            calls.require_original_source_call(slot, source)
        assert_failure(source, error)
    finally:
        await finish(case)
