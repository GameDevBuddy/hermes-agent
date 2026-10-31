"""Focused lazy bridge source-only associations/defaults; no App authorization or native IO."""
from copy import copy
from functools import partial
import contextvars
import sys

import pytest

from agent import native_receipt_origin as origin
from agent.native_bridge_construction import require_bridge_ready, BRIDGE_REFERENCE_CREDIT
from agent.tool_commit_owner import owner_call
from gateway.session_native_context import bridge_for_owner
from hermes_state_runtime import RuntimeStoreError
from tests.agent.native_bridge_constructor_fixture import bridge_case, begin, add_call, finish
from tests.gateway.native_constructor_fixture import original_self, TOOL, TOOL_REF


@pytest.mark.asyncio
async def test_successful_lazy_bridge_and_next_assistant_preserve_original_slot_behavior(monkeypatch, tmp_path):
    case = await bridge_case(monkeypatch, tmp_path)
    try:
        _, pending = begin(case)
        bridge, attempt = case.owner.native_bridge, case.owner._native_bridge_construction
        assert bridge is attempt.value and pending.native_dispatch_slot.bridge is bridge
        assert attempt.phase == case.owner._native_bridge_constructor_phase == "ready"
        assert case.keeper.bridge_construction is case.graph.runtime.bridges[id(case.owner)] is attempt
        assert case.owner._native_bridge_constructor_credit == BRIDGE_REFERENCE_CREDIT
        second_ref = add_call(case, new_assistant=True)
        second = owner_call(case.agent, "bind", second_ref)
        assert second.native_dispatch_slot.bridge is bridge and len(bridge.slots) == 2
        assert case.owner._native_bridge_construction is attempt and require_bridge_ready(bridge) is None
    finally:
        await finish(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["database", "record", "selection"])
async def test_original_initializer_callback_displacement_retains_same_partial_and_error(monkeypatch, tmp_path, changed):
    holder, calls = {}, []

    class Item:
        def __getitem__(self, key):
            case = holder["case"]
            if not calls:
                calls.append(key)
                if changed == "database":
                    case.owner.db = object()
                elif changed == "record":
                    case.owner._native_bridge_construction = object()
                else:
                    replacement = copy(case.selection)
                    assert replacement == case.selection and replacement is not case.selection
                    case.live.event_stream._gdb_native_selection = replacement
            return TOOL if key == "name" else TOOL_REF

    case = holder["case"] = await bridge_case(monkeypatch, tmp_path, [Item()])
    try:
        _, pending = begin(case)
        attempt = case.keeper.bridge_construction
        assert pending.native_origin_error == "native_original_owner_unavailable"
        assert pending.native_dispatch_slot is None and attempt.phase == "failed" and attempt.value is not None
        assert case.owner._native_bridge_constructor_error is attempt.error
        assert case.graph.runtime.construction_error is attempt.error and attempt.traceback is not None
        assert attempt.value._bridge_initialized is True and case.owner.native_bridge is None
        with pytest.raises(RuntimeStoreError):
            attempt.value.reserve(case.owner.pending[0])
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_reentrant_new_uid_refuses_before_nonreentrant_database_lock(monkeypatch, tmp_path):
    holder, observed = {}, []

    class Item:
        def __getitem__(self, key):
            case, ref = holder["case"], holder["ref"]
            with case.owner.db.lock:
                observed.append(owner_call(case.agent, "bind", ref))
            return TOOL if key == "name" else TOOL_REF

    case = holder["case"] = await bridge_case(monkeypatch, tmp_path, [Item()])
    holder["ref"] = add_call(case)
    try:
        _, pending = begin(case)
        assert pending.native_dispatch_slot is not None
        assert observed and all(value is None for value in observed)
        assert len(case.owner.pending) == 1 and len(case.graph.runtime.bridges) == 1
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_actual_initializer_base_exception_keeps_self_and_is_not_swallowed(monkeypatch, tmp_path):
    case = await bridge_case(monkeypatch, tmp_path)
    initializer, lower = origin.NativeDispatchBridge.__init__, origin.threading.RLock
    error = BaseException("actual bridge initializer abort")

    def lock(*args, **kwargs):
        if sys._getframe(1).f_code is initializer.__code__:
            raise error
        return lower(*args, **kwargs)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(origin.threading, "RLock", lock)
            with pytest.raises(BaseException) as caught:
                begin(case)
        attempt = case.owner._native_bridge_construction
        assert caught.value is error and attempt.error is error and attempt.value is original_self(error, initializer)
        assert case.owner._native_bridge_constructor_traceback is not None
        assert case.keeper.bridge_construction_error is error and case.graph.runtime.construction_error is error
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_failed_lazy_attempt_does_not_retry_but_ordinary_tool_still_binds(monkeypatch, tmp_path):
    case = await bridge_case(monkeypatch, tmp_path)
    original, error, calls = case.owner.native_bridge_factory, ValueError("actual bridge tools getter"), []

    class Item:
        def __getitem__(self, key):
            calls.append(key)
            raise error

    case.tools[:] = [Item()]
    try:
        _, pending = begin(case)
        attempt = case.owner._native_bridge_construction
        assert pending.native_origin_error == "native_original_owner_unavailable"
        native_ref = add_call(case)
        repeated = owner_call(case.agent, "bind", native_ref)
        assert repeated.native_origin_error == "native_original_owner_unavailable" and repeated.native_dispatch_slot is None
        ordinary_ref = add_call(case, call_id="ordinary", uid="ordinary-uid", name="fixture_echo", new_assistant=True)
        ordinary = owner_call(case.agent, "bind", ordinary_ref)
        assert ordinary is not None and ordinary.native_dispatch_slot is None and ordinary.native_origin_error is None
        assert len(calls) == 1 and attempt.error is error and attempt.factory is original
        assert case.owner._native_bridge_constructor_credit == BRIDGE_REFERENCE_CREDIT
    finally:
        await finish(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("allocation", ["opaque", "raised"])
async def test_allocator_outcomes_are_retained_without_adoption_or_initialization(monkeypatch, tmp_path, allocation):
    case = await bridge_case(monkeypatch, tmp_path)
    opaque, error, reached = object(), RuntimeError("original no-self allocator"), []

    def allocate(cls, *args, **kwargs):
        reached.append(cls)
        if allocation == "raised":
            raise error
        return opaque

    try:
        with monkeypatch.context() as patch:
            patch.setattr(origin.NativeDispatchBridge, "__new__", staticmethod(allocate))
            _, pending = begin(case)
        attempt = case.owner._native_bridge_construction
        assert attempt.value is None and attempt.phase == "failed"
        assert len(reached) == 1 and case.owner.native_bridge is None and case.keeper.bridge_construction is attempt
        if allocation == "opaque":
            assert pending.native_origin_error == "native_original_owner_unavailable"
            assert attempt.observed_result is opaque and attempt.error is not None
        else:
            assert pending is None
            assert attempt.error is error and attempt.observed_result is None
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_real_canonical_factory_gates_still_refuse_before_bridge_allocation(monkeypatch, tmp_path):
    case = await bridge_case(monkeypatch, tmp_path)
    case.owner.native_bridge_factory = partial(bridge_for_owner, case.authority, case.agent.session_id, 1, case.live)
    try:
        _, pending = begin(case)
        attempt = case.owner._native_bridge_construction
        assert pending.native_origin_error == "native_original_owner_unavailable" and case.owner.native_bridge is None
        assert attempt.value is None and isinstance(attempt.error, RuntimeStoreError)
        assert attempt.factory.func is bridge_for_owner and attempt.phase == "failed"
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_direct_unmanaged_observational_bridge_and_source_consumer_shape_stays_usable(monkeypatch, tmp_path):
    case = await bridge_case(monkeypatch, tmp_path)
    try:
        bridge = origin.NativeDispatchBridge(case.graph.connection, case.owner,
                                            case.selection.context, case.selection.snapshot, case.tools)
        assert bridge._bridge_initialized is True and getattr(bridge, "_bridge_construction", None) is None
        assert require_bridge_ready(bridge) is None
        case.owner.native_bridge = bridge
        _, pending = begin(case)
        assert pending.native_dispatch_slot.bridge is bridge
        assert case.owner._native_bridge_constructor_credit == 0
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_partial_bridge_source_consumers_refuse_without_masking_first_error(monkeypatch, tmp_path):
    error = ValueError("actual tools getter first error")

    class Item:
        def __getitem__(self, key):
            raise error

    case = await bridge_case(monkeypatch, tmp_path, [Item()])
    try:
        _, pending = begin(case)
        attempt = case.owner._native_bridge_construction
        with pytest.raises(RuntimeStoreError):
            attempt.value.reserve(pending)
        from agent.native_slot_construction import prepare_declaration
        from types import SimpleNamespace
        with pytest.raises(RuntimeStoreError):
            prepare_declaration(SimpleNamespace(bridge=attempt.value))
        assert attempt.error is error and case.owner._native_bridge_constructor_error is error
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_post_factory_publication_error_keeps_original_bridge_and_first_traceback(monkeypatch, tmp_path):
    case = await bridge_case(monkeypatch, tmp_path)
    error, values = RuntimeError("original bridge publication setter"), []
    setter = type(case.owner).__setattr__

    def publish(self, name, value):
        if self is case.owner and name == "native_bridge" and type(value) is origin.NativeDispatchBridge:
            values.append(value)
            raise error
        setter(self, name, value)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(type(case.owner), "__setattr__", publish)
            _, pending = begin(case)
        attempt = case.owner._native_bridge_construction
        assert pending is None and values == [attempt.value]
        assert attempt.error is error and attempt.traceback is not None
        assert attempt.value._bridge_initialized is True and case.owner.native_bridge is None
        assert case.keeper.bridge_construction_error is case.graph.runtime.construction_error is error
    finally:
        await finish(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["context", "getter"])
async def test_real_tools_callback_identity_and_context_changes_are_refused(monkeypatch, tmp_path, change):
    marker, reached = contextvars.ContextVar("bridge-source-context"), []
    token = marker.set("original")

    class Item:
        def __getitem__(self, key):
            if not reached:
                reached.append(True)
                if change == "context":
                    marker.set("changed")
                else:
                    type(self).__getitem__ = lambda self, name: TOOL if name == "name" else TOOL_REF
            return TOOL if key == "name" else TOOL_REF

    original = Item.__getitem__
    item = Item()
    case = await bridge_case(monkeypatch, tmp_path, [item])
    try:
        _, pending = begin(case)
        attempt = case.owner._native_bridge_construction
        assert pending.native_origin_error == "native_original_owner_unavailable"
        assert attempt.phase == "failed" and attempt.value._bridge_initialized is True
        assert attempt.tool_callbacks == [(item, original)]
        assert attempt.error is case.owner._native_bridge_constructor_error
        assert attempt.creator_thread is not None and attempt.loop is case.graph.runtime.loop
    finally:
        marker.reset(token)
        await finish(case)
