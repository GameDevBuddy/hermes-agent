"""Source-only object custody contracts; no kernel, native or installed acceptance."""
import asyncio
import threading
from types import SimpleNamespace

import pytest

from gateway import native_source_runtime as source
from gateway.native_source_issuance import require_original_app_context
from gateway.run import GatewayRunner
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_source_fixture import (
    finish_source_graph, runner_from_actual_constructor, source_graph,
)


@pytest.mark.asyncio
async def test_actual_constructors_keep_original_graph(monkeypatch, tmp_path):
    graph = await source_graph(monkeypatch, tmp_path)
    try:
        original = source.require_original_connection(graph.connection)
        assert original.connection is graph.connection and original.ws is graph.ws
        assert original.transport is graph.transport and original.parent.handle is graph.handle
        assert original.parent.runtime.runner is graph.runner
        assert graph.runtime.lock is graph.runner._gdb_original_source_capture_lock
        assert graph.runtime.base_credit == graph.runner._gdb_original_source_capture_base_credit
        assert original.task is asyncio.current_task()
        with pytest.raises(RuntimeStoreError, match="app_context_issuer_missing"):
            require_original_app_context(graph.connection, SimpleNamespace(profile_id=graph.authority.profile_id))
    finally:
        await finish_source_graph(graph)


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", ["registry", "authority", "server", "listener", "handle",
                                         "transport", "actor", "capture-lock", "base-credit"])
async def test_equal_label_replacements_do_not_adopt_original_source(monkeypatch, tmp_path, replacement):
    graph = await source_graph(monkeypatch, tmp_path)
    original = graph.connection._gdb_original_source_connection
    try:
        changes = {
            "registry": lambda: setattr(graph.runner, "session_authorities", SimpleNamespace(_by_key=graph.registry._by_key)),
            "authority": lambda: graph.registry._by_key.__setitem__(
                graph.authority._gdb_original_source_registry_key, SimpleNamespace(profile_id=graph.authority.profile_id)),
            "server": lambda: setattr(graph.handle, "server", SimpleNamespace(should_exit=False)),
            "listener": lambda: setattr(graph.handle, "socket", object()),
            "handle": lambda: setattr(graph.runner, "session_api", SimpleNamespace(**vars(graph.handle))),
            "transport": lambda: setattr(graph.connection, "transport", SimpleNamespace(**vars(graph.transport))),
            "actor": lambda: setattr(graph.connection, "actor", SimpleNamespace(transport_id=graph.connection.actor.transport_id)),
            "capture-lock": lambda: setattr(graph.runner, "_gdb_original_source_capture_lock", threading.RLock()),
            "base-credit": lambda: setattr(graph.runner, "_gdb_original_source_capture_base_credit", 0),
        }
        changes[replacement]()
        with pytest.raises(RuntimeStoreError):
            source.require_original_connection(graph.connection)
        assert graph.runtime.connections[id(graph.connection)] is original
        assert original.parent.handle is graph.handle and original.ws is graph.ws
    finally:
        await finish_source_graph(graph)


@pytest.mark.asyncio
async def test_failed_first_runtime_construction_keeps_consumed_original_debit(monkeypatch):
    runner = runner_from_actual_constructor(monkeypatch)
    attempts = []

    class FailingConstruction:
        def __new__(cls, *args):
            attempts.append(runner._gdb_original_source_capture_base_credit)
            raise RuntimeError("first-construction-failed")

    monkeypatch.setattr(source, "OriginalSourceRuntime", FailingConstruction)
    with pytest.raises(RuntimeError, match="first-construction-failed"):
        source.capture_original_runner(runner, asyncio.current_task(), asyncio.get_running_loop())
    assert runner._gdb_original_source_runtime is None
    assert attempts == [source.SOURCE_REFERENCE_CREDIT]
    with pytest.raises(RuntimeStoreError, match="reentry"):
        source.capture_original_runner(runner, asyncio.current_task(), asyncio.get_running_loop())
    assert attempts == [source.SOURCE_REFERENCE_CREDIT]
    with pytest.raises(RuntimeError, match="reinitialization_refused"):
        GatewayRunner.__init__(runner, SimpleNamespace(multiplex_profiles=False))
    assert runner._gdb_original_source_capture_base_credit == source.SOURCE_REFERENCE_CREDIT


@pytest.mark.asyncio
async def test_failed_first_validation_cannot_retry_or_supply_constructor_defaults(monkeypatch):
    runner = runner_from_actual_constructor(monkeypatch)
    lock = runner._gdb_original_source_capture_lock
    with pytest.raises(RuntimeStoreError):
        source.capture_original_runner(runner, object(), asyncio.get_running_loop())
    with pytest.raises(RuntimeStoreError, match="reentry"):
        source.capture_original_runner(runner, asyncio.current_task(), asyncio.get_running_loop())
    assert runner._gdb_original_source_capture_lock is lock
    assert runner._gdb_original_source_runtime is None
    missing = object.__new__(GatewayRunner)
    with pytest.raises(RuntimeStoreError, match="constructor_missing"):
        source.capture_original_runner(missing, asyncio.current_task(), asyncio.get_running_loop())
    assert not hasattr(missing, "_gdb_original_source_capture_lock")


@pytest.mark.asyncio
async def test_retired_connection_cannot_be_revived_and_keeps_refs(monkeypatch, tmp_path):
    graph = await source_graph(monkeypatch, tmp_path)
    original = source.require_original_connection(graph.connection)
    try:
        source.retire_original_connection(graph.connection)
        with pytest.raises(RuntimeStoreError, match="connection_changed"):
            source.require_original_connection(graph.connection)
        with pytest.raises(RuntimeStoreError):
            source.capture_original_connection(original.parent, graph.connection, graph.ws,
                                               graph.transport, asyncio.current_task())
        assert original.retired and original.connection is graph.connection
        assert graph.runtime.connections[id(graph.connection)] is original
    finally:
        await finish_source_graph(graph)


@pytest.mark.asyncio
async def test_listener_retirement_holds_interrupted_capture(monkeypatch, tmp_path):
    graph = await source_graph(monkeypatch, tmp_path)
    parent = graph.handle.source_owner
    slot = object()
    try:
        with graph.runtime.lock:
            graph.runtime.reserve(parent.calls, id(slot))
            parent.calls[id(slot)] = (slot, None)
        with pytest.raises(RuntimeStoreError, match="retirement_held"):
            source.retire_original_listener(parent, request_stop=True)
        assert parent.retiring and parent.calls[id(slot)] == (slot, None)
        assert graph.handle.server.should_exit is False
    finally:
        await finish_source_graph(graph)
