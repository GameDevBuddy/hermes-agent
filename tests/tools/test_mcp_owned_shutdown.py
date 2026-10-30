"""Normal MCP teardown uses source-test tasks only; no SDK process is launched."""
import asyncio
import io
import threading
from types import SimpleNamespace

import pytest

import hermes_owned_gateway as ownership
from tools.mcp_tool import MCPServerTask
from tools import mcp_tool_owned as owned


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setattr(ownership.sys, "stdin", SimpleNamespace(buffer=io.BytesIO()))
    source = ownership.OwnedGatewaySource(SimpleNamespace(attempt_id="mcp-source-fixture"), b"{}")
    source.job = SimpleNamespace(require_current_member=lambda: None)
    source.phase = "bound"
    monkeypatch.setattr(ownership, "_CURRENT", source)
    value = MCPServerTask("owned-source-test")
    value.session = object()
    return value


def test_successful_owned_shutdown_is_not_replayed(server, monkeypatch):
    calls = []
    monkeypatch.setattr(MCPServerTask, "_deregister_tools", lambda self: calls.append(self))

    async def scenario():
        await owned.shutdown_server(server)
        with pytest.raises(ownership.OwnedGatewayUnknown, match="already-attempted"):
            await owned.shutdown_server(server)

    asyncio.run(scenario())
    assert calls == [server]


def test_failed_owned_shutdown_retains_first_error_and_session(server, monkeypatch):
    calls = []
    original_session = server.session
    monkeypatch.setattr(MCPServerTask, "_deregister_tools", lambda self: calls.append(self))

    async def scenario():
        task = asyncio.get_running_loop().create_future()
        failure = ValueError("original-source-task-failed")
        task.set_exception(failure)
        server._task = task
        with pytest.raises(ValueError, match="original-source-task-failed"):
            await owned.shutdown_server(server)
        assert server._owned_shutdown_error is failure
        with pytest.raises(ownership.OwnedGatewayUnknown, match="already-attempted"):
            await owned.shutdown_server(server)
        assert server._task is task

    asyncio.run(scenario())
    assert server.session is original_session
    assert calls == []


def test_owned_bound_server_uses_normal_route_before_global_drain(server, monkeypatch):
    calls = []

    async def normal(value):
        calls.append(value)

    monkeypatch.setattr(owned, "shutdown_server", normal)
    asyncio.run(server.shutdown())
    assert calls == [server]
    assert not server._shutdown_event.is_set()


def test_legacy_bare_mixin_caller_preserves_default_shutdown():
    from tools.mcp_tool_server_run import MCPServerRunMixin

    async def scenario():
        calls = []
        value = SimpleNamespace(
            _shutdown_event=asyncio.Event(), _reconnect_event=asyncio.Event(),
            _task=None, _pending_refresh_tasks=set(), session=object(),
            _deregister_tools=lambda: calls.append("deregister"))
        await MCPServerRunMixin.shutdown(value)
        assert value._shutdown_event.is_set() and value._reconnect_event.is_set()
        assert value.session is None and calls == ["deregister"]

    asyncio.run(scenario())


def test_owned_shutdown_waits_for_original_task_before_deregistering(server, monkeypatch):
    events = []
    monkeypatch.setattr(MCPServerTask, "_deregister_tools", lambda self: events.append("deregister"))

    async def scenario():
        async def original():
            await server._shutdown_event.wait()
            events.append("task-finished")

        server._task = asyncio.create_task(original())
        await owned.shutdown_server(server)
        assert server._task.done() and not server._task.cancelled()
        assert server.session is None

    asyncio.run(scenario())
    assert events == ["task-finished", "deregister"]


def test_cancelled_original_task_does_not_deregister_or_clear_session(server, monkeypatch):
    calls = []
    original_session = server.session
    monkeypatch.setattr(MCPServerTask, "_deregister_tools", lambda self: calls.append(self))

    async def scenario():
        task = asyncio.get_running_loop().create_future()
        task.cancel()
        server._task = task
        with pytest.raises(asyncio.CancelledError):
            await owned.shutdown_server(server)
        assert server._task is task

    asyncio.run(scenario())
    assert calls == [] and server.session is original_session


def test_foreign_task_loop_is_refused_before_shutdown_signals(server):
    foreign = asyncio.new_event_loop()
    try:
        original_task = foreign.create_future()
        original_task.set_result(None)
        server._task = original_task
        with pytest.raises(ownership.OwnedGatewayUnknown, match="original-MCP-task-loop"):
            asyncio.run(owned.shutdown_server(server))
        assert server._task is original_task
        assert not server._shutdown_event.is_set()
    finally:
        foreign.close()


def test_failed_refresh_does_not_signal_or_discard_original_task(server, monkeypatch):
    calls = []
    original_session = server.session
    monkeypatch.setattr(MCPServerTask, "_deregister_tools", lambda self: calls.append(self))

    async def scenario():
        refresh = asyncio.get_running_loop().create_future()
        refresh.set_exception(ValueError("original-refresh-failed"))
        server._pending_refresh_tasks.add(refresh)
        with pytest.raises(ValueError, match="original-refresh-failed"):
            await owned.shutdown_server(server)
        assert refresh in server._pending_refresh_tasks
        assert not server._shutdown_event.is_set()

    asyncio.run(scenario())
    assert calls == [] and server.session is original_session


@pytest.fixture
def original_mcp_loop(server, monkeypatch):
    from tools import mcp_tool as origin
    from tools import mcp_tool_config as config
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever)
    ready = threading.Event()
    loop.call_soon(ready.set)
    registry = {"original": server}
    logs = []
    monkeypatch.setattr(origin, "_mcp_loop", loop)
    monkeypatch.setattr(origin, "_mcp_thread", thread)
    monkeypatch.setattr(origin, "_servers", registry)
    monkeypatch.setattr(origin, "_server_connecting", {})
    monkeypatch.setattr(config, "_close_mcp_stderr_logs", lambda **kw: logs.append(kw))
    monkeypatch.setattr(MCPServerTask, "_deregister_tools", lambda self: None)
    thread.start()
    try:
        assert ready.wait(timeout=10)
        yield SimpleNamespace(loop=loop, thread=thread, registry=registry, logs=logs, origin=origin)
    finally:
        # This fixture owns an otherwise empty source-test loop, not a service.
        if loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        thread.join()
        if not loop.is_closed():
            loop.close()


def test_normal_shutdown_closes_original_loop_and_registry(server, original_mcp_loop):
    owned.shutdown_all()
    assert original_mcp_loop.loop.is_closed()
    assert not original_mcp_loop.thread.is_alive()
    assert original_mcp_loop.registry == {}
    assert original_mcp_loop.origin._mcp_loop is None
    assert original_mcp_loop.origin._mcp_thread is None
    assert original_mcp_loop.logs == [{"scope": None}]
    assert server in server._owned_source.mcp_contexts


def test_previously_completed_server_is_not_replayed_by_global_shutdown(server, original_mcp_loop):
    asyncio.run_coroutine_threadsafe(owned.shutdown_server(server), original_mcp_loop.loop).result()
    owned.shutdown_all()
    assert original_mcp_loop.loop.is_closed()
    assert original_mcp_loop.registry == {}


@pytest.mark.parametrize("changed", ["session", "task", "refresh"])
def test_changed_completed_work_blocks_loop_release(server, original_mcp_loop, changed):
    asyncio.run_coroutine_threadsafe(owned.shutdown_server(server), original_mcp_loop.loop).result()

    async def mutate():
        if changed == "session":
            server.session = object()
        else:
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            if changed == "task":
                server._task = future
            else:
                server._pending_refresh_tasks.add(future)

    asyncio.run_coroutine_threadsafe(mutate(), original_mcp_loop.loop).result()
    with pytest.raises(ownership.OwnedGatewayUnknown, match="completed-MCP"):
        owned.shutdown_all()
    assert original_mcp_loop.loop.is_running()
    assert original_mcp_loop.registry["original"] is server
