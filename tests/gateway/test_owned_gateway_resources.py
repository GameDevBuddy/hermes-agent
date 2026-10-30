"""Resource callback tests; no gateway or external process is started."""
import asyncio
from contextlib import contextmanager
import sys
import threading
from types import SimpleNamespace

import pytest

from gateway import run_owned_resources as resources
from gateway.run_shutdown_delivery import _send_error, _send_failed
from hermes_owned_gateway import OwnedGatewayUnknown


@pytest.mark.parametrize("result,failed", [
    (None, False), (object(), False),
    (SimpleNamespace(success=True), False),
    (SimpleNamespace(success=False, error="rejected"), True),
])
def test_original_shutdown_notice_result_policy(result, failed):
    assert _send_failed(result) is failed
    assert _send_error(result) == getattr(result, "error", "send returned success=False")


@pytest.fixture
def tool_modules(monkeypatch):
    calls = []
    monkeypatch.setitem(sys.modules, "tools.mcp_tool_owned",
                        SimpleNamespace(shutdown_all=lambda: calls.append("mcp")))
    for name in ("tools.browser_session", "tools.terminal_tool"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    return calls


@pytest.mark.parametrize("name,lock,registry", [
    ("tools.browser_session", "_cleanup_lock", "_active_sessions"),
    ("tools.terminal_tool", "_env_lock", "_active_environments"),
])
def test_live_tool_instances_are_retained_without_legacy_cleanup(
        monkeypatch, tool_modules, name, lock, registry):
    instance = object()
    module = SimpleNamespace(**{
        lock: threading.Lock(), registry: {"original": instance},
        "_cleanup_running": True, "_cleanup_thread": None,
    })
    monkeypatch.setitem(sys.modules, name, module)
    source = SimpleNamespace(unresolved_resource_refs=[])
    with pytest.raises(OwnedGatewayUnknown, match="normal-tool-closure-unproven"):
        asyncio.run(resources.close_tool_resources(source))
    assert source.unresolved_resource_refs == [instance]
    assert module._cleanup_running is True
    assert getattr(module, registry)["original"] is instance
    assert tool_modules == ["mcp"]


def test_empty_tool_registry_finishes_its_original_source_test_thread(monkeypatch, tool_modules):
    thread = threading.Thread(target=lambda: None)
    thread.start()
    module = SimpleNamespace(
        _env_lock=threading.Lock(), _active_environments={},
        _cleanup_running=True, _cleanup_thread=thread,
    )
    monkeypatch.setitem(sys.modules, "tools.terminal_tool", module)
    source = SimpleNamespace(unresolved_resource_refs=[])
    asyncio.run(resources.close_tool_resources(source))
    assert not thread.is_alive()
    assert module._cleanup_thread is thread
    assert module._cleanup_running is False
    assert source.unresolved_resource_refs == []


@pytest.fixture
def remaining_modules(monkeypatch):
    stop = threading.Event()
    monkeypatch.setitem(sys.modules, "hermes_cli.nous_auth_keepalive",
                        SimpleNamespace(_keepalive_stop=stop))
    monkeypatch.setitem(sys.modules, "hermes_logging", SimpleNamespace(
        _queue_state_lock=threading.Lock(), _queue_listener=None))
    monkeypatch.setitem(sys.modules, "gateway.runtime_ownership",
                        SimpleNamespace(process_ownership=SimpleNamespace(_writers=[])))
    monkeypatch.delitem(sys.modules, "agent.auxiliary_client", raising=False)
    return stop


def test_foreign_sdk_loop_is_not_neutered_or_closed(monkeypatch, remaining_modules):
    calls = []
    client = SimpleNamespace(close=lambda: calls.append("close"), is_closed=False)
    cache = {"original": (client, "fixture", object())}
    monkeypatch.setitem(sys.modules, "agent.auxiliary_client", SimpleNamespace(
        _client_cache_lock=threading.Lock(), _client_cache=cache))
    with pytest.raises(OwnedGatewayUnknown, match="original-auxiliary-source-required"):
        asyncio.run(resources.close_remaining_python_resources(object(), None))
    assert calls == []
    assert cache["original"][0] is client
    assert client.is_closed is False


def test_cache_loop_does_not_mint_async_close_ownership(monkeypatch, remaining_modules):
    calls = []

    class Client:
        is_closed = False

        async def close(self):
            await asyncio.sleep(0)
            calls.append("closed")
            self.is_closed = True

    async def scenario():
        client = Client()
        cache = {"original": (client, "fixture", asyncio.get_running_loop())}
        monkeypatch.setitem(sys.modules, "agent.auxiliary_client", SimpleNamespace(
            _client_cache_lock=threading.Lock(), _client_cache=cache))
        with pytest.raises(OwnedGatewayUnknown, match="original-auxiliary-source-required"):
            await resources.close_remaining_python_resources(object(), None)
        assert not client.is_closed
        assert cache["original"][0] is client

    asyncio.run(scenario())
    assert calls == []


def test_sdk_close_return_without_closed_state_is_not_accepted(monkeypatch, remaining_modules):
    calls = []
    client = SimpleNamespace(close=lambda: calls.append("close"), is_closed=False)
    cache = {"original": (client, "fixture", None)}
    monkeypatch.setitem(sys.modules, "agent.auxiliary_client", SimpleNamespace(
        _client_cache_lock=threading.Lock(), _client_cache=cache))
    with pytest.raises(OwnedGatewayUnknown, match="original-auxiliary-source-required"):
        asyncio.run(resources.close_remaining_python_resources(object(), None))
    assert calls == []
    assert cache["original"][0] is client


def test_source_test_log_writer_stops_through_original_sentinel(monkeypatch, remaining_modules):
    import logging
    from logging.handlers import QueueListener
    import queue

    listener = QueueListener(queue.Queue(), logging.NullHandler())
    listener.start()
    original_thread = listener._thread
    monkeypatch.setitem(sys.modules, "hermes_logging", SimpleNamespace(
        _queue_state_lock=threading.Lock(), _queue_listener=listener))
    asyncio.run(resources.close_remaining_python_resources(object(), None))
    assert not original_thread.is_alive()
    assert listener._thread is None


def test_database_closure_keeps_original_objects_and_close_order(monkeypatch):
    calls = []
    primary = SimpleNamespace(close=lambda: calls.append("primary"))
    secondary = SimpleNamespace(close=lambda: calls.append("secondary"))
    store = SimpleNamespace(_db=secondary,
                            close_all_db_handles=lambda: calls.append("store-handles"))
    runner = SimpleNamespace(
        _session_db=primary, session_store=store,
        close_all_session_db_handles=lambda: calls.append("session-handles"))
    monkeypatch.setitem(sys.modules, "hermes_state_registry",
                        SimpleNamespace(close_all=lambda: calls.append("registry")))
    resources.close_stores(runner)
    assert calls == ["primary", "secondary", "store-handles", "session-handles", "registry"]
    assert runner._session_db is primary
    assert store._db is secondary


def test_agent_close_uses_owning_profile_and_keeps_original_cache(monkeypatch, tmp_path):
    events = []

    @contextmanager
    def profile(home):
        events.append(("enter", home))
        yield
        events.append(("leave", home))

    class Client:
        is_closed = False

        def close(self):
            events.append("client")
            self.is_closed = True

    client = Client()
    manager = SimpleNamespace(shutdown_owned=lambda: events.append("memory"))
    agent = SimpleNamespace(_memory_manager=manager, client=client)
    cache = {"profile/session": (agent, object())}
    home = tmp_path / "owning-profile"
    runner = SimpleNamespace(
        _agent_cache_lock=threading.Lock(), _agent_cache=cache,
        session_store=SimpleNamespace(_profile_home_for_key=lambda key: home))
    monkeypatch.setitem(sys.modules, "gateway.run", SimpleNamespace(_profile_runtime_scope=profile))
    monkeypatch.setitem(sys.modules, "httpx", SimpleNamespace(Client=Client, AsyncClient=Client))
    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=Client, AsyncOpenAI=Client))
    asyncio.run(resources.close_agent_resources(runner))
    assert events == [("enter", home), "memory", "client", ("leave", home)]
    assert runner._agent_cache is cache and cache["profile/session"][0] is agent
    assert client.is_closed
