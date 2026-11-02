"""Normal callbacks and explicit holds for unresolved resource families.

The optional-family guards below are unresolved acceptance gates, not fake empty
registries or permission to discard resources. A loaded resource without a known
normal closer prevents completion; its feature is never disabled to pass a test.
"""
from __future__ import annotations

import asyncio
import inspect
import sys
import threading

from hermes_owned_gateway import OwnedGatewayUnknown


async def close_authorities(runner):
    from gateway.session_authorities import all_authorities
    from gateway.session_runtime_workers import mutation_tasks, physical_workers, side_agents, executing_sessions
    for authority in all_authorities(runner):
        authority.retiring = True
        service = authority.hosted_room_service
        if service is not None:
            await asyncio.to_thread(service.runtime.stop_owned)
        for waiter in authority.waiters.values():
            waiter.set()
        if (mutation_tasks(authority) or list(physical_workers(authority))
                or side_agents(authority) or list(executing_sessions(authority))):
            raise OwnedGatewayUnknown("authority-writers-remain")
        authority.retired = True


async def close_agent_resources(runner):
    from agent.auxiliary_owned import managed_active
    if managed_active():
        from agent.primary_owned import close_agent_memories
        await close_agent_memories(runner)
        return
    from gateway.run import _profile_runtime_scope
    from hermes_constants import get_default_hermes_root
    with runner._agent_cache_lock:
        cached = list(runner._agent_cache.items())
    for key, entry in cached:
        agent = entry[0] if isinstance(entry, tuple) else entry
        # The release scope must be the owning session's, never ambient HOME.
        home = runner.session_store._profile_home_for_key(key)
        home = home if home is not None else get_default_hermes_root()
        with _profile_runtime_scope(home):
            manager = agent._memory_manager
            if manager is not None:
                if getattr(manager, "_owned_shutdown_completed", False):
                    await asyncio.to_thread(manager.require_owned_settled)
                else:
                    await asyncio.to_thread(manager.shutdown_owned)
            client = agent.client
            if client is not None:
                import httpx
                import openai
                if not isinstance(client, (httpx.Client, httpx.AsyncClient, openai.OpenAI, openai.AsyncOpenAI)):
                    raise OwnedGatewayUnknown("agent-sdk-normal-close-not-verified")
                result = client.close()
                if inspect.isawaitable(result):
                    await result
                closed = client.is_closed
                closed = closed() if callable(closed) else closed
                if closed is not True:
                    raise OwnedGatewayUnknown("agent-client-did-not-close")
    # Do not clear the cache to manufacture a zero; keep the original agent refs.


async def close_adapters(runner):
    from gateway.config import Platform
    adapters = [*runner.adapters.items()]
    for profile in runner._profile_adapters.values():
        adapters.extend(profile.items())
    for platform, adapter in adapters:
        adapter.set_message_handler(None)
        if platform == Platform.LOCAL:
            # The local adapter owns no separate listener/process. The canonical
            # session API and authorities are closed by their actual owners.
            continue
        # No arbitrary plugin hook is accepted as a non-killing closure proof.
        raise OwnedGatewayUnknown(f"normal-adapter-closure-unproven:{platform.value}")


async def close_tool_resources(source):
    from tools.mcp_tool_owned import shutdown_all
    await asyncio.to_thread(shutdown_all)
    # Actual optional instances, not a configuration/tool-count prediction.
    checks = (("tools.browser_session", "_cleanup_lock", "_active_sessions"),
              ("tools.terminal_tool", "_env_lock", "_active_environments"))
    for module_name, lock_name, registry_name in checks:
        module = sys.modules.get(module_name)
        if module is None:
            continue  # no constructor in that module has run in this interpreter
        with getattr(module, lock_name):
            instances = tuple(getattr(module, registry_name).values())
        if module_name == "tools.terminal_tool":
            from tools.terminal_tool_owned_close import close_all
            if await close_all(source, module, instances):
                continue
        else:
            from tools.browser_owned_close import close_all
            if await close_all(source, module, instances):
                continue
        if instances:
            # Existing cleanup paths can kill/replace a browser or environment.
            # They are not used here until a real owned normal closer exists.
            source.unresolved_resource_refs.extend(instances)
            raise OwnedGatewayUnknown(f"normal-tool-closure-unproven:{module_name}")
        if module_name == "tools.terminal_tool":
            module._cleanup_running = False
        else:
            module._cleanup_running = False
        thread = module._cleanup_thread
        if thread is not None:
            await asyncio.to_thread(thread.join)
            if thread.is_alive():
                raise OwnedGatewayUnknown("tool-janitor-still-running")
    if "tools.terminal_tool" not in sys.modules and getattr(source, "terminal_records", ()):
        from tools.terminal_tool_owned_close import close_all
        await close_all(source, None, ())
    if "tools.browser_session" not in sys.modules and getattr(source, "browser_records", ()):
        from tools.browser_owned_close import close_all
        await close_all(source, None, ())


async def close_remaining_python_resources(runner, keepalive):
    import hermes_cli.nous_auth_keepalive as auth
    import hermes_logging
    probe = sys.modules.get("tools.env_probe")
    if probe is not None:
        from tools.env_probe_owned import shutdown
        await asyncio.to_thread(shutdown, probe, runner._owned_gateway_source)
    auth._keepalive_stop.set()
    if keepalive is not None:
        await asyncio.to_thread(keepalive.join)
        if keepalive.is_alive():
            raise OwnedGatewayUnknown("auth-keepalive-writer-still-running")
    auxiliary = sys.modules.get("agent.auxiliary_client")
    if auxiliary is not None:
        from agent.auxiliary_owned_close import shutdown_owned
        await shutdown_owned(runner)
    with hermes_logging._queue_state_lock:
        listener = hermes_logging._queue_listener
    if listener is not None:
        # QueueListener.stop is a real sentinel + join, not the bounded queue drain.
        await asyncio.to_thread(listener.stop)
        if listener._thread is not None and listener._thread.is_alive():
            raise OwnedGatewayUnknown("log-writer-remains-live")
    from gateway.runtime_ownership import process_ownership
    for writer in tuple(process_ownership._writers):
        if writer.is_alive():
            raise OwnedGatewayUnknown(f"registered-writer-still-live:{writer.name}")


def close_stores(runner):
    # This runs only after true worker/pool/default-executor joins and actual
    # listener/control closure, with no other pending task or Job descendant.
    raw = runner._session_db
    raw = getattr(raw, "_db", raw)
    databases = [raw, getattr(runner.session_store, "_db", None)]
    for database in databases:
        if database is not None:
            database.close()
    runner.session_store.close_all_db_handles()
    runner.close_all_session_db_handles()
    from hermes_state_registry import close_all
    close_all()
