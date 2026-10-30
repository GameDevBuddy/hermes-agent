"""Real source constructors with synthetic sockets; no gateway/service/native launch."""
from types import SimpleNamespace
import asyncio

from gateway.native_source_runtime import (
    attach_original_authority, attach_original_listener_task, attach_original_registry,
    capture_original_runner,
)
from gateway.run import GatewayRunner
from gateway.run_api import GatewayAPIHandle, GatewayRuntimeAPI
from gateway.session_authorities import SessionAuthorities
from gateway.session_authority import SessionAuthority
from hermes_constants import hermes_home_key
from tui_gateway.ws import _authority_connection


def runner_from_actual_constructor(monkeypatch):
    for name in ("_warn_if_docker_media_delivery_is_risky", "_init_runtime_settings",
                 "_init_session_store", "_init_lifecycle_state", "_init_runtime_caches",
                 "_init_session_db", "_init_registries_and_clocks"):
        monkeypatch.setattr(GatewayRunner, name, lambda self: None)
    monkeypatch.setattr("hermes_cli.gateway_multiplex_mode.standalone_launcher_decision", lambda config: None)
    runner = GatewayRunner(SimpleNamespace(multiplex_profiles=False))
    runner._draining = False
    return runner


async def source_graph(monkeypatch, tmp_path, *, authority=None):
    runner = runner_from_actual_constructor(monkeypatch)
    loop, task = asyncio.get_running_loop(), asyncio.current_task()
    capture_original_runner(runner, task, loop)
    runtime = runner._gdb_original_source_runtime
    home = authority.profile_id if authority is not None else str(tmp_path)
    registry = SessionAuthorities(home)
    runner.session_authorities = registry
    attach_original_registry(runner, registry)
    if authority is None:
        authority = SessionAuthority(runner, profile_id=home, instance_id="source-instance",
                                     db=SimpleNamespace(), epoch=1)
    authority.runner = runner
    registry.add(home, None)
    registry.replace(home, authority)
    attach_original_authority(runner, authority, hermes_home_key(home))
    mount, server, listener = SimpleNamespace(), SimpleNamespace(should_exit=False), object()
    api = GatewayRuntimeAPI(None, runner, mount, source_server=server, source_listener=listener)
    ready = asyncio.Event()
    api_task = asyncio.create_task(ready.wait())
    handle = GatewayAPIHandle("http://127.0.0.1:1", server, api_task, mount, listener,
                              api._gdb_original_source_server)
    attach_original_listener_task(handle.source_owner, handle, api_task)
    runner.session_api = handle
    ws = SimpleNamespace(scope={"hermes.session_authority": authority,
                               "hermes.gdb_original_source_server": handle.source_owner})
    transport = SimpleNamespace(_ws=ws, _loop=loop, _closed=False)
    identity = {"user_id": "source-fixture", "profile_id": home,
                "instance_id": authority.instance_id}
    connection = _authority_connection(ws, transport, identity, True)
    return SimpleNamespace(runner=runner, runtime=runtime, registry=registry, authority=authority,
                           api=api, handle=handle, api_task=api_task, ready=ready, ws=ws,
                           transport=transport, connection=connection)


async def finish_source_graph(graph):
    graph.ready.set()
    await graph.api_task
