"""Actual session/launcher/CDP/cleanup consumers with mocked lower external factories only."""
import asyncio
import atexit
import socket
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest_asyncio

from tests.tools import browser_owned_fakes as fakes


def external_guards(monkeypatch):
    # Installed BEFORE the selected browser modules/runner fixture are imported.
    monkeypatch.setattr(atexit, "register", lambda callback, *args, **kwargs: callback)
    monkeypatch.setattr(subprocess, "Popen", fakes.deny)
    monkeypatch.setattr(subprocess, "run", fakes.deny)
    monkeypatch.setattr(socket, "create_connection", fakes.deny)
    monkeypatch.setattr(socket, "getaddrinfo", fakes.deny)
    monkeypatch.setattr(socket.socket, "connect", fakes.deny)
    import httpx
    import requests
    import websockets
    monkeypatch.setattr(httpx.Client, "send", fakes.deny)
    monkeypatch.setattr(httpx.AsyncClient, "send", fakes.deny)
    monkeypatch.setattr(requests, "get", fakes.deny)
    monkeypatch.setattr(requests, "post", fakes.deny)
    monkeypatch.setattr(websockets, "connect", fakes.deny)


def reset_books(monkeypatch):
    from tools import browser_owned as owned
    from tools import browser_owned_launcher as launch
    for name, value in (("_records", []), ("_index", {}), ("_books", {}), ("_origins", {}), ("_method_origins", {})):
        monkeypatch.setattr(owned, name, value)
    monkeypatch.setattr(launch, "_process_origins", {})
    return owned


def mock_session_boundaries(monkeypatch, tmp_path):
    from tools import browser_session as sessions
    from tools import browser_session_cloud as cloud
    from tools import browser_session_cdp as cdp
    from tools import browser_launcher as launcher
    from tools.bot_desktop import runtime
    for name in ("_active_sessions", "_session_last_activity", "_session_owner_homes", "_cleanup_failures"):
        monkeypatch.setattr(sessions, name, {})
    monkeypatch.setattr(sessions, "_cleanup_thread", None)
    monkeypatch.setattr(sessions, "_cleanup_running", False)
    monkeypatch.setattr(sessions, "_cleanup_done", False)
    monkeypatch.setattr(sessions, "build_browser_env", lambda: {"HOME": str(tmp_path), "PATH": "mock-only"})
    monkeypatch.setattr(cloud, "browser_section", lambda: {"cloud_provider": "local", "headed": False})
    monkeypatch.setattr(cloud, "_cached_providers", {})
    monkeypatch.setattr(cloud, "_memo_slots", {})
    monkeypatch.setattr(cdp, "_live_overrides", {})
    monkeypatch.setattr(cdp, "_live_generations", {})
    monkeypatch.setattr(cdp, "env_cdp_url", lambda: "")
    monkeypatch.setattr(runtime, "tool_placement", lambda: "host")
    monkeypatch.setattr(runtime, "published_env", dict)
    for name in ("_browsers", "_procs", "_launching", "_key_locks"):
        monkeypatch.setattr(launcher, name, {})
    monkeypatch.setattr(launcher, "_served_homes", set())
    monkeypatch.setattr(launcher, "chromium_ready", lambda: (True, ""))
    monkeypatch.setattr(launcher, "_resolve_executable", lambda: "mock-chromium-not-executed")
    monkeypatch.setattr(launcher, "_default_user_data_dir", lambda key, home: str(tmp_path / "browser" / key))
    monkeypatch.setattr(launcher, "_prepare_dir", lambda path, ephemeral: Path(path).mkdir(parents=True))
    monkeypatch.setattr(launcher, "_start_time", lambda pid: None)
    monkeypatch.setattr(launcher, "_write_record", lambda browser: None)
    monkeypatch.setattr(launcher, "_wait_for_devtools", lambda *args: ((27491, "/devtools/browser/mock"), None))
    monkeypatch.setattr(launcher, "needs_chromium_sandbox_bypass", lambda: False)
    monkeypatch.setattr(launcher.subprocess, "Popen", fakes.Process)
    return sessions, launcher, cdp


@pytest_asyncio.fixture
async def browser_fixture(monkeypatch, tmp_path):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "agent-home"))
    external_guards(monkeypatch)
    owned = reset_books(monkeypatch)
    evidence = fakes.reset(monkeypatch)
    sessions, launcher, cdp = mock_session_boundaries(monkeypatch, tmp_path)
    from tools import browser_owned_workers as workers
    from tools import browser_owned_supervisor as supervisors
    from tools import browser_owned_cdp as protocol
    from tools import browser_owned_commands as commands
    from tools import browser_owned_capture as captures
    from tools import browser_owned_events as events
    from tools.browser_supervisor import SUPERVISOR_REGISTRY
    monkeypatch.setattr(workers, "_janitor_thread", fakes.Worker)
    monkeypatch.setattr(supervisors, "_thread", fakes.Worker)
    for name in ("_worker_links", "_loop_links", "_connection_links", "_reader_links"):
        monkeypatch.setattr(supervisors, name, {})
    monkeypatch.setattr(captures, "_links", {})
    monkeypatch.setattr(commands, "_pipe_thread", fakes.Worker)
    monkeypatch.setattr(fakes.Worker, "driver", asyncio.get_running_loop())
    monkeypatch.setattr(SUPERVISOR_REGISTRY, "_by_task", {})
    monkeypatch.setattr(SUPERVISOR_REGISTRY, "_home_by_task", {})
    # No other resource family is exercised by this selected gateway close callback.
    import tools.mcp_tool_owned as mcp
    monkeypatch.setattr(mcp, "shutdown_all", lambda: None)
    fixture = SimpleNamespace(source=None, runner=None, owned=owned, sessions=sessions, launcher=launcher,
                              cdp=cdp, evidence=evidence, registry=SUPERVISOR_REGISTRY, tmp=tmp_path)

    def activate():
        if fixture.source is None:
            from tests.agent.auxiliary_owned_fixture import bind_source
            fixture.source, fixture.runner = bind_source(monkeypatch, tmp_path)
        return fixture.source

    fixture.activate = activate
    monkeypatch.setattr(protocol, "_running_loop", lambda: next(
        record.loop for record in fixture.source.browser_records if getattr(record, "kind", None) == "supervisor"))
    monkeypatch.setattr(events, "_running_loop", protocol._running_loop)
    yield fixture
    # Isolate in-memory test tasks only; never claim this as production normal settlement.
    tasks = [record.obj for record in getattr(fixture.source, "browser_records", ())
             if getattr(record, "kind", None) in ("reader", "cdp-event") and isinstance(record.obj, asyncio.Task)]
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def session_record(fixture, key="actor"):
    fixture.activate()
    info = fixture.sessions.get_session_info(key)
    return fixture.owned.find(info)


async def finish(fixture):
    from tests.agent.auxiliary_owned_fixture import allow_normal_shutdown
    from gateway.run_owned_resources import close_tool_resources
    allow_normal_shutdown(fixture.source)
    await close_tool_resources(fixture.source)
