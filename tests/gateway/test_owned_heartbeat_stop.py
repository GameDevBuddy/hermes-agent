"""Actual gateway heartbeat caller over inert heartbeat work; no listener/native IO."""
import asyncio

import pytest

import hermes_owned_gateway as ownership
from gateway import run_startup
from tests.agent.auxiliary_owned_fixture import bind_source


def test_owned_heartbeat_keeps_its_normal_stop_predicate(monkeypatch, tmp_path):
    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        runner._running, runner._background_tasks = True, set()
        seen = []

        async def heartbeat(**kwargs):
            continuing = kwargs["should_continue"]
            seen.append(continuing())
            runner._running = False
            seen.append(continuing())

        monkeypatch.setattr(run_startup, "loop_heartbeat_forever", heartbeat)
        runner._start_loop_heartbeat_task()
        task = runner._loop_heartbeat_task
        await task
        assert seen == [True, False] and not task.cancelled()
        assert source.runner is runner and source.phase == "bound"

    asyncio.run(scenario())


def test_displaced_source_refuses_before_any_heartbeat_task(monkeypatch, tmp_path):
    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        calls = []
        monkeypatch.setattr(ownership, "_CURRENT", None)
        monkeypatch.setattr(run_startup.asyncio, "create_task", lambda *args: calls.append(args))
        with pytest.raises(ownership.OwnedGatewayUnknown):
            runner._start_loop_heartbeat_task()
        assert calls == [] and source.phase == "unknown"

    asyncio.run(scenario())
