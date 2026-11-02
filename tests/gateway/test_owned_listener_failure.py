"""Actual listener-start consumer over lower listener/credential models; no socket or auth IO."""
import asyncio
from types import SimpleNamespace as Data

import pytest

import hermes_owned_gateway as owned
from gateway import run_api, run_runtime, webapp_listener
from tests.agent.auxiliary_owned_fixture import bind_source


def listener_models(monkeypatch, first):
    calls, rotations = [], []
    monkeypatch.setattr(webapp_listener, "register_control_verbs", lambda *args: None)
    monkeypatch.setattr(webapp_listener, "listener_plan", lambda home: ("127.0.0.1", 9120, Data(), None))
    monkeypatch.setattr(webapp_listener, "browser_session_token", lambda *args, **kwargs: rotations.append(kwargs))
    monkeypatch.setattr(run_runtime, "_authorities", lambda runner: ())

    async def start(runner, **kwargs):
        calls.append((runner, kwargs))
        if len(calls) == 1:
            raise first
        return Data(api_origin="http://127.0.0.1:1")

    monkeypatch.setattr(run_api, "start_gateway_api", start)
    return calls, rotations


@pytest.mark.parametrize("first", [OSError("original listener failure"), SystemExit("original bind refusal")])
def test_owned_listener_keeps_first_failure_without_rotation_or_replacement(monkeypatch, tmp_path, first):
    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        runner.session_runtime_descriptor = {}
        calls, rotations = listener_models(monkeypatch, first)
        with pytest.raises(type(first)) as caught:
            await run_runtime.start_gateway_runtime_api(runner)
        assert caught.value is first and len(calls) == 1 and rotations == []
        assert source.runner is runner and runner.session_runtime_descriptor == {}

    asyncio.run(scenario())


def test_unmanaged_listener_keeps_its_existing_fallback_policy(monkeypatch):
    async def scenario():
        monkeypatch.setattr(owned, "_CURRENT", None)
        runner = Data(session_runtime_descriptor={})
        calls, rotations = listener_models(monkeypatch, OSError("legacy fixture failure"))
        await run_runtime.start_gateway_runtime_api(runner)
        assert len(calls) == 2 and rotations == [{"rotate": True}]
        assert runner.session_runtime_descriptor["api_origin"] == "http://127.0.0.1:1"

    asyncio.run(scenario())


def test_displaced_owned_source_refuses_before_listener_construction(monkeypatch, tmp_path):
    async def scenario():
        _source, runner = bind_source(monkeypatch, tmp_path)
        calls, rotations = listener_models(monkeypatch, OSError("unreachable"))
        monkeypatch.setattr(owned, "_CURRENT", None)
        with pytest.raises(owned.OwnedGatewayUnknown, match="association-missing"):
            await run_runtime.start_gateway_runtime_api(runner)
        assert calls == rotations == []

    asyncio.run(scenario())
