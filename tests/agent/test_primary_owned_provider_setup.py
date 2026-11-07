"""Provider setup is a request failure, not lost managed SDK custody.

Uses the real agent initializer/provider router and retained resource book; SDK
construction is isolated. No inference, Windows Job or native execution proof.
"""
import asyncio
from functools import partial

import pytest

from agent import agent_init, auxiliary_owned as owned, primary_owned as primary
from agent.auxiliary_unavailable import ProviderNotConfiguredError
from hermes_owned_gateway import OwnedGatewayUnknown, active
from run_agent import AIAgent
from tests.agent.primary_owned_fixture import finish_sdks, primary_fixture


def test_missing_provider_can_be_configured_without_losing_source(monkeypatch, tmp_path):
    async def scenario():
        source, runner = primary_fixture(monkeypatch, tmp_path, initializer=agent_init.init_agent)
        monkeypatch.setattr(agent_init, "_setup_logging", lambda agent: None)
        # Keep tool discovery and model metadata IO out of the successful constructor.
        import model_tools
        from agent import model_metadata
        from hermes_cli import config
        monkeypatch.setattr(model_tools, "get_tool_definitions", lambda **kw: [])
        monkeypatch.setattr(config, "load_config_readonly", lambda *a, **kw: {
            "model": {"context_length": 128000}, "memory": {"enabled": False},
            "agent": {"environment_probe": False}})
        for module in (agent_init, model_metadata):
            monkeypatch.setattr(module, "fetch_model_metadata", lambda *a, **kw: {})
        make_agent = partial(AIAgent, provider="openrouter", model="fixture-model", quiet_mode=True,
                             skip_memory=True, skip_context_files=True, enabled_toolsets=[])
        for _ in range(2):
            with pytest.raises(ProviderNotConfiguredError, match="No LLM provider configured"):
                make_agent()

        assert source.phase == "bound"
        assert source.error is source.auxiliary_error is None
        assert active(runner) is source
        source.require_child_work()
        empty = tuple(source.auxiliary_clients)
        assert empty and all(record.phase == "empty" and record.error is None for record in empty)
        assert all(record.client is None for record in empty)

        agent = make_agent(api_key="fixture", base_url="https://fixture.invalid/v1")
        assert primary.owner_for(agent).source is source
        assert owned.find(agent.client).primary_owner is primary.owner_for(agent)
        assert active(runner) is source
        assert tuple(source.auxiliary_clients[:len(empty)]) == empty
        await finish_sdks(source, runner)
        assert agent.client.is_closed

    asyncio.run(scenario())


def test_missing_provider_with_falsey_endpoint_still_fails_closed(monkeypatch, tmp_path):
    class Endpoint:
        def __bool__(self):
            return False

    error = ProviderNotConfiguredError("missing credentials after endpoint assignment")

    def initializer(agent, **kwargs):
        agent.client = Endpoint()
        raise error

    async def scenario():
        source, _runner = primary_fixture(monkeypatch, tmp_path, initializer=initializer)
        with pytest.raises(ProviderNotConfiguredError):
            AIAgent(quiet_mode=True)
        assert source.phase == "unknown"
        assert source.auxiliary_error is error
        owner = next(record for record in source.auxiliary_clients if record.primary_agent is not None)
        assert isinstance(owner.primary_agent.client, Endpoint)
        assert owner.error is error

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["untyped", "unknown-source", "revoked-job"])
def test_empty_initialization_does_not_excuse_other_failures(monkeypatch, tmp_path, failure):
    error_type = RuntimeError if failure == "untyped" else ProviderNotConfiguredError
    error = error_type("No LLM provider configured")

    def initializer(agent, **kwargs):
        if failure == "unknown-source":
            source.unknown("original-source-failure")
        elif failure == "revoked-job":
            source.job.revoked = True
        raise error

    async def scenario():
        nonlocal source
        source, runner = primary_fixture(monkeypatch, tmp_path, initializer=initializer)
        with pytest.raises(RuntimeError):
            AIAgent(quiet_mode=True)
        assert source.phase == "unknown"
        assert source.auxiliary_error is not None
        if failure == "unknown-source":
            assert source.error == "original-source-failure"
        with pytest.raises(OwnedGatewayUnknown):
            active(runner)

    source = None
    asyncio.run(scenario())
