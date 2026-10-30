"""Actual primary route/release entry points with SDK, HTTP and session factories mocked."""
import asyncio

import pytest

import hermes_owned_gateway as ownership
from agent import auxiliary_owned as owned, primary_owned as primary
from agent.auxiliary_owned_close import shutdown_owned
from tests.agent.auxiliary_owned_fixture import MockSDK, allow_normal_shutdown
from tests.agent.primary_owned_fixture import construct_actual_agent, finish_sdks, primary_fixture


def test_unmanaged_original_close_behavior_remains(monkeypatch, tmp_path):
    async def scenario():
        _source, _runner = primary_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(ownership, "_CURRENT", None)
        agent = construct_actual_agent()
        client = agent.client
        assert owned.find(client) is None
        agent._close_openai_client(client, reason="ordinary", shared=True)
        assert client.is_closed and len(client.calls) == 1
    asyncio.run(scenario())


def test_opaque_session_is_retained_and_legacy_teardown_does_not_close_it(monkeypatch, tmp_path):
    class Session:
        def close(self):
            pytest.fail("opaque session may not be closed by legacy teardown")

    async def scenario():
        source, runner = primary_fixture(monkeypatch, tmp_path)
        agent = construct_actual_agent()
        session = primary.construct_client(agent, "provider", Session)
        agent._codex_session = session
        agent._close_codex_session()
        assert agent._codex_session is session and owned.find(session).primary_owner is primary.owner_for(agent)
        allow_normal_shutdown(source)
        with pytest.raises(ownership.OwnedGatewayUnknown, match="owner-unavailable"):
            await shutdown_owned(runner)
        assert owned.find(session).client is session and not source.auxiliary_closed
    asyncio.run(scenario())


def test_actual_anthropic_request_factory_and_teardown_retain_original_client(monkeypatch, tmp_path):
    import anthropic

    async def scenario():
        source, runner = primary_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(anthropic, "Anthropic", MockSDK)
        agent = construct_actual_agent()
        agent._anthropic_api_key, agent._anthropic_base_url = "fixture", "https://fixture.invalid"
        agent._is_anthropic_oauth = False
        client = agent._create_request_anthropic_client(reason="request")
        assert owned.find(client).primary_owner is primary.owner_for(agent)
        agent._close_cached_request_anthropic_client(reason="teardown")
        assert not client.calls
        await finish_sdks(source, runner)
        assert len(client.calls) == 1
    asyncio.run(scenario())


def test_actual_fallback_resolver_claims_primary_agent_before_auxiliary_constructor(monkeypatch, tmp_path):
    from agent import chat_completion_helpers as helpers, route_binding
    from run_agent import AIAgent

    async def scenario():
        source, runner = primary_fixture(monkeypatch, tmp_path)
        agent = construct_actual_agent()
        previous = agent.client
        agent._fallback_activated = False
        monkeypatch.setattr(AIAgent, "_anthropic_prompt_cache_policy", lambda *args, **kwargs: (False, False))
        monkeypatch.setattr(AIAgent, "_ensure_lmstudio_runtime_loaded", lambda self: None)
        for name in ("_rebind_fallback_credential_pool", "_update_fallback_context_compressor",
                     "_reresolve_fallback_reasoning_config", "_rescope_fallback_extra_body"):
            monkeypatch.setattr(helpers, name, lambda *args, **kwargs: None)
        entry = {"provider": "custom", "model": "fallback", "base_url": "https://fixture.invalid/v1", "api_key": "fixture"}
        result = route_binding.bind_route_entry(agent, entry, "custom", "fallback")
        assert result is not None and agent.client is not previous
        current = agent.client
        assert owned.find(current).primary_owner is primary.owner_for(agent)
        assert not previous.calls and not current.calls
        await finish_sdks(source, runner)
        assert len(previous.calls) == len(current.calls) == 1
    asyncio.run(scenario())
