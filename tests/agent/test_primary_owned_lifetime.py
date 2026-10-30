"""Primary creation, replacement and request-slot lifetime through actual production consumers."""
import asyncio

import pytest

from agent import auxiliary_owned as owned, primary_owned as primary
from agent.agent_runtime_helpers import force_close_tcp_sockets
from gateway.run import GatewayRunner
from gateway import run_owned_resources
from hermes_constants import hermes_home_key, set_hermes_home_override
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.agent.primary_owned_fixture import construct_actual_agent, finish_sdks, primary_fixture


def test_actual_constructor_retains_same_agent_runtime_http_and_creator(monkeypatch, tmp_path):
    async def scenario():
        source, runner = primary_fixture(monkeypatch, tmp_path)
        agent = construct_actual_agent()
        owner, client = primary.owner_for(agent), agent.client
        record = owned.find(client)
        assert owner.primary_agent is agent and owner.source is source
        assert owner.factory is type(agent).__init__.__wrapped__
        assert record.primary_owner is owner and record.runtime is runner._gdb_original_source_runtime
        assert record.creator_loop is source.loop and record.home_key == hermes_home_key()
        assert owned.find(client._client).primary_owner is owner
        await finish_sdks(source, runner)
        assert len(client.calls) == 1 and owner.phase == "view"
        assert record.close_task.done() and record.client is client
    asyncio.run(scenario())


@pytest.mark.parametrize("reason", ["credential_rotation", "restore_primary", "switch_model"])
def test_actual_replacement_retains_original_client_without_socket_retirement(monkeypatch, tmp_path, reason):
    async def scenario():
        source, runner = primary_fixture(monkeypatch, tmp_path)
        agent = construct_actual_agent()
        old = agent.client
        assert agent._replace_primary_openai_client(reason=reason)
        new = agent.client
        assert old is not new and not old.calls and not new.calls
        assert force_close_tcp_sockets(old) == 0 and not old._client.is_closed
        assert owned.find(old).primary_owner is primary.owner_for(agent)
        await finish_sdks(source, runner)
        assert len(old.calls) == len(new.calls) == 1
    asyncio.run(scenario())


def test_request_slot_reuse_poison_and_lost_publication_keep_actual_clients(monkeypatch, tmp_path):
    async def scenario():
        source, runner = primary_fixture(monkeypatch, tmp_path)
        agent = construct_actual_agent()
        shared = agent.client
        first = agent._create_request_openai_client(reason="request")
        agent._close_request_openai_client(first, reason="request_complete")
        assert agent._create_request_openai_client(reason="reuse") is first
        # While first is checked out, a concurrent request receives an uncached original client.
        loser = agent._create_request_openai_client(reason="parallel")
        assert loser is not first
        agent._close_request_openai_client(loser, reason="parallel_done")
        agent._close_request_openai_client(first, reason="request_complete")
        agent._request_client_cache["poisoned"] = True
        second = agent._create_request_openai_client(reason="poison_rebuild")
        assert second is not first
        assert all(not client.calls for client in (shared, first, loser, second))
        await finish_sdks(source, runner)
        assert all(len(client.calls) == 1 for client in (shared, first, loser, second))
    asyncio.run(scenario())


def test_cache_eviction_keeps_agent_and_closes_sdks_only_from_final_book(monkeypatch, tmp_path):
    async def scenario():
        source, runner = primary_fixture(monkeypatch, tmp_path)
        agent = construct_actual_agent()
        client = agent.client
        request = agent._create_request_openai_client(reason="request")
        runner._agent_cache["fixture"] = (agent, object())
        # Real soft eviction releases fields; retained construction, not this cache, keeps custody.
        GatewayRunner._release_evicted_agent_soft(runner, agent)
        runner._agent_cache.pop("fixture")
        assert agent.client is None and not client.calls and not request.calls
        from tests.agent.auxiliary_owned_fixture import allow_normal_shutdown
        allow_normal_shutdown(source)
        await run_owned_resources.close_agent_resources(runner)
        assert not client.calls and not request.calls and primary.retained_agents(runner) == (agent,)
        from agent.auxiliary_owned_close import shutdown_owned
        await shutdown_owned(runner)
        assert len(client.calls) == len(request.calls) == 1
    asyncio.run(scenario())


def test_profile_at_closure_is_original_not_current_actor_or_cache_home(monkeypatch, tmp_path):
    async def scenario():
        source, runner = primary_fixture(monkeypatch, tmp_path)
        agent = construct_actual_agent()
        client, home = agent.client, hermes_home_key()
        set_hermes_home_override(str(tmp_path / "other-current"))
        await finish_sdks(source, runner)
        assert client.calls[0][1] == home and hermes_home_key() != home
    asyncio.run(scenario())


def test_borrowed_agent_cannot_be_adopted_by_cache_or_sdk_factory(monkeypatch, tmp_path):
    from run_agent import AIAgent

    async def scenario():
        source, _runner = primary_fixture(monkeypatch, tmp_path)
        borrowed = AIAgent.__new__(AIAgent)
        with pytest.raises(OwnedGatewayUnknown, match="original-constructor-unavailable"):
            borrowed._create_openai_client({}, reason="borrowed", shared=True)
        record = source.auxiliary_clients[0]
        assert record.primary_agent is borrowed and record.error is not None
        assert not source.auxiliary_closed
    asyncio.run(scenario())


def test_actual_credential_adoption_rebuild_keeps_original_sdk(monkeypatch, tmp_path):
    async def scenario():
        source, runner = primary_fixture(monkeypatch, tmp_path)
        agent = construct_actual_agent()
        original = agent.client
        assert agent._adopt_openai_credentials(" fixture-new ", "https://fixture.invalid/v2/", reason="credential_refresh")
        current = agent.client
        assert current is not original and agent._client_kwargs["api_key"] == "fixture-new"
        assert agent._client_kwargs["base_url"] == "https://fixture.invalid/v2"
        assert owned.find(current).primary_owner is owned.find(original).primary_owner
        assert not original.calls and not current.calls
        await finish_sdks(source, runner)
        assert len(original.calls) == len(current.calls) == 1
    asyncio.run(scenario())


def test_managed_abort_poisons_slot_without_shutdown_or_reuse(monkeypatch, tmp_path):
    async def scenario():
        source, runner = primary_fixture(monkeypatch, tmp_path)
        agent = construct_actual_agent()
        request = agent._create_request_openai_client(reason="request")
        agent._abort_request_openai_client(request, reason="timeout")
        assert agent._request_client_cache["poisoned"] and not request.calls
        agent._close_request_openai_client(request, reason="request_complete")
        replacement = agent._create_request_openai_client(reason="after_timeout")
        assert replacement is not request and not request._client.is_closed
        await finish_sdks(source, runner)
        assert len(request.calls) == len(replacement.calls) == 1
    asyncio.run(scenario())


def test_managed_memory_only_cached_container_keeps_existing_profile_cleanup(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from tests.agent.auxiliary_owned_fixture import allow_normal_shutdown

    async def scenario():
        source, runner = primary_fixture(monkeypatch, tmp_path)
        agent = construct_actual_agent()
        client, events = agent.client, []
        home = tmp_path / "memory-only-profile"
        manager = SimpleNamespace(shutdown_owned=lambda: events.append(hermes_home_key()))
        runner._agent_cache["memory-only"] = (SimpleNamespace(_memory_manager=manager, client=None), object())
        runner.session_store = SimpleNamespace(_profile_home_for_key=lambda key: home)
        allow_normal_shutdown(source)
        await run_owned_resources.close_agent_resources(runner)
        assert events == [hermes_home_key(home)] and not client.calls
        assert primary.retained_agents(runner) == (agent,)
        from agent.auxiliary_owned_close import shutdown_owned
        await shutdown_owned(runner)
        assert len(client.calls) == 1
    asyncio.run(scenario())
