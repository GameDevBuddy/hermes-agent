"""Sticky primary construction/close failures and genuine original association checks."""
import asyncio

import pytest

from agent import auxiliary_owned as owned, primary_owned as primary
from agent.auxiliary_owned_close import shutdown_owned
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.agent.auxiliary_owned_fixture import MockSDK, allow_normal_shutdown
from tests.agent.primary_owned_fixture import construct_actual_agent, minimal_primary_init, primary_fixture


def test_partial_primary_constructor_keeps_original_self_agent_and_exception(monkeypatch, tmp_path):
    error = RuntimeError("partial-primary-sdk")

    class FailedSDK(MockSDK):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            raise error

    async def scenario():
        source, _runner = primary_fixture(monkeypatch, tmp_path, sdk=FailedSDK)
        with pytest.raises(RuntimeError):
            construct_actual_agent()
        owners = [record for record in source.auxiliary_clients if record.primary_agent is not None]
        assert len(owners) == 1 and owners[0].primary_agent._gdb_primary_sdk_owner is owners[0]
        failed = [record for record in source.auxiliary_clients if record.kind == "sdk"]
        assert len(failed) == 1 and failed[0].error is error
        trace, selves = error.__traceback__, []
        while trace is not None:
            selves.append(trace.tb_frame.f_locals.get("self"))
            trace = trace.tb_next
        assert any(isinstance(value, FailedSDK) for value in selves)
        assert source.auxiliary_error is error and not source.auxiliary_closed
    asyncio.run(scenario())


def test_agent_initialization_failure_keeps_already_returned_client(monkeypatch, tmp_path):
    error = RuntimeError("after-primary-creation")

    def failed_init(agent, **kwargs):
        minimal_primary_init(agent, **kwargs)
        raise error

    async def scenario():
        source, _runner = primary_fixture(monkeypatch, tmp_path, initializer=failed_init)
        with pytest.raises(RuntimeError) as caught:
            construct_actual_agent()
        owner = next(record for record in source.auxiliary_clients if record.primary_agent is not None)
        client = owner.primary_agent.client
        assert caught.value is error is owner.error
        assert owned.find(client).primary_owner is owner and not client.calls
        assert owner.phase == "unknown" and not source.auxiliary_closed
    asyncio.run(scenario())


@pytest.mark.parametrize("mutation", ["agent-marker", "agent-endpoint", "record-owner"])
def test_async_primary_close_rechecks_original_agent_after_yield(monkeypatch, tmp_path, mutation):
    agent = None

    class ChangingSDK(MockSDK):
        async def close(self):
            await asyncio.sleep(0)
            changes = {
                "agent-marker": lambda: setattr(agent, "_gdb_primary_sdk_owner", object()),
                "agent-endpoint": lambda: setattr(agent, "client", object()),
                "record-owner": lambda: setattr(owned.find(self), "primary_owner", None),
            }
            changes[mutation]()
            super().close()

    async def scenario():
        nonlocal agent
        source, runner = primary_fixture(monkeypatch, tmp_path, sdk=ChangingSDK)
        agent = construct_actual_agent()
        client, record = agent.client, owned.find(agent.client)
        allow_normal_shutdown(source)
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        assert record.client is client and record.close_task.done() and record.error is not None
        assert len(client.calls) == 1 and not source.auxiliary_closed
        first = record.error
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        assert record.error is first and len(client.calls) == 1
    asyncio.run(scenario())


def test_request_slot_cannot_adopt_another_agents_original_sdk(monkeypatch, tmp_path):
    async def scenario():
        source, _runner = primary_fixture(monkeypatch, tmp_path)
        first, second = construct_actual_agent(), construct_actual_agent()
        client = first.client
        with pytest.raises(OwnedGatewayUnknown, match="original-primary-client-constructor"):
            second._store_request_slot("_request_client_cache", client, {})
        assert not client.calls and owned.find(client).primary_owner is first._gdb_primary_sdk_owner
        assert not source.auxiliary_closed
    asyncio.run(scenario())


def test_constructor_reentry_consumes_failure_without_replacing_original_owner(monkeypatch, tmp_path):
    async def scenario():
        source, _runner = primary_fixture(monkeypatch, tmp_path)
        agent = construct_actual_agent()
        owner, client = primary.owner_for(agent), agent.client
        with pytest.raises(OwnedGatewayUnknown, match="construction-reentry"):
            type(agent).__init__(agent, quiet_mode=True)
        assert agent._gdb_primary_sdk_owner is owner and agent.client is client and not client.calls
        assert source.auxiliary_clients[-1].error is not None
    asyncio.run(scenario())


def test_cancelled_primary_close_keeps_original_agent_task_and_first_error(monkeypatch, tmp_path):
    caller = release = None

    class BlockingSDK(MockSDK):
        async def close(self):
            caller.cancel()
            await release.wait()
            super().close()

    async def scenario():
        nonlocal caller, release
        source, runner = primary_fixture(monkeypatch, tmp_path, sdk=BlockingSDK)
        caller, release = asyncio.current_task(), asyncio.Event()
        agent = construct_actual_agent()
        client, record = agent.client, owned.find(agent.client)
        owner = primary.owner_for(agent)
        allow_normal_shutdown(source)
        with pytest.raises(asyncio.CancelledError) as caught:
            await shutdown_owned(runner)
        assert record.error is caught.value and not record.close_task.cancelled()
        assert record.primary_owner is owner and owner.primary_agent is agent
        release.set()
        with pytest.raises(OwnedGatewayUnknown):
            await record.close_task
        assert record.error is caught.value and record.client is client and len(client.calls) == 1
        assert not source.auxiliary_closed
    asyncio.run(scenario())
