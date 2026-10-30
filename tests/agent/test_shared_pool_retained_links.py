"""Retain the original shared ownership links across normal close I/O; mocks only."""
import asyncio

import pytest

from agent import auxiliary_client, auxiliary_owned as owned
from agent.auxiliary_owned_close import shutdown_owned
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.agent.primary_owned_fixture import construct_actual_agent
from tests.agent.shared_pool_fixture import FakeTransport, join_original, request, shared_fixture


@pytest.mark.parametrize("mutation", [
    "pool-parent", "normal-parent", "connections", "views", "requests", "unclosed-omission",
])
def test_original_parent_and_membership_survive_normal_close_io(monkeypatch, tmp_path, mutation):
    source = changed_pool = original_connection = None

    class Changed(FakeTransport):
        async def close(self):
            nonlocal changed_pool, original_connection
            pool = owned.find(self)
            before = pool.connections
            super().close()
            await asyncio.sleep(0)
            if not before:
                return
            changed_pool, original_connection = pool, before[0]
            other = next(item for item in source.auxiliary_clients
                         if item is not None and item.kind == "shared-pool" and item is not pool)
            if mutation == "pool-parent":
                original_connection.pool_record = other
            elif mutation == "normal-parent":
                original_connection.normal_parent = other
            elif mutation == "connections":
                pool.connections = ()
            elif mutation == "views":
                pool.views = ()
            elif mutation == "requests":
                pool.requests = ()
            else:
                original_connection.client._connection.closed = False
                pool.connections = ()

    async def scenario():
        nonlocal source
        source, runner = shared_fixture(monkeypatch, tmp_path, transport=Changed)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        agent = construct_actual_agent()
        response = agent.client._client._transport.handle_request(request())
        response.close()
        await join_original(source)
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        assert not source.auxiliary_closed
        assert changed_pool.close_attempted and changed_pool.client.calls == 1
        assert original_connection in source.auxiliary_clients
        assert changed_pool.error is not None or original_connection.error is not None
        first = source.error
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        assert source.error is first and changed_pool.client.calls == 1

    asyncio.run(scenario())
