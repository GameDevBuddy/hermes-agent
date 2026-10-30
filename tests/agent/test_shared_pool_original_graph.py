"""Original producer links, SDK preflight and final publication; mocked lower factories only."""
import asyncio

import pytest

from agent import auxiliary_client, auxiliary_owned as owned
from agent.auxiliary_owned_close import shutdown_owned
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.agent import shared_pool_fixture as fixture
from tests.agent.primary_owned_fixture import construct_actual_agent


def shared_pools(source):
    return tuple(item for item in source.auxiliary_clients if item is not None and item.kind == "shared-pool")


@pytest.mark.parametrize("field", ["pool_record", "normal_parent"])
def test_connection_normal_call_cannot_reparent_its_original_record(monkeypatch, tmp_path, field):
    class Changed(fixture.FakeConnection):
        def close(self):
            super().close()
            record = owned.find(self)
            other = next(pool for pool in shared_pools(record.source) if pool is not record.pool_origin[0])
            setattr(record, field, other)

    async def scenario():
        import httpcore
        source, runner = fixture.shared_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(fixture, "FakeConnection", Changed)
        monkeypatch.setattr(httpcore, "HTTPConnection", Changed)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        agent = construct_actual_agent()
        response = agent.client._client._transport.handle_request(fixture.request())
        response.close()
        pool = next(pool for pool in shared_pools(source) if pool.connections)
        connection = pool.connections[0]
        await fixture.join_original(source)
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        assert connection.client.calls == 1 and connection.error is not None
        assert connection.pool_origin[0] is pool and not source.auxiliary_closed
        assert connection.phase != "closed" and pool.shared_close_links.nodes
    asyncio.run(scenario())


@pytest.mark.parametrize("mutation", [
    "view-parent", "request-parent", "request-view", "request-response", "holders", "sdk-refs",
])
def test_original_user_associations_remain_required_after_pool_io(monkeypatch, tmp_path, mutation):
    changed = None

    class Changed(fixture.FakeTransport):
        async def close(self):
            nonlocal changed
            super().close()
            await asyncio.sleep(0)
            pool = owned.find(self)
            if not pool.requests:
                return
            changed = pool
            other = next(item for item in shared_pools(pool.source) if item is not pool)
            view, request = pool.views[0], pool.requests[0]
            actions = {
                "view-parent": lambda: setattr(view, "pool_record", other),
                "request-parent": lambda: setattr(request, "pool_record", other),
                "request-view": lambda: setattr(request, "view", other.views[0]),
                "request-response": lambda: setattr(request, "response", object()),
                "holders": lambda: setattr(view, "holders", ()),
                "sdk-refs": lambda: setattr(view.holders[0], "shared_refs", ()),
            }
            actions[mutation]()

    async def scenario():
        source, runner = fixture.shared_fixture(monkeypatch, tmp_path, transport=Changed)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        agent = construct_actual_agent()
        response = agent.client._client._transport.handle_request(fixture.request())
        response.close()
        await fixture.join_original(source)
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        assert changed.shared_close_links.requests[0][2] is response
        assert changed.error is not None and changed.client.calls == 1
        assert not source.auxiliary_closed
    asyncio.run(scenario())


def test_dropped_original_member_refuses_before_first_sdk_close(monkeypatch, tmp_path):
    async def scenario():
        source, runner = fixture.shared_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        agent = construct_actual_agent()
        pool = shared_pools(source)[0]
        original = pool.views
        pool.views = ()
        await fixture.join_original(source)
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        assert original[0] in source.auxiliary_clients and original[0].pool_origin[0] is pool
        assert not agent.client.is_closed and not source.auxiliary_closed
        assert all(item.client.calls == 0 for item in shared_pools(source))
    asyncio.run(scenario())


@pytest.mark.parametrize("mutation", ["connections", "snapshot"])
def test_later_pool_io_cannot_replace_earlier_pool_original_graph(monkeypatch, tmp_path, mutation):
    changed = None

    class Changed(fixture.FakeTransport):
        async def close(self):
            nonlocal changed
            super().close()
            await asyncio.sleep(0)
            pool = owned.find(self)
            earlier = next((item for item in shared_pools(pool.source) if item is not pool
                            and getattr(item, "normal_api_returned", False)), None)
            if earlier is not None:
                changed = earlier
                if mutation == "connections":
                    earlier.connections = ()
                else:
                    from dataclasses import replace
                    earlier.shared_close_links = replace(earlier.shared_close_links)

    async def scenario():
        source, runner = fixture.shared_fixture(monkeypatch, tmp_path, transport=Changed)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        construct_actual_agent()
        for pool in shared_pools(source):
            response = pool.views[0].client.handle_request(fixture.request())
            response.close()
        await fixture.join_original(source)
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        original = next(values for name, values in changed.shared_close_links.members if name == "connections")
        assert original and original[0] in source.auxiliary_clients
        assert original[0].phase == "shared-connection-closed" and changed.error is not None
        assert not source.auxiliary_closed
    asyncio.run(scenario())
