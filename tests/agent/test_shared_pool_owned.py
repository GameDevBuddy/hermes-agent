"""Original managed pool alias/overflow/request/normal API lifecycle, with fake network objects only."""
import asyncio
import contextvars

import pytest

from agent import auxiliary_client, auxiliary_owned as owned, process_bootstrap
from agent.auxiliary_owned_close import shutdown_owned
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.agent.primary_owned_fixture import construct_actual_agent
from tests.agent.shared_pool_fixture import FakeHTTP, FakeTransport, join_original, request, shared_fixture


def pools(source):
    return tuple(record for record in source.auxiliary_clients if record is not None and record.kind == "shared-pool")


def test_actual_sdk_aliases_close_original_pools_after_join_only(monkeypatch, tmp_path):
    async def scenario():
        source, runner = shared_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        first, second = construct_actual_agent(), construct_actual_agent()
        original = pools(source)
        assert len(original) == 2
        assert first.client._client._transport._inner is second.client._client._transport._inner
        assert all(len(record.views) == 2 for record in original)
        await join_original(source)
        await shutdown_owned(runner)
        assert source.auxiliary_closed and all(record.phase == "closed" for record in original)
        assert all(record.client.calls == record.pool.calls == 1 for record in original)
        assert len(process_bootstrap._SHARED_TRANSPORTS) == 2
        await shutdown_owned(runner)
        assert all(record.client.calls == 1 for record in original)
    asyncio.run(scenario())


def test_cache_overflow_is_retained_even_when_global_map_is_empty(monkeypatch, tmp_path):
    async def scenario():
        source, runner = shared_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        monkeypatch.setattr(process_bootstrap, "_SHARED_TRANSPORTS_MAX", 0)
        first, second = construct_actual_agent(), construct_actual_agent()
        assert len(pools(source)) == 4 and process_bootstrap._SHARED_TRANSPORTS == {}
        first.release_clients()
        second.release_clients()
        await join_original(source)
        await shutdown_owned(runner)
        assert all(record.pool.calls == 1 for record in pools(source))
    asyncio.run(scenario())


def test_actual_request_connection_factory_and_original_executor_join(monkeypatch, tmp_path):
    async def scenario():
        source, runner = shared_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        agent = construct_actual_agent()
        view = agent.client._client._transport

        def use():
            response = view.handle_request(request())
            response.close()

        future = runner._executor.submit(contextvars.copy_context().run, use)
        await asyncio.wrap_future(future)
        pool = owned.find(view).pool_record
        assert len(pool.requests) == len(pool.connections) == 1
        connection = pool.connections[0]
        assert connection.factory is pool.pool_create and connection.normal_parent is pool
        assert connection.contained and not connection.client.is_closed()
        await join_original(source)
        assert not pool.requests[0].worker.is_alive()
        await shutdown_owned(runner)
        assert connection.client.calls == 1 and connection.is_closed_method() is True
        assert pool.pool.connections == [] and pool.phase == "closed"
    asyncio.run(scenario())


def test_shared_pool_without_actual_original_worker_join_stays_pending(monkeypatch, tmp_path):
    from tests.agent.auxiliary_owned_fixture import allow_normal_shutdown

    async def scenario():
        source, runner = shared_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        construct_actual_agent()
        allow_normal_shutdown(source)
        with pytest.raises(OwnedGatewayUnknown, match="worker-join-unavailable"):
            await shutdown_owned(runner)
        assert all(record.client.calls == 0 for record in pools(source))
        assert not source.auxiliary_closed
        runner._executor.shutdown(wait=True)
    asyncio.run(scenario())


def test_open_response_is_not_settled_by_sdk_or_view_closed_bits(monkeypatch, tmp_path):
    async def scenario():
        source, runner = shared_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        agent = construct_actual_agent()
        response = agent.client._client._transport.handle_request(request())
        await join_original(source)
        with pytest.raises(OwnedGatewayUnknown, match="response-not-closed"):
            await shutdown_owned(runner)
        assert not response.is_closed and not source.auxiliary_closed
        assert all(record.pool.calls == 0 for record in pools(source))
    asyncio.run(scenario())


def test_cached_pre_source_pool_is_not_adopted(monkeypatch, tmp_path):
    async def scenario():
        source, _runner = shared_fixture(monkeypatch, tmp_path)
        borrowed = FakeTransport()
        process_bootstrap._SHARED_TRANSPORTS[("borrowed",)] = borrowed
        with pytest.raises(OwnedGatewayUnknown, match="original-constructor-unavailable"):
            process_bootstrap._get_shared_transport(("borrowed",), lambda: pytest.fail("must use existing cache"))
        assert source.auxiliary_clients[-1].result is borrowed and borrowed.calls == 0
        assert process_bootstrap._SHARED_TRANSPORTS[("borrowed",)] is borrowed
    asyncio.run(scenario())


def test_legacy_shared_close_does_not_clear_managed_registry(monkeypatch, tmp_path):
    async def scenario():
        source, _runner = shared_fixture(monkeypatch, tmp_path)
        construct_actual_agent()
        with pytest.raises(OwnedGatewayUnknown, match="original-final-closer"):
            process_bootstrap.close_shared_transports()
        assert len(process_bootstrap._SHARED_TRANSPORTS) == 2
        assert all(record.client.calls == 0 for record in pools(source))
    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["async", "proxy"])
def test_original_async_proxy_and_tls_builder_defaults_preserved(monkeypatch, tmp_path, mode):
    import httpx

    async def scenario():
        source, _runner = shared_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(httpx, "AsyncHTTPTransport", FakeTransport)
        monkeypatch.setattr(httpx, "AsyncClient", FakeHTTP)
        if mode == "proxy":
            monkeypatch.setattr(process_bootstrap, "_get_proxy_for_base_url", lambda base: "http://fixture.invalid:123")
        client = process_bootstrap.build_keepalive_http_client("https://fixture.invalid", async_mode=mode == "async", verify=False)
        assert client.kwargs["verify"] is False
        assert not pools(source) and process_bootstrap._SHARED_TRANSPORTS == {}
        if mode == "proxy":
            assert client.kwargs["proxy"] == "http://fixture.invalid:123"
        else:
            assert len(client._mounts) == 2
    asyncio.run(scenario())
