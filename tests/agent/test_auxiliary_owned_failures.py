"""First-attempt failure retention at actual managed construction/close boundaries."""
import asyncio
from types import SimpleNamespace

import pytest

from agent import auxiliary_client as auxiliary
from agent import auxiliary_owned as owned
from agent.auxiliary_owned_close import shutdown_owned
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.agent.auxiliary_owned_fixture import (
    MockSDK, allow_normal_shutdown, bind_source, sdk_through_actual_proxy,
)


def test_close_failure_keeps_original_sdk_task_exception_and_no_retry(monkeypatch, tmp_path):
    error = RuntimeError("first-actual-mock-close-error")

    class FailedSDK(MockSDK):
        def close(self):
            self.calls.append("attempt")
            raise error

    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        client = sdk_through_actual_proxy(monkeypatch, FailedSDK)
        record = owned.find(client)
        allow_normal_shutdown(source)
        with pytest.raises(RuntimeError) as caught:
            await shutdown_owned(runner)
        assert caught.value is error is record.error is source.auxiliary_error
        assert record.close_attempted and record.close_task.exception() is error
        assert record.close_completion.exception() is error and record.client is client
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        assert client.calls == ["attempt"] and not source.auxiliary_closed
    asyncio.run(scenario())


def test_swallowed_provider_factory_failure_is_retained_before_fallback(monkeypatch, tmp_path):
    import providers
    error = RuntimeError("provider-partial-constructor-fixture")

    class FailedProvider:
        def create_client(self, **kwargs):
            partial = MockSDK()
            assert not partial.is_closed
            raise error

    async def scenario():
        source, _runner = bind_source(monkeypatch, tmp_path)
        provider = FailedProvider()
        monkeypatch.setattr(providers, "get_provider_profile", lambda name: provider)
        with pytest.raises(OwnedGatewayUnknown):
            owned.construct("resolve", auxiliary._api_key_profile_supplied_client, "fixture")
        assert source.auxiliary_error is error
        records = tuple(source.auxiliary_clients)
        assert len(records) == 2 and records[1].error is error
        assert all(record.error.__traceback__ is not None for record in records)
        assert all(record.phase == "unknown" for record in records)
        trace, partials = records[1].error.__traceback__, []
        while trace is not None:
            partials.append(trace.tb_frame.f_locals.get("partial"))
            trace = trace.tb_next
        assert any(isinstance(partial, MockSDK) and not partial.is_closed for partial in partials)
    asyncio.run(scenario())


def test_main_agent_shared_builder_does_not_adopt_auxiliary_custody(monkeypatch, tmp_path):
    async def scenario():
        source, _runner = bind_source(monkeypatch, tmp_path)
        client = owned.construct_during_auxiliary("sdk", MockSDK)
        assert source.auxiliary_clients == [] and owned.find(client) is None
        client.close()
        assert client.is_closed and source.auxiliary_credit == 0
    asyncio.run(scenario())


def test_closed_bit_on_outer_sdk_does_not_prove_inner_close(monkeypatch, tmp_path):
    class IncompleteSDK(MockSDK):
        def close(self):
            self.calls.append("outer-only")
            self.is_closed = True

    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        client = sdk_through_actual_proxy(monkeypatch, IncompleteSDK)
        allow_normal_shutdown(source)
        with pytest.raises(OwnedGatewayUnknown, match="normal-close-unconfirmed"):
            await shutdown_owned(runner)
        assert client.is_closed and not client._client.is_closed
        assert owned.find(client).phase == "unknown" and not source.auxiliary_closed
    asyncio.run(scenario())


def test_cache_entry_cannot_mint_genuine_constructor_ownership(monkeypatch, tmp_path):
    async def scenario():
        source, _runner = bind_source(monkeypatch, tmp_path)
        client = SimpleNamespace(is_closed=False, close=lambda: pytest.fail("borrowed close must not run"))
        monkeypatch.setattr(auxiliary, "_client_cache", {("fixture",): (client, "m", source.loop)})
        with pytest.raises(OwnedGatewayUnknown, match="original-creator-unavailable"):
            owned.retain_cache_client(client)
        assert source.auxiliary_clients[0].client is client and owned.find(client) is None
        assert auxiliary._client_cache[("fixture",)][0] is client and not source.auxiliary_closed
    asyncio.run(scenario())


def test_cache_consumer_cannot_reuse_client_after_normal_shutdown(monkeypatch, tmp_path):
    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        client = sdk_through_actual_proxy(monkeypatch)
        key = auxiliary._client_cache_key("custom", async_mode=False, model="m")
        monkeypatch.setattr(auxiliary, "_client_cache", {key: (client, "m", None)})
        allow_normal_shutdown(source)
        await shutdown_owned(runner)
        with pytest.raises(OwnedGatewayUnknown, match="consumer-after-normal-close-started"):
            auxiliary._get_cached_client("custom", model="m")
        assert len(client.calls) == 1 and auxiliary._client_cache[key][0] is client
        assert owned.find(client).client is client and owned.find(client).error is not None
    asyncio.run(scenario())
