"""Managed source claims/cache consumers with mock SDKs; never model, network or OS lifetimes."""
import asyncio
from types import SimpleNamespace

import pytest

import hermes_owned_gateway as ownership
from agent import auxiliary_client as auxiliary
from agent import auxiliary_owned as owned
from agent.auxiliary_owned_close import shutdown_owned
from hermes_constants import hermes_home_key, set_hermes_home_override
from tests.agent.auxiliary_owned_fixture import (
    allow_normal_shutdown, bind_source, sdk_through_actual_proxy,
)


@pytest.mark.parametrize("eviction", ["provider", "instance", "replacement", "fifo", "race", "stale"])
def test_cache_mutations_keep_original_created_clients(monkeypatch, tmp_path, eviction):
    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        monkeypatch.setattr(auxiliary, "_pool_cache_hint", lambda *args, **kwargs: "")
        first = sdk_through_actual_proxy(monkeypatch)
        key = auxiliary._client_cache_key("custom", async_mode=False, model="m")
        auxiliary._store_cached_client(key, first, "m")
        second = sdk_through_actual_proxy(monkeypatch)
        def stale():
            # A real closed in-memory loop is cache metadata; it must not authorize closure.
            old_loop = asyncio.new_event_loop()
            old_loop.close()
            auxiliary._client_cache[key] = (first, "m", old_loop)
            auxiliary.cleanup_stale_async_clients()
        def cache_race_or_fifo():
            monkeypatch.setattr(auxiliary, "_CLIENT_CACHE_MAX_SIZE", 1)
            if eviction == "race":
                auxiliary._client_cache.pop(key)

            def resolve(*args, **kwargs):
                if eviction == "race":
                    # Another genuine creator publishes while the actual cache consumer is building.
                    auxiliary._store_cached_client(key, first, "m")
                return second, "m"

            monkeypatch.setattr(auxiliary, "resolve_provider_client", resolve)
            result, _model = auxiliary._get_cached_client("custom", model="m" if eviction == "race" else "other")
            assert result is (first if eviction == "race" else second)
        actions = {
            "provider": lambda: auxiliary._evict_cached_clients("custom"),
            "instance": lambda: auxiliary._evict_cached_client_instance(first),
            "replacement": lambda: auxiliary._store_cached_client(key, second, "m"),
            "stale": stale, "race": cache_race_or_fifo, "fifo": cache_race_or_fifo,
        }
        actions[eviction]()
        assert not first.calls and not second.calls
        assert owned.find(first).source is source and owned.find(second).source is source
        assert owned.find(first).client is first
        allow_normal_shutdown(source)
        await shutdown_owned(runner)
        assert source.auxiliary_closed and len(first.calls) == len(second.calls) == 1
        assert all(record.client in (first, second) for record in source.auxiliary_clients)
    asyncio.run(scenario())


def test_actual_creator_profile_and_empty_cache_normal_consumer(monkeypatch, tmp_path):
    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        client = sdk_through_actual_proxy(monkeypatch)
        record = owned.find(client)
        original_profile = hermes_home_key()
        set_hermes_home_override(str(tmp_path / "shutdown-profile"))
        allow_normal_shutdown(source)
        await shutdown_owned(runner)
        await shutdown_owned(runner)
        assert client.calls[0][:2] == (source.loop, original_profile)
        assert hermes_home_key() != original_profile
        assert record.source is source and record.runner is runner
        assert record.runtime is runner._gdb_original_source_runtime
        assert record.close_task.get_loop() is record.creator_loop is source.loop
        assert record.close_completion.done() and len(client.calls) == 1
        assert source.auxiliary_clients == [record] and record.phase == "closed"
    asyncio.run(scenario())


def test_failed_constructor_keeps_first_exception_and_partial_self(monkeypatch, tmp_path):
    error = RuntimeError("actual-first-mock-constructor-error")

    class FailedSDK:
        def __init__(self, **kwargs):
            self.partial = object()
            raise error

    async def scenario():
        source, _runner = bind_source(monkeypatch, tmp_path)
        with pytest.raises(RuntimeError) as caught:
            sdk_through_actual_proxy(monkeypatch, FailedSDK)
        record = source.auxiliary_clients[0]
        assert caught.value is record.error is error and record.phase == "unknown"
        frames, trace = [], record.error.__traceback__
        while trace is not None:
            frames.append(trace.tb_frame)
            trace = trace.tb_next
        assert any(isinstance(frame.f_locals.get("self"), FailedSDK) for frame in frames)
        assert source.auxiliary_credit == owned.CLAIM_CREDIT
        with pytest.raises(ownership.OwnedGatewayUnknown):
            sdk_through_actual_proxy(monkeypatch)
        assert source.auxiliary_clients == [record] and record.error is error
    asyncio.run(scenario())


def test_record_allocation_failure_consumes_hole_before_work(monkeypatch, tmp_path):
    error = MemoryError("record-allocation-fixture")

    class FailedRecord:
        def __new__(cls, *args, **kwargs):
            raise error

    async def scenario():
        source, _runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(owned, "AuxiliaryConstruction", FailedRecord)
        with pytest.raises(MemoryError):
            sdk_through_actual_proxy(monkeypatch)
        assert source.auxiliary_clients == [None]
        assert source.auxiliary_credit == owned.CLAIM_CREDIT and source.auxiliary_error is error
    asyncio.run(scenario())


def test_capacity_fails_before_factory_and_keeps_consumed_debits(monkeypatch, tmp_path):
    async def scenario():
        source, _runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(owned, "MAX_CLAIMS", 1)
        client = sdk_through_actual_proxy(monkeypatch)
        with pytest.raises(ownership.OwnedGatewayUnknown, match="capacity"):
            sdk_through_actual_proxy(monkeypatch)
        assert len(source.auxiliary_clients) == 1 and owned.find(client).client is client
        assert source.auxiliary_credit == owned.CLAIM_CREDIT and not client.calls
    asyncio.run(scenario())


@pytest.mark.parametrize("mutation", ["revoked-job", "other-source", "other-runtime", "changed-close", "closed-early"])
def test_original_relationship_changes_cannot_authorize_close(monkeypatch, tmp_path, mutation):
    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        client = sdk_through_actual_proxy(monkeypatch)
        record = owned.find(client)
        allow_normal_shutdown(source)
        changes = {
            "revoked-job": lambda: setattr(source.job, "revoked", True),
            "other-source": lambda: monkeypatch.setattr(ownership, "_CURRENT", None),
            "other-runtime": lambda: setattr(runner, "_gdb_original_source_runtime", object()),
            "changed-close": lambda: setattr(client, "close", lambda: pytest.fail("foreign close must not run")),
            "closed-early": lambda: setattr(client, "is_closed", True),
        }
        changes[mutation]()
        with pytest.raises(ownership.OwnedGatewayUnknown):
            await shutdown_owned(runner)
        assert not client.calls and record.client is client and not source.auxiliary_closed
    asyncio.run(scenario())


def test_legacy_close_and_shutdown_never_neuter_managed_clients(monkeypatch, tmp_path):
    async def scenario():
        source, _runner = bind_source(monkeypatch, tmp_path)
        client = sdk_through_actual_proxy(monkeypatch)
        monkeypatch.setattr(auxiliary, "_client_cache", {("fixture",): (client, "m", source.loop)})
        auxiliary._close_cached_client(client, close_async=True)
        auxiliary._force_close_async_httpx(client)
        assert not client.calls and not client._client.is_closed
        with pytest.raises(ownership.OwnedGatewayUnknown, match="original-normal-shutdown"):
            auxiliary.shutdown_cached_clients()
        assert auxiliary._client_cache[("fixture",)][0] is client
    asyncio.run(scenario())


def test_opaque_provider_view_is_retained_without_adopting_a_close_owner(monkeypatch, tmp_path):
    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        view = SimpleNamespace(close=lambda: pytest.fail("opaque close must not run"), is_closed=False)
        result = owned.construct("resolve", lambda: (view, "m"), shape="pair")
        assert result == (view, "m") and owned.find(view).source is source
        allow_normal_shutdown(source)
        with pytest.raises(ownership.OwnedGatewayUnknown, match="owner-unavailable"):
            await shutdown_owned(runner)
        assert owned.find(view).client is view and not source.auxiliary_closed
    asyncio.run(scenario())
