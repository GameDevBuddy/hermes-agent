"""Real in-memory owning loops/threads with mock SDKs; no SDK/network/OS lifetime proof."""
import asyncio
import concurrent.futures
import contextvars
import sys
import threading
from types import SimpleNamespace

import pytest

from agent import auxiliary_client as auxiliary
from agent import auxiliary_owned as owned
from agent.auxiliary_owned_close import shutdown_owned
from gateway import run_owned_resources
from hermes_constants import hermes_home_key, set_hermes_home_override
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.agent.auxiliary_owned_fixture import (
    MockHTTP, MockSDK, allow_normal_shutdown, bind_source, sdk_through_actual_proxy,
)


class LoopThread:
    def __init__(self):
        self.ready = threading.Event()
        self.loop = None
        self.thread = threading.Thread(target=self.run)
        self.thread.start()
        assert self.ready.wait(5)

    def run(self):
        self.loop = asyncio.new_event_loop()
        self.ready.set()
        try:
            self.loop.run_forever()
        finally:
            self.loop.close()

    def stop(self):
        if not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        assert not self.thread.is_alive()


@pytest.mark.parametrize("closed", [False, True])
def test_genuine_foreign_creator_loop_is_used_or_retained_not_replaced(monkeypatch, tmp_path, closed):
    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        owner, ready = LoopThread(), concurrent.futures.Future()
        profile = hermes_home_key()

        def create():
            try:
                ready.set_result(sdk_through_actual_proxy(monkeypatch))
            except BaseException as error:
                ready.set_exception(error)

        try:
            owner.loop.call_soon_threadsafe(create, context=contextvars.copy_context())
            client = await asyncio.wrap_future(ready)
            record = owned.find(client)
            if closed:
                owner.stop()
            set_hermes_home_override(str(tmp_path / "different-current-profile"))
            allow_normal_shutdown(source)
            if closed:
                with pytest.raises(OwnedGatewayUnknown, match="owner-loop-unavailable"):
                    await shutdown_owned(runner)
                assert not client.calls and not client._client.is_closed
                assert record.close_attempted and record.creator_loop is owner.loop
                assert record.client is client and record.error is not None
            else:
                await shutdown_owned(runner)
                assert client.calls == [(owner.loop, profile, owner.thread)]
                assert record.close_task.get_loop() is owner.loop and source.auxiliary_closed
        finally:
            owner.stop()
    asyncio.run(scenario())


def test_async_normal_close_through_actual_remaining_resource_consumer(monkeypatch, tmp_path):
    class AsyncSDK(MockSDK):
        async def close(self):
            await asyncio.sleep(0)
            super().close()

    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        stop = threading.Event()
        monkeypatch.setitem(sys.modules, "hermes_cli.nous_auth_keepalive", SimpleNamespace(_keepalive_stop=stop))
        monkeypatch.setitem(sys.modules, "hermes_logging", SimpleNamespace(
            _queue_state_lock=threading.Lock(), _queue_listener=None))
        monkeypatch.setitem(sys.modules, "gateway.runtime_ownership", SimpleNamespace(
            process_ownership=SimpleNamespace(_writers=[])))
        client = sdk_through_actual_proxy(monkeypatch, AsyncSDK)
        allow_normal_shutdown(source)
        await run_owned_resources.close_remaining_python_resources(runner, None)
        record = owned.find(client)
        assert stop.is_set() and record.close_task.done() and not record.close_task.cancelled()
        assert len(client.calls) == 1 and record.client is client and source.auxiliary_closed
    asyncio.run(scenario())


def test_cancelled_shutdown_keeps_real_close_task_and_first_diagnostic(monkeypatch, tmp_path):
    release = None
    caller = None

    class BlockingSDK(MockSDK):
        async def close(self):
            caller.cancel()
            await release.wait()
            super().close()

    async def scenario():
        nonlocal caller, release
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        caller, release = asyncio.current_task(), asyncio.Event()
        client = sdk_through_actual_proxy(monkeypatch, BlockingSDK)
        record = owned.find(client)
        allow_normal_shutdown(source)
        with pytest.raises(asyncio.CancelledError) as caught:
            await shutdown_owned(runner)
        assert record.error is caught.value and not record.close_task.cancelled()
        assert record.close_attempted and record.close_coroutine is not None
        release.set()
        with pytest.raises(OwnedGatewayUnknown):
            await record.close_task
        assert record.error is caught.value and len(client.calls) == 1
        assert record.phase == "unknown" and not source.auxiliary_closed
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        assert len(client.calls) == 1
    asyncio.run(scenario())


def test_shared_pool_view_close_never_counts_as_pool_normal_closure(monkeypatch, tmp_path):
    from agent import process_bootstrap

    class Pool:
        def __init__(self):
            self.calls = 0

        def close(self):
            self.calls += 1

    class ViewHTTP(MockHTTP):
        def close(self):
            for view in self._mounts.values():
                view.close()
            self.is_closed = True

    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        pool, http = Pool(), ViewHTTP()
        view = process_bootstrap._shared_transport_cls()(pool)
        http._mounts = {"https://": view}
        monkeypatch.setattr(process_bootstrap, "_SHARED_TRANSPORTS", {("fixture",): pool})
        monkeypatch.setattr(auxiliary, "_load_openai_cls", lambda: MockSDK)
        client = auxiliary.OpenAI(http_client=http)
        record = owned.find(client)
        allow_normal_shutdown(source)
        with pytest.raises(OwnedGatewayUnknown, match="shared-pool-original-normal-owner"):
            await shutdown_owned(runner)
        assert client.is_closed and http.is_closed and pool.calls == 0
        assert any(item is pool for item in record.shared_refs) and record.client is client
        assert not source.auxiliary_closed
        first_error = source.error
        with pytest.raises(OwnedGatewayUnknown) as caught:
            process_bootstrap.close_shared_transports()
        assert str(caught.value) == first_error and record.error is source.auxiliary_error
        assert process_bootstrap._SHARED_TRANSPORTS[("fixture",)] is pool and pool.calls == 0
    asyncio.run(scenario())


def test_actual_resolver_and_injected_http_constructor_share_original_owner(monkeypatch, tmp_path):
    from agent import process_bootstrap

    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        monkeypatch.setattr(auxiliary, "_load_openai_cls", lambda: MockSDK)
        monkeypatch.setattr(process_bootstrap, "build_keepalive_http_client", lambda *args, **kwargs: MockHTTP())
        client, model = auxiliary.resolve_provider_client(
            "custom", "m", explicit_base_url="https://fixture.invalid/v1", explicit_api_key="fixture")
        record, http_record = owned.find(client), owned.find(client._client)
        assert model == "m" and record.source is http_record.source is source
        assert http_record in record.children and len(source.auxiliary_clients) == 3
        allow_normal_shutdown(source)
        await shutdown_owned(runner)
        assert len(client.calls) == 1 and http_record.phase == "closed" and source.auxiliary_closed
        assert http_record.close_parent is record and http_record.close_attempted
        assert http_record.close_task is record.close_task and http_record.close_completion is record.close_completion
    asyncio.run(scenario())


def test_provider_probe_does_not_claim_stub_as_a_resource(monkeypatch, tmp_path):
    async def scenario():
        source, _runner = bind_source(monkeypatch, tmp_path)
        with auxiliary.aux_probe_mode():
            client, _model = auxiliary.resolve_provider_client(
                "custom", "m", explicit_base_url="https://fixture.invalid/v1", explicit_api_key="fixture")
        assert client is not None and source.auxiliary_clients == [] and source.auxiliary_credit == 0
    asyncio.run(scenario())


def test_cache_loop_metadata_cannot_replace_actual_creator_loop(monkeypatch, tmp_path):
    async def scenario():
        source, _runner = bind_source(monkeypatch, tmp_path)
        owner, ready = LoopThread(), concurrent.futures.Future()

        def create():
            try:
                ready.set_result(sdk_through_actual_proxy(monkeypatch))
            except BaseException as error:
                ready.set_exception(error)

        try:
            owner.loop.call_soon_threadsafe(create, context=contextvars.copy_context())
            client = await asyncio.wrap_future(ready)
            key = auxiliary._client_cache_key("custom", async_mode=True, model="m")
            monkeypatch.setattr(auxiliary, "_client_cache", {key: (client, "m", source.loop)})
            with pytest.raises(OwnedGatewayUnknown, match="original-consumer-loop-required"):
                auxiliary._get_cached_client("custom", model="m", async_mode=True)
            assert not client.calls and owned.find(client).creator_loop is owner.loop
            assert auxiliary._client_cache[key][0] is client and not source.auxiliary_closed
        finally:
            owner.stop()
    asyncio.run(scenario())


def test_async_constructor_without_running_loop_has_no_adopted_normal_owner(monkeypatch, tmp_path):
    import openai
    from agent import process_bootstrap

    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        monkeypatch.setattr(openai, "AsyncOpenAI", MockSDK)
        monkeypatch.setattr(process_bootstrap, "build_keepalive_http_client", lambda *args, **kwargs: MockHTTP())
        client, model = await asyncio.to_thread(auxiliary._to_async_client, MockSDK(), "m")
        record = owned.find(client)
        assert model == "m" and record.creator_loop is None and not record.supported
        allow_normal_shutdown(source)
        with pytest.raises(OwnedGatewayUnknown, match="normal-close-owner-unavailable"):
            await shutdown_owned(runner)
        assert not client.calls and record.client is client and not client._client.is_closed
        assert not source.auxiliary_closed and record.close_attempted
    asyncio.run(scenario())
