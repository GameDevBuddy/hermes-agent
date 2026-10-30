"""Original shared factory/normal attempts stay retained on failure, cancellation and changed IO links."""
import asyncio

import pytest

from agent import auxiliary_client, auxiliary_owned as owned, process_bootstrap
from agent.auxiliary_owned_close import shutdown_owned
from hermes_constants import set_hermes_home_override
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.agent.primary_owned_fixture import construct_actual_agent
from tests.agent.shared_pool_fixture import FakePool, FakeTransport, join_original, shared_fixture


def test_actual_transport_constructor_failure_keeps_partial_self_and_debit(monkeypatch, tmp_path):
    error = RuntimeError("partial-transport-constructor")

    class Failed(FakeTransport):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            raise error

    async def scenario():
        source, _runner = shared_fixture(monkeypatch, tmp_path, transport=Failed)
        with pytest.raises(RuntimeError):
            construct_actual_agent()
        record = next(item for item in source.auxiliary_clients if item.kind == "shared-pool")
        assert record.error is error and record.ctor_attempted and record.ctor_result is None
        trace, values = error.__traceback__, []
        while trace is not None:
            values.append(trace.tb_frame.f_locals.get("self"))
            trace = trace.tb_next
        assert any(isinstance(item, Failed) for item in values)
        assert source.auxiliary_credit >= owned.CLAIM_CREDIT and not source.auxiliary_closed
    asyncio.run(scenario())


def test_normal_pool_failure_keeps_original_method_task_and_first_exception(monkeypatch, tmp_path):
    error = RuntimeError("original-pool-close")

    class Failed(FakeTransport):
        def close(self):
            self.calls += 1
            raise error

    async def scenario():
        source, runner = shared_fixture(monkeypatch, tmp_path, transport=Failed)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        construct_actual_agent()
        await join_original(source)
        with pytest.raises(RuntimeError) as caught:
            await shutdown_owned(runner)
        record = next(item for item in source.auxiliary_clients if item.kind == "shared-pool" and item.close_attempted)
        assert caught.value is record.error is error and record.close_task.exception() is error
        assert record.client.calls == 1 and not source.auxiliary_closed
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        assert record.client.calls == 1 and record.error is error
    asyncio.run(scenario())


@pytest.mark.parametrize("mutation", ["contained-pool", "normal-method", "profile", "job"])
def test_original_pool_links_rechecked_after_async_normal_io(monkeypatch, tmp_path, mutation):
    source = None

    class Changed(FakeTransport):
        async def close(self):
            await asyncio.sleep(0)
            super().close()
            changes = {
                "contained-pool": lambda: setattr(self, "_pool", FakePool()),
                "normal-method": lambda: setattr(self, "close", lambda: pytest.fail("foreign close")),
                "profile": lambda: set_hermes_home_override(str(tmp_path / "wrong")),
                "job": lambda: setattr(source.job, "revoked", True),
            }
            changes[mutation]()

    async def scenario():
        nonlocal source
        source, runner = shared_fixture(monkeypatch, tmp_path, transport=Changed)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        construct_actual_agent()
        await join_original(source)
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        original = next(item for item in source.auxiliary_clients if item.kind == "shared-pool" and item.close_attempted)
        assert original.client.calls == 1 and original.error is not None
        assert not source.auxiliary_closed and original.close_task.done()
    asyncio.run(scenario())


def test_cancelled_shared_normal_close_retains_uninterrupted_original_task(monkeypatch, tmp_path):
    caller = release = None

    class Blocking(FakeTransport):
        async def close(self):
            caller.cancel()
            await release.wait()
            super().close()

    async def scenario():
        nonlocal caller, release
        source, runner = shared_fixture(monkeypatch, tmp_path, transport=Blocking)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        construct_actual_agent()
        await join_original(source)
        caller, release = asyncio.current_task(), asyncio.Event()
        with pytest.raises(asyncio.CancelledError) as caught:
            await shutdown_owned(runner)
        record = next(item for item in source.auxiliary_clients if item.kind == "shared-pool" and item.close_attempted)
        assert record.error is caught.value and not record.close_task.cancelled()
        assert record.close_coroutine is not None and record.close_completion is not None
        release.set()
        with pytest.raises(OwnedGatewayUnknown):
            await record.close_task
        assert record.error is caught.value and record.client.calls == 1 and not source.auxiliary_closed
    asyncio.run(scenario())


def test_arbitrary_cache_miss_factory_cannot_adopt_preconstructed_transport(monkeypatch, tmp_path):
    async def scenario():
        source, _runner = shared_fixture(monkeypatch, tmp_path)
        borrowed = FakeTransport()
        with pytest.raises(OwnedGatewayUnknown, match="original-constructor-unavailable"):
            process_bootstrap._get_shared_transport(("fresh-key",), lambda: borrowed)
        record = source.auxiliary_clients[-1]
        assert record.result is borrowed and record.ctor_attempted is False
        assert borrowed.calls == 0 and ("fresh-key",) not in process_bootstrap._SHARED_TRANSPORTS
    asyncio.run(scenario())


@pytest.mark.parametrize("mutation", ["inner", "getter", "requests"])
def test_retained_contained_connection_graph_rechecked_after_normal_pool_io(monkeypatch, tmp_path, mutation):
    from types import SimpleNamespace
    from tests.agent.shared_pool_fixture import request

    class Changed(FakeTransport):
        async def close(self):
            original = tuple(self._pool.connections)
            super().close()
            await asyncio.sleep(0)
            if not original:
                return
            changes = {
                "inner": lambda: setattr(original[0], "_connection", SimpleNamespace(closed=True, _network_stream=object())),
                "getter": lambda: setattr(original[0], "is_closed", lambda: True),
                "requests": lambda: setattr(self._pool, "_requests", []),
            }
            changes[mutation]()

    async def scenario():
        source, runner = shared_fixture(monkeypatch, tmp_path, transport=Changed)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        agent = construct_actual_agent()
        response = agent.client._client._transport.handle_request(request())
        response.close()
        await join_original(source)
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        changed = next(item for item in source.auxiliary_clients if item.kind == "shared-pool" and item.connections)
        assert changed.error is not None and changed.connections[0].contained
        assert not source.auxiliary_closed
    asyncio.run(scenario())


@pytest.mark.parametrize("mutation", ["closed-bit", "holder-inner"])
def test_sdk_holder_phase_cannot_replace_actual_original_links_after_pool_io(monkeypatch, tmp_path, mutation):
    from tests.agent.auxiliary_owned_fixture import MockHTTP
    agent = None

    class Changed(FakeTransport):
        async def close(self):
            super().close()
            await asyncio.sleep(0)
            if mutation == "closed-bit":
                agent.client.is_closed = False
            else:
                replacement = MockHTTP()
                replacement.close()
                agent.client._client = replacement

    async def scenario():
        nonlocal agent
        source, runner = shared_fixture(monkeypatch, tmp_path, transport=Changed)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        agent = construct_actual_agent()
        await join_original(source)
        with pytest.raises(OwnedGatewayUnknown, match="holder-closed-state-changed"):
            await shutdown_owned(runner)
        assert not source.auxiliary_closed
        assert any(item.error is not None for item in source.auxiliary_clients if item.kind == "shared-pool")
    asyncio.run(scenario())
