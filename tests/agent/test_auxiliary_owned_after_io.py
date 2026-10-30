"""Retain changed original owners across mock async close; no SDK/kernel lifetime."""
import asyncio

import pytest

from agent import auxiliary_client as auxiliary
from agent import auxiliary_owned as owned
from agent.auxiliary_owned_close import shutdown_owned
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.agent.auxiliary_owned_fixture import (
    MockHTTP, MockSDK, allow_normal_shutdown, bind_source, sdk_through_actual_proxy,
)


@pytest.mark.parametrize("mutation", ["runtime", "inner-client", "job-membership", "close-method"])
def test_owner_change_during_async_close_never_becomes_settled(monkeypatch, tmp_path, mutation):
    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()

        class PausedSDK(MockSDK):
            async def close(self):
                original = self._client
                self.calls.append("original-close")
                entered.set()
                await release.wait()
                original.close()
                self.is_closed = True

        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary, "_client_cache", {})
        client = sdk_through_actual_proxy(monkeypatch, PausedSDK)
        record, original_runtime = owned.find(client), runner._gdb_original_source_runtime
        original_inner = client._client
        allow_normal_shutdown(source)

        async def change_owner():
            await entered.wait()
            if mutation == "runtime":
                runner._gdb_original_source_runtime = object()
            elif mutation == "inner-client":
                client._client = MockHTTP()
            elif mutation == "job-membership":
                source.job.revoked = True
            else:
                client.close = lambda: pytest.fail("replacement close must never run")
            release.set()

        mutation_task = asyncio.create_task(change_owner())
        try:
            with pytest.raises(OwnedGatewayUnknown):
                await shutdown_owned(runner)
        finally:
            await mutation_task
        assert record.client is client and record.inner is original_inner
        assert record.runtime is original_runtime and record.error is not None
        assert record.close_task.done() and record.close_attempted
        assert record.phase == "unknown" and not source.auxiliary_closed
        first = record.error
        with pytest.raises(OwnedGatewayUnknown):
            await shutdown_owned(runner)
        assert record.error is first and client.calls == ["original-close"]

    asyncio.run(scenario())
