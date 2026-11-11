"""Early normal Quit joins the real watcher startup delays without cancellation."""
import asyncio
from types import SimpleNamespace

import pytest

from gateway.run_watchers import GatewaySessionWatchersMixin


@pytest.mark.parametrize("name", ["_session_housekeeping_watcher", "_model_catalog_refresh_watcher", "_session_stall_watcher"])
def test_startup_delay_observes_shutdown(name):
    class Runner(GatewaySessionWatchersMixin, SimpleNamespace):
        pass

    async def scenario():
        runner = Runner(_running=True)
        task = asyncio.create_task(getattr(runner, name)())
        await asyncio.sleep(0)
        assert not task.done()
        runner._running = False
        # wait() does not cancel the watcher when this regression fails.
        done, _ = await asyncio.wait({task}, timeout=2)
        assert task in done, "startup delay ignored normal shutdown"
        assert task.result() is None and not task.cancelled()

    asyncio.run(scenario())
