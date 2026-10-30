"""Legacy unmanaged close scheduling, behind managed ownership guards."""
import contextlib
from typing import Any


def schedule_async_close(close_result: Any, client: Any) -> None:
    """Finish an unmanaged async close without leaking an unawaited coroutine."""
    from agent.auxiliary_owned import defer_legacy_close
    if defer_legacy_close(client, close_result):
        return
    from agent.auxiliary_client import _force_close_async_httpx

    async def _await_close() -> None:
        try:
            await close_result
        except Exception:  # health: allow BLE001 -- Preserve unmanaged best-effort teardown; managed owners return above.
            pass
        finally:
            _force_close_async_httpx(client)
    runner = _await_close()
    try:
        import asyncio as _aio
        try:
            loop = _aio.get_running_loop()
        except RuntimeError:
            _aio.run(runner)
        else:
            task = loop.create_task(runner)

            def _consume(completed_task) -> None:
                with contextlib.suppress(BaseException):
                    completed_task.exception()
            task.add_done_callback(_consume)
            runner = None
    except Exception:  # health: allow BLE001 -- Preserve unmanaged legacy scheduling; managed attempts cannot enter here.
        if runner is not None:
            with contextlib.suppress(Exception):
                runner.close()
        _force_close_async_httpx(client)
