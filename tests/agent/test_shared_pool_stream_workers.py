"""Real standalone stream request threads settle before the original shared pool closes."""
import asyncio
import contextvars
import threading
from types import SimpleNamespace

import pytest

from agent import auxiliary_client, auxiliary_owned as owned, shared_pool_owned
from agent.auxiliary_owned_close import shutdown_owned
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.agent.primary_owned_fixture import construct_actual_agent
from tests.agent.shared_pool_fixture import join_original, request, shared_fixture


def test_finished_standalone_stream_worker_is_normally_joined(monkeypatch, tmp_path):
    async def scenario():
        source, runner = shared_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        agent = construct_actual_agent()
        view = agent.client._client._transport
        errors = []

        def stream():
            try:
                response = view.handle_request(request())
                response.close()
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=contextvars.copy_context().run, args=(stream,))
        worker.start()
        await asyncio.to_thread(worker.join)
        assert not errors and not worker.is_alive()
        pool = owned.find(view).pool_record
        original = pool.requests[0]
        assert original.worker is worker and not original.active and original.response.is_closed
        assert worker not in runner._executor._threads
        joins, normal_join = [], worker.join

        def joined(*args, **kwargs):
            joins.append((threading.current_thread(), args, kwargs))
            return normal_join(*args, **kwargs)

        monkeypatch.setattr(worker, "join", joined)
        await join_original(source)
        await shutdown_owned(runner)
        assert len(joins) == 1 and joins[0][0] is not source.loop_thread
        assert joins[0][1:] == ((), {})
        assert source.auxiliary_closed and pool.phase == "closed"
        assert pool.client.calls == pool.pool.calls == 1
        assert pool.requests == (original,) and original.worker is worker
    asyncio.run(scenario())


def test_live_standalone_stream_worker_join_waits_off_loop(monkeypatch, tmp_path):
    async def scenario():
        source, runner = shared_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        agent = construct_actual_agent()
        view = agent.client._client._transport
        release, response_closed = threading.Event(), asyncio.Event()
        loop, notices, errors = asyncio.get_running_loop(), asyncio.Queue(), []

        def stream():
            try:
                response = view.handle_request(request())
                response.close()
                loop.call_soon_threadsafe(response_closed.set)
                assert release.wait(10), "loop did not release the live request worker"
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=contextvars.copy_context().run, args=(stream,))
        worker.start()
        normal_join = worker.join
        joins = []

        def joined(*args, **kwargs):
            joins.append((threading.current_thread(), args, kwargs))
            loop.call_soon_threadsafe(notices.put_nowait, "joining")
            return normal_join(*args, **kwargs)

        monkeypatch.setattr(worker, "join", joined)

        async def observe_wait():
            try:
                assert await asyncio.wait_for(notices.get(), 5) == "joining"
                assert worker.is_alive() and not release.is_set()
                assert not source.settlement._shared_join["done"]
                assert pool.client.calls == pool.pool.calls == 0
                assert not source.auxiliary_closed
            finally:
                release.set()

        observer = None
        try:
            await asyncio.wait_for(response_closed.wait(), 5)
            pool = owned.find(view).pool_record
            original = pool.requests[0]
            assert original.worker is worker and not original.active and original.response.is_closed
            observer = asyncio.create_task(observe_wait())
            await join_original(source)
            notices.put_nowait("finished")
            await observer
            assert len(joins) == 1 and joins[0][0] is not source.loop_thread
            assert joins[0][1:] == ((), {})
            assert not errors and not worker.is_alive()
            await shutdown_owned(runner)
            assert source.auxiliary_closed and pool.phase == "closed"
            assert pool.requests == (original,) and original.worker is worker
            assert pool.client.calls == pool.pool.calls == 1
        finally:
            release.set()
            notices.put_nowait("finished")
            try:
                if observer is not None:
                    await observer
            finally:
                await asyncio.to_thread(normal_join)
    asyncio.run(scenario())


def test_thread_shaped_request_worker_is_refused_without_join_or_retry(monkeypatch, tmp_path):
    class ThreadShaped:
        ident = 123

        def __eq__(self, other):
            return True

        def join(self):
            pytest.fail("must not join an unrecognized request worker")

        def is_alive(self):
            return False

    async def scenario():
        source, runner = shared_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        agent = construct_actual_agent()
        worker = ThreadShaped()
        monkeypatch.setattr(shared_pool_owned, "threading", SimpleNamespace(current_thread=lambda: worker))
        view = agent.client._client._transport
        response = view.handle_request(request())
        response.close()
        pool = owned.find(view).pool_record
        original = pool.requests[0]
        assert original.worker is worker
        with pytest.raises(OwnedGatewayUnknown, match="unrecognized-original-request-thread") as caught:
            await join_original(source)
        claim = source.settlement._shared_join
        assert claim["error"] is caught.value and not claim["done"]
        assert claim["request_workers"] == ((original, worker),)
        assert pool.client.calls == pool.pool.calls == 0 and not source.auxiliary_closed
        assert runner._executor.shutdown_calls == []
        first_reason = source.error
        with pytest.raises(OwnedGatewayUnknown, match="unrecognized-original-request-thread"):
            await source.settlement._join_pools()
        assert source.error is first_reason
        assert source.settlement._shared_join is claim and claim["error"] is caught.value
        assert pool.requests == (original,) and original.worker is worker
    asyncio.run(scenario())
