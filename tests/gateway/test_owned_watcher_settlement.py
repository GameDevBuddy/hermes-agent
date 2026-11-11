"""Actual supervised/stop consumers over lower resource models, with no product lifetime."""
import asyncio

import pytest

import hermes_owned_gateway as owned
from tests.agent.auxiliary_owned_fixture import bind_source
from tests.gateway.test_owned_gateway_lifecycle import (
    bound_settlement, source as lifecycle_source, stop_resources,
)

source = lifecycle_source


def test_normal_stop_signals_original_watchers_before_joining_accepted_work(source, monkeypatch):
    async def scenario():
        settlement = bound_settlement(source)
        original_event = asyncio.Event()
        settlement.runner._shutdown_event = original_event
        events = []
        stop_resources(settlement, monkeypatch, events)

        async def join_accepted_work():
            assert original_event.is_set()
            assert settlement.runner._running is False and source.phase == "draining"
            events.append("accepted-work-joined")

        monkeypatch.setattr(settlement, "_join_work", join_accepted_work)
        await settlement.stop()
        assert settlement.runner._shutdown_event is original_event
        assert "accepted-work-joined" in events and settlement._stop_completed

    asyncio.run(scenario())


def test_unmatched_shutdown_never_signals_or_joins_the_original_resources(source, monkeypatch):
    async def scenario():
        settlement = bound_settlement(source)
        original_event = asyncio.Event()
        settlement.runner._shutdown_event = original_event
        events = []
        stop_resources(settlement, monkeypatch, events)
        source._shutdown = False
        with pytest.raises(owned.OwnedGatewayUnknown, match="matching-shutdown"):
            await settlement.stop()
        assert not original_event.is_set() and events == []
        settlement.cron_stop.set()  # Test cleanup: the refused stop never signalled its ticker.
        settlement.cron_thread.join()
        settlement.housekeeping.join()

    asyncio.run(scenario())


def setup_watcher(monkeypatch, tmp_path):
    value, runner = bind_source(monkeypatch, tmp_path)
    runner._running, runner._background_tasks = True, set()
    runner.session_runtime_descriptor = {"state": "ready", "capabilities": ["fixture"]}
    monkeypatch.setattr(runner, "_supervised_backoff", lambda attempt: 0)
    return value, runner


@pytest.mark.parametrize("failure", [ValueError("original watcher failed"),
                                      asyncio.CancelledError("original watcher cancelled")],
                         ids=["exception", "cancelled-return"])
def test_failed_managed_watcher_retains_first_error_and_never_respawns(monkeypatch, tmp_path, failure):
    async def scenario():
        value, runner = setup_watcher(monkeypatch, tmp_path)
        calls = []

        async def watcher():
            calls.append("body")
            if len(calls) == 1:
                raise failure

        task = runner._spawn_supervised(watcher, "original-watcher")
        callback_finished = asyncio.Event()
        task.add_done_callback(lambda completed: callback_finished.set())
        try:
            await callback_finished.wait()
            first_traceback = value._startup_traceback
            if value._startup_error is not None:
                assert first_traceback is failure.__traceback__
            with pytest.raises(type(failure)):
                await task
            assert value.phase == "unknown" and value._startup_error is failure
            assert value._startup_traceback is first_traceback
            assert task in value.unresolved_resource_refs and task in runner._background_tasks
            assert runner._draining and not runner._running
            assert runner.session_runtime_descriptor == {"state": "unavailable", "capabilities": []}
            assert calls == ["body"]
        finally:
            # Finish any legacy backoff task in the RED scope cooperatively;
            # no cancellation, product shutdown, or normal-stop claim.
            runner._running = False
            for pending in tuple(runner._background_tasks):
                if pending is not task:
                    await pending

    asyncio.run(scenario())


def test_clean_managed_watcher_return_remains_a_single_successful_task(monkeypatch, tmp_path):
    async def scenario():
        value, runner = setup_watcher(monkeypatch, tmp_path)
        calls = []

        async def watcher():
            calls.append("body")

        task = runner._spawn_supervised(watcher, "clean-watcher")
        callback_finished = asyncio.Event()
        task.add_done_callback(lambda completed: callback_finished.set())
        await callback_finished.wait()
        await task
        assert calls == ["body"] and value.phase == "bound" and value._startup_error is None
        assert task not in runner._background_tasks and value.unresolved_resource_refs == []

    asyncio.run(scenario())


def test_failed_managed_spawn_tracker_keeps_first_error_and_original_task(monkeypatch, tmp_path):
    async def scenario():
        value, runner = setup_watcher(monkeypatch, tmp_path)
        first = ValueError("original tracker failed")

        async def body():
            assert value.phase == "unknown"

        def tracker(task):
            assert task in runner._background_tasks
            raise first

        task = runner._spawn_supervised(body, "original-tracker", on_spawn=tracker)
        first_traceback = first.__traceback__
        callback_finished = asyncio.Event()
        task.add_done_callback(lambda completed: callback_finished.set())
        await callback_finished.wait()
        await task
        assert value._startup_error is first and value._startup_traceback is first_traceback
        assert value.unresolved_resource_refs == [task] and runner._background_tasks == {task}
        assert value.phase == "unknown" and not runner._running

    asyncio.run(scenario())


def test_custom_task_factory_refuses_before_the_original_coroutine_factory(monkeypatch, tmp_path):
    async def scenario():
        _value, runner = setup_watcher(monkeypatch, tmp_path)
        loop = asyncio.get_running_loop()
        calls, created = [], []

        async def body():
            calls.append("body")

        def coroutine_factory():
            calls.append("coroutine-factory")
            return body()

        def task_factory(loop, coroutine, **kwargs):
            calls.append("task-factory")
            task = asyncio.Task(coroutine, loop=loop, **kwargs)
            created.append(task)
            return task

        loop.set_task_factory(task_factory)
        try:
            with pytest.raises(owned.OwnedGatewayUnknown, match="task-factory"):
                runner._spawn_supervised(coroutine_factory, "unreachable")
            assert calls == []
        finally:
            loop.set_task_factory(None)
            for task in created:
                await task

    asyncio.run(scenario())


def test_missing_source_refuses_before_the_original_coroutine_factory(monkeypatch, tmp_path):
    async def scenario():
        _value, runner = setup_watcher(monkeypatch, tmp_path)
        calls = []

        async def body():
            calls.append("body")

        def coroutine_factory():
            calls.append("coroutine-factory")
            return body()

        monkeypatch.setattr(owned, "_CURRENT", None)
        try:
            with pytest.raises(owned.OwnedGatewayUnknown, match="association-missing"):
                runner._spawn_supervised(coroutine_factory, "unreachable")
            assert calls == []
        finally:
            for task in tuple(runner._background_tasks):
                await task

    asyncio.run(scenario())


def test_failed_managed_background_task_is_retained_before_tracker_removal(monkeypatch, tmp_path):
    async def scenario():
        value, runner = setup_watcher(monkeypatch, tmp_path)
        first = ValueError("original unsupervised task failed")

        async def body():
            raise first

        task = asyncio.create_task(body())
        assert runner._retain_background_task(task) is task
        callback_finished = asyncio.Event()
        task.add_done_callback(lambda completed: callback_finished.set())
        await callback_finished.wait()
        assert value.phase == "unknown" and value._startup_error is first
        assert value._startup_traceback is first.__traceback__
        assert task in runner._background_tasks and value.unresolved_resource_refs == [task]
        with pytest.raises(ValueError) as caught:
            await task
        assert caught.value is first and not runner._running

    asyncio.run(scenario())


def test_missing_source_during_background_registration_retains_the_created_task(monkeypatch, tmp_path):
    async def scenario():
        value, runner = setup_watcher(monkeypatch, tmp_path)

        async def body():
            return None

        task = asyncio.create_task(body())
        monkeypatch.setattr(owned, "_CURRENT", None)
        try:
            with pytest.raises(owned.OwnedGatewayUnknown, match="association-missing") as caught:
                runner._retain_background_task(task)
            assert value._startup_error is caught.value and value.phase == "unknown"
            assert task in runner._background_tasks and value.unresolved_resource_refs == [task]
        finally:
            await task

    asyncio.run(scenario())
