"""Source-only lifecycle ordering; no gateway, SDK process or native API is started."""
import asyncio
import io
import threading
from types import SimpleNamespace

import pytest

import hermes_owned_gateway as ownership
from gateway import run_owned
from gateway import run_owned_resources as resources
from gateway import run_owned_shutdown as shutdown


@pytest.fixture
def source(monkeypatch):
    contract = SimpleNamespace(attempt_id="source-fixture")
    stream = SimpleNamespace(buffer=io.BytesIO())
    monkeypatch.setattr(ownership.sys, "stdin", stream)
    value = ownership.OwnedGatewaySource(contract, b"{}")
    value.job = SimpleNamespace(require_current_member=lambda: None)
    value.phase = "attached"
    monkeypatch.setattr(ownership, "_CURRENT", value)
    return value


def test_entry_refuses_an_object_with_fabricated_guard_methods():
    calls = []
    fake = SimpleNamespace(contract=object(), require_attached=lambda value: calls.append(value))
    with pytest.raises(ownership.OwnedGatewayUnknown):
        run_owned.ManagedEntry(fake)
    assert calls == []


def test_entry_refuses_a_different_original_source(source, monkeypatch):
    other = ownership.OwnedGatewaySource(source.contract, b"{}")
    monkeypatch.setattr(ownership, "_CURRENT", other)
    with pytest.raises(ownership.OwnedGatewayUnknown):
        run_owned.ManagedEntry(source)


def test_entry_keeps_the_same_source_and_actual_implementations(source):
    entry = run_owned.ManagedEntry(source)
    assert entry.source is source
    assert entry.main is run_owned.main
    assert entry.start is run_owned.start_managed
    assert entry.settlement_type is shutdown.ManagedSettlement


def test_actual_launch_refusal_still_precedes_entry_preparation(source, monkeypatch):
    calls = []
    monkeypatch.setattr(run_owned, "prepare_entry", lambda value: calls.append(value))
    with pytest.raises(ownership.OwnedGatewayUnknown, match="not-composed"):
        ownership.require_lifecycle_available(source)
    assert calls == []


def test_low_level_entry_refuses_before_creating_an_event_loop(source, monkeypatch):
    calls = []

    def forbidden():
        calls.append("loop")
        raise AssertionError("unavailable entry created a loop")

    monkeypatch.setattr(run_owned.asyncio, "new_event_loop", forbidden)
    with pytest.raises(ownership.OwnedGatewayUnknown, match="not-composed"):
        run_owned.main(source)
    assert calls == []


def test_bootstrap_entry_refuses_before_imports_or_profile_claim(source, monkeypatch):
    calls = []

    def forbidden(*args):
        calls.append(args)
        raise AssertionError("unavailable entry claimed a profile")

    monkeypatch.setattr(run_owned, "_claim_profile_ownership", forbidden)
    with pytest.raises(ownership.OwnedGatewayUnknown, match="not-composed"):
        asyncio.run(run_owned.start_managed(source))
    assert calls == []


def test_entry_cannot_replace_the_refusal_with_a_mutated_callback(source):
    calls = []
    entry = run_owned.ManagedEntry(source)
    entry.main = lambda value: calls.append(value)
    with pytest.raises(ownership.OwnedGatewayUnknown, match="not-composed"):
        entry.run()
    assert calls == []


def bound_settlement(source):
    runner = SimpleNamespace(
        _owned_gateway_source=source, session_runtime_descriptor={"state": "ready"},
        _running=True, _background_tasks=set(), _shutdown_event=asyncio.Event(),
    )
    source.runner = runner
    source.task = asyncio.current_task()
    source.loop = asyncio.get_running_loop()
    source.phase = "bound"
    source._shutdown = source._control_closed = True
    return shutdown.ManagedSettlement(source, runner)


def stop_resources(settlement, monkeypatch, events, *, work_error=None):
    settlement.control = object()
    settlement.listener = object()
    settlement.authorities = object()
    settlement.cron_stop = threading.Event()
    settlement.cron = SimpleNamespace(stop=lambda: events.append("cron-stop"))
    from cron.scheduler_thread import SupervisedTickerThread
    settlement.cron_thread = SupervisedTickerThread(
        settlement.cron_stop.wait, stop_event=settlement.cron_stop,
    )
    settlement.housekeeping = threading.Thread(target=lambda: None)
    settlement.cron_thread.start()
    settlement.housekeeping.start()

    async def work():
        events.append("work")
        if work_error is not None:
            raise work_error

    async def stage():
        events.append("stage")

    async def resource(value):
        events.append("resource")

    monkeypatch.setattr(settlement, "_join_work", work)
    monkeypatch.setattr(settlement, "_close_listeners", stage)
    monkeypatch.setattr(settlement, "_join_pools", stage)
    for name in ("close_authorities", "close_adapters", "close_agent_resources", "close_tool_resources"):
        monkeypatch.setattr(resources, name, resource)


def test_finish_before_stop_never_reaches_a_resource_closer(source, monkeypatch):
    async def scenario():
        settlement = bound_settlement(source)
        calls = []

        async def forbidden(*args):
            calls.append(args)
            raise AssertionError("close reached before successful stop")

        monkeypatch.setattr(resources, "close_remaining_python_resources", forbidden)
        with pytest.raises(ownership.OwnedGatewayUnknown, match="stop"):
            await settlement.finish()
        assert calls == []
        assert not settlement.finished

    asyncio.run(scenario())


def test_successful_stop_is_not_replayed(source, monkeypatch):
    async def scenario():
        settlement = bound_settlement(source)
        events = []
        stop_resources(settlement, monkeypatch, events)
        await settlement.stop()
        before = list(events)
        with pytest.raises(ownership.OwnedGatewayUnknown, match="stop"):
            await settlement.stop()
        assert events == before
        assert source.phase == "draining"
        assert settlement.runner._running is False

    asyncio.run(scenario())


def test_failed_stop_retains_original_refs_without_a_second_attempt(source, monkeypatch):
    async def scenario():
        settlement = bound_settlement(source)
        events = []
        failure = ValueError("owned-work-failed")
        stop_resources(settlement, monkeypatch, events, work_error=failure)
        original = (settlement.control, settlement.listener, settlement.authorities)
        with pytest.raises(ValueError, match="owned-work-failed"):
            await settlement.stop()
        before = list(events)
        with pytest.raises(ownership.OwnedGatewayUnknown, match="stop"):
            await settlement.stop()
        assert events == before
        assert original == (settlement.control, settlement.listener, settlement.authorities)
        assert not settlement.finished
        assert settlement.source is source

    asyncio.run(scenario())


def test_a_completed_finish_is_not_replayed(source, monkeypatch):
    async def scenario():
        settlement = bound_settlement(source)
        settlement.finished = True
        calls = []

        async def forbidden(*args):
            calls.append(args)
            raise AssertionError("finished lifecycle was entered again")

        monkeypatch.setattr(resources, "close_remaining_python_resources", forbidden)
        with pytest.raises(ownership.OwnedGatewayUnknown, match="finish|finished|stop"):
            await settlement.finish()
        assert calls == []

    asyncio.run(scenario())


def test_foreign_async_task_cannot_begin_settlement(source):
    async def scenario():
        settlement = bound_settlement(source)
        original = settlement.runner

        async def foreign():
            with pytest.raises(ownership.OwnedGatewayUnknown, match="task"):
                await settlement.stop()

        await asyncio.create_task(foreign())
        assert settlement.runner is original
        assert source.phase == "unknown"

    asyncio.run(scenario())


def test_join_task_does_not_accept_a_non_future():
    async def scenario():
        with pytest.raises(ownership.OwnedGatewayUnknown, match="work-object"):
            await shutdown.join_task(object())

    asyncio.run(scenario())


def test_cancelled_work_cannot_be_a_normal_join_receipt():
    async def scenario():
        future = asyncio.get_running_loop().create_future()
        future.cancel()
        with pytest.raises(asyncio.CancelledError):
            await shutdown.join_task(future)

    asyncio.run(scenario())


def test_source_owned_thread_finishes_naturally():
    finished = threading.Event()
    thread = threading.Thread(target=finished.set)
    thread.start()
    asyncio.run(shutdown.join_thread(thread))
    assert finished.is_set()
    assert not thread.is_alive()


def test_supervised_cron_join_waits_for_its_real_worker_without_blocking_the_loop():
    from cron.scheduler_thread import SupervisedTickerThread

    async def scenario():
        stop = threading.Event()
        entered = threading.Event()
        finished = threading.Event()

        def tick():
            entered.set()
            stop.wait()
            finished.set()

        ticker = SupervisedTickerThread(tick, stop_event=stop)
        ticker.start()
        try:
            await asyncio.to_thread(entered.wait)
            joining = asyncio.create_task(shutdown.join_thread(ticker))
            await asyncio.sleep(0)
            assert not joining.done()
            stop.set()
            await asyncio.wait_for(joining, 5)
            assert finished.is_set()
            assert not ticker.is_alive()
            assert ticker.restart_if_dead() is False
        finally:
            stop.set()
            ticker.join(5)

    asyncio.run(scenario())


def test_join_thread_still_refuses_unrecognized_thread_shaped_objects():
    fake = SimpleNamespace(ident=1, join=lambda: None, is_alive=lambda: False)
    with pytest.raises(ownership.OwnedGatewayUnknown, match="writer-thread"):
        asyncio.run(shutdown.join_thread(fake))
