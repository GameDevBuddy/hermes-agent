"""Original cron partial construction, first diagnostic and no-retry consumers."""
import concurrent.futures

import pytest

from tests.cron.owned_fixture import submit, settle

pytest_plugins = ("tests.cron.owned_fixture",)
pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("stage", ["pool", "thread"])
async def test_partial_original_constructor_is_retained_before_initializer_failure(cron_fixture, monkeypatch, stage):
    from tests.cron.owned_fakes import Pool, Thread
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    failure = ValueError("synthetic partial " + stage)
    seen = []
    def fail_init(obj):
        record = f.owned.find(obj)
        assert record is not None and record.obj is obj and record.attempted
        seen.append(record)
        raise failure
    monkeypatch.setattr(Pool if stage == "pool" else Thread, "on_init", fail_init)
    with pytest.raises(ValueError) as caught:
        submit(f)
    assert caught.value is failure and f.source.cron_error is failure and len(seen) == 1
    assert seen[0].obj is not None and seen[0] in f.source.cron_records and f.source.cron_credit > 0
    count = len(f.source.cron_records)
    with pytest.raises(OwnedGatewayUnknown):
        submit(f)
    assert len(f.source.cron_records) == count and f.source.cron_error is failure


async def test_failed_queue_submission_retains_actual_future_and_possibly_queued_work(cron_fixture):
    f = cron_fixture
    f.activate()
    pool = f.scheduler._get_parallel_pool(2)
    failure = OSError("synthetic failure after enqueue")
    def fail_put(item):
        raise failure
    pool._work_queue.on_put = fail_put
    with pytest.raises(OSError) as caught:
        submit(f, pool=pool)
    work = next(r for r in f.source.cron_records if r.kind == "work")
    assert caught.value is failure and f.source.cron_error is failure
    assert work.future.obj is pool._work_queue.items[0].future and work.queue_attempted and not work.queued
    assert f.scheduler._running_futures and not f.calls
    assert not pool.shut_calls


@pytest.mark.parametrize("kind", ["failure", "cancelled"])
async def test_failed_or_cancelled_original_future_remains_owned_and_refusing(cron_fixture, monkeypatch, kind):
    from tests.cron.owned_fakes import Pool
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    monkeypatch.setattr(Pool, "immediate", False)
    future = submit(f)
    failure = OSError("synthetic future failure")
    if kind == "failure":
        future.set_exception(failure)
        assert f.source.cron_error is failure
    else:
        assert future.cancel()  # in-memory fixture Future only, never a product cancellation
        assert isinstance(f.source.cron_error, concurrent.futures.CancelledError)
    with pytest.raises(OwnedGatewayUnknown):
        await settle(f)
    assert f.owned.find(future).obj is future and not f.pools[0].shut_calls


async def test_failed_original_worker_releases_business_state_but_not_resource_owner(cron_fixture):
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    failure = ValueError("synthetic job callable failure")
    def fail_job(job):
        raise failure
    with pytest.raises(OwnedGatewayUnknown):
        submit(f, process=fail_job)
    assert f.source.cron_error is failure
    work = next(r for r in f.source.cron_records if r.kind == "work")
    assert work.future.obj is not None and not work.done and not work.worker_finished
    assert not f.pools[0].shut_calls


@pytest.mark.parametrize("field", ["job", "runtime", "profile", "pool", "future", "members",
                                      "source", "creator", "registration", "callback", "context",
                                      "bootstrap-task", "bootstrap-loop"])
async def test_actual_original_associations_are_rechecked_after_shutdown_io(cron_fixture, monkeypatch, field):
    from tests.cron.owned_fakes import Pool
    from hermes_constants import set_hermes_home_override
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    submit(f)
    work = next(r for r in f.source.cron_records if r.kind == "work")
    def change(pool, wait):
        import hermes_owned_gateway as ownership
        from types import SimpleNamespace
        def replace_pair(runtime_field, source_field):
            other = object()
            setattr(f.runner._gdb_original_source_runtime, runtime_field, other)
            setattr(f.source, source_field, other)
        actions = {
            "job": lambda: setattr(f.source, "job", object()),
            "runtime": lambda: setattr(f.runner, "_gdb_original_source_runtime", object()),
            "profile": lambda: set_hermes_home_override(str(f.tmp / "foreign")),
            "pool": lambda: setattr(pool, "_work_queue", object()),
            "future": lambda: setattr(work, "future", object()),
            "members": lambda: pool._threads.clear(),
            "source": lambda: monkeypatch.setattr(ownership, "_CURRENT", SimpleNamespace()),
            "creator": lambda: setattr(work, "creator_thread", object()),
            "registration": lambda: setattr(work, "registration_owner", object()),
            "callback": lambda: setattr(work, "worker_callback", object()),
            "context": lambda: setattr(work, "dispatch_context", object()),
            "bootstrap-task": lambda: replace_pair("bootstrap_task", "task"),
            "bootstrap-loop": lambda: replace_pair("loop", "loop"),
        }
        actions[field]()
    monkeypatch.setattr(Pool, "on_shutdown", change)
    with pytest.raises(OwnedGatewayUnknown):
        await settle(f)
    assert f.source.cron_error is not None and not f.source.cron_closed
    assert f.pools[0].shut_calls == [(True, False)]


async def test_failed_normal_executor_join_is_sticky_without_second_shutdown(cron_fixture, monkeypatch):
    from tests.cron.owned_fakes import Thread
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    submit(f)
    failure = OSError("synthetic normal thread join failure")
    def fail_join(thread):
        raise failure
    monkeypatch.setattr(Thread, "on_join", fail_join)
    with pytest.raises(OSError) as caught:
        await settle(f)
    assert caught.value is failure and f.source.cron_error is failure
    with pytest.raises(OwnedGatewayUnknown):
        await settle(f)
    assert f.pools[0].shut_calls == [(True, False)] and f.threads[0].joins == [None]


async def test_unawaited_async_work_is_retained_instead_of_reported_as_sync_completion(cron_fixture):
    import asyncio
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    pending = asyncio.get_running_loop().create_future()
    with pytest.raises(OwnedGatewayUnknown):
        submit(f, process=lambda job: pending)
    work = next(r for r in f.source.cron_records if r.kind == "work")
    assert work.worker_result is pending and not pending.done() and not work.worker_finished
    assert f.source.cron_error is not None and not f.pools[0].shut_calls


async def test_changed_worker_join_method_is_refused_before_executor_io(cron_fixture):
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    submit(f)
    original = f.owned.find(f.threads[0]).methods[1][1]
    f.threads[0].join = lambda *args: None
    with pytest.raises(OwnedGatewayUnknown):
        await settle(f)
    assert not f.pools[0].shut_calls and not f.threads[0].joins
    assert original.__self__ is f.threads[0]
