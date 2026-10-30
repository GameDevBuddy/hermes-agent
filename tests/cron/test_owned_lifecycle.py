"""Actual scheduler producer and canonical settlement APIs, lower owners mocked only."""
import asyncio

import pytest

from tests.cron.owned_fixture import submit, settle

pytest_plugins = ("tests.cron.owned_fixture",)
pytestmark = pytest.mark.asyncio


async def test_original_work_survives_completion_and_release_before_future_publication(cron_fixture):
    f = cron_fixture
    f.activate()
    observations = []

    def process(job):
        work = next(r for r in f.source.cron_records if r.kind == "work")
        key = f.scheduler._inflight_key(job["id"])
        observations.append((work.submit_returned, work.future.obj, f.scheduler._running_futures[key]))
        return "done"

    future = submit(f, process=process)
    work = next(r for r in f.source.cron_records if r.kind == "work")
    assert observations == [(False, future, f.scheduler._FUTURE_PENDING)]
    assert work.released and work.worker_finished and work.future.obj is future
    assert not f.scheduler._running_futures and not f.scheduler._running_job_ids
    await settle(f)
    assert f.source.cron_closed and work.wait_done and work.done
    assert f.pools[0].shut_calls == [(True, False)] and f.threads[0].joins == [None]
    assert f.owned.find(future) in [link[0] for link in f.source.cron_snapshot.links]


async def test_replaced_original_executor_remains_owned_until_same_normal_shutdown(cron_fixture):
    f = cron_fixture
    f.activate()
    old = f.scheduler._get_parallel_pool(1)
    submit(f, "old", old)
    new = f.scheduler._get_parallel_pool(4)
    submit(f, "new", new)
    assert old is not new and f.owned.find(old).retired
    assert old.shut_calls == [(False, False)] and f.scheduler._get_parallel_pool(4) is new
    await settle(f)
    assert old.shut_calls == [(False, False), (True, False)] and new.shut_calls == [(True, False)]
    assert f.owned.find(old).joined and f.owned.find(new).joined


async def test_discarded_original_pool_is_joined_after_cache_and_business_entries_are_gone(cron_fixture):
    f = cron_fixture
    f.activate()
    future = submit(f)
    pool = f.pools[0]
    key = f.owned.find(pool).home_key
    f.scheduler.discard_parallel_pools([key])
    assert not f.scheduler._parallel_pools and not f.scheduler._running_futures
    assert f.owned.find(future).parent.future.obj is future and pool.shut_calls == [(False, False)]
    await settle(f)
    assert f.owned.find(pool).joined and pool.shut_calls == [(False, False), (True, False)]


async def test_distinct_profile_pools_and_same_job_ids_keep_their_original_contexts(cron_fixture):
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    f = cron_fixture
    f.activate()
    seen = []
    for name in ("alpha", "beta"):
        token = set_hermes_home_override(str(f.tmp / name))
        try:
            submit(f, "same", process=lambda job: seen.append(str(f.scheduler._get_hermes_home())))
        finally:
            reset_hermes_home_override(token)
    assert seen == [str(f.tmp / "alpha"), str(f.tmp / "beta")]
    assert len(f.scheduler._parallel_pools) == 2
    await settle(f)
    assert f.source.cron_closed and all(f.owned.find(pool).joined for pool in f.pools)


async def test_pending_original_future_is_awaited_before_normal_executor_join(cron_fixture, monkeypatch):
    from tests.cron.owned_fakes import Pool
    f = cron_fixture
    f.activate()
    monkeypatch.setattr(Pool, "immediate", False)
    future = submit(f)
    assert not future.done() and not f.calls
    observations = []
    def finish_worker():
        observations.append((f.source.cron_closing, list(f.pools[0].shut_calls)))
        f.threads[0].run_one()
    asyncio.get_running_loop().call_soon(finish_worker)
    await settle(f)
    assert observations == [(True, [])] and future.result() == "done"
    assert f.calls == ["daily"] and f.source.cron_closed


async def test_atexit_uses_verified_originals_and_never_repeats_normal_close(cron_fixture):
    f = cron_fixture
    submit(f)
    await settle(f)
    original = f.pools[0]
    before = list(original.shut_calls)
    f.scheduler._shutdown_parallel_pool()
    assert original.shut_calls == before and f.source.cron_closed


async def test_after_final_snapshot_actual_original_members_are_checked_again(cron_fixture):
    from cron.scheduler_owned_close import require_closed
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    submit(f)
    await settle(f)
    work = next(r for r in f.source.cron_records if r.kind == "work")
    work.children = ()
    with pytest.raises(OwnedGatewayUnknown):
        require_closed(f.source)


async def test_duplicate_registration_does_not_construct_or_submit_another_future(cron_fixture, monkeypatch):
    from tests.cron.owned_fakes import Pool
    f = cron_fixture
    f.activate()
    monkeypatch.setattr(Pool, "immediate", False)
    future = submit(f)
    assert submit(f) is None
    assert len([r for r in f.source.cron_records if r.kind == "future"]) == 1
    f.threads[0].run_one()
    await settle(f)
    assert future.result() == "done" and f.source.cron_closed
