"""Unowned/foreign boundaries and ordinary unmanaged scheduler controls."""
import pytest

from tests.cron.owned_fixture import submit, settle

pytest_plugins = ("tests.cron.owned_fixture",)
pytestmark = pytest.mark.asyncio


async def test_pre_source_cached_executor_is_retained_and_never_adopted(cron_fixture):
    from tests.cron.owned_fakes import Pool
    from hermes_constants import hermes_home_key
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    old = Pool(2)
    key = hermes_home_key()
    f.scheduler._parallel_pools[key] = old
    f.scheduler._parallel_pool_max_workers[key] = 2
    with pytest.raises(OwnedGatewayUnknown):
        f.scheduler._get_parallel_pool(2)
    assert f.scheduler._parallel_pools[key] is old and not old.shut_calls
    assert f.source.cron_records[0].references == (old,)


async def test_unmapped_business_future_is_not_assumed_settled(cron_fixture):
    import concurrent.futures
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    future = concurrent.futures.Future()
    f.scheduler._running_futures["foreign"] = future
    with pytest.raises(OwnedGatewayUnknown):
        await settle(f)
    assert f.source.cron_business_refs == (future,) and not future.done()


async def test_external_process_is_refused_before_handoff_or_launch(cron_fixture):
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    job = {"id": "fixture", "execution_id": "fixture-execution"}
    with pytest.raises(OwnedGatewayUnknown):
        f.scheduler._launch_external_cron_worker(job)
    assert not f.pools and f.source.cron_records[0].result is job


async def test_external_provider_is_refused_before_loading_or_fallback(cron_fixture, monkeypatch):
    from hermes_cli import config
    from cron.scheduler_provider import resolve_cron_scheduler
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    monkeypatch.setattr(config, "load_config", lambda: {"cron": {"provider": "fixture-external"}})
    with pytest.raises(OwnedGatewayUnknown):
        resolve_cron_scheduler()
    assert f.source.cron_records[0].result == "fixture-external" and not f.pools


async def test_script_process_is_refused_before_path_resolution_or_command(cron_fixture):
    from cron.scheduler_script import _run_job_script
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    with pytest.raises(OwnedGatewayUnknown):
        _run_job_script("fixture.py")
    assert not f.pools and f.source.cron_records[0].kind == "unsupported"


async def test_original_pool_does_not_accept_an_unregistered_callable(cron_fixture):
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    pool = f.scheduler._get_parallel_pool(2)
    with pytest.raises(OwnedGatewayUnknown):
        pool.submit(lambda: None)
    assert not f.threads and not pool._work_queue.items


async def test_unmanaged_pool_cache_replacement_and_discard_keep_original_defaults(cron_fixture):
    f = cron_fixture
    first = f.scheduler._get_parallel_pool(1)
    assert f.scheduler._get_parallel_pool(1) is first
    second = f.scheduler._get_parallel_pool(3)
    assert first.shut_calls == [(False, False)] and second is not first
    f.scheduler.discard_parallel_pools(list(f.scheduler._parallel_pools))
    assert second.shut_calls == [(False, False)] and not f.scheduler._parallel_pools
    assert not f.owned._records and f.source is None


async def test_unmanaged_submit_and_release_use_original_dedupe_and_callback(cron_fixture):
    f = cron_fixture
    pool = f.scheduler._get_parallel_pool(2)
    job = {"id": "fixture", "schedule": {"kind": "interval"}}
    ran = []
    future = f.scheduler._submit_with_guard(job, pool, lambda job: ran.append(job["id"]))
    assert f.scheduler._running_futures[f.scheduler._inflight_key("fixture")] is future
    callback, args, kwargs, original = pool.vanilla_calls[0]
    original.set_result(callback(*args, **kwargs))
    assert ran == ["fixture"] and not f.scheduler._running_futures and not f.owned._records


async def test_native_lifecycle_gate_remains_unconditional(cron_fixture):
    from hermes_owned_gateway import require_lifecycle_available, OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    with pytest.raises(OwnedGatewayUnknown):
        require_lifecycle_available(f.source)
    assert not f.pools and not f.threads


async def test_managed_orphan_reaper_is_refused_before_legacy_process_sweeping(cron_fixture):
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    with pytest.raises(OwnedGatewayUnknown):
        f.scheduler._sweep_mcp_orphans()
    assert f.source.cron_records[0].kind == "unsupported" and not f.pools


async def test_unmanaged_orphan_reaper_keeps_original_callback(cron_fixture, monkeypatch):
    import sys
    from types import SimpleNamespace
    f = cron_fixture
    calls = []
    monkeypatch.setitem(sys.modules, "tools.mcp_tool_lifecycle", SimpleNamespace(
        _kill_orphaned_mcp_children=lambda: calls.append("legacy")))
    f.scheduler._sweep_mcp_orphans()
    assert calls == ["legacy"] and not f.owned._records


async def test_business_future_added_after_work_join_refuses_before_executor_shutdown(cron_fixture):
    import concurrent.futures
    from tests.agent.auxiliary_owned_fixture import allow_normal_shutdown
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    submit(f)
    allow_normal_shutdown(f.source)
    await f.settlement._join_work()
    future = concurrent.futures.Future()
    f.scheduler._running_futures["foreign-after-join"] = future
    with pytest.raises(OwnedGatewayUnknown):
        await f.settlement._join_pools()
    assert not f.pools[0].shut_calls and f.source.cron_business_refs == (future,)
