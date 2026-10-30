"""Actual scheduler/settlement consumers; deny lower external factories before selected imports."""
import atexit
import concurrent.futures
import socket
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest_asyncio

from tests.cron import owned_fakes as fakes


def guard_external(monkeypatch):
    # No scheduler constructor/worker/store can run before these guards are installed.
    original_executor = concurrent.futures.ThreadPoolExecutor
    monkeypatch.setattr(concurrent.futures, "ThreadPoolExecutor", fakes.Pool)
    monkeypatch.setattr(atexit, "register", lambda callback, *args, **kwargs: callback)
    monkeypatch.setattr(subprocess, "Popen", fakes.deny)
    monkeypatch.setattr(subprocess, "run", fakes.deny)
    monkeypatch.setattr(socket, "create_connection", fakes.deny)
    monkeypatch.setattr(socket, "getaddrinfo", fakes.deny)
    monkeypatch.setattr(socket.socket, "connect", fakes.deny)
    import httpx
    import requests
    import sqlite3
    monkeypatch.setattr(httpx.Client, "send", fakes.deny)
    monkeypatch.setattr(httpx.AsyncClient, "send", fakes.deny)
    monkeypatch.setattr(requests, "get", fakes.deny)
    monkeypatch.setattr(requests, "post", fakes.deny)
    monkeypatch.setattr(sqlite3, "connect", fakes.deny)
    return original_executor


@pytest_asyncio.fixture
async def cron_fixture(monkeypatch, tmp_path):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "agent-home"))
    original_executor = guard_external(monkeypatch)
    from cron import scheduler
    from cron import scheduler_owned as owned
    from cron import scheduler_owned_pools as pools
    from cron import scheduler_owned_executor as executor
    from cron import scheduler_owned_work as work
    # Scope only actual cron constructors. Ordinary pytest driver/executor machinery
    # may use its own stdlib executor; it never executes cron products or jobs.
    monkeypatch.setattr(scheduler, "concurrent", SimpleNamespace(futures=SimpleNamespace(
        ThreadPoolExecutor=fakes.Pool, Future=concurrent.futures.Future)))
    monkeypatch.setattr(concurrent.futures, "ThreadPoolExecutor", original_executor)
    for name, value in (("_records", []), ("_books", {}), ("_origins", {}), ("_index", {}), ("_methods", {})):
        monkeypatch.setattr(owned, name, value)
    monkeypatch.setattr(pools, "_links", {})
    monkeypatch.setattr(executor, "_backend", {})
    monkeypatch.setattr(executor, "_thread", fakes.Thread)
    monkeypatch.setattr(executor, "_executor_type", fakes.Pool)
    monkeypatch.setattr(executor, "_current_thread", lambda: fakes.Thread.current)
    for name in ("_entries", "_registrations", "_prepared", "_callbacks", "_futures"):
        monkeypatch.setattr(work, name, {})
    for name in ("_parallel_pools", "_parallel_pool_max_workers", "_running_since", "_running_futures",
                 "_running_registration_owners", "_running_worker_pids", "_inflight_home_paths", "_running_fire_owners"):
        monkeypatch.setattr(scheduler, name, {})
    for name in ("_running_job_ids", "_scope_isolated_job_ids", "_interrupted_job_ids"):
        monkeypatch.setattr(scheduler, name, set())
    monkeypatch.setattr(scheduler, "_interpreter_shutting_down", lambda *args: False)
    monkeypatch.setattr(scheduler, "create_execution", lambda job_id, **kwargs: {"id": "fixture-" + job_id})
    monkeypatch.setattr(scheduler, "note_cron_execution", lambda *args: None)
    for name in ("finish_execution", "settle_unstarted_execution", "clear_run_claim"):
        monkeypatch.setattr(scheduler, name, fakes.deny)
    for cls in (fakes.Pool, fakes.Thread):
        monkeypatch.setattr(cls, "instances", [])
        for name in ("on_init", "on_start", "on_join", "on_shutdown"):
            if hasattr(cls, name):
                monkeypatch.setattr(cls, name, None)
    monkeypatch.setattr(fakes.Pool, "immediate", True)
    monkeypatch.setattr(fakes.Thread, "current", None)
    import gateway.session_authorities as authorities
    import gateway.platforms.api_server_runs as api
    monkeypatch.setattr(authorities, "all_authorities", lambda runner: ())
    monkeypatch.setattr(api, "api_worker_live_count", lambda: 0)
    fixture = SimpleNamespace(source=None, runner=None, settlement=None, scheduler=scheduler, owned=owned,
                              tmp=tmp_path, calls=[], pools=fakes.Pool.instances, threads=fakes.Thread.instances)

    def activate():
        if fixture.source is None:
            from tests.agent.auxiliary_owned_fixture import bind_source
            from gateway.run_owned_shutdown import ManagedSettlement
            fixture.source, fixture.runner = bind_source(monkeypatch, tmp_path)
            fixture.runner._running_agents = {}
            fixture.runner._executor = fixture.runner._housekeeping_executor = None
            import threading
            fixture.runner._executor_lock = threading.RLock()
            fixture.settlement = ManagedSettlement(fixture.source, fixture.runner)
            fixture.source.settlement = fixture.settlement
        return fixture.source

    fixture.activate = activate
    yield fixture
    # Only in-memory observer tasks are cleaned up as fixture machinery. This is
    # not a normal product-close receipt and never closes/forgets owned resources.
    import asyncio
    tasks = [r.obj for r in getattr(fixture.source, "cron_records", ())
             if getattr(r, "kind", None) == "waiter" and isinstance(r.obj, asyncio.Task)]
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def submit(fixture, job_id="daily", pool=None, process=None):
    fixture.activate()
    pool = pool if pool is not None else fixture.scheduler._get_parallel_pool(2)
    job = {"id": job_id, "schedule": {"kind": "interval"}}
    process = process if process is not None else lambda job: fixture.calls.append(job["id"]) or "done"
    return fixture.scheduler._submit_with_guard(job, pool, process)


async def settle(fixture):
    from tests.agent.auxiliary_owned_fixture import allow_normal_shutdown
    allow_normal_shutdown(fixture.source)
    await fixture.settlement._join_work()
    await fixture.settlement._join_pools()
