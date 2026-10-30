"""Canonical normal consumers for original cron Futures, workers and executors."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from cron import scheduler_owned as owned
from cron.scheduler_owned_pools import require_pool
from cron.scheduler_owned_work import require_work, require_registration


@dataclass(frozen=True)
class Snapshot:
    source: object
    book: object
    links: tuple


def capture(source):
    return Snapshot(source, source.cron_records,
                    tuple((r, r.obj, r.parent, r.children, r.methods) for r in owned.members(source)))


def require_snapshot(snapshot):
    values = owned.members(snapshot.source)
    if (snapshot.source.cron_records is not snapshot.book or len(values) != len(snapshot.links)
            or any(r is not link[0] for r, link in zip(values, snapshot.links))):
        raise snapshot.source.unknown("cron-final-original-book-changed")
    for record, obj, parent, children, methods in snapshot.links:
        if (record.obj is not obj or record.parent is not parent or record.children is not children
                or record.methods is not methods):
            owned.refuse(record, "cron-final-original-links-changed")


async def _wait_original(waiter):
    record = waiter.parent
    try:
        require_work(record, pending=False)
        waiter.loop, waiter.task = asyncio.get_running_loop(), asyncio.current_task()
        waiter.forward = asyncio.wrap_future(record.future.obj)
        waiter.references += (waiter.loop, waiter.task, waiter.forward, record.future.obj)
        waiter.result = await asyncio.shield(waiter.forward)
        require_work(record, pending=False)
        owned.require_methods(record.future)
        if not record.future.obj.done() or record.future.obj.cancelled():
            owned.refuse(record, "cron-original-future-not-normally-complete")
        waiter.actual_result = record.future.methods[0][1]()
        require_work(record, pending=False)
        if not record.worker_finished or not record.released:
            owned.refuse(record, "cron-original-worker-or-release-still-unsettled")
        waiter.done, waiter.active = True, False
        record.wait_done = record.done = True
        record.active = False
    except BaseException as error:
        owned.fail(waiter, error)
        raise


async def wait_work(record):
    if not record.registered:
        if not record.done or record.factory_attempted:
            owned.refuse(record, "cron-unregistered-original-work-unsettled")
        return
    if record.wait_attempted:
        if not record.wait_done:
            owned.refuse(record, "cron-original-work-wait-attempt-retained")
        record.context.copy().run(lambda: require_work(record, pending=False))
        return
    if not record.dispatch_returned or not record.submit_returned or record.future is None:
        owned.refuse(record, "cron-original-submission-publication-incomplete")
    record.wait_attempted = True
    def start():
        require_work(record, pending=False)
        waiter = owned.reserve("waiter", asyncio.create_task, record, continuation=True)
        waiter.coro = _wait_original(waiter)
        waiter.references = (waiter.coro, record.future, record.future.obj)
        waiter.attempted = waiter.active = True
        owned.bind(waiter, asyncio.create_task(waiter.coro))
        owned.remember_methods(waiter, ("done", "cancelled", "exception", "get_loop"))
        record.waiter = waiter
        return waiter
    waiter = record.context.copy().run(start)
    try:
        await asyncio.shield(waiter.obj)
        record.context.copy().run(lambda: require_work(record, pending=False))
    except BaseException as error:
        owned.fail(record, error)
        raise


def retained_pools(source):
    return tuple(record.obj for record in owned.members(source) if record.kind == "pool")


def _require_business_members(source):
    from cron import scheduler
    with scheduler._running_lock:
        current = tuple(scheduler._running_futures.values())
    source.cron_business_refs = current
    for future in current:
        record = owned.find(future)
        if record is None or record.source is not source or record.kind != "future":
            raise source.unknown("cron-current-work-without-original-future-owner")
    for pool in tuple(scheduler._parallel_pools.values()):
        record = owned.find(pool)
        if record is None or record.source is not source or record.kind != "pool":
            source.cron_business_refs += (pool,)
            raise source.unknown("cron-current-pool-without-original-constructor")


async def join_work(settlement):
    source = settlement.source
    source.require_shutdown(settlement.runner)
    if source.cron_closing:
        if not source.cron_work_joined:
            raise source.unknown("cron-original-work-join-attempt-retained")
        return
    source.cron_closing = True
    try:
        initial = owned.members(source)
        _require_business_members(source)
        for record in initial:
            if record.kind == "work":
                await wait_work(record)
        _require_business_members(source)
        owned.members(source)
        source.cron_work_joined = True
    except BaseException as error:
        owned.fail_source(source, error)
        raise


def shutdown_pool(record):
    require_pool(record)
    source = record.source
    if not source.cron_closing or not source.cron_work_joined:
        owned.refuse(record, "cron-original-work-not-joined-before-executor")
    if record.join_attempted:
        if not record.joined:
            owned.refuse(record, "cron-original-executor-join-attempt-retained")
        require_joined_pool(record)
        return record.join_result
    record.join_attempted = True
    try:
        owned.members(source)
        for work in record.works:
            require_work(work, pending=False)
            if not work.done or work.active:
                owned.refuse(work, "cron-original-work-active-before-pool-close")
        record.join_result = record.normal_shutdown(wait=True, cancel_futures=False)
        require_pool(record)
        for work in record.works:
            require_work(work, pending=False)
        for thread in record.threads:
            owned.require_methods(thread)
            if thread.methods[2][1]():
                owned.refuse(thread, "cron-original-worker-still-alive-after-normal-join")
            owned.require_methods(thread)
            thread.closed = True
        record.joined = record.closed = True
        require_joined_pool(record)
        return record.join_result
    except BaseException as error:
        owned.fail(record, error)
        raise


def require_joined_pool(record):
    require_pool(record)
    if not record.joined or not record.closed or record.obj._shutdown is not True:
        owned.refuse(record, "cron-original-executor-normal-join-incomplete")
    for thread in record.threads:
        owned.require_methods(thread)
        if not thread.published or not thread.closed or thread.obj.is_alive():
            owned.refuse(thread, "cron-original-worker-join-unconfirmed")


async def join_pools(settlement):
    source = settlement.source
    source.require_shutdown(settlement.runner)
    if source.cron_closed:
        require_closed(source)
        return
    if source.cron_pool_join_attempted or not source.cron_work_joined:
        raise source.unknown("cron-original-pool-join-attempt-or-work-unsettled")
    source.cron_pool_join_attempted = True
    try:
        _require_business_members(source)
        for record in owned.members(source):
            if record.kind == "pool":
                await asyncio.to_thread(record.context.copy().run, shutdown_pool, record)
        source.cron_snapshot = capture(source)
        require_final(source.cron_snapshot)
        source.cron_closed = True
    except BaseException as error:
        owned.fail_source(source, error)
        raise


def _final_work(record):
    require_work(record, pending=False)
    if record.registered:
        require_registration(record)


def _final_thread(record):
    owned.require_methods(record)
    if not record.closed or record.obj.is_alive():
        owned.refuse(record, "cron-final-worker-still-live")


def _final_future(record):
    owned.require_methods(record)
    if not record.obj.done() or record.obj.cancelled() or record.obj.exception() is not None:
        owned.refuse(record, "cron-final-original-future-unsettled")


def _final_waiter(record):
    _final_future(record)
    if (record.task is not record.obj or record.loop is not record.source.loop
            or record.obj.get_loop() is not record.loop):
        owned.refuse(record, "cron-final-original-waiter-unsettled")


def _final_item(record):
    if record.obj.future is not record.parent.future.obj:
        owned.refuse(record, "cron-final-original-work-item-future-changed")


_FINAL = {"pool": require_joined_pool, "work": _final_work, "thread": _final_thread,
          "future": _final_future, "waiter": _final_waiter, "work-item": _final_item}


def require_final(snapshot):
    _require_business_members(snapshot.source)
    require_snapshot(snapshot)
    for record, *_ in snapshot.links:
        def check():
            record.require()
            if handler := _FINAL.get(record.kind):
                handler(record)
            if not record.done or record.active:
                owned.refuse(record, "cron-final-original-resource-unsettled")
        record.context.copy().run(check)


def require_closed(source):
    if not source.cron_closed or source.cron_snapshot is None:
        raise source.unknown("cron-original-final-settlement-unavailable")
    require_final(source.cron_snapshot)
