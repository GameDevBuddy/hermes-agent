"""Actual per-profile executor construction/cache consumers and their original owners."""
from __future__ import annotations

from cron import scheduler_owned as owned

_links = {}


def require_pool(record, *, pending=False):
    record.require()
    obj, cls = record.obj, record.factory
    current = (obj, cls, record.normal_submit, record.normal_shutdown, record.normal_adjust,
               obj._threads, obj._work_queue, obj._shutdown_lock, obj._idle_semaphore,
               obj._max_workers, obj._thread_name_prefix, record.config)
    if (any(a is not b for a, b in zip(current, _links[record]))
            or obj.submit is not record.submit_hook or obj.shutdown is not record.shutdown_hook
            or obj._adjust_thread_count is not record.adjust_hook
            or not owned._same_callable(cls.submit.__get__(obj, cls), record.normal_submit)
            or not owned._same_callable(cls.shutdown.__get__(obj, cls), record.normal_shutdown)):
        owned.refuse(record, "cron-original-pool-method-or-members-changed")
    require_threads(record, pending)


def require_threads(record, pending):
    if (tuple(child for child in record.children if child.kind == "thread") != record.threads
            or tuple(child for child in record.children if child.kind == "work") != record.works):
        owned.refuse(record, "cron-original-pool-child-domain-changed")
    threads = record.obj._threads
    expected = tuple(thread.obj for thread in record.threads if thread.published)
    if len(threads) != len(expected) or any(thread not in expected for thread in threads):
        record.references += (tuple(threads),)
        owned.refuse(record, "cron-original-pool-worker-membership-changed")
    for thread in record.threads:
        thread.require()
        if thread.methods:
            owned.require_methods(thread)
        if thread.parent is not record or owned.find(thread.obj) is not thread:
            owned.refuse(thread, "cron-original-thread-parent-changed")
        if not thread.published and not pending:
            owned.refuse(thread, "cron-original-thread-publication-incomplete")


def install(record):
    from cron import scheduler_owned_executor as executor
    obj, cls = record.obj, record.factory
    record.normal_submit, record.normal_shutdown, record.normal_adjust = obj.submit, obj.shutdown, obj._adjust_thread_count
    record.threads, record.works = (), ()
    record.stop_attempted = record.stopped = record.join_attempted = record.joined = False
    record.retired = False
    record.submit_hook = lambda fn, *args, **kwargs: executor.submit(record, fn, args, kwargs)
    record.adjust_hook = lambda: executor.adjust(record)
    record.shutdown_hook = lambda wait=True, cancel_futures=False: shutdown(record, wait, cancel_futures)
    _links[record] = (obj, cls, record.normal_submit, record.normal_shutdown, record.normal_adjust,
                      obj._threads, obj._work_queue, obj._shutdown_lock, obj._idle_semaphore,
                      obj._max_workers, obj._thread_name_prefix, record.config)
    executor.pin_backend(record)
    obj.submit, obj.shutdown, obj._adjust_thread_count = record.submit_hook, record.shutdown_hook, record.adjust_hook
    require_pool(record)


def shutdown(record, wait, cancel_futures):
    require_pool(record)
    if cancel_futures or type(wait) is not bool:
        owned.refuse(record, "cron-cancelling-or-opaque-shutdown-unavailable")
    if wait:
        from cron.scheduler_owned_close import shutdown_pool
        return shutdown_pool(record)
    if record.stop_attempted:
        if not record.stopped:
            owned.refuse(record, "cron-original-stop-attempt-retained")
        return None
    record.stop_attempted = True
    try:
        record.stop_result = record.normal_shutdown(wait=False, cancel_futures=False)
        require_pool(record)
        if record.obj._shutdown is not True:
            owned.refuse(record, "cron-original-pool-admission-not-stopped")
        record.stopped = True
        owned.members(record.source)
        return record.stop_result
    except BaseException as error:
        owned.fail(record, error)
        raise


def get_pool(max_workers):
    from cron import scheduler
    from hermes_constants import hermes_home_key
    if owned.active() is None and not owned._records:
        return False, None
    home = scheduler._remember_inflight_home(scheduler._get_hermes_home())
    key = hermes_home_key(home)
    previous = scheduler._parallel_pools.get(key)
    record = owned.protect(previous, "pool") if previous is not None else None
    if owned.active() is None and record is None:
        return False, None
    if record is not None:
        require_pool(record)
        if record.home_key != key or scheduler._parallel_pool_max_workers.get(key) != record.config:
            owned.refuse(record, "cron-original-cached-pool-or-profile-changed")
        if record.config == max_workers and not record.retired:
            return True, previous
        record.retired = True
        shutdown(record, False, False)
    cls = scheduler.concurrent.futures.ThreadPoolExecutor
    from cron.scheduler_owned_executor import _executor_type
    if cls is not _executor_type:
        owned.unsupported("opaque-executor-constructor", cls)
    created = owned.reserve("pool", cls)
    try:
        created.config = max_workers
        pool = owned.construct(created, cls, (), {"max_workers": max_workers, "thread_name_prefix": "cron-parallel"})
        install(created)
        scheduler._parallel_pools[key] = pool
        scheduler._parallel_pool_max_workers[key] = max_workers
        require_pool(created)
        owned.members(created.source)
        return True, pool
    except BaseException as error:
        owned.fail(created, error)
        raise


def discard(home_keys):
    from cron import scheduler
    if owned.active() is None:
        return False
    for key in tuple(home_keys):
        pool = scheduler._parallel_pools.get(key)
        if pool is None:
            continue
        record = owned.protect(pool, "pool")
        def retire():
            require_pool(record)
            if record.home_key != key:
                owned.refuse(record, "cron-discard-original-profile-changed")
            record.retired = True
            shutdown(record, False, False)
            if scheduler._parallel_pools.get(key) is not pool:
                owned.refuse(record, "cron-discard-original-cache-entry-changed")
            scheduler._parallel_pools.pop(key)
            scheduler._parallel_pool_max_workers.pop(key, None)
            require_pool(record)
        record.context.copy().run(retire)
    return True


def at_exit():
    source = owned.active()
    if source is None:
        return False
    from cron.scheduler_owned_close import require_closed
    require_closed(source)
    return True
