"""Instrument the original local stdlib executor's allocation boundaries, never a second pool."""
from __future__ import annotations

import concurrent.futures
import concurrent.futures.thread as backend
import threading
import weakref

from cron import scheduler_owned as owned

_thread = threading.Thread
_executor_type = backend.ThreadPoolExecutor
_current_thread = threading.current_thread
_backend = {}


def pin_backend(record):
    pool = record.obj
    record.backend = (backend._worker, backend._WorkItem, concurrent.futures.Future,
                      getattr(pool, "_create_worker_context", None), getattr(pool, "_resolve_work_item_task", None))
    _backend[record] = record.backend


def require_backend(record):
    worker, item, future, context, resolver = _backend[record]
    pool = record.obj
    if (record.backend is not _backend[record] or backend._worker is not worker
            or backend._WorkItem is not item or concurrent.futures.Future is not future
            or not _optional_same(getattr(pool, "_create_worker_context", None), context)
            or not _optional_same(getattr(pool, "_resolve_work_item_task", None), resolver)):
        owned.refuse(record, "cron-original-executor-backend-changed")


def _optional_same(left, right):
    return left is right or left is not None and right is not None and owned._same_callable(left, right)


def submit(pool, callback, args, kwargs):
    from cron.scheduler_owned_pools import require_pool
    from cron import scheduler_owned_work as work
    record = owned._submission.get()
    if record is None or record.parent is not pool or callback is not record.worker_callback or args or kwargs:
        owned.refuse(pool, "cron-submit-without-original-registration-and-worker")
    require_pool(pool)
    require_backend(pool)
    work.require_work(record)
    if record.factory_attempted:
        owned.refuse(record, "cron-original-submit-attempt-retained")
    record.factory_attempted = True
    obj = pool.obj
    try:
        # Same stdlib locks/checks/queue/worker path. Allocate its actual Future and
        # WorkItem under custody before either constructor can acquire resources.
        with obj._shutdown_lock, backend._global_shutdown_lock:
            if obj._broken:
                raise getattr(obj, "BROKEN", backend.BrokenThreadPool)(obj._broken)
            if obj._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")
            if backend._shutdown:
                raise RuntimeError("cannot schedule new futures after interpreter shutdown")
            future = owned.reserve("future", pool.backend[2], record)
            work.attach_future(record, future)
            owned.construct(future, future.factory, (), {})
            owned.remember_methods(future, ("result", "done", "cancelled", "exception", "add_done_callback"))
            future.done, future.active = False, True
            work.observe_future(record)
            item = owned.reserve("work-item", pool.backend[1], record)
            record.work_item = item
            resolver = pool.backend[4]
            task = resolver(callback, args, kwargs) if resolver is not None else None
            item_args = (future.obj, task) if resolver is not None else (future.obj, callback, args, kwargs)
            owned.construct(item, item.factory, item_args, {})
            record.queue_attempted = True
            obj._work_queue.put(item.obj)
            record.queued = True
            require_pool(pool, pending=True)
            obj._adjust_thread_count()
            require_pool(pool)
            work.require_work(record)
            record.submit_returned = True
            return future.obj
    except BaseException as error:
        owned.fail(record, error)
        raise


def adjust(pool):
    from cron.scheduler_owned_pools import require_pool
    work = owned._submission.get()
    if (work is None or work.parent is not pool or not work.factory_attempted or work.submit_returned
            or threading.current_thread() is not work.creator_thread):
        owned.refuse(pool, "cron-thread-factory-without-original-submission")
    require_pool(pool, pending=True)
    require_backend(pool)
    obj = pool.obj
    if obj._idle_semaphore.acquire(timeout=0):
        require_pool(pool, pending=True)
        return
    if len(obj._threads) >= obj._max_workers:
        return
    record = owned.reserve("thread", _thread, pool, continuation=True)
    record.published = record.start_attempted = record.started = False
    pool.threads += (record,)
    name = f"{obj._thread_name_prefix or obj}_{len(obj._threads)}"
    weakref_cb = lambda _, q=obj._work_queue: q.put(None)
    try:
        ref = weakref.ref(obj, weakref_cb)
        creator = pool.backend[3]
        context = creator() if creator is not None else None
        args = (ref, context, obj._work_queue) if creator is not None else (
            ref, obj._work_queue, obj._initializer, obj._initargs)
        record.references = (ref, weakref_cb, context, args)
        owned.construct(record, record.factory, (), {"name": name, "target": pool.backend[0], "args": args})
        owned.remember_methods(record, ("start", "join", "is_alive"))
        record.start_attempted = True
        record.methods[0][1]()
        owned.require_methods(record)
        require_pool(pool, pending=True)
        obj._threads.add(record.obj)
        backend._threads_queues[record.obj] = obj._work_queue
        record.published = record.started = True
        require_pool(pool)
    except BaseException as error:
        owned.fail(record, error)
        raise
