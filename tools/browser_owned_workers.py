"""Original browser workers, retained before thread/loop factories and joined normally."""
from __future__ import annotations

import threading

from tools import browser_owned as owned


def thread(parent, target, name, factory=threading.Thread, publish=None):
    record = owned.reserve("worker", factory, parent)
    record.target, record.stop = target, threading.Event()
    record.attempted = record.active = True
    try:
        obj = factory.__new__(factory)
        owned.bind(record, obj)

        def run():
            try:
                record.context.copy().run(target)
                record.context.copy().run(record.require)
                record.active, record.done = False, True
                record.finished.set()
            except BaseException as error:  # health: allow BLE001 -- retain exact error/traceback and original resources in this non-unwinding worker boundary
                owned.fail(record, error)
                # No thread finally drops an original loop/task/connection after an unknown result.
                record.stop.wait()

        factory.__init__(obj, target=run, name=name, daemon=True)
        owned.remember_methods(record, ("start", "join", "is_alive"))
        record.references = (target, run, record.stop)
        if publish is not None:
            publish(record)
        record.methods[0][1]()
        record.require()
        return record
    except BaseException as error:
        owned.fail(record, error)
        raise


def join(record):
    owned.require_methods(record)
    if record.obj is threading.current_thread():
        owned.refuse(record, "browser-worker-cannot-join-itself")
    if record.close_attempted:
        if not record.closed:
            owned.refuse(record, "browser-original-worker-join-attempt-retained")
        return
    record.close_attempted = True
    try:
        record.methods[1][1]()
        owned.require_methods(record)
        if record.methods[2][1]() or not record.finished.is_set() or not record.done:
            owned.refuse(record, "browser-original-worker-not-joined")
        record.closed = True
    except BaseException as error:
        owned.fail(record, error)
        raise


def start_janitor(module):
    source = owned.active()
    if source is None:
        return False
    existing = module._cleanup_thread
    if existing is not None:
        record = owned.require_object(existing, "worker")
        if record.module is not module or record.source is not source or record.closed:
            owned.refuse(record, "browser-original-janitor-replaced")
        return True
    parent = owned._building.get()
    if parent is None:
        owned.unsupported("janitor-without-original-session")
    module._cleanup_running = True
    record = owned.reserve("janitor", start_janitor, parent)
    record.module, record.done = module, True

    def run():
        from tools.browser_session_lifecycle import cleanup_inactive_sessions
        while module._cleanup_running:
            record.require()
            cleanup_inactive_sessions()
            if record.worker.stop.wait(30):
                break
        record.require()

    # Publish the worker record before start; the target never races an absent owner link.
    def publish(worker):
        record.worker, worker.module = worker, module
        module._cleanup_thread = worker.obj

    record.worker = None
    thread(record, run, "browser-owned-cleanup", _janitor_thread, publish)
    return True


_janitor_thread = threading.Thread


def stop_janitor(module):
    obj = module._cleanup_thread
    if obj is None:
        return False
    record = owned.protect(obj)
    if record is None:
        return False
    if record.kind != "worker" or record.module is not module or record.original_parent.kind != "janitor":
        owned.refuse(record, "browser-original-janitor-association-changed")
    module._cleanup_running = False
    record.stop.set()
    join(record)
    record.original_parent.closed = True
    return True
