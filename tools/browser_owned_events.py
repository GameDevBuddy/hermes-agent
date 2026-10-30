"""Retain original CDP event tasks and dialog timers before their lower factories run."""
import asyncio

from tools import browser_owned as owned
from tools.browser_owned_supervisor import require_supervisor

_running_loop = asyncio.get_running_loop


async def _run(record):
    try:
        record.require()
        require_supervisor(record.original_parent)
        record.result = await record.coro
        record.require()
        require_supervisor(record.original_parent)
        record.done, record.active = True, False
    except BaseException as error:
        owned.fail(record, error)
        raise


def _observed(record, task):
    # Retrieving a completed Task's error prevents an unobserved-exception log;
    # the original Task, coroutine and exact first error remain in custody.
    if task.cancelled():
        owned.fail(record, asyncio.CancelledError("original browser event task cancelled"))
    elif error := task.exception():
        owned.fail(record, error)


def spawn(supervisor, factory, *args, **kwargs):
    parent = owned.protect(supervisor)
    if parent is None:
        return asyncio.create_task(factory(*args, **kwargs))
    require_supervisor(parent)
    if _running_loop() is not parent.loop:
        owned.refuse(parent, "browser-event-original-loop-changed")
    record = owned.reserve("cdp-event", factory, parent)
    record.arguments, record.attempted, record.active = (args, kwargs), True, True
    try:
        record.coro = factory(*args, **kwargs)
        record.runner_coro = _run(record)
        record.observed = lambda task: _observed(record, task)
        record.references = (record.coro, record.runner_coro, record.observed)
        owned.bind(record, asyncio.create_task(record.runner_coro))
        owned.remember_methods(record, ("done", "cancelled", "exception", "add_done_callback"))
        record.obj.add_done_callback(record.observed)
        record.require()
        return record.obj
    except BaseException as error:
        owned.fail(record, error)
        raise


def _timer_done(record, task):
    _observed(record, task)
    if record.source.browser_error is None:
        record.require()
        record.done = record.closed = True
        record.active = False


def timer(supervisor, delay, factory, *args):
    parent = owned.protect(supervisor)
    if parent is None:
        return asyncio.get_running_loop().call_later(delay, lambda: spawn(supervisor, factory, *args))
    require_supervisor(parent)
    if _running_loop() is not parent.loop:
        owned.refuse(parent, "browser-timer-original-loop-changed")
    record = owned.reserve("cdp-timer", factory, parent)
    record.arguments, record.fired = (delay, args), False
    record.attempted = record.active = True

    def fire():
        try:
            owned.require_methods(record)
            record.fired = True
            record.child = spawn(supervisor, factory, *args)
            record.references += (record.child,)
            record.child.add_done_callback(lambda task: _timer_done(record, task))
            return record.child
        except BaseException as error:
            owned.fail(record, error)
            raise

    record.callback, record.references = fire, (fire,)
    try:
        owned.bind(record, parent.loop.call_later(delay, fire))
        owned.remember_methods(record, ("cancel", "cancelled"))
        record.require()
        return record.obj
    except BaseException as error:
        owned.fail(record, error)
        raise


def cancel(handle):
    record = owned.protect(handle)
    if record is None:
        return handle.cancel()
    owned.require_methods(record)
    require_supervisor(record.original_parent)
    if record.close_attempted:
        if not record.closed:
            owned.refuse(record, "browser-original-timer-cancel-attempt-retained")
        return
    record.close_attempted = True
    try:
        record.methods[0][1]()
        owned.require_methods(record)
        if record.methods[1][1]() is not True:
            owned.refuse(record, "browser-original-timer-cancel-unconfirmed")
        record.closed = True
        if not record.fired:
            record.done, record.active = True, False
    except BaseException as error:
        owned.fail(record, error)
        raise


def require_quiet(supervisor):
    require_supervisor(supervisor)
    kinds = {"cdp-command", "captured-command", "scheduled-command", "cdp-event", "cdp-timer"}
    for record in owned.members(supervisor.source):
        if record.kind in kinds and (record.active or not record.done):
            owned.refuse(record, "browser-original-operation-still-active-before-close")
        if record.kind == "command" and record.session is supervisor.original_parent and record.active:
            owned.refuse(record, "browser-original-command-still-active-before-close")
    obj = supervisor.obj
    if obj._pending_calls or obj._pending_dialogs or obj._dialog_watchdogs:
        owned.refuse(supervisor, "browser-original-dialog-or-request-still-pending-before-close")


def require_settled(record):
    owned.require_methods(record)
    require_supervisor(record.original_parent)
    if not record.done or record.active:
        owned.refuse(record, "browser-original-event-or-timer-still-active")
    if record.kind == "cdp-event":
        if not record.obj.done() or record.obj.cancelled() or record.obj.exception() is not None:
            owned.refuse(record, "browser-original-event-task-incomplete")
    elif not record.closed or not (record.fired or record.obj.cancelled()):
        owned.refuse(record, "browser-original-dialog-timer-incomplete")
