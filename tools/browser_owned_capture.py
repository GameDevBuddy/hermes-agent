"""Actual captured-page consumers retain their original supervisor/connection and waits."""
import time

from tools import browser_owned as owned
from tools.browser_owned_supervisor import require_supervisor

_links = {}


def created(capture):
    record = owned.find(capture)
    if record is None:
        return
    owner = owned.require_object(capture._supervisor, "supervisor")
    require_supervisor(owner)
    record.owner = owner
    record.references = (capture._supervisor, capture._registry, capture._loop, capture._ws, capture.page_session_id)
    _links[record] = record.references
    owned.remember_methods(record, ("call", "_send", "is_valid"))
    require_capture(record)


def require_capture(record):
    owned.require_methods(record)
    require_supervisor(record.owner)
    obj = record.obj
    actual = (obj._supervisor, obj._registry, obj._loop, obj._ws, obj.page_session_id)
    if (record.references is not _links[record]
            or any(a is not b for a, b in zip(actual, _links[record]))
            or obj._supervisor is not record.owner.obj or obj._ws is not record.owner.connection.obj
            or obj._registry is not record.owner.registry or obj._loop is not record.owner.loop
            or obj._registry.get(obj.task_id) is not record.owner.obj):
        owned.refuse(record, "browser-original-captured-page-association-changed")


def call(capture, method, params, session_id, timeout, before_send):
    record = owned.protect(capture)
    if record is None:
        return False, None
    require_capture(record)
    operation = owned.reserve("captured-command", capture._send, record)
    operation.arguments = (method, params, session_id, timeout, before_send)
    operation.attempted = operation.active = True
    try:
        deadline = None if timeout is None else time.monotonic() + timeout
        operation.coro = capture._send(method, params, session_id, None, deadline, before_send)
        operation.references = (capture, record.owner, operation.coro)
        from tools.browser_owned_cdp import schedule
        operation.result = schedule(record.owner, operation.coro, timeout)
        require_capture(record)
        operation.require()
        owned.members(record.source)
        operation.active, operation.done = False, True
        return True, operation.result
    except BaseException as error:
        owned.fail(operation, error)
        raise
