"""Normal local browser settlement on original owners; retained books never imply graph authority."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from tools import browser_owned as owned
from tools.browser_owned_launcher import require_browser, wait_and_close_log
from tools.browser_owned_session import require_session


@dataclass(frozen=True)
class OriginalBook:
    source: object
    book: object
    records: tuple
    links: tuple


def capture(source):
    records = owned.members(source)
    return OriginalBook(source, source.browser_records, records,
                        tuple((record, record.obj, record.original_parent, record.children) for record in records))


def require_book(snapshot):
    current = owned.members(snapshot.source)
    if (snapshot.source.browser_records is not snapshot.book or len(current) != len(snapshot.records)
            or any(a is not b for a, b in zip(current, snapshot.records))):
        raise snapshot.source.unknown("browser-original-final-book-membership-changed")
    for record, obj, parent, children in snapshot.links:
        if record.obj is not obj or record.original_parent is not parent or record.children is not children:
            owned.refuse(record, "browser-original-final-book-links-changed")


def close_session(record):
    require_session(record)
    if record.close_attempted:
        if not record.closed:
            owned.refuse(record, "browser-original-session-close-attempt-retained")
        require_closed_session(record)
        return
    record.close_attempted = True

    def normal():
        token = owned._normal.set(record)
        try:
            require_session(record)
            if record.supervisor is None or record.browser is None:
                owned.refuse(record, "browser-original-session-normal-closer-unavailable")
            from tools.browser_owned_cdp import close
            close(record.supervisor)
            require_session(record)
            wait_and_close_log(record.browser)
            require_session(record)
            for browser in record.browser.launcher.browsers:
                require_browser(browser)
                browser.closed = True
            record.browser.launcher.closed = True
            record.closed = True
            require_closed_session(record)
            owned.members(record.source)
        finally:
            owned._normal.reset(token)

    try:
        record.context.copy().run(normal)
    except BaseException as error:
        owned.fail(record, error)
        raise


def require_closed_session(record):
    require_session(record)
    from tools.browser_owned_supervisor import require_supervisor
    supervisor, process, log = record.supervisor, record.browser.process, record.browser.launcher.log
    require_supervisor(supervisor)
    if (not record.closed or not supervisor.closed or not supervisor.worker.closed
            or not supervisor.worker.finished.is_set() or not supervisor.loop.is_closed()
            or not supervisor.connection.closed or not supervisor.reader.done
            or not process.wait_done or process.wait_result != 0 or process.obj.returncode != 0
            or not log.closed or log.obj.closed is not True):
        owned.refuse(record, "browser-original-local-session-settlement-incomplete")


def cleanup_session(module, key, info=None):
    with module._cleanup_lock:
        actual = module._active_sessions.get(key) if info is None else info
    if actual is None:
        if owned.active() is not None:
            # A missing cache cannot hide a previously created original session.
            records = [record for record in owned.members(owned.active()) if record.kind == "session" and record.key == key]
            if records:
                owned.refuse(records[0], "browser-original-session-cache-entry-lost")
        return False
    record = owned.protect(actual)
    if record is None:
        return False
    if record.kind != "session" or record.obj is not actual or record.key != key:
        owned.refuse(record, "browser-original-cleanup-session-changed")
    close_session(record)
    # Keep original cache and ledger references. Closed is local settlement, never permission to reset/relaunch.
    return True


def stop_launcher(key):
    from tools import browser_launcher as launcher
    with launcher._lock:
        browser = launcher._browsers.get(key)
    if browser is None:
        return False
    record = owned.protect(browser)
    if record is None:
        return False
    require_browser(record)
    parent = record.launcher.original_parent
    if parent is None or parent.kind != "session" or parent.browser is not record:
        owned.refuse(record, "browser-launcher-without-original-session-normal-closer")
    close_session(parent)
    return True


def guard_tracking(module, key):
    with module._cleanup_lock:
        info = module._active_sessions.get(key)
    if info is not None and owned.protect(info) is not None:
        owned.refuse(owned.find(info), "browser-original-session-cannot-be-forgotten")


def cleanup_all(module):
    if owned.active() is None:
        return False
    with module._cleanup_lock:
        instances = tuple(module._active_sessions.values())
    source = owned.active()
    for instance in instances:
        owned.require_object(instance, "session")
    for record in owned.members(source):
        if record.kind == "session":
            close_session(record)
    from tools.browser_owned_workers import stop_janitor
    stop_janitor(module)
    owned.members(source)
    return True


def _require_settled(snapshot):
    require_book(snapshot)
    for record in snapshot.records:
        if record.kind == "session":
            require_closed_session(record)
        elif record.kind == "capture":
            from tools.browser_owned_capture import require_capture
            require_capture(record)
        elif record.kind in ("cdp-event", "cdp-timer"):
            from tools.browser_owned_events import require_settled
            require_settled(record)
        elif record.kind == "browser":
            require_browser(record)
            if not record.closed:
                owned.refuse(record, "browser-original-launch-wrapper-not-settled")
        elif record.kind in ("worker", "loop", "connection", "supervisor", "log-writer", "janitor"):
            if not record.closed:
                owned.refuse(record, "browser-original-resource-not-closed:" + record.kind)
        elif record.kind == "process":
            if (not record.wait_done or record.wait_result is None or record.obj.returncode != record.wait_result
                    or record.original_parent.kind == "launcher" and record.wait_result != 0):
                owned.refuse(record, "browser-original-process-exit-unconfirmed")
        elif not record.done or record.active:
            owned.refuse(record, "browser-original-operation-incomplete:" + record.kind)


async def close_all(source, module, instances):
    from hermes_owned_gateway import OwnedGatewaySource
    if type(source) is not OwnedGatewaySource:
        return False
    source.require_shutdown(source.runner)
    for instance in instances:
        owned.require_object(instance, "session")
    if module is not None and module._cleanup_thread is not None:
        owned.require_object(module._cleanup_thread, "worker")
    if not source.browser_records:
        return False
    if source.browser_closing:
        if not source.browser_closed:
            raise source.unknown("browser-original-shutdown-attempt-retained")
        _require_settled(source.browser_snapshot)
        return True
    source.browser_closing = True
    initial = owned.members(source)
    try:
        for record in initial:
            if record.kind == "session":
                await asyncio.to_thread(close_session, record)
        if module is not None:
            from tools.browser_owned_workers import stop_janitor
            await asyncio.to_thread(stop_janitor, module)
        source.browser_snapshot = capture(source)
        _require_settled(source.browser_snapshot)
        source.browser_closed = True
        return True
    except BaseException as error:
        source.browser_error = source.browser_error or error
        source.unknown("original-browser-normal-settlement-incomplete")
        raise
