"""Original CDP sends, replies and normal connection close; no reconnect or force fallback."""
from __future__ import annotations

import asyncio
import json

from tools import browser_owned as owned
from tools.browser_owned_supervisor import require_supervisor

_running_loop = asyncio.get_running_loop


async def command(supervisor, method, params, session_id, timeout):
    record = owned.require_object(supervisor, "supervisor")
    require_supervisor(record)
    if _running_loop() is not record.loop:
        owned.refuse(record, "browser-original-cdp-loop-changed")
    operation = owned.reserve("cdp-command", supervisor._cdp, record)
    operation.attempted = operation.active = True
    operation.connection = record.connection
    operation.arguments = (method, params, session_id, timeout)
    record.requests.append(operation)
    try:
        call_id = supervisor._next_call_id
        supervisor._next_call_id += 1
        payload = {"id": call_id, "method": method}
        payload.update({k: v for k, v in (("params", params), ("sessionId", session_id)) if v})
        operation.obj = record.loop.create_future()
        owned.bind(operation, operation.obj)
        supervisor._pending_calls[call_id] = operation.obj
        owned.require_methods(record.connection)
        operation.send = record.connection.methods[0][1](json.dumps(payload))
        operation.references = (operation.obj, operation.send, record.connection)
        await operation.send
        require_supervisor(record)
        operation.require()
        operation.wait = asyncio.shield(operation.obj)
        # The external caller may stop observing; the original reply future is never cancelled/retried.
        operation.result = await operation.wait
        operation.require()
        require_supervisor(record)
        if operation.connection is not record.connection or "error" in operation.result:
            owned.refuse(operation, "browser-original-cdp-result-or-connection-changed")
        operation.done, operation.active = True, False
        return operation.result
    except BaseException as error:
        owned.fail(operation, error)
        raise


def schedule(record, coro, timeout):
    require_supervisor(record)
    if owned.running_loop() is record.loop:
        owned.refuse(record, "browser-blocking-call-on-original-supervisor-loop")
    operation = owned.reserve("scheduled-command", asyncio.run_coroutine_threadsafe, record)
    operation.arguments, operation.references = (coro, timeout), (coro,)
    operation.attempted = operation.active = True
    try:
        operation.obj = asyncio.run_coroutine_threadsafe(coro, record.loop)
        owned.bind(operation, operation.obj)
        operation.result = operation.obj.result(timeout=timeout)
        require_supervisor(record)
        operation.require()
        operation.done, operation.active = True, False
        return operation.result
    except BaseException as error:
        owned.fail(operation, error)
        raise


async def close_connection(record):
    require_supervisor(record)
    supervisor = record.obj
    reply = await supervisor._cdp("Browser.close", timeout=None)
    record.close_result = reply  # protocol response is not process exit or descendant settlement
    require_supervisor(record)
    connection = record.connection
    owned.require_methods(connection)
    connection.close_attempted = True
    connection.close_coro = connection.methods[1][1]()
    connection.references += (connection.close_coro,)
    await connection.close_coro
    owned.require_methods(connection)
    require_supervisor(record)
    connection.close_wait = connection.methods[2][1]()
    connection.references += (connection.close_wait,)
    await connection.close_wait
    owned.require_methods(connection)
    require_supervisor(record)
    record.reader_wait = asyncio.shield(record.reader.obj)
    await record.reader_wait
    require_supervisor(record)
    if not record.reader.done or record.reader.obj.cancelled() or record.reader.obj.exception() is not None:
        owned.refuse(record, "browser-original-reader-eof-unconfirmed")
    connection.closed = True
    record.stop_event.set()


def close(record):
    require_supervisor(record)
    if record.close_attempted:
        if not record.closed:
            owned.refuse(record, "browser-original-supervisor-close-attempt-retained")
        return
    from tools.browser_owned_events import require_quiet
    require_quiet(record)
    record.close_attempted = True
    token = owned._normal.set(record)
    try:
        schedule(record, close_connection(record), None)
        from tools.browser_owned_workers import join
        join(record.worker)
        require_supervisor(record)
        if not record.loop.is_closed() or not record.connection.closed or record.obj._active:
            owned.refuse(record, "browser-original-supervisor-loop-or-connection-not-closed")
        record.closed = True
    except BaseException as error:
        owned.fail(record, error)
        raise
    finally:
        owned._normal.reset(token)
