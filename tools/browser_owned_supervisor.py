"""Managed behavior of the original CDPSupervisor, connection and original worker loop."""
from __future__ import annotations

import asyncio
import json
import threading

from tools import browser_owned as owned
from tools import browser_owned_workers as workers
from tools.browser_owned_session import current, require_session

_worker_links, _loop_links, _connection_links, _reader_links = {}, {}, {}, {}


def attach_loop(record, loop_record):
    if record in _loop_links or loop_record.original_parent is not record:
        owned.refuse(record, "browser-original-loop-binding-attempt-retained")
    record.loop, record.obj._loop = loop_record.obj, loop_record.obj
    _loop_links[record] = (loop_record, loop_record.obj)


def attach_connection(record, connector, connection):
    if (record in _connection_links or connector.original_parent is not record
            or connection.original_parent is not connector):
        owned.refuse(record, "browser-original-connection-binding-attempt-retained")
    record.connector, record.connection = connector, connection
    _connection_links[record] = (connector, connection)


def attach_reader(record, reader):
    if record in _reader_links or reader.original_parent is not record:
        owned.refuse(record, "browser-original-reader-binding-attempt-retained")
    record.reader = reader
    _reader_links[record] = reader


def require_supervisor(record):
    owned.require_methods(record)
    require_session(record.original_parent)
    supervisor = record.obj
    worker, obj = _worker_links[record]
    loop_record, loop = _loop_links[record]
    connector, connection = _connection_links[record]
    if (record.worker is not worker or worker.obj is not obj or worker.original_parent is not record
            or record.loop is not loop or loop_record.obj is not loop
            or record.connector is not connector or record.connection is not connection
            or record.reader is not _reader_links.get(record)
            or supervisor.task_id != record.task_id or supervisor.cdp_url != record.cdp_url
            or supervisor._loop is not record.loop or supervisor._thread is not record.worker.obj
            or supervisor._ws is not (record.connection.obj if record.connection is not None else None)):
        owned.refuse(record, "browser-original-supervisor-loop-thread-or-connection-changed")


def registry_start(registry, task_id, cdp_url, dialog_policy, dialog_timeout_s, start_timeout):
    from tools.browser_supervisor import CDPSupervisor
    with registry._lock:
        previous = registry._by_task.get(task_id)
    if previous is not None:
        record = owned.protect(previous)
        if record is not None:
            require_supervisor(record)
            if record.closed or cdp_url != record.cdp_url:
                owned.refuse(record, "browser-original-supervisor-cannot-be-replaced")
            return True, previous
    if owned.active() is None:
        return False, None
    parent = current()
    if parent is None or parent.obj is None or parent.browser is None:
        owned.unsupported("supervisor-without-original-browser-session", previous)
    require_session(parent)
    if cdp_url != parent.obj["cdp_url"]:
        owned.refuse(parent, "browser-supervisor-original-endpoint-changed")
    if parent.supervisor is not None:
        owned.refuse(parent, "browser-original-supervisor-cache-entry-lost-or-aliased")
    token = owned._building.set(parent)
    try:
        supervisor = CDPSupervisor(task_id, cdp_url, dialog_policy=dialog_policy, dialog_timeout_s=dialog_timeout_s)
        record = owned.find(supervisor)
        parent.supervisor = record
        record.registry, record.registry_key = registry, task_id
        supervisor.start(timeout=start_timeout)
        require_supervisor(record)
        with registry._lock:
            if registry._by_task.get(task_id) is not None:
                owned.refuse(record, "browser-original-supervisor-concurrent-publication")
            registry._by_task[task_id] = supervisor
            registry._home_by_task[task_id] = record.home_key
        return True, supervisor
    except BaseException as error:
        owned.fail(parent, error)
        raise
    finally:
        owned._building.reset(token)


def start(supervisor, timeout):
    record = owned.require_object(supervisor, "supervisor")
    if record is None:
        return False
    if getattr(record, "started", False):
        owned.refuse(record, "browser-original-supervisor-start-attempt-retained")
    record.started = True
    record.task_id, record.cdp_url = supervisor.task_id, supervisor.cdp_url
    record.loop = record.connection = record.worker = None
    record.reader = record.main_task = record.connector = None
    record.requests = []
    record.stop_event = record.normal_finished = None
    owned.remember_methods(record, ("start", "stop", "_thread_main", "_run", "_read_loop", "_cdp"))

    def publish(worker):
        record.worker, supervisor._thread = worker, worker.obj
        _worker_links[record] = (worker, worker.obj)

    try:
        workers.thread(record, supervisor._thread_main, "cdp-owned-" + supervisor.task_id, _thread, publish)
        if not supervisor._ready_event.wait(timeout=timeout):
            owned.refuse(record, "browser-original-supervisor-passive-start-deadline")
        require_supervisor(record)
        if supervisor._start_error is not None:
            raise supervisor._start_error
        return True
    except BaseException as error:
        owned.fail(record, error)
        raise


_thread = threading.Thread


def thread_main(supervisor):
    record = owned.require_object(supervisor, "supervisor")
    loop_record = owned.reserve("loop", asyncio.new_event_loop, record)
    try:
        loop = owned.call(loop_record, asyncio.new_event_loop)
        owned.bind(loop_record, loop)
        attach_loop(record, loop_record)
        asyncio.set_event_loop(loop)
        record.stop_event = asyncio.Event()
        record.normal_finished = asyncio.Event()
        main = owned.reserve("supervisor-main", loop.create_task, record)
        record.main_coro = supervisor._run()
        main.arguments, main.references = (record.main_coro,), (record.main_coro,)
        main.attempted = main.active = True
        record.main_task = loop.create_task(record.main_coro)
        owned.bind(main, record.main_task)
        record.references += (loop_record, main, record.main_coro, record.main_task, record.stop_event, record.normal_finished)
        loop.run_until_complete(record.main_task)
        record.require()
        if record.main_task.cancelled() or record.main_task.exception() is not None:
            owned.refuse(main, "browser-original-supervisor-main-incomplete")
        main.done, main.active = True, False
        pending = tuple(task for task in asyncio.all_tasks(loop) if not task.done())
        record.remaining_tasks = pending
        if pending or supervisor._dialog_watchdogs or supervisor._pending_calls:
            owned.refuse(record, "browser-original-supervisor-workers-or-watchdogs-remain")
        loop.close()
        if not loop.is_closed():
            owned.refuse(record, "browser-original-supervisor-loop-not-closed")
        loop_record.closed = True
        supervisor._set_active(False)
    except BaseException as error:  # health: allow BLE001 -- keep the original loop/thread/task refs and exact failure instead of unwinding unknown custody
        owned.fail(record, error)
        supervisor._start_error = supervisor._start_error or error
        supervisor._ready_event.set()
        # Keep the same actual loop, tasks, connection and thread, without cancellation or replacement.
        threading.Event().wait()


async def run(supervisor):
    record = owned.require_object(supervisor, "supervisor")
    import websockets
    from agent.proxy_bypass import loopback_connect_kwargs
    connector = owned.reserve("connection-attempt", websockets.connect, record)
    connection = owned.reserve("connection", websockets.connect, connector)
    attach_connection(record, connector, connection)
    try:
        kwargs = {"max_size": 50 * 1024 * 1024, **loopback_connect_kwargs(supervisor.cdp_url)}
        connector.arguments, connector.attempted, connector.active = ((supervisor.cdp_url,), kwargs), True, True
        connector.obj = websockets.connect(supervisor.cdp_url, **kwargs)
        owned.bind(connector, connector.obj)
        connection.obj = await connector.obj
        owned.bind(connection, connection.obj)
        supervisor._ws = connection.obj
        owned.remember_methods(connection, ("send", "close", "wait_closed"))
        connector.done, connector.active = True, False
        connection.done = True
        require_supervisor(record)
        reader = owned.reserve("reader", supervisor._read_loop, record)
        attach_reader(record, reader)
        reader.coro = supervisor._read_loop()
        reader.references = (reader.coro, connection)
        reader.attempted = reader.active = True
        reader.obj = asyncio.create_task(reader.coro, name="cdp-owned-reader")
        owned.bind(reader, reader.obj)
        await supervisor._attach_initial_page()
        require_supervisor(record)
        supervisor._set_active(True)
        supervisor._ready_event.set()
        record.reader_wait = asyncio.shield(reader.obj)
        await record.reader_wait
        require_supervisor(record)
        if not record.close_attempted:
            owned.refuse(record, "browser-original-connection-ended-outside-normal-close")
        await record.stop_event.wait()
        require_supervisor(record)
        record.normal_finished.set()
    except BaseException as error:  # health: allow BLE001 -- retain the original connection/task and first exception without reconnecting or cancelling IO
        owned.fail(record, error)
        supervisor._start_error = supervisor._start_error or error
        supervisor._ready_event.set()
        await asyncio.Future()


async def read(supervisor):
    record = owned.require_object(supervisor, "supervisor")
    try:
        ws = record.connection.obj
        async for raw in ws:
            require_supervisor(record)
            msg = json.loads(raw)
            if "id" in msg:
                future = supervisor._pending_calls.pop(msg["id"], None)
                if future is None or future.done():
                    owned.refuse(record, "browser-original-cdp-response-without-live-request")
                if "error" in msg:
                    future.set_exception(RuntimeError("Original CDP command failed"))
                else:
                    future.set_result(msg)
            elif handler := supervisor._EVENT_HANDLERS.get(msg.get("method")):
                result = handler(supervisor, msg.get("params", {}), msg.get("sessionId"))
                if result is not None:
                    await result
            require_supervisor(record)
        require_supervisor(record)
        record.reader.active, record.reader.done = False, True
    except BaseException as error:
        owned.fail(record, error)
        raise
