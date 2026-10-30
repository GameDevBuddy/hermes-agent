"""In-memory lower browser/process/connection/worker models, never kernel or native evidence."""
import asyncio
import json
from types import SimpleNamespace


class Process:
    instances = []
    on_init = on_wait = None

    def __init__(self, args, **kwargs):
        import subprocess
        self.args, self.kwargs, self.pid = args, kwargs, 71
        self.waits, self.signals, self.returncode = [], [], None
        self.stdin = Pipe() if kwargs.get("stdin") is subprocess.PIPE else None
        self.stdout = Pipe(["mock stdout\n"]) if kwargs.get("stdout") is subprocess.PIPE else None
        self.stderr = Pipe(["mock stderr\n"]) if kwargs.get("stderr") is subprocess.PIPE else None
        type(self).instances.append(self)
        if type(self).on_init is not None:
            type(self).on_init(self)

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.waits.append(timeout)
        if type(self).on_wait is not None:
            type(self).on_wait(self)
        self.returncode = 0
        return 0

    def terminate(self):
        raise AssertionError("Real termination is forbidden in this fixture")

    def kill(self):
        raise AssertionError("Real kill is forbidden in this fixture")


class Pipe:
    def __init__(self, chunks=()):
        self.chunks, self.closed, self.writes = list(chunks), False, []

    def read(self, size=-1):
        return self.chunks.pop(0) if self.chunks else ""

    def write(self, value):
        self.writes.append(value)
        return len(value)

    def flush(self):
        return None

    def close(self):
        self.closed = True


class Socket:
    def __init__(self):
        self.queue, self.closed_event, self.sent = asyncio.Queue(), asyncio.Event(), []
        self.on_send = self.on_wait_closed = None

    async def send(self, raw):
        message = json.loads(raw)
        self.sent.append(message)
        if self.on_send is not None:
            self.on_send(message)
        self.queue.put_nowait(json.dumps({"id": message["id"], "result": {}}))
        if message["method"] == "Browser.close":
            self.queue.put_nowait(None)
            self.closed_event.set()

    async def close(self):
        self.queue.put_nowait(None)
        self.closed_event.set()

    async def wait_closed(self):
        if self.on_wait_closed is not None:
            self.on_wait_closed()
        await self.closed_event.wait()

    def __aiter__(self):
        return self.messages()

    async def messages(self):
        while True:
            item = await self.queue.get()
            if item is None:
                return
            yield item


class Timer:
    def __init__(self, delay, callback):
        self.delay, self.callback, self.is_cancelled = delay, callback, False

    def cancel(self):
        self.is_cancelled = True

    def cancelled(self):
        return self.is_cancelled

    def fire(self):
        assert not self.is_cancelled
        return self.callback()


class Loop:
    """Mock browser loop: schedules only in-memory protocol work on pytest's ordinary driver loop."""
    def __init__(self, driver):
        self.driver, self.closed = driver, False

    def create_future(self):
        return self.driver.create_future()

    def create_task(self, coro):
        return self.driver.create_task(coro)

    def call_later(self, delay, callback):
        return Timer(delay, callback)

    def call_soon_threadsafe(self, callback, *args, **kwargs):
        return self.driver.call_soon_threadsafe(callback, *args, **kwargs)

    def is_running(self):
        return not self.closed

    def is_closed(self):
        return self.closed


class Worker:
    """No OS/browser worker is started. Protocol reads use only pytest's existing driver task."""
    instances = []
    driver = None

    def __init__(self, target, name, daemon):
        self.target, self.name, self.alive, self.joins = target, name, False, []
        type(self).instances.append(self)

    def start(self):
        from tools import browser_owned as owned
        self.alive = True
        worker = owned.find(self)
        parent = worker.original_parent
        if parent.kind == "supervisor":
            self._supervisor(parent)
        elif self.name.startswith(("browser-owned-reader", "browser-owned-writer")):
            self.target()
            self.alive = False

    def _supervisor(self, record):
        from tools import browser_owned as owned
        from tools import browser_owned_supervisor as supervisors
        supervisor = record.obj
        loop = Loop(type(self).driver)
        loop_record = owned.reserve("loop", Loop, record)
        owned.bind(loop_record, loop)
        loop_record.done = True
        supervisors.attach_loop(record, loop_record)
        connector = owned.reserve("connection-attempt", Socket, record)
        connector.done = True
        connection = owned.reserve("connection", Socket, connector)
        owned.bind(connection, Socket())
        connection.done = True
        owned.remember_methods(connection, ("send", "close", "wait_closed"))
        supervisors.attach_connection(record, connector, connection)
        supervisor._ws = connection.obj
        record.stop_event, record.normal_finished = asyncio.Event(), asyncio.Event()
        reader = owned.reserve("reader", supervisor._read_loop, record)
        reader.coro = supervisor._read_loop()
        reader.references = (reader.coro, connection)
        reader.active = reader.attempted = True
        owned.bind(reader, type(self).driver.create_task(reader.coro))
        supervisors.attach_reader(record, reader)
        record.test_loop_record = loop_record
        supervisor._page_session_id, supervisor._active = "mock-page", True
        supervisor._ready_event.set()

    def is_alive(self):
        return self.alive

    def join(self, timeout=None):
        from tools import browser_owned as owned
        self.joins.append(timeout)
        record = owned.find(self)
        parent = record.original_parent
        if parent.kind == "supervisor":
            assert parent.stop_event.is_set() and parent.reader.obj.done()
            parent.loop.closed = True
            parent.test_loop_record.closed = True
            parent.obj._set_active(False)
        if parent.kind == "janitor":
            assert not parent.module._cleanup_running and record.stop.is_set()
        self.alive, record.active, record.done = False, False, True
        record.finished.set()


def deny(*args, **kwargs):
    raise AssertionError("A real browser, discovery, network, install or process boundary was reached")


def reset(monkeypatch):
    monkeypatch.setattr(Process, "instances", [])
    monkeypatch.setattr(Process, "on_init", None)
    monkeypatch.setattr(Process, "on_wait", None)
    monkeypatch.setattr(Worker, "instances", [])
    return SimpleNamespace(processes=Process.instances, workers=Worker.instances)
