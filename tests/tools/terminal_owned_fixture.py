"""Actual LocalEnvironment/execute consumers with mocked lower process, pipe and janitor factories."""
import io
import sys
import threading
from types import SimpleNamespace
import pytest

from tests.agent.auxiliary_owned_fixture import bind_source, allow_normal_shutdown


@pytest.fixture(autouse=True)
def join_memory_workers(monkeypatch):
    """Finish only the real Python workers of these immediate in-memory mock pipes before unpatching."""
    yield
    owned = sys.modules.get("tools.terminal_tool_owned")
    if owned is not None:
        for source in dict.fromkeys(owned._sources):
            for record in tuple(source.terminal_records):
                if (record is not None and record.kind in ("pipe-reader", "pipe-writer", "executor")
                        and isinstance(record.obj, threading.Thread) and record.obj is not threading.current_thread()):
                    record.obj.join(timeout=5)
                    assert not record.obj.is_alive()


class Pipe:
    def __init__(self, chunks=None):
        self.chunks, self.closed, self.writes = list(chunks or []), False, []
        self.buffer = self
        self.on_read = self.on_write = self.on_close = None

    def read(self, size=-1):
        if self.on_read is not None:
            self.on_read()
        return self.chunks.pop(0) if self.chunks else ""

    def write(self, data):
        self.writes.append(data)
        if self.on_write is not None:
            self.on_write()
        return len(data)

    def close(self):
        if self.on_close is not None:
            self.on_close()
        self.closed = True

    def fileno(self):
        raise io.UnsupportedOperation("in-memory pipe only")

    def __iter__(self):
        while True:
            piece = self.read()
            if not piece:
                return
            yield piece


class Process:
    def __init__(self, args, **kwargs):
        import subprocess
        self.args, self.kwargs, self.returncode = args, kwargs, 0
        self.stdout, self.stderr = Pipe(["fixture-output\n"]), None
        self.stdin = Pipe() if kwargs.get("stdin") is subprocess.PIPE else None
        self.waits, self.signals = [], []
        self.on_poll = self.on_wait = None

    def poll(self):
        if self.on_poll is not None:
            self.on_poll()
        return self.returncode

    def wait(self, timeout=None):
        if self.on_wait is not None:
            self.on_wait()
        self.waits.append(timeout)
        return self.returncode

    def kill(self):
        self.signals.append("kill")

    def terminate(self):
        self.signals.append("terminate")


class Janitor:
    def __init__(self, module):
        self.module, self.alive, self.joins = module, False, 0

    def start(self):
        self.alive = True

    def is_alive(self):
        return self.alive

    def join(self, timeout=None):
        assert not self.module._cleanup_running
        self.joins += 1
        self.alive = False


def terminal_fixture(monkeypatch, tmp_path):
    from tools import terminal_tool as terminal
    from tools.environments import local
    source, runner = bind_source(monkeypatch, tmp_path)
    owned = sys.modules.get("tools.terminal_tool_owned")
    if owned is not None:
        monkeypatch.setattr(owned, "_index", {})
        monkeypatch.setattr(owned, "_sources", [])
        monkeypatch.setattr(owned, "_claims", 0)
        from tools import terminal_tool_owned_workers as workers
        monkeypatch.setattr(workers, "_janitor_thread", lambda *args: Janitor(terminal))
    else:
        # Baseline comparison also replaces its lower janitor producer; no real reaper runs.
        def old_janitor():
            terminal._cleanup_running = True
            terminal._cleanup_thread = Janitor(terminal)
            terminal._cleanup_thread.start()
        monkeypatch.setattr(terminal, "_start_cleanup_thread", old_janitor)
    for name in ("_active_environments", "_last_activity", "_creation_locks", "_calls_in_flight"):
        monkeypatch.setattr(terminal, name, {})
    monkeypatch.setattr(terminal, "_cleanup_thread", None)
    monkeypatch.setattr(terminal, "_cleanup_running", False)
    monkeypatch.setattr(terminal, "_env_lock", threading.RLock())
    monkeypatch.setattr(terminal, "_creation_locks_lock", threading.RLock())
    monkeypatch.setattr(local, "_find_bash", lambda: "fixture-shell-not-executed")
    monkeypatch.setattr(local, "_make_run_env", lambda env: {"FIXTURE": "mock-only"})
    monkeypatch.setattr(local, "_resolve_shell_init_files", list)
    monkeypatch.setattr(local.LocalEnvironment, "get_temp_dir", lambda self: str(tmp_path))
    created = []

    def factory(*args, **kwargs):
        process = Process(*args, **kwargs)
        created.append(process)
        return process

    monkeypatch.setattr(local.subprocess, "Popen", factory)
    monkeypatch.setitem(sys.modules, "tools.mcp_tool_owned", SimpleNamespace(shutdown_all=lambda: None))
    browser = sys.modules.get("tools.browser_session")
    if browser is not None:
        monkeypatch.setattr(browser, "_active_sessions", {})
        monkeypatch.setattr(browser, "_cleanup_thread", None)
    config = {"env_type": "local", "cwd": str(tmp_path), "timeout": 30, "lifetime_seconds": 300}
    plan = SimpleNamespace(config=config, env_type="local", image="", cwd=str(tmp_path), host_cwd=None,
                           effective_timeout=30, effective_task_id="fixture-task")
    return source, runner, terminal, plan, created


def construct(terminal, plan):
    return terminal._acquire_env(plan, "fixture-task")


async def finish(source):
    from gateway.run_owned_resources import close_tool_resources
    allow_normal_shutdown(source)
    await close_tool_resources(source)
