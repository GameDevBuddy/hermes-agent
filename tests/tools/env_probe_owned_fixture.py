"""Actual probe/source/runtime and context builder; lower threads/processes/pipes are models."""
import io
import threading

from tests.agent.auxiliary_owned_fixture import bind_source, allow_normal_shutdown
from tools import env_probe, env_probe_owned as owned, env_probe_owned_io as pipe_io


class Thread:
    current = None

    def __init__(self, target, *, args=(), kwargs=None, name, daemon=True):
        self.target, self.args, self.kwargs, self.name = target, args, kwargs or {}, name
        self.alive, self.joined, self.started = False, 0, False

    def start(self):
        self.started = self.alive = True
        if self.name != "env-probe":
            self.run()

    def run(self):
        prior, Thread.current = Thread.current, self
        try:
            self.target(*self.args, **self.kwargs)
        finally:
            self.alive, Thread.current = False, prior

    def join(self):
        self.joined += 1
        if self.alive:
            self.run()

    def is_alive(self):
        return self.alive


class Process:
    instances = []
    on_init = None
    wait_error = None
    output = b"probe fixture\n"

    def __init__(self, argv, **kwargs):
        self.argv, self.kwargs, self.returncode = argv, kwargs, None
        self.stdout, self.stderr = io.BytesIO(self.output), io.BytesIO(b"")
        self.waited = self.force_calls = 0
        self.instances.append(self)
        if self.on_init is not None:
            type(self).on_init(self)

    def wait(self, timeout=None):
        self.waited += 1
        if self.wait_error is not None:
            raise self.wait_error
        self.returncode = 0
        return 0

    def kill(self):
        self.force_calls += 1
        raise AssertionError("diagnostic child must never be killed")

    terminate = kill


class Case:
    def __init__(self, monkeypatch, tmp_path, *, build=None):
        from gateway.runtime_ownership import process_ownership
        from gateway.session_authorities import SessionAuthorities
        from gateway.native_source_runtime import attach_original_registry
        from tools.environments import local
        self.source, self.runner = bind_source(monkeypatch, tmp_path)
        self.runner.session_authorities = SessionAuthorities(tmp_path / "creator-profile")
        attach_original_registry(self.runner, self.runner.session_authorities)
        self.argv = ["fixture-python", "--version"]
        self.environment = {"HERMES_HOME": str(tmp_path / "creator-profile"), "PATH": "fixture-path"}
        self.environment_calls = []

        def environment(**kwargs):
            self.environment_calls.append(kwargs)
            return self.environment

        monkeypatch.setattr(local, "build_subprocess_env", environment)
        monkeypatch.setattr(process_ownership, "_writers", [])
        for name, value in (("instances", []), ("on_init", None), ("wait_error", None), ("output", b"probe fixture\n")):
            monkeypatch.setattr(Process, name, value)
        monkeypatch.setattr(Thread, "current", None)
        monkeypatch.setattr(owned, "_thread_type", Thread)
        monkeypatch.setattr(owned, "_current_thread", lambda: Thread.current)
        monkeypatch.setattr(pipe_io, "_popen_type", Process)
        monkeypatch.setattr(env_probe, "_PROBE_THREAD", None)
        monkeypatch.setattr(env_probe, "_PROBE_DONE", threading.Event())
        monkeypatch.setattr(env_probe, "_PROBE_GEN", 23)
        monkeypatch.setattr(env_probe, "_CACHED_LINE", None)
        monkeypatch.setattr(env_probe, "_build_probe_line", build or (lambda: env_probe._run(self.argv)[1]))
        self.processes, self.writers = Process.instances, process_ownership._writers

    def start(self):
        env_probe.warm_environment_probe_async()
        self.owner = self.source.environment_probe
        return self.owner

    def run(self):
        self.owner.thread.run()

    def close(self):
        self.runner._draining = True
        allow_normal_shutdown(self.source)
        owned.shutdown(env_probe, self.source)
