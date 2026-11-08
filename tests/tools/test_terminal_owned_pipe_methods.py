"""Managed terminal workers with real OS pipes, without launching a shell or App."""
import asyncio
from contextlib import ExitStack
import io
import os
from types import BuiltinMethodType

import pytest

from hermes_owned_gateway import active
from tools import terminal_tool_owned as owned

pytest_plugins = ("tests.tools.terminal_owned_fixture",)


@pytest.mark.parametrize("stdin_data", [None, "original input\n"])
def test_original_builtin_pipe_methods_complete_without_losing_source(monkeypatch, tmp_path, stdin_data):
    async def scenario():
        from tools.environments import local
        from tests.tools.terminal_owned_fixture import Process, finish, terminal_fixture

        source, runner, _terminal, _plan, _created = terminal_fixture(monkeypatch, tmp_path)
        processes, input_readers = [], []
        with ExitStack() as pipes:
            def factory(*args, **kwargs):
                proc = Process(*args, **kwargs)
                read_fd, write_fd = os.pipe()
                proc.stdout = pipes.enter_context(os.fdopen(read_fd, "r", encoding="utf-8"))
                with os.fdopen(write_fd, "w", encoding="utf-8", newline="") as output:
                    output.write("original output\n")
                if proc.stdin is not None:
                    read_fd, write_fd = os.pipe()
                    input_readers.append(pipes.enter_context(os.fdopen(read_fd, "rb")))
                    proc.stdin = pipes.enter_context(os.fdopen(write_fd, "w", encoding="utf-8"))
                processes.append(proc)
                return proc

            monkeypatch.setattr(local.subprocess, "Popen", factory)
            env = local.LocalEnvironment(cwd=str(tmp_path), persistent=False)
            result = env.execute("fixture command", stdin_data=stdin_data)
            assert result == {"output": "original output\n", "returncode": 0}
            assert source.terminal_error is None and active(runner) is source
            process = owned.find(processes[-1])
            assert process.wait_done and process.reader.eof
            for worker in (process.reader, process.writer):
                if worker is not None:
                    assert type(worker.close_method) is BuiltinMethodType
                    assert worker.thread is worker.obj and not worker.thread.is_alive()
                    assert worker.done and worker.normal_closed and worker.error is None
            if stdin_data is not None:
                assert input_readers[-1].read() == stdin_data.encode("utf-8")
            await finish(source)
            assert source.terminal_closed

    asyncio.run(scenario())


@pytest.mark.parametrize("leave_survivor", [False, True])
def test_stop_running_command_and_use_same_environment(monkeypatch, tmp_path, leave_survivor):
    import subprocess
    import sys
    import threading
    import time
    import psutil
    from hermes_owned_gateway import OwnedGatewayUnknown
    from tools import interrupt
    from tools.environments import local
    from tests.tools.terminal_owned_fixture import terminal_fixture, finish

    real_popen = subprocess.Popen
    marker = tmp_path / "child.pid"
    child_code = "import time; time.sleep(60)"
    parent_code = (
        "import subprocess,sys,time; from pathlib import Path; "
        f"p=subprocess.Popen([sys.executable,'-c',{child_code!r}]); "
        f"Path({str(marker)!r}).write_text(str(p.pid)); time.sleep(60)"
    )
    processes, descendants = [], []

    async def scenario():
        source, runner, _terminal, _plan, _created = terminal_fixture(monkeypatch, tmp_path)
        env = local.LocalEnvironment(cwd=str(tmp_path), persistent=False)

        def spawn(args, **kwargs):
            if args[0] != "fixture-shell-not-executed":
                return real_popen(args, **kwargs)
            code = parent_code if not processes else "print('next call works')"
            kwargs["env"] = os.environ.copy()
            proc = real_popen([sys._base_executable, "-c", code], **kwargs)
            processes.append(proc)
            return proc

        monkeypatch.setattr(local.subprocess, "Popen", spawn)

        if leave_survivor:
            # A failed tree teardown kills only the wrapper, leaving inherited
            # stdout open in its children. Stop must not announce completion.
            name = "_kill_process_windows" if local._IS_WINDOWS else "_kill_process_group_posix"
            monkeypatch.setattr(local, name, lambda proc: proc.kill())
        tid = threading.get_ident()
        cancel_watcher = threading.Event()

        def stop_when_running():
            until = time.monotonic() + 10
            while not cancel_watcher.wait(.01) and time.monotonic() < until:
                if marker.exists() and marker.read_text():
                    descendants.extend(psutil.Process(processes[0].pid).children(recursive=True))
                    interrupt.set_interrupt(True, tid)
                    return

        watcher = threading.Thread(target=stop_when_running)
        watcher.start()
        try:
            if leave_survivor:
                with pytest.raises(OwnedGatewayUnknown, match="descendant-still-running"):
                    env.execute("running command", timeout=30)
                assert source.terminal_error is not None
            else:
                result = env.execute("running command", timeout=30)
                assert result["returncode"] == 130 and result["hermes_interrupted"], result
                assert "interrupted by user" in result["output"]
                assert descendants and not psutil.wait_procs(descendants, timeout=2)[1]
                assert processes[0].poll() is not None
                process = owned.find(processes[0])
                assert process.wait_done and process.reader.eof and not process.reader.thread.is_alive()
                assert source.terminal_error is None and active(runner) is source
                interrupt.set_interrupt(False, tid)
                next_result = env.execute("printf 'next call works'")
                assert next_result["returncode"] == 0
                assert "next call works" in next_result["output"]
                await finish(source)
                assert source.terminal_closed
        finally:
            cancel_watcher.set()
            watcher.join(timeout=10)
            interrupt.set_interrupt(False, tid)
            for child in reversed(descendants):
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            for proc in processes:
                if proc.poll() is None:
                    proc.kill()
                proc.wait(timeout=5)

    asyncio.run(scenario())


@pytest.mark.parametrize("method", ["read", "write", "close"])
def test_builtin_pipe_method_comparison_keeps_receiver_and_function_identity(method):
    from tools.environments.local_owned_io import _same_pipe_method

    with io.BytesIO() as original, io.BytesIO() as foreign:
        retained = getattr(original, method)
        assert _same_pipe_method(getattr(original, method), retained)
        assert not _same_pipe_method(getattr(foreign, method), retained)
        assert not _same_pipe_method(original.flush, retained)

        class Impostor:
            __self__ = original
            __name__ = method

            def __eq__(self, other) -> bool:
                raise AssertionError("custom callable equality must not authorize pipe IO")

        assert not _same_pipe_method(Impostor(), retained)
