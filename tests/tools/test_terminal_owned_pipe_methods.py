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
