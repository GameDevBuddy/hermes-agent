"""One declared real Windows print child/readers; Job/runtime authority remains modeled."""
import asyncio
import json
import os
from pathlib import Path
import sys

import pytest

from tests.tools import env_probe_test_guard as guard
from tests.tools.env_probe_owned_fixture import Case
from tools import env_probe, env_probe_owned as owned, env_probe_owned_io as pipe_io


@pytest.mark.platforms("windows")
def test_original_print_child_and_both_real_reader_threads_finish_normally(monkeypatch, tmp_path):
    async def scenario():
        assert tmp_path.resolve().is_relative_to(Path(os.environ["TEMP"]).resolve())
        case = Case(monkeypatch, tmp_path)
        Path(case.environment["HERMES_HOME"]).mkdir()
        case.environment["SystemRoot"] = os.environ["SystemRoot"]
        argv = [sys._base_executable, "-I", "-S", "-B", "-c", "print('probe-native-fixture')"]
        children = []

        class PrintChild(guard.ORIGINAL_POPEN):
            def __init__(self, original_argv, **kwargs):
                assert original_argv is argv
                assert kwargs["env"] is case.environment and kwargs["cwd"] == case.environment["HERMES_HOME"]
                assert kwargs["close_fds"] is True and kwargs["stdin"] == -3
                super().__init__(original_argv, **kwargs)
                children.append(self)
                print(json.dumps({"event": "original_print_child_started", "pid": self.pid,
                                  "binary": original_argv[0], "argv": original_argv[1:],
                                  "cwd": kwargs["cwd"], "environment": kwargs["env"],
                                  "lowerJobIsModel": True}), flush=True)

            def kill(self):
                raise AssertionError("normal diagnostic test cannot kill its child")

            terminate = kill

        monkeypatch.setattr(owned, "_thread_type", guard.ORIGINAL_THREAD)
        monkeypatch.setattr(owned, "_current_thread", guard.ORIGINAL_CURRENT_THREAD)
        monkeypatch.setattr(pipe_io, "_popen_type", PrintChild)
        monkeypatch.setattr(env_probe, "_build_probe_line", lambda: env_probe._run(argv, timeout=3)[1])
        owner = case.start()
        assert env_probe._PROBE_DONE.wait(10), "original diagnostic worker did not finish"
        case.close()  # Joins the SAME original worker, after its original child/readers.
        record, = owner.commands
        child, = children
        assert child.returncode == record["exit"] == 0 and record["eof"] == [True, True]
        assert record["process"]["original"] is child and record["finished"]
        assert bytes(record["output"][0]) == b"probe-native-fixture\r\n"
        assert bytes(record["output"][1]) == b"" and child.stdout.closed and child.stderr.closed
        assert len(case.writers) == 3 and all(not thread.is_alive() for thread in case.writers)
        assert owner.closed and case.source._environment_probe_error is None
        print(json.dumps({"event": "original_print_child_complete", "pid": child.pid,
                          "exitCode": child.returncode, "bothOriginalEof": True,
                          "originalThreadIds": [thread.ident for thread in case.writers],
                          "allOriginalThreadsJoined": True, "sourceInterpreter": sys.version.split()[0],
                          "lowerJobIsModel": True, "AppStarted": False,
                          "nativeToolCalls": 0, "sourceReady": False}), flush=True)

    asyncio.run(scenario())
