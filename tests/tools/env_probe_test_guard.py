"""Probe source models plus only the original bounded legacy probe fixture worker."""
import threading
import subprocess

from tests.gateway.owned_entry_test_guard import pytest_configure as configure
from tests.gateway.owned_entry_test_guard import pytest_unconfigure

_Thread = threading.Thread
ORIGINAL_THREAD = threading.Thread
ORIGINAL_POPEN = subprocess.Popen
ORIGINAL_CURRENT_THREAD = threading.current_thread


def pytest_configure(config):
    configure(config)
    base = threading.Thread

    class ProbeFixtureThread(base):
        def __new__(cls, *args, **kwargs):
            target = kwargs.get("target", args[1] if len(args) > 1 else None)
            if (getattr(target, "__module__", None) == "tools.env_probe"
                    and getattr(target, "__name__", None) == "_probe_worker"):
                return _Thread.__new__(cls)
            if (getattr(target, "__module__", None) == "tests.tools.test_env_probe"
                    and getattr(target, "__qualname__", None) == (
                        "TestStuckProbeNeverBlocksCallers."
                        "test_hung_probe_fails_open_for_concurrent_callers.<locals>.caller")):
                return _Thread.__new__(cls)
            return base.__new__(cls, *args, **kwargs)

    config._native_sql_source_guards.setattr(threading, "Thread", ProbeFixtureThread)
