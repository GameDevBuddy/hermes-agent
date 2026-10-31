"""Existing pre-import product guards, with SQLite restricted to this fresh private scope."""
from pathlib import Path
import sqlite3
import sys
import threading
from urllib.parse import unquote

from tests.agent import native_sql_constructor_guard as inherited
from tests.agent.shared_pool_test_fakes import DriverThread


class CanonicalSourceThread(DriverThread):
    """Model the unrelated heartbeat factory only; no thread or heartbeat body runs."""
    def start(self):
        if (getattr(self.target, "__module__", None) == "agent.tool_executor"
                and getattr(self.target, "__name__", None) == "_run_tool_activity_heartbeat"):
            self.alive = False
            return
        super().start()


def source_connect(database, *args, **kwargs):
    value = str(database)
    if value == ":memory:":
        return inherited._connect(database, *args, **kwargs)
    if value.startswith("file:"):
        value = unquote(value[5:].split("?", 1)[0])
    if not Path(value).resolve().is_relative_to(Path(sys.prefix).resolve().parent):
        raise RuntimeError("Canonical constructor fixture outside fresh declared scope")
    return inherited._connect(database, *args, **kwargs)


def pytest_configure(config):
    inherited.pytest_configure(config)
    config._shared_source_guards.setattr(sqlite3, "connect", source_connect)
    config._shared_source_guards.setattr(threading, "Thread", CanonicalSourceThread)


def pytest_unconfigure(config):
    inherited.pytest_unconfigure(config)
