"""Actual fresh SQLite and declared source threads; other product factories remain models/refusing."""
import asyncio
import atexit
import concurrent.futures
import threading

from tests.agent.shared_pool_test_fakes import DriverThread, Executor
from tests.hermes_state.native_lease_test_guard import pytest_configure as configure_sql
from tests.hermes_state.native_lease_test_guard import pytest_unconfigure as unconfigure_sql

_Thread = threading.Thread


class SourceThread(_Thread):
    def __new__(cls, *args, **kwargs):
        target = kwargs.get("target")
        if (getattr(target, "__module__", None) == "tests.gateway.native_app_producer_fixture"
                and getattr(target, "__name__", None) in ("_control", "_worker")):
            return super().__new__(cls)
        if (getattr(target, "__module__", None) == "tests.hermes_state.test_native_writer_lease"
                and getattr(target, "__name__", None) in ("worker", "reader", "drain_after_reservation")):
            return super().__new__(cls)
        if getattr(target, "__func__", None) is asyncio.base_events.BaseEventLoop._do_shutdown:
            return super().__new__(cls)
        return DriverThread(*args, **kwargs)


def pytest_configure(config):
    configure_sql(config)
    patch = config._native_sql_source_guards
    patch.setattr(concurrent.futures, "ThreadPoolExecutor", Executor)
    patch.setattr(threading, "Thread", SourceThread)
    # Unexercised import-time TUI finalizers cannot own cleanup of this source-only fixture.
    patch.setattr(atexit, "register", lambda callback, *args, **kwargs: callback)


def pytest_unconfigure(config):
    unconfigure_sql(config)
