"""Entry source isolation plus only the original bounded lifecycle fixture threads."""
import threading

from tests.gateway.native_app_producer_guard import pytest_configure as configure
from tests.gateway.native_app_producer_guard import pytest_unconfigure


_Thread, _Event = threading.Thread, threading.Event


def pytest_configure(config):
    configure(config)
    base = threading.Thread

    class EntryThread(base):
        def __new__(cls, *args, **kwargs):
            target = kwargs.get("target", args[1] if len(args) > 1 else None)
            noop = (getattr(target, "__module__", None) == "tests.gateway.test_owned_gateway_lifecycle"
                    and getattr(target, "__qualname__", None) == "stop_resources.<locals>.<lambda>")
            setter = (getattr(target, "__func__", None) is _Event.set
                      and type(getattr(target, "__self__", None)) is _Event)
            if noop or setter:
                return _Thread.__new__(cls)
            return base.__new__(cls, *args, **kwargs)

    config._native_sql_source_guards.setattr(threading, "Thread", EntryThread)
