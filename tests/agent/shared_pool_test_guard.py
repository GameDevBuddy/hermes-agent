"""Explicit pytest plugin: install lower product guards before selected source/test imports."""
import concurrent.futures
import atexit
import contextvars
import socket
import sqlite3
import subprocess
import threading

import pytest

from tests.agent.shared_pool_test_fakes import DriverThread, Executor, deny

_driver_socketpair = contextvars.ContextVar("_source_driver_socketpair", default=False)


def guard_socketpair(patch):
    original_pair, original_connect = socket.socketpair, socket.socket.connect

    def pair(*args, **kwargs):
        # Windows' real asyncio driver uses a local socketpair self-pipe. Only
        # the original synchronous socketpair factory may use this exception.
        token = _driver_socketpair.set(True)
        try:
            return original_pair(*args, **kwargs)
        finally:
            _driver_socketpair.reset(token)

    def connect(obj, address):
        if not _driver_socketpair.get():
            deny()
        return original_connect(obj, address)

    patch.setattr(socket, "socketpair", pair)
    patch.setattr(socket.socket, "connect", connect)


def blocked_class(base):
    # Preserve an importable class shape for SDK definitions that subclass HTTP classes.
    class Blocked(base):
        def __new__(cls, *args, **kwargs):
            deny()
    return Blocked


def pytest_configure(config):
    import httpx
    import httpcore
    import requests
    patch = config._shared_source_guards = pytest.MonkeyPatch()
    patch.setattr(concurrent.futures, "ThreadPoolExecutor", Executor)
    patch.setattr(threading, "Thread", DriverThread)
    patch.setattr(atexit, "register", lambda callback, *args, **kwargs: callback)
    patch.setattr(subprocess, "Popen", blocked_class(subprocess.Popen))
    patch.setattr(subprocess, "run", deny)
    patch.setattr(socket, "create_connection", deny)
    patch.setattr(socket, "getaddrinfo", deny)
    guard_socketpair(patch)
    patch.setattr(sqlite3, "connect", deny)
    for name in ("Client", "AsyncClient", "HTTPTransport", "AsyncHTTPTransport"):
        patch.setattr(httpx, name, blocked_class(getattr(httpx, name)))
    for name in ("ConnectionPool", "AsyncConnectionPool", "HTTPConnection"):
        patch.setattr(httpcore, name, blocked_class(getattr(httpcore, name)))
    patch.setattr(requests, "get", deny)
    patch.setattr(requests, "post", deny)
    import openai
    patch.setattr(openai, "OpenAI", deny)
    patch.setattr(openai, "AsyncOpenAI", deny)


def pytest_unconfigure(config):
    patch = getattr(config, "_shared_source_guards", None)
    if patch is not None:
        patch.undo()
