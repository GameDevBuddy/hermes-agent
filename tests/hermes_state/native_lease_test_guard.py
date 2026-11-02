"""Pre-import guards for fresh SQLite and declared ordinary test-thread validation."""
import concurrent.futures
import socket
import subprocess

import pytest

from tests.agent.shared_pool_test_guard import guard_socketpair


def deny(*_args, **_kwargs):
    raise AssertionError("SessionDB source validation cannot start product IO or a diagnostic process")


def blocked_class(base):
    class Blocked(base):
        def __new__(cls, *args, **kwargs):
            deny()
    return Blocked


def pytest_configure(config):
    import httpcore
    import httpx
    import openai
    import requests
    patch = config._native_sql_source_guards = pytest.MonkeyPatch()
    patch.setattr(subprocess, "Popen", blocked_class(subprocess.Popen))
    patch.setattr(subprocess, "run", deny)
    patch.setattr(concurrent.futures, "ThreadPoolExecutor", blocked_class(concurrent.futures.ThreadPoolExecutor))
    patch.setattr(concurrent.futures, "ProcessPoolExecutor", blocked_class(concurrent.futures.ProcessPoolExecutor))
    guard_socketpair(patch)  # Only the test driver's original local asyncio socketpair may connect.
    patch.setattr(socket, "create_connection", deny)
    patch.setattr(socket, "getaddrinfo", deny)
    for name in ("Client", "AsyncClient", "HTTPTransport", "AsyncHTTPTransport"):
        patch.setattr(httpx, name, blocked_class(getattr(httpx, name)))
    for name in ("ConnectionPool", "AsyncConnectionPool", "HTTPConnection"):
        patch.setattr(httpcore, name, blocked_class(getattr(httpcore, name)))
    patch.setattr(requests, "get", deny)
    patch.setattr(requests, "post", deny)
    patch.setattr(openai, "OpenAI", deny)
    patch.setattr(openai, "AsyncOpenAI", deny)
    import hermes_state
    # Original busy behavior remains under test; diagnostics may not enumerate estate processes.
    patch.setattr(hermes_state, "log_write_lock_holders", lambda *_args, **_kwargs: None)


def pytest_unconfigure(config):
    patch = getattr(config, "_native_sql_source_guards", None)
    if patch is not None:
        patch.undo()
