"""Actual factory/view/SDK/worker-join consumers; every HTTP/pool/connection constructor is mocked."""
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from agent import process_bootstrap
from gateway.run_owned_shutdown import ManagedSettlement
from tests.agent.primary_owned_fixture import primary_fixture
from tests.agent.auxiliary_owned_fixture import allow_normal_shutdown


class FakeConnection:
    def __init__(self):
        self._connection = SimpleNamespace(closed=False, _network_stream=object())
        self.calls = 0

    def close(self):
        self.calls += 1
        self._connection.closed = True

    def is_closed(self):
        return self._connection.closed


class FakePool:
    def __init__(self):
        self._connections, self._requests = [], []
        self._optional_thread_lock, self.calls = threading.RLock(), 0

    @property
    def connections(self):
        return list(self._connections)

    def create_connection(self, origin):
        return FakeConnection()

    def _close_connections(self, connections):
        for connection in connections:
            connection.close()

    def close(self):
        self.calls += 1
        with self._optional_thread_lock:
            connections = list(self._connections)
            self._connections = []
        self._close_connections(connections)


class FakeResponse:
    def __init__(self, pool):
        self.stream, self.is_closed, self.pool = object(), False, pool
        pool._requests.append(self)

    def close(self):
        self.is_closed = True
        self.pool._requests.remove(self)


class FakeTransport:
    def __init__(self, **kwargs):
        self._pool, self.calls = FakePool(), 0
        self.kwargs = kwargs

    def handle_request(self, request):
        connection = self._pool.create_connection("fixture")
        self._pool._connections.append(connection)
        return FakeResponse(self._pool)

    def close(self):
        self.calls += 1
        self._pool.close()


class FakeHTTP:
    def __init__(self, *, transport=None, mounts=None, **kwargs):
        self._transport, self._mounts = transport, mounts or {}
        self.is_closed, self.kwargs = False, kwargs

    def close(self):
        if self._transport is not None:
            self._transport.close()
        for transport in self._mounts.values():
            if transport is not None:
                transport.close()
        self.is_closed = True


def shared_fixture(monkeypatch, tmp_path, *, transport=FakeTransport):
    import httpx
    import httpcore
    actual_builder = process_bootstrap.build_keepalive_http_client
    source, runner = primary_fixture(monkeypatch, tmp_path)
    monkeypatch.setattr(process_bootstrap, "build_keepalive_http_client", actual_builder)
    monkeypatch.setattr(process_bootstrap, "_get_proxy_for_base_url", lambda base: None)
    monkeypatch.setattr(process_bootstrap, "_SHARED_TRANSPORTS", {})
    monkeypatch.setattr(httpx, "HTTPTransport", transport)
    monkeypatch.setattr(httpx, "Client", FakeHTTP)
    monkeypatch.setattr(httpcore, "ConnectionPool", FakePool)
    monkeypatch.setattr(httpcore, "HTTPConnection", FakeConnection)
    runner._executor_lock = threading.RLock()
    runner._executor, runner._housekeeping_executor = ThreadPoolExecutor(max_workers=1), None
    source.settlement = ManagedSettlement(source, runner)
    return source, runner


async def join_original(source):
    allow_normal_shutdown(source)
    await source.settlement._join_pools()


def request():
    return SimpleNamespace(extensions={})
