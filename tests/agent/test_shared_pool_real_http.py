"""Real httpx/httpcore streams must allow managed Quit after their workers join."""
import asyncio
import concurrent.futures
import contextvars
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import sqlite3
import threading
import time
from types import SimpleNamespace

import httpx
import pytest

from agent import auxiliary_client, auxiliary_owned as owned, process_bootstrap
from agent.auxiliary_owned_close import shutdown_owned
from gateway.run_owned_shutdown import ManagedSettlement
from tests.agent.auxiliary_owned_fixture import allow_normal_shutdown, bind_source


@pytest.mark.parametrize("mode", ["keep-alive", "connection-close", "abandoned"])
def test_real_stream_pool_shutdown(monkeypatch, tmp_path, mode):
    handler_threads = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            handler_threads.append(threading.current_thread())
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            if mode == "connection-close":
                self.send_header("Connection", "close")
            self.end_headers()
            try:
                for chunk in (b"first", b"second", b"third", b"fourth"):
                    self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                    self.wfile.flush()
                    time.sleep(0.02)
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass  # An abandoned body deliberately closes the peer early.

        def handle(self):
            try:
                super().handle()
            except (ConnectionResetError, ConnectionAbortedError):
                pass

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    serving = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    serving.start()

    async def scenario():
        source, runner = bind_source(monkeypatch, tmp_path)
        monkeypatch.setattr(auxiliary_client, "_client_cache", {})
        monkeypatch.setattr(auxiliary_client, "_resolve_aux_verify", lambda base: True)
        monkeypatch.setattr(process_bootstrap, "_SHARED_TRANSPORTS", {})
        monkeypatch.setattr(process_bootstrap, "_get_proxy_for_base_url", lambda base: None)
        runner._executor_lock = threading.RLock()
        runner._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        runner._housekeeping_executor = None
        settlement = source.settlement = ManagedSettlement(source, runner)
        sdk = auxiliary_client._create_openai_client(
            api_key="local-test", base_url=f"http://127.0.0.1:{server.server_port}/v1")
        client = sdk._client
        assert type(client) is httpx.Client
        pools = tuple(r for r in source.auxiliary_clients if r.kind == "shared-pool")
        assert pools and all(type(r.client) is httpx.HTTPTransport for r in pools)
        errors, chunks = [], []

        def stream():
            try:
                with client.stream("GET", f"http://127.0.0.1:{server.server_port}/body") as response:
                    for chunk in response.iter_raw():
                        chunks.append(chunk)
                        if mode == "abandoned" and len(chunks) == 2:
                            break
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=contextvars.copy_context().run, args=(stream,))
        worker.start()
        await asyncio.to_thread(worker.join, 5)
        assert not worker.is_alive() and not errors
        assert chunks == ([b"first", b"second"] if mode == "abandoned"
                          else [b"first", b"second", b"third", b"fourth"])
        records = tuple(r for pool in pools for r in pool.connections)
        assert len(records) == 1 and owned.find(records[0].client) is records[0]
        assert all(r.worker is worker for pool in pools for r in pool.requests)
        try:
            allow_normal_shutdown(source)
            await settlement._join_work()
            await settlement._join_pools()
            await shutdown_owned(runner)
            assert source.auxiliary_closed
            assert all(pool.pool.connections == [] for pool in pools)
            assert all(r.client.is_closed() for r in records)
            assert not worker.is_alive()
            assert not any(t.is_alive() for t in runner._executor._threads)
            # Only the unrelated process/listener setup is modeled here. Finish
            # uses the real auxiliary closer and real SQLite close_stores path.
            from gateway import run_owned_resources, status
            stores = [sqlite3.connect(tmp_path / name) for name in ("session.db", "routing.db")]
            runner._session_db = stores[0]
            runner.session_store = SimpleNamespace(_db=stores[1], close_all_db_handles=lambda: None)
            runner.close_all_session_db_handles = lambda: None
            source.job.require_only_current_process = lambda: None
            monkeypatch.setattr(run_owned_resources, "close_remaining_python_resources",
                                lambda value, keepalive: shutdown_owned(value))
            released = []
            monkeypatch.setattr(status, "remove_pid_file", lambda: released.append("pid"))
            monkeypatch.setattr(status, "release_gateway_runtime_lock", lambda: released.append("lock"))
            settlement._stop_completed = True
            await settlement.finish()
            assert settlement.finished and released == ["pid", "lock"]
            for store in stores:
                with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                    store.execute("select 1")
        finally:
            # Even a failing regression must release its real local sockets.
            for pool in pools:
                for record in pool.connections:
                    record.close_method()
                pool.pool._connections.clear()
            runner._executor.shutdown(wait=True)

    try:
        asyncio.run(scenario())
    finally:
        server.shutdown()
        server.server_close()
        serving.join(5)
        assert not serving.is_alive()
        assert not any(t.is_alive() for t in handler_threads)
