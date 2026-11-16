"""Actual HTTP bodies, standalone stream workers and executor request reuse."""
import asyncio
import concurrent.futures
import contextvars
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

import pytest

from agent import auxiliary_client, auxiliary_owned as owned, process_bootstrap
from agent.auxiliary_owned_close import shutdown_owned
from agent.shared_pool_reclaim import prune_completed
from gateway.run_owned_shutdown import ManagedSettlement
from tests.agent.auxiliary_owned_fixture import allow_normal_shutdown, bind_source


@pytest.mark.parametrize("executor_worker", [False, True])
@pytest.mark.parametrize("keep_alive", [False, True])
def test_completed_http_requests_return_source_and_process_credit(monkeypatch, tmp_path, executor_worker, keep_alive):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "4")
            if not keep_alive:
                self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(b"real")
            self.wfile.flush()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    serving = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .01})
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
        source.settlement = ManagedSettlement(source, runner)
        sdk = auxiliary_client._create_openai_client(api_key="local-test", base_url=f"http://127.0.0.1:{server.server_port}/v1")
        baseline = len(source.auxiliary_clients), source.auxiliary_credit, owned._claim_count, len(owned._source_claims)
        pools = tuple(r for r in source.auxiliary_clients if r.kind == "shared-pool")
        seen = []

        def request():
            with sdk._client.stream("GET", f"http://127.0.0.1:{server.server_port}/body") as response:
                assert b"".join(response.iter_raw()) == b"real"
            seen.append(response)

        try:
            # More completed claims than the unchanged 1,024-entry limit. One
            # source, real HTTP connection reuse/close, no counter resets.
            for _ in range(540):
                context = contextvars.copy_context()
                if executor_worker:
                    await asyncio.wrap_future(runner._executor.submit(context.run, request))
                else:
                    worker = threading.Thread(target=context.run, args=(request,))
                    worker.start()
                    await asyncio.to_thread(worker.join, 5)
                    assert not worker.is_alive()
                with source._state_lock:
                    prune_completed(source)
                retained_connections = sum(len(pool.connections) for pool in pools)
                assert len(source.auxiliary_clients) == baseline[0] + retained_connections
                assert source.auxiliary_credit == baseline[1] + retained_connections * owned.CLAIM_CREDIT
                assert owned._claim_count == baseline[2] + retained_connections
                assert len(owned._source_claims) == baseline[3] + retained_connections
                assert all(not pool.requests for pool in pools)
            assert len(seen) == 540 and all(response.is_closed for response in seen)
            assert not source.auxiliary_error
            # A still-open body is not a completed transport request. Even
            # after close, its standalone owner must finish before reclamation.
            entered, read_body, body_closed, exit_worker = (threading.Event() for _ in range(4))

            def held_request():
                with sdk._client.stream("GET", f"http://127.0.0.1:{server.server_port}/held") as response:
                    entered.set()
                    assert read_body.wait(5)
                    assert b"".join(response.iter_raw()) == b"real"
                body_closed.set()
                assert exit_worker.wait(5)

            worker = threading.Thread(target=contextvars.copy_context().run, args=(held_request,))
            worker.start()
            try:
                assert await asyncio.to_thread(entered.wait, 5)
                request_record = next(r for pool in pools for r in pool.requests if r.worker is worker)
                before = len(source.auxiliary_clients), source.auxiliary_credit, owned._claim_count
                with source._state_lock:
                    prune_completed(source)
                assert (len(source.auxiliary_clients), source.auxiliary_credit, owned._claim_count) == before
                read_body.set()
                assert await asyncio.to_thread(body_closed.wait, 5)
                with source._state_lock:
                    prune_completed(source)
                assert request_record in source.auxiliary_clients
            finally:
                read_body.set()
                exit_worker.set()
                await asyncio.to_thread(worker.join, 5)
            assert not worker.is_alive()
            with source._state_lock:
                prune_completed(source)
            assert request_record not in source.auxiliary_clients
            # Repeating reclamation cannot refund twice.
            after = len(source.auxiliary_clients), source.auxiliary_credit, owned._claim_count
            with source._state_lock:
                prune_completed(source)
            assert (len(source.auxiliary_clients), source.auxiliary_credit, owned._claim_count) == after
            allow_normal_shutdown(source)
            await source.settlement._join_work()
            await source.settlement._join_pools()
            await shutdown_owned(runner)
            assert source.auxiliary_closed
        finally:
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


def test_incomplete_request_constructor_stays_charged(monkeypatch, tmp_path):
    async def scenario():
        source, _runner = bind_source(monkeypatch, tmp_path)
        # Reclamation must ignore incomplete constructors and foreign-shaped
        # entries rather than allocating around or discarding them.
        record = owned.reserve("shared-request", object)
        initial = len(source.auxiliary_clients), source.auxiliary_credit, owned._claim_count
        with source._state_lock:
            prune_completed(source)
        assert (len(source.auxiliary_clients), source.auxiliary_credit, owned._claim_count) == initial
        assert source.auxiliary_clients == [record]
    asyncio.run(scenario())
