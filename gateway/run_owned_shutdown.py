"""Normal settlement of the canonical managed lifecycle's resource objects.

No timeout, cancellation, interrupt, subprocess sweep, signal, hard exit,
adoption or restart is used as a close receipt. Unproved resources remain held.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import threading

from hermes_owned_gateway import OwnedGatewayUnknown


async def join_task(task):
    if not isinstance(task, (asyncio.Future, concurrent.futures.Future)):
        raise OwnedGatewayUnknown("unrecognized-owned-work-object")
    if isinstance(task, concurrent.futures.Future):
        task = asyncio.wrap_future(task)
    await asyncio.shield(task)


async def join_thread(thread):
    from cron.scheduler_thread import SupervisedTickerThread

    # The gateway's cron starter returns its supervised handle, not a Thread.
    # Join the actual worker retained by that handle; shutdown has already set
    # its stop event, so housekeeping cannot intentionally respawn the ticker.
    ticker = thread if type(thread) is SupervisedTickerThread else None
    if ticker is not None:
        thread = ticker._thread
    if not isinstance(thread, threading.Thread):
        raise OwnedGatewayUnknown("unrecognized-owned-writer-thread")
    if thread.ident is not None:
        await asyncio.to_thread(thread.join)
    if thread.is_alive() or (ticker is not None and ticker._thread is not thread):
        raise OwnedGatewayUnknown("owned-writer-did-not-finish")


class ManagedSettlement:
    def __init__(self, source, runner):
        source.require_bound(runner)
        self.source, self.runner = source, runner
        self.task, self.loop = asyncio.current_task(), asyncio.get_running_loop()
        self.control = self.listener = self.authorities = None
        self.cron_stop = self.cron = self.cron_thread = self.housekeeping = None
        self.keepalive = self.integrity_writer = None
        self.finished = False
        self._stop_attempted = False
        self._stop_completed = False
        self._finish_attempted = False
        self.resources = []

    def bind_control(self, server):
        if self.control is not None or server is not self.runner.session_control_server:
            raise OwnedGatewayUnknown("different-canonical-control-server")
        self.control = server
        self.resources.append(server)

    def bind_runtime(self, listener, authorities):
        if (self.listener is not None or listener is not self.runner.session_api
                or authorities is not self.runner.session_authorities):
            raise OwnedGatewayUnknown("different-canonical-runtime-initializer")
        self.listener, self.authorities = listener, authorities
        self.resources.extend((listener, authorities))

    def bind_schedulers(self, stop, provider, cron_thread, housekeeping):
        if self.cron_stop is not None:
            raise OwnedGatewayUnknown("scheduler-initializer-repeated")
        self.cron_stop, self.cron, self.cron_thread, self.housekeeping = stop, provider, cron_thread, housekeeping
        self.resources.extend((stop, provider, cron_thread, housekeeping))

    def bind_keepalive(self, thread):
        self.keepalive = thread
        if thread is not None:
            self.resources.append(thread)

    def bind_integrity_writer(self, thread):
        if not isinstance(thread, threading.Thread):
            raise OwnedGatewayUnknown("integrity-writer-initializer-did-not-return-its-thread")
        self.integrity_writer = thread
        self.resources.append(thread)

    def require_task(self):
        self.source.require_bound(self.runner)
        if self.source._original_settlement_construction is not None:
            from gateway.owned_startup_construction import require_original
            require_original(self.source, "settlement", self)
            if self.source.settlement is not self:
                raise OwnedGatewayUnknown("original-settlement-publication-changed")
        if self.task is not asyncio.current_task() or self.loop is not asyncio.get_running_loop():
            raise OwnedGatewayUnknown("different-normal-settlement-task")

    async def _join_authority_work(self, authority):
        from gateway.session_runtime_workers import physical_workers, executing_sessions, mutation_tasks, side_agents
        from gateway.run_runtime import _authority_tasks
        for worker, _holder in physical_workers(authority):
            await asyncio.to_thread(worker.worker_done.wait)
        for thread, _holder in side_agents(authority):
            await join_thread(thread)
        for task in _authority_tasks(authority):
            await join_task(task)
        if (list(physical_workers(authority)) or list(executing_sessions(authority))
                or mutation_tasks(authority) or side_agents(authority)):
            raise OwnedGatewayUnknown("authority-still-has-physical-or-mutation-work")

    async def _join_work(self):
        from gateway.session_authorities import all_authorities
        from gateway.platforms.api_server_runs import api_worker_live_count
        from cron import scheduler
        for authority in all_authorities(self.runner):
            await self._join_authority_work(authority)
        from cron.scheduler_owned_close import join_work
        await join_work(self)
        with scheduler._running_lock:
            cron_futures = tuple(scheduler._running_futures.values())
        for future in cron_futures:
            if future is scheduler._FUTURE_PENDING:
                raise OwnedGatewayUnknown("cron-submission-not-yet-associated-with-its-future")
            await join_task(future)
        deferred = getattr(self.runner, "_deferred_agent_workers", None)
        if deferred is not None and type(deferred) is not dict:
            raise OwnedGatewayUnknown("deferred-worker-registry-unreadable")
        for future in tuple(deferred if deferred is not None else ()):
            await join_task(future)
        # The canonical counter is incremented before submission and decremented
        # in the actual worker's finally. No import/read failure is mapped to zero;
        # the real default executor is joined later, before any database close.
        while api_worker_live_count():
            await asyncio.sleep(0.05)
        if scheduler.get_running_job_ids() or self.runner._running_agents:
            raise OwnedGatewayUnknown("unmapped-active-work-remains")

    async def stop(self):
        self.require_task()
        if self._stop_attempted or self.finished:
            raise OwnedGatewayUnknown("normal-stop-already-attempted")
        self._stop_attempted = True
        self.source.require_shutdown(self.runner)
        if any(value is None for value in (self.control, self.listener, self.authorities, self.cron_stop)):
            raise OwnedGatewayUnknown("incomplete-canonical-resource-initialization")
        self.source.phase = "draining"
        self.runner._external_drain_active = True
        self.runner._draining = True
        self.runner.session_runtime_descriptor.update(state="draining", capabilities=[])
        # Cooperative predicates stop new watcher ticks before accepted work is
        # joined. Neither the event nor the predicate is a completion receipt.
        self.runner._running = False
        self.runner._shutdown_event.set()
        self.cron_stop.set()
        self.cron.stop()
        await join_thread(self.cron_thread)
        await join_thread(self.housekeeping)
        await self._join_work()
        from gateway.run_owned_resources import close_authorities, close_adapters, close_agent_resources, close_tool_resources
        await close_authorities(self.runner)
        await close_agent_resources(self.runner)
        await close_adapters(self.runner)
        await close_tool_resources(self.source)
        for task in tuple(self.runner._background_tasks):
            if task is not self.task:
                await join_task(task)
        await self._close_listeners()
        await self._join_pools()
        self._stop_completed = True

    async def _close_listeners(self):
        if self.listener.server.config.timeout_graceful_shutdown is not None:
            raise OwnedGatewayUnknown("listener-still-has-forced-cancellation-policy")
        self.listener.server.should_exit = True
        await join_task(self.listener.task)
        if self.listener.socket.fileno() != -1:
            raise OwnedGatewayUnknown("canonical-listener-socket-remains-open")
        await self.control.stop_owned()
        self.runner.session_ticket_store.revoke()

    async def _join_pools(self):
        with self.runner._executor_lock:
            self.runner._executor_closing = True
            pools = [self.runner._executor, self.runner._housekeeping_executor]
        from agent.shared_pool_close import begin_worker_join, finish_worker_join
        from cron.scheduler_owned_close import retained_pools, join_pools
        claim = begin_worker_join(self, pools + list(retained_pools(self.source)))
        try:
            for pool in pools:
                if pool is not None:
                    self.resources.append(pool)
                    await asyncio.to_thread(pool.shutdown, wait=True, cancel_futures=False)
                    if any(thread.is_alive() for thread in pool._threads):
                        raise OwnedGatewayUnknown("canonical-executor-still-has-writers")
            await join_pools(self)
            if claim is not None:
                for thread in claim["standalone_threads"]:
                    await join_thread(thread)
            finish_worker_join(claim)
        except BaseException as error:
            if claim is not None:
                claim["error"] = claim["error"] or error
            raise

    async def finish(self):
        self.require_task()
        if not self._stop_completed or self.source.phase != "draining":
            raise OwnedGatewayUnknown("normal-stop-not-complete")
        if self._finish_attempted or self.finished:
            raise OwnedGatewayUnknown("normal-finish-already-attempted")
        self._finish_attempted = True
        from gateway.run_owned_resources import close_remaining_python_resources, close_stores
        await close_remaining_python_resources(self.runner, self.keepalive)
        if self.integrity_writer is not None:
            await join_thread(self.integrity_writer)
        await self.loop.shutdown_asyncgens()
        await self.loop.shutdown_default_executor()
        from cron.scheduler_owned_close import require_closed
        require_closed(self.source)
        if asyncio.all_tasks(self.loop) - {self.task}:
            raise OwnedGatewayUnknown("unsettled-loop-work-is-not-normal-exit")
        self.source.job.require_only_current_process()
        if self.source.child_transports:
            raise OwnedGatewayUnknown("owned-stdio-transports-still-open")
        close_stores(self.runner)
        from gateway.status import remove_pid_file, release_gateway_runtime_lock
        remove_pid_file()
        release_gateway_runtime_lock()
        self.finished = True

    def require_settled(self, source, runner):
        self.require_task()
        if (source is not self.source or runner is not self.runner or not self.finished
                or not self._stop_completed or not self._finish_attempted):
            raise OwnedGatewayUnknown("actual-normal-settlement-body-required")
        if (not self.listener.task.done() or self.listener.task.cancelled()
                or self.listener.socket.fileno() != -1 or source.child_transports):
            raise OwnedGatewayUnknown("settled-resource-state-changed")
        source.job.require_only_current_process()
