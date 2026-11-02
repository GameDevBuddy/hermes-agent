"""Retain failed original managed watchers without applying legacy respawn policy."""
from __future__ import annotations

import asyncio
import logging
import threading

from hermes_owned_gateway import OwnedGatewaySource, OwnedGatewayUnknown, active

logger = logging.getLogger("gateway.run")


def prepare_supervised(runner):
    source = active(runner)
    if source is None:
        return None
    source.require_child_work()
    loop = asyncio.get_running_loop()
    if source.loop is not loop or source.loop_thread is not threading.current_thread():
        raise OwnedGatewayUnknown("original-watcher-loop-and-thread-required")
    if loop.get_task_factory() is not None:
        raise OwnedGatewayUnknown("custom-managed-task-factory-is-not-supported")
    if type(runner._background_tasks) is not set:
        raise OwnedGatewayUnknown("original-background-task-set-required")
    return source


def observe_background(runner, task, tasks):
    try:
        source = prepare_supervised(runner)
    except OwnedGatewayUnknown as error:
        source = getattr(runner, "_owned_gateway_source", None)
        if type(source) is OwnedGatewaySource:
            # The caller has already created this task. Keep its actual object
            # before propagating refusal; never discard it to fake no work.
            retain_failure(source, runner, task, error)
            task.add_done_callback(lambda completed: retain_failure(source, runner, completed))
        raise
    if source is None:
        task.add_done_callback(tasks.discard)
        return

    def observed(completed):
        if not retain_failure(source, runner, completed):
            tasks.discard(completed)

    task.add_done_callback(observed)


def retain_failure(source, runner, task, error=None):
    if source is None:
        return False
    if error is None:
        try:
            error = task.exception()
        except asyncio.CancelledError as failure:
            error = failure
    if error is None:
        try:
            if active(runner) is not source or source.loop is not asyncio.get_running_loop():
                raise OwnedGatewayUnknown("original-watcher-source-changed")
        except OwnedGatewayUnknown as failure:
            error = failure
    if error is None:
        return False
    from gateway.owned_startup_construction import remember_failure
    first = remember_failure(source, error)
    with source._state_lock:
        if not any(value is task for value in source.unresolved_resource_refs):
            source.unresolved_resource_refs.append(task)
    source.unknown(str(first))
    runner._draining, runner._external_drain_active, runner._running = True, True, False
    descriptor = getattr(runner, "session_runtime_descriptor", None)
    if descriptor is not None:
        descriptor.update(state="unavailable", capabilities=[])
    logger.error("Managed watcher failed; retaining original task and resources",
                 exc_info=(type(first), first, source._startup_traceback))
    return True
