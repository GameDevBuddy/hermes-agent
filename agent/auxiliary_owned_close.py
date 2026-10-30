"""Invoke genuine auxiliary normal closers on their original existing owning loops."""
from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import threading

from hermes_owned_gateway import OwnedGatewayUnknown
from agent.auxiliary_owned import fail, find


def _closed(client):
    value = getattr(client, "is_closed", None)
    return (value() if callable(value) else value) is True


def _same_callable(left, right):
    return (left is right or (getattr(left, "__self__", None) is getattr(right, "__self__", None)
                             and getattr(left, "__func__", None) is not None
                             and left.__func__ is getattr(right, "__func__", None)))


def _claim_original_children(record):
    for child in record.children:
        child.require_current()
        if child.close_attempted or child.phase == "closed":
            raise OwnedGatewayUnknown("original-auxiliary-child-close-attempt-already-consumed")
        # Reserve before invoking the SDK method, which may close its injected HTTP client then fail.
        child.close_attempted, child.close_parent = True, record
        child.close_task, child.close_completion = record.close_task, record.close_completion
        child.close_dispatch = record.close_dispatch


def _require_original_close_owner(record, primary_snapshot=None):
    from hermes_constants import hermes_home_key
    record.require_current()
    if (asyncio.get_running_loop() is not record.owner_loop or hermes_home_key() != record.home_key
            or threading.current_thread() is not record.owner_thread):
        raise OwnedGatewayUnknown("original-auxiliary-loop-or-profile-changed")
    if (find(record.client) is not record
            or (record.inner is not None and getattr(record.client, "_client", None) is not record.inner)
            or any(child.owner_loop is not record.owner_loop or child.home_key != record.home_key
                   or child.owner_thread is not record.owner_thread for child in record.children)):
        raise OwnedGatewayUnknown("original-auxiliary-client-or-child-owner-changed")
    actual = getattr(record.client, "close", None) or getattr(record.client, "aclose", None)
    if not _same_callable(actual, record.close_method):
        raise OwnedGatewayUnknown("original-auxiliary-close-entry-changed-or-already-closed")
    for child in record.children:
        child.require_current()
    from agent.primary_owned import require_close_links
    require_close_links(record, primary_snapshot)


async def _normal_on_owner(record):
    try:
        _require_original_close_owner(record)
        if _closed(record.client):
            raise OwnedGatewayUnknown("original-auxiliary-close-entry-changed-or-already-closed")
        from agent.primary_owned import capture_close_links
        primary_snapshot = capture_close_links(record)
        _claim_original_children(record)
        record.close_result = record.close_method()
        if inspect.isawaitable(record.close_result):
            await record.close_result
        # Work may yield on its original owner loop. A prior observation cannot
        # settle a replaced runtime, child/client link, method or Job relation.
        _require_original_close_owner(record, primary_snapshot)
        if not _closed(record.client) or (record.inner is not None and not _closed(record.inner)):
            raise OwnedGatewayUnknown("original-auxiliary-normal-close-unconfirmed")
        if record.shared_refs:
            # Closing a _SharedTransport view does NOT close its actual process-shared pool.
            raise OwnedGatewayUnknown("auxiliary-shared-pool-original-normal-owner-unavailable")
        with record.source._state_lock:
            if record.error is not None:
                raise OwnedGatewayUnknown("original-auxiliary-close-remains-unknown")
            for child in record.children:
                if not _closed(child.client) or child.error is not None or child.shared_refs:
                    raise OwnedGatewayUnknown("original-auxiliary-child-close-unconfirmed")
                child.phase = "closed"
            record.phase = "closed"
    except BaseException as error:
        fail(record, error)
        raise


def _start_on_owner(record):
    try:
        record.close_coroutine = _normal_on_owner(record)
        record.close_task = record.owner_loop.create_task(record.close_coroutine, context=record.context)
        record.close_task.add_done_callback(lambda task: _complete(record, task))
    except BaseException as error:  # health: allow BLE001 -- Keep the original traceback and forward through the retained completion.
        fail(record, error)
        record.close_completion.set_exception(error)


def _complete(record, task):
    try:
        value = task.result()
    except BaseException as error:  # health: allow BLE001 -- Done-callback errors belong to the original retained completion, never a retry.
        fail(record, error)
        record.close_completion.set_exception(error)
    else:
        record.close_completion.set_result(value)


def _observe_waiter(record, future):
    if future.cancelled():
        return
    record.close_waiter_error = future.exception()


async def close_record(record):
    record.require_current()
    if record.phase == "empty":
        return
    if record.phase == "view":
        for child in record.children:
            await close_record(child)
        return
    if record.phase == "closed":
        if not _closed(record.client):
            raise OwnedGatewayUnknown("original-auxiliary-closed-state-changed")
        return
    try:
        if record.close_attempted:
            raise OwnedGatewayUnknown("original-auxiliary-normal-close-owner-unavailable")
        record.close_attempted = True
        if not record.supported:
            raise OwnedGatewayUnknown("original-auxiliary-normal-close-owner-unavailable")
        record.close_completion = concurrent.futures.Future()
        if record.owner_loop.is_closed() or not record.owner_loop.is_running():
            raise OwnedGatewayUnknown("original-auxiliary-owner-loop-unavailable")
        record.close_dispatch = record.owner_loop.call_soon_threadsafe(_start_on_owner, record, context=record.context)
        record.close_waiter = asyncio.wrap_future(record.close_completion)
        record.close_waiter.add_done_callback(lambda future: _observe_waiter(record, future))
        record.close_shield = asyncio.shield(record.close_waiter)
        await record.close_shield
    except BaseException as error:
        fail(record, error)
        raise


async def shutdown_owned(runner):
    from hermes_owned_gateway import active
    from agent import auxiliary_client as auxiliary
    source = active(runner)
    if source is None:
        raise OwnedGatewayUnknown("original-auxiliary-source-required")
    source.require_shutdown(runner)
    with auxiliary._client_cache_lock:
        cached = tuple(entry[0] for entry in auxiliary._client_cache.values())
    from agent.auxiliary_owned import retain_cache_client
    for client in cached:
        retain_cache_client(client)
    with source._state_lock:
        if source.auxiliary_closing_attempted:
            if source.auxiliary_closed:
                records = tuple(source.auxiliary_clients)
            else:
                raise source.unknown("original-auxiliary-shutdown-attempt-retained")
        else:
            source.auxiliary_closing_attempted = True
            records = tuple(source.auxiliary_clients)
    try:
        # Parent SDK owners close their original injected HTTP clients, never the reverse.
        children = {child for record in records if record is not None for child in record.children}
        for record in records:
            if record is None:
                raise OwnedGatewayUnknown("original-auxiliary-construction-hole-retained")
            if record not in children:
                await close_record(record)
        for record in records:
            if record.error is not None or record.phase not in ("closed", "view", "empty"):
                raise OwnedGatewayUnknown("original-auxiliary-resources-remain-held")
        source.auxiliary_closed = True
    except BaseException as error:
        source.auxiliary_error = source.auxiliary_error or error
        source.unknown("original-auxiliary-shutdown-unknown")
        raise
