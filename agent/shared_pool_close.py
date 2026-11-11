"""Final original sync pool normal API after SDK/view users and original workers settle.

HTTPTransport/ConnectionPool have no closed bit. Their actual normal APIs, retained original
connections' public is_closed methods, empty original pool requests/connections and same
relationships are observations only; they prove no kernel graph or native authority.
"""
from __future__ import annotations

import asyncio
import inspect
import threading

from agent import auxiliary_owned as owned
from agent import shared_pool_owned as shared
from agent import shared_pool_links as links
from hermes_owned_gateway import OwnedGatewayUnknown


def begin_worker_join(settlement, pools):
    from gateway.run_owned_shutdown import ManagedSettlement
    source = settlement.source
    if not any(record is not None and record.kind == "shared-pool" for record in source.auxiliary_clients):
        return None
    source.require_shutdown(settlement.runner)
    if type(settlement) is not ManagedSettlement or source.settlement is not settlement:
        raise source.unknown("shared-pool-original-settlement-required")
    if getattr(settlement, "_shared_join", None) is not None:
        raise source.unknown("shared-pool-original-worker-join-reentry")
    claim = {"settlement": settlement, "source": source, "pools": tuple(pools), "threads": (),
             "request_workers": (), "standalone_threads": (), "done": False, "error": None}
    settlement._shared_join = claim  # One pre-work normal attempt; failed refs/latch never reset.
    try:
        threads = tuple(thread for pool in pools if pool is not None for thread in pool._threads)
        claim["request_workers"] = tuple((record, record.worker) for record in source.auxiliary_clients
                                         if record is not None and record.kind == "shared-request")
        for request, worker in claim["request_workers"]:
            shared.require_parent(request)
            if any(worker is thread for thread in (threading.current_thread(), source.loop_thread, *threads)):
                continue
            # Streaming uses a standalone request Thread, not an executor worker.
            if not isinstance(worker, threading.Thread):
                raise source.unknown("shared-pool-unrecognized-original-request-thread")
            claim["standalone_threads"] += (worker,)
            threads += (worker,)
        claim["threads"] = threads
        if len(threads) > 256:
            raise source.unknown("shared-pool-worker-reference-capacity")
    except BaseException as error:
        claim["error"] = error
        raise
    return claim


def finish_worker_join(claim):
    if claim is None:
        return
    settlement, source = claim["settlement"], claim["source"]
    source.require_shutdown(settlement.runner)
    _validate_join(claim)
    claim["done"] = True


def _validate_join(claim):
    settlement, source = claim["settlement"], claim["source"]
    current = (settlement.runner._executor, settlement.runner._housekeeping_executor)
    if (source.settlement is not settlement or settlement._shared_join is not claim
            or settlement.source is not source or settlement.task is not source.task
            or any(request.worker is not worker for request, worker in claim["request_workers"])
            or any(thread.is_alive() for thread in claim["threads"])
            or any(before is not after for before, after in zip(claim["pools"], current))
            or any(thread not in claim["threads"] for pool in current if pool is not None for thread in pool._threads)):
        raise source.unknown("shared-pool-original-workers-not-joined")


def require_workers(record):
    settlement = record.source.settlement
    claim = getattr(settlement, "_shared_join", None)
    if claim is None or not claim["done"] or claim["error"] is not None:
        shared.refuse(record, "shared-pool-original-worker-join-unavailable")
    _validate_join(claim)  # Re-read the actual original executor/thread relationships.
    for request in record.requests:
        shared.require_parent(request, record)
        request.require_current()
        if request.active or (request.worker not in (threading.current_thread(), record.source.loop_thread)
                              and (request.worker not in claim["threads"] or request.worker.is_alive())):
            shared.refuse(record, "shared-pool-original-request-worker-still-live")
        response = request.response
        if response is None or getattr(response, "is_closed", None) is not True:
            shared.refuse(record, "shared-pool-original-response-not-closed")
        stream = getattr(response, "stream", None)
        if stream is not request.response_stream:
            from httpx._client import BoundSyncStream
            if type(stream) is not BoundSyncStream or stream._stream is not request.response_stream:
                shared.refuse(record, "shared-pool-original-response-stream-changed")


def require_users(record):
    links.require_snapshot(record.shared_close_links)
    shared.require_pool(record)
    require_workers(record)
    for view in record.views:
        shared.require_parent(view, record)
        shared.require_view(view)
        if not view.view_close_attempted or not view.view_close_confirmed or not view.client._closed or not view.holders:
            shared.refuse(record, "shared-pool-original-view-holder-unsettled")
        for holder in view.holders:
            holder.require_current()
            if holder.phase not in ("closed", "sdk-shared-pending") or holder.error is not None:
                shared.refuse(record, "shared-pool-original-sdk-holder-unsettled")
            _require_closed_holder(holder)
    if record.pool._requests:
        shared.refuse(record, "shared-pool-original-requests-remain")
    for connection in record.pool.connections:
        item = owned.find(connection)
        if item is None or item.pool_record is not record:
            record.extra_refs = tuple(record.pool.connections)
            shared.refuse(record, "shared-pool-contained-connection-owner-unavailable")


def _require_closed_holder(record):
    from agent.auxiliary_owned_close import _closed
    from agent.primary_owned import require_close_links
    actual_close = getattr(record.client, "close", None) or getattr(record.client, "aclose", None)
    if (owned.find(record.client) is not record or not _closed(record.client)
            or not shared._same(actual_close, record.close_method)
            or (record.inner is not None and getattr(record.client, "_client", None) is not record.inner)):
        shared.refuse(record, "shared-pool-original-sdk-holder-closed-state-changed")
    require_close_links(record, record.primary_close_links)


def require_normal_context(record):
    from hermes_constants import hermes_home_key
    require_users(record)
    if (asyncio.get_running_loop() is not record.owner_loop or threading.current_thread() is not record.owner_thread
            or hermes_home_key() != record.home_key):
        shared.refuse(record, "shared-pool-original-normal-context-changed")


async def normal_pool_close(record):
    try:
        snapshot = record.shared_close_links
        require_normal_context(record)
        record.original_connections = tuple(record.pool.connections)
        original = next(values for name, values in snapshot.members if name == "connections")
        for item in original:
            shared.retain_contained(item)
        record.close_result = record.close_method()
        if inspect.isawaitable(record.close_result):
            await record.close_result
        record.normal_api_returned = True
        links.require_snapshot(snapshot)
        require_normal_context(record)
        if record.pool.connections:
            shared.refuse(record, "shared-pool-original-connections-remain")
        for item in original:
            item.require_current()
            require_connection_closed(item, record)
        links.require_snapshot(snapshot)
        record.phase = "closed"
    except BaseException as error:
        owned.fail(record, error)
        raise


def require_settled(record):
    require_users(record)
    if not record.close_attempted or not getattr(record, "normal_api_returned", False) or record.pool.connections:
        shared.refuse(record, "shared-pool-original-normal-close-unconfirmed")
    for connection in record.connections:
        require_connection_closed(connection, record)


def require_connection_closed(record, parent):
    shared.require_parent(record, parent)
    connection = record.client
    if (record.phase not in ("shared-connection-closed", "closed") or record.error is not None
            or connection.close is not record.connection_close_hook
            or not shared._same(connection.is_closed, record.is_closed_method)
            or getattr(connection, "_connection", None) is not record.original_inner
            or getattr(record.original_inner, "_network_stream", None) is not record.original_stream
            or record.is_closed_method() is not True):
        shared.refuse(record, "shared-pool-original-connection-close-unconfirmed")


async def close_all(records, snapshots):
    from agent.auxiliary_owned_close import close_record
    pools = tuple(record for record in records if record is not None and record.kind == "shared-pool")
    if len(pools) != len(snapshots) or any(record is not snapshot.owner for record, snapshot in zip(pools, snapshots)):
        raise OwnedGatewayUnknown("shared-original-shutdown-pool-membership-changed")
    for snapshot in snapshots:
        links.require_snapshot(snapshot)
    for record in pools:
        require_users(record)
    for record in pools:
        await close_record(record, shared_final=True)
    for snapshot in snapshots:
        links.require_snapshot(snapshot)
        require_settled(snapshot.owner)
    for record in records:
        if record is None:
            continue
        if record.kind in shared.KINDS:
            parent = record if record.kind == "shared-pool" else shared.require_parent(record)
            links.require_member(record, parent.shared_close_links)
            if parent.phase != "closed" or record.error is not None:
                shared.refuse(record, "shared-original-pool-remains-unsettled")
            if record.kind == "shared-connection":
                require_connection_closed(record, parent)
    for record in records:
        if record is not None and record.phase == "sdk-shared-pending":
            shared.sdk_views_closed(record)
            _require_closed_holder(record)
            if any(owned.find(item).kind not in ("shared-view", "shared-pool") for item in record.shared_refs):
                shared.refuse(record, "sdk-original-shared-pool-remains-unsettled")
    for snapshot in snapshots:
        links.require_snapshot(snapshot)
    for record in records:
        if record is not None and (record.kind in shared.KINDS or record.phase == "sdk-shared-pending"):
            record.phase = "closed"
