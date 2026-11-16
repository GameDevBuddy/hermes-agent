"""Return completed HTTP bookkeeping without closing a live client or shared pool."""
from __future__ import annotations

import threading

from agent import auxiliary_owned as owned
from agent import shared_pool_owned as shared


def _worker_settled(record):
    worker = record.worker
    if not isinstance(worker, threading.Thread):
        return False
    if not worker.is_alive():
        return True
    # Non-streaming uses persistent executor threads. Their original executor
    # remains in the normal shutdown join even after this request is returned.
    return any(worker in pool._threads for pool in (
        getattr(record.runner, "_executor", None),
        getattr(record.runner, "_housekeeping_executor", None),
    ) if pool is not None)


def _response_closed(record):
    response = record.response
    if response is None or getattr(response, "is_closed", None) is not True:
        return False
    stream = getattr(response, "stream", None)
    if stream is record.response_stream:
        return True
    from httpx._client import BoundSyncStream
    return type(stream) is BoundSyncStream and stream._stream is record.response_stream


def _release(record):
    # Caller holds the source lock and has confirmed this exact terminal record.
    source = record.source
    with owned._index_lock:
        if owned._client_records.get(id(record.client)) is record:
            del owned._client_records[id(record.client)]
        owned._source_claims.remove(source)
        owned._claim_count -= 1
    source.auxiliary_clients.remove(record)
    source.auxiliary_credit -= owned.CLAIM_CREDIT
    if record.primary_owner is not None:
        owner = record.primary_owner
        owner.children = tuple(child for child in owner.children if child is not record)


def prune_completed(source):
    """Called under the source lock before reserving more work, never during Quit."""
    if source.auxiliary_closing_attempted or source.auxiliary_error is not None:
        return
    for record in tuple(source.auxiliary_clients):
        if (record is None or record.kind != "shared-request"
                or record.phase != "shared-pending" or record.error is not None):
            continue
        if record.active or not _response_closed(record) or not _worker_settled(record):
            continue
        pool = shared.require_parent(record)
        pool.requests = tuple(item for item in pool.requests if item is not record)
        _release(record)
    for record in tuple(source.auxiliary_clients):
        if (record is None or record.kind != "shared-connection"
                or record.phase not in ("shared-pending", "shared-connection-closed")
                or record.error is not None):
            continue
        pool = record.pool_record
        # A connection still in the actual pool, or a pool with unfinished body
        # work, stays owned by the existing final closer.
        if pool.requests or record.client in pool.pool.connections:
            continue
        shared.require_parent(record)
        if not record.is_closed_method():
            continue
        pool.connections = tuple(item for item in pool.connections if item is not record)
        _release(record)
