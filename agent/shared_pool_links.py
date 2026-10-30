"""Retained original shared associations, never a heap/kernel graph or authority proof."""
from dataclasses import dataclass

from agent import auxiliary_owned as owned
from agent import shared_pool_owned as shared


@dataclass(frozen=True)
class PoolLinks:
    owner: object
    records: tuple
    source_book: object
    members: tuple
    nodes: tuple
    holders: tuple
    sdk_refs: tuple
    requests: tuple


def _same_sequence(left, right):
    return len(left) == len(right) and all(before is after for before, after in zip(left, right))


def _sdk_refs(records):
    result = []
    for record in records:
        if record is None:
            continue
        original = getattr(record, "original_shared_refs", ())
        if original or record.shared_refs:
            if record.shared_refs is not original:
                shared.refuse(record, "shared-original-sdk-references-changed")
            result.append((record, original))
    return tuple(result)


def _capture(pool, records):
    members = tuple((name, getattr(pool, name)) for name in ("views", "requests", "connections"))
    kinds = {"views": "shared-view", "requests": "shared-request", "connections": "shared-connection"}
    for name, values in members:
        expected = tuple(item for item in records if item is not None and item.kind == kinds[name]
                         and item.pool_origin[0] is pool)
        if type(values) is not tuple or not _same_sequence(values, expected):
            shared.refuse(pool, "shared-original-membership-unavailable:" + name)
    sdk_refs = _sdk_refs(records)
    holders = tuple((view, view.holders) for view in pool.views)
    for view, values in holders:
        expected = tuple(holder for holder, refs in sdk_refs if any(ref is view.client for ref in refs))
        if (type(values) is not tuple or len(values) != len(expected)
                or any(not any(item is holder for holder in expected) for item in values)
                or any(not any(holder is item for item in values) for holder in expected)):
            shared.refuse(pool, "shared-original-holder-membership-unavailable")
    nodes = tuple((item, item.client, item.pool_origin) for _name, values in members for item in values)
    requests = tuple((item, item.view, item.response, item.response_stream, item.worker) for item in pool.requests)
    return PoolLinks(pool, records, pool.source.auxiliary_clients, members, nodes, holders, sdk_refs, requests)


def prepare(records):
    if len(records) > owned.MAX_CLAIMS:
        raise ValueError("shared shutdown exceeds original source capacity")
    for record in records:
        if record is not None and record.kind in shared.KINDS - {"shared-pool"}:
            shared.require_parent(record)
    snapshots = []
    for record in records:
        if record is None or record.kind != "shared-pool":
            continue
        if not hasattr(record, "shared_close_links"):
            record.shared_close_links = _capture(record, records)
        snapshot = record.shared_close_links
        if snapshot.owner is not record or not _same_sequence(snapshot.records, records):
            shared.refuse(record, "shared-original-shutdown-snapshot-changed")
        require_snapshot(snapshot)
        snapshots.append(snapshot)
    return tuple(snapshots)


def require_snapshot(snapshot):
    pool = snapshot.owner
    shared.require_pool(pool)
    if (pool.shared_close_links is not snapshot or pool.source.auxiliary_clients is not snapshot.source_book
            or not _same_sequence(snapshot.source_book, snapshot.records)):
        shared.refuse(pool, "shared-original-source-membership-changed")
    for name, original in snapshot.members:
        if getattr(pool, name) is not original:
            shared.refuse(pool, "shared-original-membership-changed:" + name)
    for item, client, origin in snapshot.nodes:
        shared.require_parent(item, pool, origin=origin)
        if item.client is not client:
            shared.refuse(pool, "shared-original-member-client-changed")
    for view, holders in snapshot.holders:
        if view.holders is not holders:
            shared.refuse(pool, "shared-original-holder-membership-changed")
    for holder, refs in snapshot.sdk_refs:
        if holder.shared_refs is not refs or holder.original_shared_refs is not refs:
            shared.refuse(pool, "shared-original-sdk-references-changed")
    for request, view, response, stream, worker in snapshot.requests:
        if (request.view is not view or request.response is not response or request.response_stream is not stream
                or request.worker is not worker):
            shared.refuse(pool, "shared-original-request-associations-changed")


def require_member(record, snapshot):
    require_snapshot(snapshot)
    if record is snapshot.owner:
        return
    if not any(item is record for item, _client, _origin in snapshot.nodes):
        shared.refuse(record, "shared-original-member-omitted")
    shared.require_parent(record, snapshot.owner)
