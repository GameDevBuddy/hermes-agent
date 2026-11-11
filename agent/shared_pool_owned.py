"""Original sync shared-pool constructors and users on the existing SDK source book."""
from __future__ import annotations

import contextvars
import threading

from agent import auxiliary_owned as owned
from hermes_owned_gateway import OwnedGatewayUnknown

KINDS = frozenset({"shared-pool", "shared-view", "shared-connection", "shared-request"})
_building = contextvars.ContextVar("_original_shared_pool_build", default=None)
_request = contextvars.ContextVar("_original_shared_pool_request", default=None)
_batch = contextvars.ContextVar("_original_shared_pool_close_batch", default=None)


def refuse(record, reason):
    error = OwnedGatewayUnknown(reason)
    owned.fail(record, error)
    raise error


def _same(left, right):
    from agent.auxiliary_owned_close import _same_callable
    return _same_callable(left, right)


def retain_parent(record, pool, *, view=None):
    normal = pool if record.kind == "shared-connection" else None
    record.pool_origin = (pool, normal, view)
    record.pool_record, record.normal_parent = pool, normal
    if view is not None:
        record.view = view


def require_parent(record, pool=None, *, origin=None):
    original = record.pool_origin if origin is None else origin
    parent, normal, view = original
    if (record.pool_origin is not original or (pool is not None and parent is not pool)
            or record.pool_record is not parent or record.normal_parent is not normal
            or normal is not (parent if record.kind == "shared-connection" else None)
            or record.source is not parent.source
            or (record.kind == "shared-request" and (record.view is not view or view.pool_origin[0] is not parent))):
        refuse(record, "shared-original-parent-association-changed")
    record.require_current()
    require_pool(parent)
    if record.kind in ("shared-view", "shared-connection") and owned.find(record.client) is not record:
        refuse(record, "shared-original-member-client-association-changed")
    return parent


def construct_transport(factory, *args, **kwargs):
    record = _building.get()
    if record is None:
        return factory(*args, **kwargs)
    if record.ctor_attempted:
        refuse(record, "original-shared-transport-constructor-reentry")
    record.ctor_attempted, record.transport_factory = True, factory
    result = factory(*args, **kwargs)
    record.ctor_result = result
    return result


def build_pool(build):
    record = owned.reserve("shared-pool", build)
    if record is None:
        return build()
    import httpx
    import httpcore
    record.spec = (httpx.HTTPTransport, httpcore.ConnectionPool,
                   httpx.HTTPTransport.close, httpcore.ConnectionPool.close,
                   httpcore.ConnectionPool.create_connection, httpcore.ConnectionPool._close_connections,
                   httpcore.HTTPConnection, httpcore.HTTPConnection.close, httpcore.HTTPConnection.is_closed)
    record.ctor_attempted, record.ctor_result = False, None
    record.views, record.requests, record.connections = (), (), ()
    record.extra_refs = ()
    token = _building.set(record)
    try:
        result = build()
        record.result, record.client = result, result
        if not record.ctor_attempted or record.ctor_result is not result:
            refuse(record, "shared-pool-original-constructor-unavailable")
        record.require_current(creating=True)
        _install(record)
        owned.attach(record, result, "client")
        record.supported = True
        record.phase = "shared-pending"
        return result
    except BaseException as error:
        owned.fail(record, error)
        raise
    finally:
        _building.reset(token)


def _install(record):
    transport, spec = record.client, record.spec
    pool = record.pool = getattr(transport, "_pool", None)
    record.close_method = getattr(transport, "close", None)
    record.pool_close = getattr(pool, "close", None)
    record.pool_create = getattr(pool, "create_connection", None)
    record.pool_batch = getattr(pool, "_close_connections", None)
    record.transport_handle = getattr(transport, "handle_request", None)
    record.pool_lock = getattr(pool, "_optional_thread_lock", None)
    record.pool_requests = getattr(pool, "_requests", None)
    record.initial_connections = getattr(pool, "_connections", None)
    if (not isinstance(transport, spec[0]) or type(pool) is not spec[1]
            or getattr(record.close_method, "__func__", None) is not spec[2]
            or getattr(record.pool_close, "__func__", None) is not spec[3]
            or getattr(record.pool_create, "__func__", None) is not spec[4]
            or getattr(record.pool_batch, "__func__", None) is not spec[5]):
        refuse(record, "shared-pool-pinned-normal-implementation-unavailable")
    record.create_hook = lambda *args, **kwargs: _create_connection(record, args, kwargs)
    record.batch_hook = lambda connections: _close_batch(record, connections)
    record.handle_hook = lambda request: _handle_transport(record, request)
    pool.create_connection, pool._close_connections = record.create_hook, record.batch_hook
    transport.handle_request = record.handle_hook


def require_pool(record):
    record.require_current()
    transport, pool = record.client, record.pool
    if (owned.find(transport) is not record or transport._pool is not pool
            or pool._optional_thread_lock is not record.pool_lock
            or pool._requests is not record.pool_requests
            or pool.create_connection is not record.create_hook
            or pool._close_connections is not record.batch_hook
            or transport.handle_request is not record.handle_hook
            or not _same(transport.close, record.close_method)
            or not _same(pool.close, record.pool_close)):
        refuse(record, "original-shared-pool-association-changed")


def cached_pool(transport):
    record = owned.find(transport)
    if record is None:
        if not owned.managed_active():
            return
        record = owned.reserve("unowned-shared-pool", cached_pool)
        record.result = transport
        refuse(record, "cached-shared-pool-original-constructor-unavailable")
    if record.kind != "shared-pool":
        refuse(record, "cached-shared-pool-kind-unavailable")
    require_pool(record)


def begin_view(view, transport):
    pool = owned.find(transport)
    if pool is None and not owned.managed_active():
        return None
    record = owned.reserve("shared-view", type(view))
    if record is None:
        # The original pool retains the unreturned view through this first traceback.
        refuse(pool, "managed-shared-view-original-source-unavailable")
    record.client, record.result = view, (view, transport)
    record.holders = ()
    retain_parent(record, pool)
    record.view_close_attempted = record.view_close_confirmed = False
    if pool is None or pool.kind != "shared-pool":
        refuse(record, "shared-view-original-pool-unavailable")
    require_pool(pool)
    record.view_close_method = type(view).close
    with pool.source._state_lock:
        pool.views += (record,)
    return record


def finish_view(record):
    if record is None:
        return
    owned.attach(record, record.client, "client")
    record.phase = "shared-pending"


def require_view(record):
    pool = require_parent(record)
    if (record.client._inner is not pool.client
            or type(record.client).close is not record.view_close_method):
        refuse(record, "original-shared-view-association-changed")


def close_view(view):
    record = owned.find(view)
    if record is None:
        return False
    require_view(record)
    if record.view_close_attempted:
        if not record.view_close_confirmed or not view._closed:
            refuse(record, "original-shared-view-close-attempt-unknown")
        return True
    record.view_close_attempted = True
    view._closed = True  # Actual existing view normal API; it does not close the pool.
    require_view(record)
    record.view_close_confirmed = True
    return True


def register_holders(record):
    record.original_shared_refs = record.shared_refs
    for item in record.shared_refs:
        view = owned.find(item)
        if view is not None and view.kind == "shared-view":
            require_view(view)
            if view.source is not record.source:
                refuse(record, "shared-view-foreign-sdk-holder")
            with record.source._state_lock:
                if record not in view.holders:
                    view.holders += (record,)


def sdk_views_closed(record):
    if record.shared_refs is not record.original_shared_refs:
        refuse(record, "shared-original-sdk-references-changed")
    for item in record.shared_refs:
        view = owned.find(item)
        if view is not None and view.kind == "shared-view":
            require_view(view)
            if not view.view_close_confirmed or not view.client._closed or record not in view.holders:
                refuse(record, "shared-sdk-holder-view-close-unconfirmed")
        elif view is None or view.kind != "shared-pool":
            refuse(record, "shared-sdk-holder-original-pool-unavailable")


def handle_view(view, request):
    record = owned.find(view)
    if record is None:
        return view._inner.handle_request(request)
    require_view(record)
    record.source.require_child_work()
    from hermes_constants import hermes_home_key
    if view._closed or record.source.auxiliary_closing_attempted or hermes_home_key() != record.home_key:
        refuse(record, "shared-view-request-original-context-unavailable")
    attempt = owned.reserve("shared-request", record.pool_record.transport_handle)
    retain_parent(attempt, record.pool_record, view=record)
    attempt.result, attempt.active = request, True
    attempt.worker, attempt.response, attempt.response_stream = threading.current_thread(), None, None
    attempt.phase = "shared-pending"
    with record.source._state_lock:
        record.pool_record.requests += (attempt,)
    token = _request.set(attempt)
    try:
        response = view._inner.handle_request(request)
        attempt.response, attempt.response_stream = response, getattr(response, "stream", None)
        for connection in record.pool_record.connections:
            if connection.client is not None:
                retain_contained(connection)
        require_view(record)
        return response
    except BaseException as error:
        owned.fail(attempt, error)
        raise
    finally:
        attempt.active = False
        _request.reset(token)


def _handle_transport(pool, request):
    require_pool(pool)
    attempt = _request.get()
    if attempt is None or attempt.pool_record is not pool or attempt.result is not request:
        refuse(pool, "shared-transport-request-has-no-original-view")
    return pool.transport_handle(request)


def _create_connection(pool, args, kwargs):
    require_pool(pool)
    attempt = _request.get()
    if attempt is None or attempt.pool_record is not pool:
        refuse(pool, "shared-connection-has-no-original-request")
    record = owned.reserve("shared-connection", pool.pool_create)
    retain_parent(record, pool)
    record.owner_loop, record.owner_thread, record.context = pool.owner_loop, pool.owner_thread, pool.context
    record.original_create, record.contained = pool.pool_create, ()
    with pool.source._state_lock:
        pool.connections += (record,)
    try:
        connection = pool.pool_create(*args, **kwargs)
        record.result = connection
        owned.attach(record, connection, "client")
        record.close_method = connection.close
        record.is_closed_method = connection.is_closed
        spec = pool.spec
        if (type(connection) is not spec[6] or getattr(record.close_method, "__func__", None) is not spec[7]
                or getattr(record.is_closed_method, "__func__", None) is not spec[8]):
            refuse(record, "shared-connection-pinned-normal-implementation-unavailable")
        record.connection_close_hook = lambda: _close_connection(record)
        connection.close = record.connection_close_hook
        record.phase = "shared-pending"
        return connection
    except BaseException as error:
        owned.fail(record, error)
        raise


def retain_contained(record):
    connection = record.client
    inner = getattr(connection, "_connection", None)
    stream = getattr(inner, "_network_stream", None)
    values = (inner, stream)
    if any(value is not None and all(value is not prior for prior in record.contained) for value in values):
        if len(record.contained) + 2 > 32:
            refuse(record, "shared-contained-original-graph-capacity")
        record.contained += tuple(value for value in values if value is not None)


def _close_batch(pool, connections):
    require_pool(pool)
    values = tuple(connections)
    if len(values) > 1024:
        refuse(pool, "shared-normal-close-batch-capacity")
    records = tuple(owned.find(connection) for connection in values)
    origins = tuple(record.pool_origin if record is not None else None for record in records)
    for connection, record, origin in zip(values, records, origins):
        if record is None or record.pool_record is not pool:
            pool.extra_refs = values
            refuse(pool, "shared-pool-contained-connection-original-owner-unavailable")
        require_parent(record, pool, origin=origin)
    token = _batch.set(pool)
    try:
        result = pool.pool_batch(values)
        require_pool(pool)
        for record, origin in zip(records, origins):
            require_parent(record, pool, origin=origin)
        return result
    except BaseException as error:
        owned.fail(pool, error)
        raise
    finally:
        _batch.reset(token)


def _close_connection(record):
    origin = record.pool_origin
    pool = require_parent(record, origin=origin)
    connection = record.client
    if (_batch.get() is not pool or connection.close is not record.connection_close_hook
            or not _same(connection.is_closed, record.is_closed_method)):
        refuse(record, "shared-connection-normal-parent-unavailable")
    retain_contained(record)
    if record.close_attempted:
        if record.error is not None or record.phase != "shared-connection-closed" or not record.is_closed_method():
            refuse(record, "shared-connection-original-close-attempt-unknown")
        return
    record.close_attempted = True
    original_inner = getattr(connection, "_connection", None)
    original_stream = getattr(original_inner, "_network_stream", None)
    record.original_inner, record.original_stream = original_inner, original_stream
    try:
        record.close_result = record.close_method()
        import inspect
        if inspect.isawaitable(record.close_result):
            refuse(record, "shared-sync-connection-close-became-async")
        require_parent(record, pool, origin=origin)
        retain_contained(record)
        # Contained httpcore/socket identity and closed-state observations are
        # checked after the pool's normal API returns, not inside its close hook.
        # They must not prevent Quit once every state-writing worker is joined.
        record.phase = "shared-connection-closed"
    except BaseException as error:
        owned.fail(record, error)
        raise
