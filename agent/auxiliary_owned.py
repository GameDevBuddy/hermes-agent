"""Retained managed auxiliary construction, never cache-derived authority or lifetime proof."""
from __future__ import annotations

import asyncio
import contextvars
import functools
import threading

from hermes_owned_gateway import OwnedGatewayUnknown

# Fixed before-work nominal accounting only, never SDK heap/kernel or admission proof.
CLAIM_CREDIT = 65536
MAX_CLAIMS = 1024
MAX_PROCESS_CLAIMS = 4096
MAX_BYTES = 64 * 1024 * 1024
_index_lock = threading.RLock()
_client_records = {}
_source_claims = []
_claim_count = 0
_construction = contextvars.ContextVar("_original_auxiliary_construction", default=None)
_primary_scope = contextvars.ContextVar("_original_primary_agent_construction", default=None)


def managed_active():
    from hermes_owned_gateway import active
    return active() is not None


def running_loop():
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


def find(client):
    with _index_lock:
        record = _client_records.get(id(client))
    return record if record is not None and record.client is client else None


def fail(record, error):
    source = record.source
    with source._state_lock:
        from agent.auxiliary_unavailable import ProviderNotConfiguredError
        from agent.primary_owned import links
        owner = record if record.kind == "primary-agent" else record.primary_owner
        # Missing setup before any SDK work is a request refusal, not lost
        # custody. Settle only this empty construction; never revive an unknown
        # source or excuse a partial SDK/HTTP constructor (even of this type).
        if (isinstance(error, ProviderNotConfiguredError)
                and record.kind in ("primary-agent", "resolve")
                and record.phase == "constructing" and record.error is None
                and record.client is None and record.inner is None
                and source.auxiliary_error is None
                and owner is not None and owner.primary_agent is not None
                and owner.phase == "constructing"
                and all(client is None for client in links(owner.primary_agent))
                and all(child is record or (child.phase == "empty" and child.error is None)
                        for child in owner.children)):
            record.require_current(creating=True)
            record.phase = "empty"
            return
        if record.error is None:
            # Keep the original exception/traceback: failed constructors can retain an unreturned self.
            record.error = error
        source.auxiliary_error = source.auxiliary_error or record.error
        record.phase = "unknown"
        source.unknown("managed-auxiliary-resource-unknown:" + type(record.error).__name__)


class AuxiliaryConstruction:
    def __init__(self, source, kind, factory):
        from hermes_constants import get_hermes_home, hermes_home_key
        self.source, self.kind, self.factory = source, kind, factory
        self.primary_owner, self.primary_agent = _primary_scope.get(), None
        self.voice_owner = None
        self.normal_parent = None
        self.primary_close_links = None
        self.runner, self.runtime = source.runner, getattr(source.runner, "_gdb_original_source_runtime", None)
        self.creator_loop, self.creator_thread = running_loop(), threading.current_thread()
        self.owner_loop = self.creator_loop or source.loop
        self.owner_thread = self.creator_thread if self.creator_loop is not None else source.loop_thread
        self.context, self.home = contextvars.copy_context(), get_hermes_home()
        self.home_key = hermes_home_key(self.home)
        self.client = self.close_method = self.inner = None
        self.children, self.shared_refs = (), ()
        self.phase, self.error = "constructing", None
        self.close_attempted = False
        self.close_task = self.close_completion = self.close_result = None
        self.close_parent = None
        self.close_coroutine = self.close_dispatch = self.close_waiter = self.close_shield = None
        self.close_waiter_error = None
        self.result = self.unowned_close_result = None
        self.supported = False
        self.require_current(creating=True)

    def require_current(self, *, creating=False):
        from hermes_owned_gateway import active
        try:
            if (active(self.runner) is not self.source or self.runtime is None
                    or self.runner._gdb_original_source_runtime is not self.runtime
                    or self.runtime.runner is not self.runner or self.source.loop is not self.runtime.loop
                    or self.runtime.bootstrap_task is not self.source.task
                    or self.runtime.bootstrap_task.done()
                    or self.source.runner is not self.runner or self.owner_loop is None
                    or not _original_normal_context(self)):
                raise OwnedGatewayUnknown("original-auxiliary-runtime-required")
            if creating:
                self.source.require_child_work()
            if self.error is not None:
                raise OwnedGatewayUnknown("original-auxiliary-first-failure-retained")
            from agent.primary_owned import require_record_association
            require_record_association(self)
        except BaseException as error:
            fail(self, error)
            raise


def _original_normal_context(record):
    parent = record.normal_parent
    if parent is not None:
        parent.require_current()
        return (record.kind == "shared-connection" and parent.kind == "shared-pool"
                and parent.source is record.source and record.owner_loop is parent.owner_loop
                and record.owner_thread is parent.owner_thread)
    return (record.owner_loop is (record.creator_loop or record.source.loop)
            and record.owner_thread is (record.creator_thread if record.creator_loop is not None else record.source.loop_thread))


def reserve(kind, factory):
    from hermes_owned_gateway import active
    source = active()
    if source is None:
        return None
    source.require_child_work()
    global _claim_count
    with source._state_lock:
        if (source.auxiliary_closing_attempted or len(source.auxiliary_clients) >= MAX_CLAIMS
                or source.auxiliary_credit + CLAIM_CREDIT > MAX_BYTES):
            raise source.unknown("original-auxiliary-construction-capacity-or-shutdown")
        with _index_lock:
            if _claim_count >= MAX_PROCESS_CLAIMS:
                raise source.unknown("original-auxiliary-process-claim-capacity")
            _claim_count += 1
        source.auxiliary_credit += CLAIM_CREDIT
        index = len(source.auxiliary_clients)
        source.auxiliary_clients.append(None)  # Consumed before context/record/factory allocation.
        try:
            with _index_lock:
                _source_claims.append(source)  # Keep failed/empty claims even if _CURRENT changes.
            record = AuxiliaryConstruction(source, kind, factory)
        except BaseException as error:
            source.auxiliary_error = source.auxiliary_error or error
            source.unknown("original-auxiliary-record-construction-failed")
            raise
        source.auxiliary_clients[index] = record
        if record.primary_owner is not None:
            record.primary_owner.children += (record,)
    return record


def _view_child(client):
    # These are actual auxiliary adapter links, not a generic object walker.
    from agent import auxiliary_client as aux
    wrappers = (aux.CodexAuxiliaryClient, aux.AnthropicAuxiliaryClient, aux._AsyncAuxiliaryClientBase)
    if isinstance(client, wrappers):
        return getattr(client, "_real_client", None)
    return None


def _shared_references(inner):
    if inner is None:
        return ()
    mounts = getattr(inner, "_mounts", {})
    if not isinstance(mounts, dict) or len(mounts) > 32:
        raise OwnedGatewayUnknown("auxiliary-transport-graph-unbounded")
    from agent import process_bootstrap
    view_class = process_bootstrap._SharedTransport
    result = []
    for transport in (getattr(inner, "_transport", None), *mounts.values()):
        if view_class is not None and isinstance(transport, view_class):
            result.append(transport)
            result.append(transport._inner)
    return tuple(result)


def attach(record, result, shape):
    record.result = result
    client = result[0] if shape == "pair" else (result.get("http_client") if shape == "http" else result)
    if client is None:
        record.phase = "empty"
        return
    record.client = client
    existing = find(client)
    if existing is not None:
        if existing.source is not record.source:
            raise OwnedGatewayUnknown("auxiliary-client-belongs-to-another-source")
        record.children, record.phase = (existing,), "view"
        return
    child = _view_child(client)
    if child is not None:
        child_record = find(child)
        if child_record is not None and child_record.source is record.source:
            record.children, record.phase = (child_record,), "view"
        else:
            record.inner, record.phase = child, "held"
    else:
        record.close_method = getattr(client, "close", None) or getattr(client, "aclose", None)
        record.inner = getattr(client, "_client", None)
        record.shared_refs = _shared_references(record.inner if record.inner is not None else client)
        if record.shared_refs:
            from agent.shared_pool_owned import register_holders
            register_holders(record)
        known = record.kind in ("sdk", "http", "sdk-async", "http-async")
        bound = not record.kind.endswith("-async") or record.creator_loop is not None
        record.supported = known and bound and callable(record.close_method)
        record.phase = "held"
        if record.inner is not None and (inner_record := find(record.inner)) is not None:
            if inner_record.source is not record.source:
                raise OwnedGatewayUnknown("auxiliary-inner-client-belongs-to-another-source")
            record.children = (inner_record,)
    with _index_lock:
        if id(client) in _client_records:
            raise OwnedGatewayUnknown("original-auxiliary-client-index-race")
        _client_records[id(client)] = record


def construct(kind, factory, *args, shape="client", **kwargs):
    record = reserve(kind, factory)
    if record is None:
        return factory(*args, **kwargs)
    token = _construction.set(record)
    try:
        result = factory(*args, **kwargs)
        record.require_current(creating=True)
        attach(record, result, shape)
        return result
    except BaseException as error:
        fail(record, error)
        raise
    finally:
        _construction.reset(token)


def construct_during_auxiliary(kind, factory, *args, **kwargs):
    # Shared main-agent builders retain their ordinary ownership; only an actual aux creator nests here.
    if _construction.get() is None:
        return factory(*args, **kwargs)
    return construct(kind, factory, *args, **kwargs)


def owning_factory(kind, shape="client"):
    def decorate(factory):
        @functools.wraps(factory)
        def invoke(*args, **kwargs):
            if kind == "resolve":
                from agent.auxiliary_client import _aux_probe_active
                if _aux_probe_active():
                    return factory(*args, **kwargs)
            selected = "http-async" if kind == "http" and kwargs.get("async_mode", False) else kind
            return construct(selected, factory, *args, shape=shape, **kwargs)
        return invoke
    return decorate


def constructor_failure(error):
    record = _construction.get()
    if record is not None:
        fail(record, error)
        raise error


def retain_cache_client(client):
    """Before a cache hit/drop/replacement, keep actual managed custody independently."""
    if client is None:
        return
    record = find(client)
    if record is not None:
        record.require_current()
        return
    from hermes_owned_gateway import active
    source = active()
    if source is not None:
        record = reserve("unowned-cache", retain_cache_client)
        record.client, record.phase = client, "held"
        error = OwnedGatewayUnknown("cached-auxiliary-original-creator-unavailable")
        fail(record, error)
        raise error


def defer_legacy_close(client, result=None):
    record = find(client)
    if record is None:
        from hermes_owned_gateway import active
        if active() is None:
            return False
        retain_cache_client(client)
        return True
    if result is not None:
        record.unowned_close_result = record.unowned_close_result or result
        error = OwnedGatewayUnknown("managed-auxiliary-close-result-has-no-original-owner-task")
        fail(record, error)
        raise error
    # Eviction/replacement cannot know that a concurrent request finished. Original shutdown owns closure.
    return True


def require_cache_consumer(client, *, async_mode):
    retain_cache_client(client)
    record = find(client)
    if record is not None and record.source.auxiliary_closing_attempted:
        error = OwnedGatewayUnknown("managed-auxiliary-consumer-after-normal-close-started")
        fail(record, error)
        raise error
    if record is not None:
        record.require_current(creating=True)
    if record is not None and async_mode and running_loop() is not record.owner_loop:
        error = OwnedGatewayUnknown("cached-auxiliary-original-consumer-loop-required")
        fail(record, error)
        raise error


def retain_cache_shutdown():
    from hermes_owned_gateway import active
    source = active()
    if source is None:
        from agent import auxiliary_client as auxiliary
        with auxiliary._client_cache_lock:
            records = tuple(find(entry[0]) for entry in auxiliary._client_cache.values())
        for record in records:
            if record is not None:
                raise record.source.unknown("managed-auxiliary-original-shutdown-source-unavailable")
        return False
    if not source.auxiliary_closed:
        raise source.unknown("managed-auxiliary-requires-original-normal-shutdown")
    for record in source.auxiliary_clients:
        if record is None or record.error is not None or record.phase not in ("closed", "empty", "view"):
            raise source.unknown("managed-auxiliary-shutdown-still-held")
    return True


def guard_shared_transport_close(transports):
    """Legacy atexit/test teardown cannot clear managed shared-pool custody."""
    from hermes_owned_gateway import _CURRENT
    for transport in transports:
        pool = find(transport)
        if pool is not None and pool.kind == "shared-pool":
            raise OwnedGatewayUnknown("managed-shared-pool-requires-original-final-closer")
    with _index_lock:
        records = tuple(_client_records.values())
    for record in records:
        if any(pool is transport for pool in record.shared_refs for transport in transports):
            raise record.source.unknown("managed-auxiliary-shared-pool-normal-owner-unavailable")
    if _CURRENT is not None and _CURRENT.auxiliary_error is not None:
        raise _CURRENT.unknown("managed-auxiliary-failed-construction-retains-shared-pools")
