"""Single lazy bridge construction in original owner/retention/source books; no native authority."""
from __future__ import annotations

import threading

from agent.tool_commit_construction import CommitConstruction
from hermes_constants import hermes_home_key
from hermes_state_runtime import RuntimeStoreError

BRIDGE_REFERENCE_CREDIT = 4096  # Nominal retained-reference accounting, not heap/RSS.


def bridge_in_progress(owner):
    return getattr(owner, "_native_bridge_constructor_phase", None) == "constructing"


def _owner_refs(owner, pending):
    agent = owner.agent()
    writer = getattr(owner.db, "_execute_write", None)
    return (agent, owner.db, getattr(owner.db, "_conn", None), getattr(owner.db, "_lock", None),
            getattr(writer, "__self__", None), getattr(writer, "__func__", writer),
            agent._session_db, getattr(agent, "_canonical_tool_commit_owner", None),
            owner.scope, owner.lock, owner.pending, owner._pending_constructions, owner.staged,
            owner.validate, owner.publish, owner.native_bridge_factory, owner.native_source_assistant,
            owner.native_source_message_factory, owner._native_retention_ref, pending, pending.ref)


def _same(before, after):
    return len(before) == len(after) and all(left is right for left, right in zip(before, after))


class BridgeConstruction(CommitConstruction):
    def __init__(self, owner, pending, factory):
        super().__init__(owner, "native_bridge")
        self.owner, self.pending, self.factory = owner, pending, factory
        self.retention = self.source = self.runtime = self.book = None
        self.arguments = self.initializer = self.allocator = None
        self.observed_result = None
        self.initializer_started = False
        self.lower_callbacks, self.tool_callbacks = (), []
        self.creator_thread = self.profile_home = None
        self.owner_refs = self.scope_json = self.source_refs = None

    def prepare(self):
        from agent.native_receipt_origin import frozen_json
        from agent.native_turn_retention import native_retention_for_owner
        self.phase = "factory"
        self.creator_thread, self.profile_home = threading.current_thread(), hermes_home_key()
        self.retention = native_retention_for_owner(self.owner)
        if self.retention is not None:
            if self.retention.bridge_construction is not None:
                raise RuntimeStoreError("native_original_bridge_retention_consumed")
            self.retention.bridge_construction = self
        self.owner_refs = _owner_refs(self.owner, self.pending)
        self.scope_json = frozen_json(self.owner.scope)
        self.capture((self.owner, self.pending, self.factory),
                     (self.owner.validate, self.owner.publish, self.factory), self.owner_refs)

    def allocate(self, cls, connection, owner, context, snapshot, tools):
        from agent.native_receipt_origin import NativeDispatchBridge
        if cls is not NativeDispatchBridge or self.phase != "factory" or self.value is not None:
            raise RuntimeStoreError("native_original_bridge_constructor_reentry")
        self.require_owner()
        self.arguments = (connection, owner, context, snapshot, tools)
        self.initializer, self.allocator = cls.__init__, cls.__new__
        from agent.native_receipt_origin import threading
        self.lower_callbacks = (self.allocator, self.initializer, threading.RLock,
                                getattr(type(tools), "__iter__", None))
        self.attach_source(connection)
        self.phase = "initializing"
        self.value = object.__new__(cls)
        self.value._bridge_construction = self
        return self.value

    def attach_source(self, connection):
        from gateway.native_source_runtime import OriginalSourceConnection
        source = getattr(connection, "_gdb_original_source_connection", None)
        if type(source) is not OriginalSourceConnection:
            return  # Original initializer still performs the real connection refusal.
        self.source, self.runtime = source, source.parent.runtime
        with self.runtime.lock:
            self.book = self.runtime.bridges
            self.runtime.reserve(self.book, id(self.owner))
            self.book[id(self.owner)] = self
            self.source_refs = self.current_source_refs()

    def current_source_refs(self):
        source, retention = self.source, self.retention
        authority = source.authority
        live = retention.live if retention is not None else None
        return (source, source.parent, source.parent.runtime, source.connection, source.authority,
                source.actor, source.ws, source.transport, source.task,
                authority.runner, authority.db, authority.sessions.get(self.owner.scope["session_id"]),
                retention, retention.authority if retention is not None else None,
                retention.owner if retention is not None else None,
                retention.agent if retention is not None else None,
                retention.task if retention is not None else None, retention.loop if retention is not None else None,
                live, live.event_stream if live is not None else None,
                authority.profile_id, authority.instance_id, authority.epoch,
                getattr(live.event_stream, "_gdb_native_selection", None) if live is not None else None)

    def require_owner(self, *, constructing=True):
        from agent.native_receipt_origin import frozen_json
        current = _owner_refs(self.owner, self.pending)
        excluded = () if constructing else (12, 13, 14, 15, 16, 17, 19, 20)
        retained = tuple(item for index, item in enumerate(self.owner_refs) if index not in excluded)
        observed = tuple(item for index, item in enumerate(current) if index not in excluded)
        if (self.owner._native_bridge_construction is not self
                or self.owner._native_bridge_constructor_credit != BRIDGE_REFERENCE_CREDIT
                or self.owner._native_bridge_constructor_phase not in ("constructing", "ready")
                or self.error is not None or not _same(retained, observed)
                or frozen_json(self.owner.scope) != self.scope_json
                or self.retention is not None and self.retention.bridge_construction is not self):
            raise RuntimeStoreError("native_original_bridge_constructor_owner_changed")

    def require_source(self):
        if self.source is None:
            raise RuntimeStoreError("native_original_bridge_source_unavailable")
        if (self.runtime.bridges is not self.book or self.book.get(id(self.owner)) is not self
                or not _same(self.source_refs, self.current_source_refs())):
            raise RuntimeStoreError("native_original_bridge_constructor_source_changed")
        from gateway.native_source_runtime import require_original_connection
        if require_original_connection(self.arguments[0]) is not self.source:
            raise RuntimeStoreError("native_original_bridge_source_changed")

    def require_creator(self):
        self.require_current_context()
        if self.creator_thread is not threading.current_thread() or self.profile_home != hermes_home_key():
            raise RuntimeStoreError("native_original_bridge_constructor_creator_changed")

    def finish(self, result):
        from agent.native_receipt_origin import NativeDispatchBridge
        self.observed_result = result
        if (type(result) is not NativeDispatchBridge or self.value is not result
                or self.phase != "initializing" or not self.initializer_started
                or result._bridge_initialized is not True):
            raise RuntimeStoreError("native_original_bridge_constructor_incomplete")
        self.require_creator()
        self.require_owner()
        self.require_source()
        connection, owner, context, snapshot, _tools = self.arguments
        if (result.connection is not connection or result.owner is not owner
                or result.context is not context or result.snapshot is not snapshot
                or result._bridge_lock_factory is not self.lower_callbacks[2]
                or type(result).__init__ is not self.initializer or type(result).__new__ is not self.allocator):
            raise RuntimeStoreError("native_original_bridge_constructor_inputs_changed")
        if any(getattr(type(item), "__getitem__", None) is not getter for item, getter in self.tool_callbacks):
            raise RuntimeStoreError("native_original_bridge_constructor_callback_changed")
        selection = self.source_refs[-1]
        if selection is not None and result.tools != selection.tools:
            raise RuntimeStoreError("native_original_bridge_constructor_tools_changed")
        self.phase = "ready"

    def fail(self, error):
        super().fail(error)
        if self.retention is not None and self.retention.bridge_construction_error is None:
            self.retention.bridge_construction_error = self.error
        if self.runtime is not None:
            if self.runtime.construction_error is None:
                self.runtime.construction_error = self.error
            self.runtime.blocked = True


def create_lazy_bridge(owner, pending):
    if owner._native_bridge_constructor_phase != "fresh":
        raise RuntimeStoreError("native_original_bridge_construction_consumed")
    owner._native_bridge_constructor_credit = BRIDGE_REFERENCE_CREDIT
    owner._native_bridge_constructor_phase = "constructing"
    attempt = None
    try:
        attempt = BridgeConstruction(owner, pending, owner.native_bridge_factory)
        owner._native_bridge_construction = attempt
        attempt.prepare()
        value = attempt.factory(owner)  # Existing original lazy factory/gates, never a replacement issuer.
        attempt.finish(value)
        owner.native_bridge = value  # Same original post-factory publication, inside first-error custody.
        attempt.require_creator()
        attempt.require_owner()
        if owner.native_bridge is not value:
            raise RuntimeStoreError("native_original_bridge_constructor_publication_changed")
        owner._native_bridge_constructor_phase = "ready"
        return value
    except BaseException as error:
        if owner._native_bridge_constructor_error is None:
            owner._native_bridge_constructor_error = error
            owner._native_bridge_constructor_traceback = error.__traceback__
        owner._native_bridge_constructor_phase = "failed"
        if attempt is not None:
            attempt.fail(error)
        raise


def allocate_bridge(cls, connection, owner, context, snapshot, tools):
    attempt = getattr(owner, "_native_bridge_construction", None)
    if not bridge_in_progress(owner):
        return object.__new__(cls)  # Documented direct unmanaged observational constructor shape.
    if type(attempt) is not BridgeConstruction:
        raise RuntimeStoreError("native_original_bridge_constructor_missing")
    return attempt.allocate(cls, connection, owner, context, snapshot, tools)


def begin_bridge_initializer(value, connection, owner, context, snapshot, tools):
    attempt = getattr(value, "_bridge_construction", None)
    if attempt is None:
        return
    if (type(attempt) is not BridgeConstruction or attempt.value is not value
            or attempt.phase != "initializing" or attempt.initializer_started
            or not _same(attempt.arguments, (connection, owner, context, snapshot, tools))):
        raise RuntimeStoreError("native_original_bridge_initializer_reentry")
    attempt.initializer_started = True


def bridge_tool_pair(bridge, item):
    attempt = getattr(bridge, "_bridge_construction", None)
    if type(attempt) is BridgeConstruction:
        if len(attempt.tool_callbacks) >= 512:
            raise RuntimeStoreError("native_original_bridge_tool_callback_capacity")
        attempt.tool_callbacks.append((item, getattr(type(item), "__getitem__", None)))
    return item["name"], item["tool_ref"]


def require_bridge_ready(bridge):
    if not getattr(bridge, "_bridge_initialized", False):
        raise RuntimeStoreError("native_original_bridge_constructor_incomplete")
    attempt = getattr(bridge, "_bridge_construction", None)
    if attempt is None:
        return  # Direct fixture metadata is neither App authority nor managed adoption.
    if (type(attempt) is not BridgeConstruction or attempt.value is not bridge
            or attempt.phase != "ready" or attempt.owner.native_bridge is not bridge):
        raise RuntimeStoreError("native_original_bridge_constructor_incomplete")
    attempt.require_owner(constructing=False)
    attempt.require_source()
