"""Original source dispatch references, never an App origin or native permission."""
from __future__ import annotations

from dataclasses import dataclass
import json
import threading

from agent.tool_commit_rows import MAX_TEXT_BYTES, json_digest
from hermes_state_runtime import RuntimeStoreError

_KEY = object()


class NativeAdmissionRefused(ValueError):
    pass


def is_native_tool_name(name):
    return isinstance(name, str) and name.startswith("gdb_native_")


def frozen_json(value, limit=MAX_TEXT_BYTES):
    digest = json_digest(value)
    if digest is None or digest["bytes"] > limit:
        raise RuntimeStoreError("native_source_json_unavailable")
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False,
                     separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(raw) > limit:
        raise RuntimeStoreError("native_source_json_unavailable")
    return raw


@dataclass(frozen=True)
class NativeSourceDeclarationRefs:
    slot: object
    pending: object
    ref: object
    owner: object
    agent: object
    db: object
    bridge: object
    connection: object
    authority: object
    actor: object
    transport: object
    transport_id: str
    retention: object
    task: object
    loop: object
    live: object
    selection: object
    scope_json: bytes
    assistant_json: bytes
    call_json: bytes
    tool_json: bytes
    arguments_json: bytes
    original_source_call: object


class NativeDispatchBridge:
    def __new__(cls, connection, owner, context, snapshot, tools):
        from agent.native_bridge_construction import allocate_bridge
        return allocate_bridge(cls, connection, owner, context, snapshot, tools)

    def __init__(self, connection, owner, context, snapshot, tools):
        self._bridge_initialized = False
        self.slots = {}
        from agent.native_bridge_construction import begin_bridge_initializer
        begin_bridge_initializer(self, connection, owner, context, snapshot, tools)
        from gateway.native_source_runtime import require_original_connection
        require_original_connection(connection)
        self.connection, self.owner = connection, owner
        self.context, self.snapshot = context, snapshot
        from agent.native_bridge_construction import bridge_tool_pair
        self.tools = tuple(bridge_tool_pair(self, item) for item in tools)
        self._bridge_lock_factory = threading.RLock
        self.lock = self._bridge_lock_factory()
        self._bridge_initialized = True

    def reserve(self, pending):
        from agent.native_slot_construction import reserve_slot
        return reserve_slot(self, pending)


class NativeSlot:
    def __init__(self, key, bridge, pending):
        if key is not _KEY:
            raise RuntimeStoreError("native_original_slot_factory_missing")
        self.bridge, self.pending = bridge, pending
        self._source_constructions = {}
        self.lock = threading.RLock()
        self.arguments_json = frozen_json(pending.ref.args)
        self.call_json = frozen_json({"call_id": pending.call_id, "call_uid": pending.uid,
                                      "message_uid": pending.assistant["message_uid"]})
        tools = [item for item in bridge.tools if item[0] == pending.tool_name]
        if len(tools) != 1:
            raise RuntimeStoreError("native_selected_tool_missing")
        self.tool_json = frozen_json({"name": tools[0][0], "tool_ref": tools[0][1]})
        self._source_declaration_phase, self._source_declaration_refs = "fresh", None
        self._native_pin, self._native_active_use = None, None
        self._native_request_object, self._native_request_candidate_id = None, None
        self._native_write_attempted = False
        self.origin = self.projection = None

    def prepare_source_declaration(self):
        from agent.native_slot_construction import prepare_declaration
        from gateway.native_source_construction import require_constructed
        require_constructed(self)
        with self.lock:
            if self._source_declaration_phase != "fresh":
                raise RuntimeStoreError("native_original_declaration_reentry")
            self._source_declaration_phase = "staging"
        try:
            return prepare_declaration(self)
        except BaseException:
            self._source_declaration_phase = "blocked"
            raise

    def execute(self, _gate=None):
        from gateway.session_server_requests import register_native_request
        self.prepare_source_declaration()
        register_native_request(self)  # Source authentication ends at the explicit execution hold.
        raise NativeAdmissionRefused("native_original_app_call_issuer_missing")


def _declaration_local_refs(slot, source):
    from gateway.native_source_calls import _require_original_source_call_refs
    _require_original_source_call_refs(slot, source)
    if source.slot is not slot or source.pending is not slot.pending:
        raise RuntimeStoreError("native_original_declaration_changed")
    if not source.selection.current() or not source.owner.binding_current(source.pending):
        raise RuntimeStoreError("native_original_selection_or_owner_changed")


def native_unavailable_result():
    return json.dumps({"error": "native_dispatch_unavailable", "isError": True,
                       "message": "Original native call ownership is unavailable; do not replay.",
                       "replayAllowed": False}, separators=(",", ":"))


def execute_native_call(agent, ref, gate):
    pending = ref.canonical_commit
    slot = getattr(pending, "native_dispatch_slot", None)
    if pending is None or pending.ref is not ref or slot is None or slot.pending is not pending:
        return native_unavailable_result()
    try:
        return slot.execute(gate)
    except (NativeAdmissionRefused, RuntimeStoreError):
        return native_unavailable_result()
