"""Original source dispatch references, never an App origin or native permission."""
from __future__ import annotations

from dataclasses import dataclass
import json
import threading

from agent.tool_commit_rows import MAX_CALLS, MAX_TEXT_BYTES, json_digest
from hermes_state_runtime import RuntimeStoreError

_KEY = object()


class NativeAdmissionRefused(ValueError):
    pass


def is_native_tool_name(name):
    return isinstance(name, str) and name.startswith("gdb_native_")


def frozen_json(value, limit=MAX_TEXT_BYTES):
    if json_digest(value) is None:
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
    def __init__(self, connection, owner, context, snapshot, tools):
        from gateway.native_source_runtime import require_original_connection
        require_original_connection(connection)
        self.connection, self.owner = connection, owner
        self.context, self.snapshot = context, snapshot
        self.tools = tuple((item["name"], item["tool_ref"]) for item in tools)
        self.lock, self.slots = threading.RLock(), {}

    def reserve(self, pending):
        with self.lock:
            if pending.owner is not self.owner or pending.ref is None:
                raise RuntimeStoreError("native_original_owner_unavailable")
            if pending.uid in self.slots or len(self.slots) >= MAX_CALLS:
                raise RuntimeStoreError("native_original_call_reentry_or_capacity")
            self.slots[pending.uid] = None  # A failed first construction remains consumed.
            slot = NativeSlot(_KEY, self, pending)
            self.slots[pending.uid] = slot
            return slot


class NativeSlot:
    def __init__(self, key, bridge, pending):
        if key is not _KEY:
            raise RuntimeStoreError("native_original_slot_factory_missing")
        self.bridge, self.pending = bridge, pending
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
        from agent.native_turn_retention import native_retention_for_owner
        from gateway.native_source_calls import capture_original_call
        with self.lock:
            if self._source_declaration_phase != "fresh":
                raise RuntimeStoreError("native_original_declaration_reentry")
            self._source_declaration_phase = "staging"
        try:
            owner, connection = self.pending.owner, self.bridge.connection
            retention = native_retention_for_owner(owner)
            if retention is None:
                raise RuntimeStoreError("native_original_task_unavailable")
            selection = getattr(retention.live.event_stream, "_gdb_native_selection", None)
            if selection is None or not selection.current():
                raise RuntimeStoreError("native_original_selection_changed")
            scope, assistant = frozen_json(owner.scope), frozen_json(self.pending.assistant)
            original = capture_original_call(self, retention, selection, scope, assistant)
            source = NativeSourceDeclarationRefs(
                self, self.pending, self.pending.ref, owner, owner.agent(), owner.db, self.bridge,
                connection, connection.authority, connection.actor, connection.transport,
                connection.actor.transport_id, retention, retention.task, retention.loop,
                retention.live, selection, scope, assistant, self.call_json, self.tool_json,
                self.arguments_json, original)
            with self.lock:
                self._source_declaration_refs = source
                original.attach_declaration(source)
                _declaration_local_refs(self, source)
                self._source_declaration_phase = "ready"
            return source
        except BaseException:
            self._source_declaration_phase = "blocked"
            raise

    def execute(self, _gate=None):
        from gateway.session_server_requests import register_native_request
        self.prepare_source_declaration()
        register_native_request(self)  # Missing App issuance refuses before any frame/write.
        raise NativeAdmissionRefused("native_original_app_call_issuer_missing")


def _declaration_local_refs(slot, source):
    from gateway.native_source_calls import require_original_source_call
    require_original_source_call(slot, source)
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
