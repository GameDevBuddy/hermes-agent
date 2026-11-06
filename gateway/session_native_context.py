"""Immutable selected native metadata on the existing verified connection/live stream."""
from __future__ import annotations

from dataclasses import dataclass
import json
import re

from agent.native_receipt_origin import NativeDispatchBridge, frozen_json
from gateway.session_authorities import require_serving
from hermes_state_runtime import RuntimeStoreError

_SHA = re.compile(r"[0-9a-f]{64}\Z")
_ULID = re.compile(r"[0-9A-HJKMNP-TV-Z]{26}\Z")

def _text(value, limit):
    return (isinstance(value, str) and 0 < len(value.encode("utf-8", "strict")) <= limit
            and not any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in value))

def _selected(params, ref):
    if (set(params) != {"session_id", "context_id", "snapshot_sha256", "tools"}
            or not _text(params["session_id"], 512) or params["session_id"] != ref.session_id
            or not _text(params["context_id"], 256)
            or not isinstance(params["snapshot_sha256"], str)
            or not _SHA.fullmatch(params["snapshot_sha256"])
            or not isinstance(params["tools"], list) or not 1 <= len(params["tools"]) <= 512):
        raise RuntimeStoreError("invalid_params")
    pairs, names, refs = [], set(), set()
    for item in params["tools"]:
        if (not isinstance(item, dict) or set(item) != {"name", "tool_ref", "description", "input_schema"}
                or not isinstance(item["description"], str) or type(item["input_schema"]) is not dict
                or not isinstance(item["name"], str)
                or not isinstance(item["tool_ref"], str) or not _ULID.fullmatch(item["tool_ref"])
                or item["name"] != "gdb_native_" + item["tool_ref"].lower()
                or item["name"] in names or item["tool_ref"] in refs):
            raise RuntimeStoreError("invalid_params")
        names.add(item["name"])
        refs.add(item["tool_ref"])
        pairs.append((item["name"], item["tool_ref"]))
    # Freeze ALL descriptors, not only dispatch references. The aggregate is bounded
    # before retention; frozen_json also bounds JSON depth/nodes and rejects non-JSON values.
    # These bytes carry model data only, never per-call authority.
    try:
        encoded = frozen_json(params["tools"], 1024 * 1024)
    except RuntimeStoreError:
        raise RuntimeStoreError("invalid_params") from None
    return params["context_id"], params["snapshot_sha256"], tuple(pairs), encoded

@dataclass(frozen=True)
class NativeSelection:
    connection: object
    live: object
    ref: object
    instance_id: str
    authority_epoch: int
    context: str
    snapshot: str
    tools: tuple[tuple[str, str], ...]
    tools_json: bytes
    source_connection: object

    def current(self):
        c, a = self.connection, self.connection.authority
        try:
            if self.source_connection.connection is not c:
                return False
            with self.source_connection.parent.runtime.lock:
                self.source_connection.require_connection()
            require_serving(a)
            if (c._operator is not True or "session:operator" not in c.actor.capabilities
                    or getattr(a, "retiring", False) or getattr(a.runner, "_draining", False)
                    or a.profile_id != self.ref.profile_id
                    or a.instance_id != self.instance_id or a.epoch != self.authority_epoch
                    or a.sessions.get(self.ref.session_id) is not self.live
                    or a.events.get(c.actor.transport_id) is not c.transport
                    or not any(member is c.actor for member in self.live.subscribers.values())):
                return False
            a.authorize(c.actor, self.ref, "session:control", restore_if_missing=False)
            return a.sessions.get(self.ref.session_id) is self.live
        except (RuntimeStoreError, KeyError):
            return False

    def same_selection(self, other):
        return (isinstance(other, NativeSelection)
                and self.connection is other.connection and self.live is other.live
                and self.ref == other.ref and self.instance_id == other.instance_id
                and self.authority_epoch == other.authority_epoch
                and self.context == other.context and self.snapshot == other.snapshot
                and self.tools == other.tools and self.tools_json == other.tools_json
                and self.source_connection is other.source_connection)

    def result(self):
        return {
            "schema": "gdb-native-context-selection-v1",
            "ref": {"profile_id": self.ref.profile_id, "session_id": self.ref.session_id},
            "instance_id": self.instance_id, "authority_epoch": self.authority_epoch,
            "context_id": self.context, "snapshot_sha256": self.snapshot,
            "selected_tool_count": len(self.tools),
            "native_work_admitted": False, "replay_allowed": False,
        }

async def bind_context(c, ref, params):
    # Only the original owned stdin consumer installs this context association.
    # Staging on the authenticated websocket supplies no native-call rights.
    from gateway.native_source_issuance import require_original_app_context
    require_original_app_context(c, ref)
    # Current verified dashboard upgrade already passes operator=True out of band.
    # native_owner/native_bootstrap is NOT this app bridge's permission source.
    if c._operator is not True or "session:operator" not in c.actor.capabilities:
        raise RuntimeStoreError("permission_denied")
    try:
        context, snapshot, tools, encoded = _selected(params, ref)
    except (UnicodeError, ValueError, TypeError, RecursionError):
        raise RuntimeStoreError("invalid_params") from None
    a = c.authority
    require_serving(a)
    live = a.sessions.get(ref.session_id)
    if live is None:
        raise RuntimeStoreError("not_found")
    from gateway.native_source_runtime import require_original_connection
    original_connection = require_original_connection(c)
    selection = NativeSelection(c, live, ref, a.instance_id, a.epoch,
                                context, snapshot, tools, encoded, original_connection)
    with live.event_stream.lock:
        if not selection.current():
            raise RuntimeStoreError("permission_denied")
        old = getattr(live.event_stream, "_gdb_native_selection", None)
        if old is not None:
            if not selection.same_selection(old) or not old.current():
                raise RuntimeStoreError("stale_generation")
            return old.result()
        live.event_stream._gdb_native_selection = selection
        return selection.result()

def bridge_for_owner(authority, session_id, generation, live, owner):
    # Called lazily from SAME ToolCommitOwner.bind under its existing lock, AFTER
    # actual adoption/executing generation and assistant-row witness are usable.
    # _make_owner runs before installation, so constructing here at _make_owner
    # time would incorrectly fail owner.usable().
    if (owner.scope["session_id"] != session_id or owner.scope["generation"] != generation
            or authority.sessions.get(session_id) is not live or not owner.usable()):
        raise RuntimeStoreError("stale_generation")
    with live.event_stream.lock:
        selection = getattr(live.event_stream, "_gdb_native_selection", None)
        if not isinstance(selection, NativeSelection) or not selection.current():
            raise RuntimeStoreError("permission_denied")
        from gateway.native_source_issuance import require_original_app_context
        require_original_app_context(selection.connection, selection.ref, source_owner=owner)
        if (selection.ref.profile_id != owner.scope["profile_id"]
                or selection.instance_id != owner.scope["instance_id"]
                or selection.authority_epoch != owner.scope["epoch"]):
            raise RuntimeStoreError("stale_generation")
        from agent.native_turn_retention import native_retention_for_owner
        retention = native_retention_for_owner(owner)
        if retention is None:
            raise RuntimeStoreError("native_original_task_unavailable")
        agent = owner.agent()
        if agent is None:
            raise RuntimeStoreError("native_original_owner_unavailable")
        retention.bind_owner(owner, agent)
        return NativeDispatchBridge(
            selection.connection, owner, selection.context, selection.snapshot,
            json.loads(selection.tools_json),
        )
