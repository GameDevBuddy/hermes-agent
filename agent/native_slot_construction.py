"""Retain original slot/declaration construction, without issuing a native call."""
from __future__ import annotations

from gateway.native_source_construction import reserve_construction, require_constructed
from gateway.native_source_runtime import require_original_connection, refuse


def reserve_slot(bridge, pending):
    from agent.native_receipt_origin import NativeSlot, _KEY
    from agent.tool_commit_rows import MAX_CALLS
    with bridge.lock:
        if pending.owner is not bridge.owner or pending.ref is None:
            refuse("native_original_owner_unavailable")
        if pending.uid in bridge.slots or len(bridge.slots) >= MAX_CALLS:
            refuse("native_original_call_reentry_or_capacity")
        connection = require_original_connection(bridge.connection)
        attempt = reserve_construction(connection, bridge.slots, pending.uid, parent=bridge,
                                       book_owner=bridge, book_name="slots")
        with attempt:
            originals = (bridge.owner, bridge.connection, bridge.context, bridge.snapshot, bridge.tools,
                         pending.owner, pending.ref, pending.uid, bridge.owner.native_source_assistant)
            attempt.inputs = originals
            slot = attempt.construct(NativeSlot, _KEY, bridge, pending)
            current = (bridge.owner, bridge.connection, bridge.context, bridge.snapshot, bridge.tools,
                       pending.owner, pending.ref, pending.uid, bridge.owner.native_source_assistant)
            if (any(before is not after for before, after in zip(originals, current))
                    or slot.bridge is not bridge or slot.pending is not pending):
                refuse("native_original_slot_construction_changed")
        return slot


def prepare_declaration(slot):
    from agent.native_receipt_origin import NativeSourceDeclarationRefs, frozen_json, _declaration_local_refs
    from agent.native_turn_retention import native_retention_for_owner
    from gateway.native_source_calls import capture_original_call
    connection = require_original_connection(slot.bridge.connection)
    attempt = reserve_construction(connection, slot._source_constructions, "declaration", parent=slot,
                                   book_owner=slot, book_name="_source_constructions")
    with attempt:
        owner, pending, bridge = slot.pending.owner, slot.pending, slot.bridge
        attempt.arguments = (slot, pending, pending.ref, owner, bridge, connection)
        retention = native_retention_for_owner(owner)
        if retention is None:
            refuse("native_original_task_unavailable")
        selection = getattr(retention.live.event_stream, "_gdb_native_selection", None)
        attempt.inputs = _declaration_inputs(slot, retention, selection)
        if selection is None or not selection.current():
            refuse("native_original_selection_changed")
        scope, assistant = frozen_json(owner.scope), frozen_json(pending.assistant)
        attempt.require_inputs(_declaration_inputs(slot, retention, selection))
        original = capture_original_call(slot, retention, selection, scope, assistant)
        source = attempt.construct(
            NativeSourceDeclarationRefs, slot, pending, pending.ref, owner, owner.agent(), owner.db, bridge,
            bridge.connection, connection.authority, connection.actor, connection.transport,
            connection.actor.transport_id, retention, retention.task, retention.loop,
            retention.live, selection, scope, assistant, slot.call_json, slot.tool_json,
            slot.arguments_json, original, retain=lambda value: setattr(slot, "_source_declaration_refs", value))
        with slot.lock:
            attempt.require_inputs(_declaration_inputs(slot, retention, selection))
            original.attach_declaration(source)
            _declaration_local_refs(slot, source)
            attempt.require_inputs(_declaration_inputs(slot, retention, selection))
    with slot.lock:
        require_constructed(source)
        slot._source_declaration_phase = "ready"
    return source


def _declaration_inputs(slot, retention, selection):
    owner, bridge = slot.pending.owner, slot.bridge
    return (slot.pending, slot.pending.ref, owner, owner.agent(), owner.db, owner.scope, bridge,
            bridge.connection, retention, retention.authority, retention.task, retention.loop,
            retention.live, retention.live.event_stream, selection,
            slot.call_json, slot.tool_json, slot.arguments_json)
