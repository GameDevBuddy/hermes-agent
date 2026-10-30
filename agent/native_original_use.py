"""Original source pin/use graph with the App issuance boundary still closed.

These source-held objects are not an SQL pin or commit proof. The real App
OriginalCanonicalCall/Origin issuer is absent; all issuance, invoke and native
retirement paths refuse. No protected SQL schema is fabricated or installed.
"""
from __future__ import annotations

import uuid

from hermes_state_runtime import RuntimeStoreError

_OWNER_KEY = object()
_NATIVE_USE_KEY = object()
NATIVE_USE_CREDIT = 81920


class NativeRetirementPin:
    __slots__ = (
        "active_use", "capture", "committed_data", "committed_marker", "credit", "db",
        "expected", "owner_id", "phase", "pin_id", "scopes", "settlement", "settlement_sha256",
        "source_original_call", "source_retention", "source_slot", "source_stamp",
    )

    def __init__(self, key, db, owner_id, expected, *, source_slot, source_stamp,
                 source_retention, source_original_call):
        if key is not _OWNER_KEY:
            raise RuntimeStoreError("native_pin_not_original_local_owner")
        self.db, self.owner_id, self.expected = db, owner_id, expected
        self.pin_id, self.phase = uuid.uuid4().hex, "created"
        self.capture, self.scopes, self.credit = None, None, 0
        self.settlement, self.settlement_sha256 = None, None
        self.committed_data, self.committed_marker = None, None
        self.source_slot, self.source_stamp = source_slot, source_stamp
        self.source_retention, self.source_original_call = source_retention, source_original_call
        self.active_use = None


class NativeOriginalUse:
    __slots__ = (
        "call_json", "capture", "credit", "data", "error", "expected", "io_checked",
        "keeper", "original_source_call", "owner", "pending", "phase", "pin", "request",
        "slot", "source", "sql_values",
    )

    def __init__(self, key, pin, slot, source, request):
        if key is not _NATIVE_USE_KEY:
            raise RuntimeStoreError("native_use_not_original")
        self.pin, self.slot, self.source, self.request = pin, slot, source, request
        self.pending, self.owner, self.keeper = slot.pending, slot.pending.owner, source.retention
        self.expected, self.capture, self.call_json = pin.expected, pin.capture, slot.call_json
        self.phase, self.data, self.sql_values = "staging", None, None
        self.io_checked, self.credit, self.error = False, NATIVE_USE_CREDIT, None
        self.original_source_call = source.original_source_call


def reserve_original_native_pin(slot):
    from agent.native_receipt_origin import _declaration_local_refs
    from gateway.native_source_issuance import require_original_app_call
    source = slot._source_declaration_refs
    _declaration_local_refs(slot, source)
    source.retention.retain_pin_attempt(slot)
    # An object/current tuple alone cannot issue a protected SQL pin or native use.
    require_original_app_call(source.original_source_call)
    pin = NativeRetirementPin(
        _OWNER_KEY, source.db, source.owner.scope["admission_id"], source.owner.scope,
        source_slot=slot, source_stamp=source.original_source_call.assistant.witness,
        source_retention=source.retention, source_original_call=source.original_source_call)
    slot._native_pin = pin
    source.retention.retain_before_send(slot, pin)
    return pin  # Passive source reference; require_native_pin_for_invoke still refuses.


def _acquire_original_native_use(slot, request):
    from agent.native_receipt_origin import _declaration_local_refs
    from gateway.native_source_issuance import require_original_app_call
    source = slot._source_declaration_refs
    _declaration_local_refs(slot, source)
    source.original_source_call.attach_request(slot, request)
    require_original_app_call(source.original_source_call, request)
    pin = reserve_original_native_pin(slot)
    keeper = source.retention
    with keeper.lock:
        if id(slot) in keeper.active_uses or keeper.use_credit + NATIVE_USE_CREDIT > 64 * 1024 * 1024:
            raise RuntimeStoreError("native_original_use_reentry_or_capacity")
        keeper.use_credit += NATIVE_USE_CREDIT
        keeper.active_uses[id(slot)] = None  # Failure retains the debit and original slot.
        use = NativeOriginalUse(_NATIVE_USE_KEY, pin, slot, source, request)
        pin.active_use = slot._native_active_use = use
        keeper.active_uses[id(use)] = use
    return use


def _original_use_local(use):
    from gateway.native_source_calls import require_original_source_call
    if type(use) is not NativeOriginalUse or type(use.pin) is not NativeRetirementPin:
        raise RuntimeStoreError("native_original_use_unbound")
    require_original_source_call(use.slot, use.source, request=use.request, use=use)
    if (use.slot._native_active_use is not use or use.pin.active_use is not use
            or use.keeper.active_uses.get(id(use)) is not use):
        raise RuntimeStoreError("native_original_use_changed")


def _check_original_native_use_after_io(slot, request, use):
    from gateway.native_source_issuance import require_original_app_call
    _original_use_local(use)
    if (use.slot is not slot or use.request is not request
            or slot._native_write_attempted is not True or use.io_checked is not False
            or slot.origin is None or slot.projection is None):
        raise RuntimeStoreError("native_original_use_response_unavailable")
    # No external writer callback or raw receipt substitutes for the original App issuer.
    require_original_app_call(use.original_source_call, request)
    raise RuntimeStoreError("native_original_sql_after_io_unavailable")


def require_original_native_use_for_row(db, conn, pin, pending):
    if pin is None or pin.db is not db or not conn.in_transaction:
        raise RuntimeStoreError("native_original_writer_unbound")
    use = pin.active_use
    _original_use_local(use)
    if use.pending is not pending or use.owner is not pending.owner:
        raise RuntimeStoreError("native_original_use_writer_changed")
    from gateway.native_source_issuance import require_original_app_call
    require_original_app_call(use.original_source_call, use.request)
    raise RuntimeStoreError("native_original_sql_writer_unavailable")


def require_original_source_writer_rows(db, conn, session_id, messages, sources):
    from agent.message_metadata import MESSAGE_UID
    for index, pending, original_message in sources:
        if (pending.owner.db is not db or pending.owner.scope["session_id"] != session_id
                or pending.message is not original_message
                or not 0 <= index < len(messages)
                or messages[index].get(MESSAGE_UID) != pending.message_uid):
            raise RuntimeStoreError("native_original_writer_unbound")
        slot = pending.native_dispatch_slot
        if slot is None or slot.pending is not pending:
            raise RuntimeStoreError("native_original_writer_unbound")
        require_original_native_use_for_row(db, conn, slot._native_pin, pending)
