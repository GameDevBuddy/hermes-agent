"""Original local SQL pin/use custody; App issuance and native permission stay closed."""
from __future__ import annotations

import uuid
import json

from hermes_state_runtime import RuntimeStoreError

_OWNER_KEY = object()
_NATIVE_USE_KEY = object()
NATIVE_USE_CREDIT = 81920


class NativeRetirementPin:
    __slots__ = (
        "active_use", "book", "capture", "committed_data", "committed_marker", "credit", "db", "error",
        "expected", "owner_id", "phase", "pin_id", "scopes", "settlement", "settlement_sha256",
        "source_original_call", "source_retention", "source_slot", "source_stamp",
    )

    def __init__(self, key, db, owner_id, expected, *, source_slot, source_stamp,
                 source_retention, source_original_call):
        if key is not _OWNER_KEY:
            raise RuntimeStoreError("native_pin_not_original_local_owner")
        self.db, self.owner_id, self.expected = db, owner_id, expected
        self.pin_id, self.phase = uuid.uuid4().hex, "created"
        self.capture, self.scopes, self.credit = None, None, 65536
        self.book, self.error = None, None
        self.settlement, self.settlement_sha256 = None, None
        self.committed_data, self.committed_marker = None, None
        self.source_slot, self.source_stamp = source_slot, source_stamp
        self.source_retention, self.source_original_call = source_retention, source_original_call
        self.active_use = None


class NativeOriginalUse:
    __slots__ = (
        "call_json", "capture", "credit", "data", "error", "expected", "io_checked",
        "keeper", "observation", "original_source_call", "owner", "pending", "phase", "pin", "projection", "provisional_link",
        "raw_result", "request", "slot", "source", "sql_values", "writer_entered", "writer_returned",
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
        self.raw_result = self.projection = self.observation = None
        self.provisional_link = None
        self.writer_entered = self.writer_returned = False


def reserve_original_native_pin(slot):
    from agent.native_receipt_origin import _declaration_local_refs
    from gateway.native_source_issuance import require_original_app_call
    source = slot._source_declaration_refs
    _declaration_local_refs(slot, source)
    source.retention.retain_pin_attempt(slot)
    # An object/current tuple alone cannot issue a protected SQL pin or native use.
    require_original_app_call(source.original_source_call)
    return reserve_local_sql_pin(slot)


def reserve_local_sql_pin(slot):
    """Protect the cooperating writer's source relation; return no App authority."""
    from agent.native_receipt_origin import _declaration_local_refs
    from hermes_state_native_book import block, book_for, guarded_write
    from hermes_state_native_capture import capture, precharge_inspections
    from hermes_state_native_schema import install_schema
    from agent.native_receipt_origin import frozen_json
    from agent.tool_commit_rows import text_digest
    source, pin = slot._source_declaration_refs, None
    _declaration_local_refs(slot, source)
    source.retention.retain_pin_attempt(slot)
    book = book_for(source.db)
    try:
        with source.db._lock:
            book.reserve_pin(slot)
            pin = NativeRetirementPin(
                _OWNER_KEY, source.db, source.owner.scope["admission_id"], source.scope_json,
                source_slot=slot, source_stamp=source.original_source_call.assistant.witness,
                source_retention=source.retention, source_original_call=source.original_source_call)
            pin.book = book
            book.pins[id(slot)] = (slot, pin)
            slot._native_pin = pin
            source.retention.retain_before_send(slot, pin)

        def write(conn):
            install_schema(conn)
            precharge_inspections(pin, conn)
            value, scopes = capture(pin, conn)
            encoded = frozen_json(value, 262144).decode("utf-8")
            pin.capture = (json.loads(encoded), encoded, text_digest(encoded)["sha256"])
            pin.scopes = scopes
            e, a = value["scope"], value["assistant"]
            ceiling = conn.execute("SELECT COALESCE(MAX(id),0) FROM messages").fetchone()[0]
            conn.execute("INSERT INTO native_original_pins VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                         (pin.pin_id, pin.owner_id, e["epoch"], e["instance_id"], value["logical_session_id"],
                          e["session_id"], e["admission_id"], e["generation"], a["row_id"], a["message_uid"],
                          a["tool_call_uid"], ceiling, encoded, pin.capture[2], len(scopes), sum(len(s.encode()) for s in scopes), pin.credit))
            conn.executemany("INSERT INTO native_pin_scopes VALUES (?,?)", ((pin.pin_id, sid) for sid in scopes))
        guarded_write(book, write)
        with book.lock:
            book.require_current()
            if pin.phase != "created":
                raise RuntimeStoreError("native_sql_pin_construction_unknown")
            pin.phase = "reserved"
        return pin
    except BaseException as error:
        block(book, error, pin)
        raise


def _acquire_original_native_use(slot, request):
    from agent.native_receipt_origin import _declaration_local_refs
    from gateway.native_source_issuance import require_original_app_call
    source = slot._source_declaration_refs
    _declaration_local_refs(slot, source)
    if source.original_source_call.request is None:
        source.original_source_call.attach_request(slot, request)
    elif source.original_source_call.request is not request:
        raise RuntimeStoreError("native_original_request_changed")
    require_original_app_call(source.original_source_call, request)
    pin = slot._native_pin or reserve_original_native_pin(slot)
    return acquire_local_sql_use(pin, request)


def acquire_local_sql_use(pin, request):
    from hermes_state_native_book import block, guarded_write
    from hermes_state_native_capture import require_capture
    from hermes_state_native_schema import require_schema
    from agent.native_receipt_origin import frozen_json
    from agent.tool_commit_rows import text_digest
    slot, book, use = pin.source_slot, pin.book, None
    source, keeper = slot._source_declaration_refs, pin.source_retention
    try:
        if source.original_source_call.request is None:
            source.original_source_call.attach_request(slot, request)
        _require_request(slot, request)
        with pin.db._lock:
            book.require_current()
            if pin.phase != "reserved" or pin.active_use is not None or pin.pin_id in book.uses:
                raise RuntimeStoreError("native_original_use_reentry_or_capacity")
            book.charge(NATIVE_USE_CREDIT)
            book.uses[pin.pin_id] = None
            with keeper.lock:
                keeper.use_credit += NATIVE_USE_CREDIT
                keeper.active_uses[id(slot)] = None
                use = NativeOriginalUse(_NATIVE_USE_KEY, pin, slot, source, request)
                book.uses[pin.pin_id] = use
                pin.active_use = slot._native_active_use = request.native_active_use = use
                keeper.active_uses[id(use)] = use

        def write(conn):
            require_schema(conn)
            require_capture(pin, conn)
            encoded = frozen_json({"pin_id": pin.pin_id, "request_id": slot._native_request_candidate_id,
                                   "capture_sha256": pin.capture[2]}, 8192).decode("utf-8")
            use.sql_values = (pin.pin_id, slot._native_request_candidate_id, pin.capture[2], encoded,
                              text_digest(encoded)["sha256"], NATIVE_USE_CREDIT)
            conn.execute("INSERT INTO native_pin_active_uses VALUES (?,?,?,?,?,?)", use.sql_values)
        guarded_write(book, write)
        with book.lock:
            book.require_current()
            if use.phase != "staging":
                raise RuntimeStoreError("native_sql_use_construction_unknown")
            use.phase = "held"
        return use
    except BaseException as error:
        block(book, error, pin, use)
        raise


def _require_request(slot, request):
    from gateway.session_server_requests import _Pending, _lock, _open
    with _lock:
        if (type(request) is not _Pending or _open.get(slot._native_request_candidate_id) is not request
                or slot._native_request_object is not request or request.native_slot is not slot
                or request.native_phase != "invoke" or request.native_pair_credit != 2
                or request.method != "native.invoke" or request.replay is not False
                or request.session_id != slot.pending.owner.scope["session_id"]
                or request.events is not slot._source_declaration_refs.live.event_stream):
            raise RuntimeStoreError("native_original_request_changed")


def _original_use_local(use):
    from gateway.native_source_calls import require_original_source_call
    if type(use) is not NativeOriginalUse or type(use.pin) is not NativeRetirementPin:
        raise RuntimeStoreError("native_original_use_unbound")
    require_original_source_call(use.slot, use.source, request=use.request, use=use)
    use.pin.book.require_current()
    _require_request(use.slot, use.request)
    if (use.slot._native_active_use is not use or use.pin.active_use is not use
            or use.request.native_active_use is not use or use.keeper.active_uses.get(id(use)) is not use
            or use.keeper.pins.get(id(use.slot)) is not use.pin or use.pin.book.uses.get(use.pin.pin_id) is not use
            or use.pin.book.pins.get(id(use.slot)) != (use.slot, use.pin)
            or use.pin.phase != "reserved"):
        raise RuntimeStoreError("native_original_use_changed")


def _check_original_native_use_after_io(slot, request, use, *, raw_result=None, projection=None):
    from gateway.native_source_issuance import require_original_app_call
    _original_use_local(use)
    if (use.slot is not slot or use.request is not request
            or slot._native_write_attempted is not True or use.io_checked is not False
            or slot.origin is None or slot.projection is None):
        raise RuntimeStoreError("native_original_use_response_unavailable")
    # No external writer callback or raw receipt substitutes for the original App issuer.
    require_original_app_call(use.original_source_call, request)
    # Root's genuine owning caller must supply its actual raw result and persisted projection.
    return check_local_sql_after_io(use, raw_result, projection)


def check_local_sql_after_io(use, raw_result, projection):
    """Validate local custody/results without issuing an Origin or native permit."""
    from agent.tool_commit_rows import MAX_TEXT_BYTES, text_digest
    from hermes_state_native_book import block, guarded_write
    try:
        _original_use_local(use)
        if use.phase != "held" or use.io_checked:
            raise RuntimeStoreError("native_original_use_after_io_reentry")
        if any(not isinstance(value, str) or len(value) > MAX_TEXT_BYTES for value in (raw_result, projection)):
            raise RuntimeStoreError("native_original_result_capacity")
        with use.pin.db._lock:
            # Retain these bounded original references even if the inspection debit fails.
            use.raw_result, use.projection = raw_result, projection
            use.pin.book.charge(4 * (len(raw_result) + len(projection)) + 65536)
            raw, persisted = text_digest(raw_result), text_digest(projection)
            if raw is None or persisted is None:
                raise RuntimeStoreError("native_original_result_capacity")
            use.data = (raw, persisted)
        guarded_write(use.pin.book, lambda conn: require_local_use_on_conn(use, conn))
        with use.pin.book.lock:
            use.pin.book.require_current()
            if use.phase != "held":
                raise RuntimeStoreError("native_sql_after_io_unknown")
            use.io_checked, use.phase = True, "checked"
        return use
    except BaseException as error:
        block(use.pin.book, error, use.pin, use)
        raise


def require_local_use_on_conn(use, conn):
    from hermes_state_native_capture import require_capture
    from hermes_state_native_schema import require_schema
    _original_use_local(use)
    if conn is not use.pin.book.connection or not conn.in_transaction or use.phase not in ("held", "checked"):
        raise RuntimeStoreError("native_original_writer_unbound")
    require_schema(conn)
    require_capture(use.pin, conn)
    row = conn.execute("SELECT * FROM native_pin_active_uses WHERE pin_id=?", (use.pin.pin_id,)).fetchone()
    if row is None or tuple(row) != use.sql_values:
        raise RuntimeStoreError("native_original_use_row_changed")


def require_original_native_use_for_row(db, conn, pin, pending):
    if pin is None or pin.db is not db or not conn.in_transaction:
        raise RuntimeStoreError("native_original_writer_unbound")
    use = pin.active_use
    _original_use_local(use)
    if use.pending is not pending or use.owner is not pending.owner:
        raise RuntimeStoreError("native_original_use_writer_changed")
    if not use.io_checked or use.phase != "checked":
        raise RuntimeStoreError("native_original_use_after_io_missing")
    require_local_use_on_conn(use, conn)
    return use


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
        from agent.native_sql_writer import link_local_row
        link_local_row(db, conn, pending, messages[index])
