"""Original canonical writer consumers for local SQL custody, never App proof."""
from __future__ import annotations

from agent.message_metadata import DB_ROW_SNAPSHOT, MESSAGE_UID, TOOL_CALL_UID, stamp_message_uid
from agent.tool_commit_rows import WITNESS, text_digest
from hermes_state_native_book import NativeSQLFailure, block
from hermes_state_runtime import RuntimeStoreError


def _use(pending):
    from agent.native_original_use import NativeOriginalUse, _original_use_local
    slot = pending.native_dispatch_slot
    use = slot._native_active_use if slot is not None else None
    if type(use) is not NativeOriginalUse or use.pending is not pending:
        raise RuntimeStoreError("native_original_writer_unbound")
    _original_use_local(use)
    return use


def prepare_local_message(pending, message, raw_digest):
    slot = pending.native_dispatch_slot
    if slot is None or slot._native_active_use is None:
        return  # Unissued/unavailable ordinary error rows keep their existing behavior.
    use = _use(pending)
    try:
        if (use.phase != "checked" or not use.io_checked or pending.message is not None
                or use.data[0] != raw_digest or text_digest(message.get("content")) != use.data[1]):
            raise NativeSQLFailure("native_sql_result_projection_changed")
        stamp_message_uid(message)
        pending.message_uid = message[MESSAGE_UID]
        message[TOOL_CALL_UID] = pending.uid
        pending.source_digest, pending.projection_digest = use.data
        pending.message = message
        pending.owner.watch(message, pending)
    except BaseException as error:
        block(use.pin.book, error, use.pin, use)
        raise


def fail_sources(sources, error):
    for _index, pending, _message in sources:
        slot = pending.native_dispatch_slot
        pin = slot._native_pin if slot is not None else None
        if pin is not None:
            block(pin.book, error, pin, pin.active_use)


def retained_writer_sources(agent, messages):
    """An unavailable/replaced current owner must never turn native rows into ordinary rows."""
    book = getattr(agent._session_db, "_native_pin_book", None)
    if book is None:
        return ()
    result = []
    for index, message in enumerate(messages):
        for slot, pin in tuple(book.pins.values()):
            pending = slot.pending
            if pin is None or pending.owner.agent() is not agent:
                continue
            if (message is pending.message or (pending.message_uid is not None and message.get(MESSAGE_UID) == pending.message_uid)
                    or message.get(TOOL_CALL_UID) == pending.uid):
                result.append((index, pending, message))
    return tuple(result)


def preflight_original_batch(sources):
    for _index, pending, message in sources:
        use = _use(pending)
        if message is not pending.message or use.phase != "checked" or use.writer_entered:
            raise NativeSQLFailure("native_sql_original_batch_changed")


def begin_original_batch(db, sources):
    if type(sources) is not tuple or len(sources) > 128:
        raise NativeSQLFailure("native_sql_original_batch_capacity")
    for _index, pending, _message in sources:
        use = _use(pending)
        if use.pin.db is not db or use.phase != "checked" or use.writer_entered:
            raise NativeSQLFailure("native_sql_original_batch_reentry")
        use.writer_entered = True  # Before the original body's first row mutation, never reset.


def writer_returned(sources):
    for _index, pending, _message in sources:
        use = _use(pending)
        if not use.writer_entered or use.writer_returned or use.provisional_link is None:
            raise NativeSQLFailure("native_sql_original_writer_did_not_return")
        use.writer_returned = True


def _witness(pending, row):
    witness = row.get(WITNESS)
    if not isinstance(witness, dict):
        raise NativeSQLFailure("native_sql_original_witness_missing")
    parent = witness.get("assistant")
    keys = ("row_id", "message_uid", "row_version", "calls_digest", "call_uids_digest")
    if (not isinstance(parent, dict) or any(parent[key] != pending.assistant[key] for key in keys)
            or witness["role"] != "tool" or witness["active"] != 1
            or witness["tool_call_uid"] != pending.uid or witness["tool_call_id"] != pending.call_id
            or witness["tool_name"] != pending.tool_name or witness["message_uid"] != pending.message_uid
            or witness["content_digest"] != pending.projection_digest):
        raise NativeSQLFailure("native_sql_original_witness_changed")
    return witness


def link_local_row(db, conn, pending, row):
    from agent.native_original_use import require_original_native_use_for_row
    from agent.native_receipt_origin import frozen_json
    use = require_original_native_use_for_row(db, conn, pending.native_dispatch_slot._native_pin, pending)
    if not use.writer_entered or use.writer_returned or use.provisional_link is not None:
        raise NativeSQLFailure("native_sql_original_link_reentry")
    witness = _witness(pending, row)
    count = conn.execute("SELECT COUNT(*) FROM messages WHERE session_id=? AND active=1 AND tool_call_uid=?",
                         (pending.owner.scope["session_id"], pending.uid)).fetchone()[0]
    if count != 1:
        raise NativeSQLFailure("native_sql_ambiguous_original_tool_uid")
    # Persist bounded metadata; the witness/call/result is not a commit proof or App origin.
    data = {key: witness[key] for key in ("row_id", "message_uid", "tool_call_uid", "row_version")}
    encoded = frozen_json(data, 8192).decode("utf-8")
    values = (use.pin.pin_id, pending.owner.scope["session_id"], witness["row_id"], pending.message_uid,
              pending.uid, witness["row_version"], use.data[0]["sha256"], use.data[1]["sha256"], encoded)
    conn.execute("INSERT INTO native_pin_row_links VALUES (?,?,?,?,?,?,?,?,?)", values)
    use.provisional_link = values  # Still private: writer return, marker sync and current owner are required.


def before_consume(pending, witness):
    use = _use(pending)
    message = pending.message
    if (not use.writer_returned or use.provisional_link is None or use.observation is not None
            or not isinstance(witness, dict) or message.get("_db_persisted") is not True or message.get("_row_id") != witness.get("row_id")
            or message.get(DB_ROW_SNAPSHOT) != witness.get("row_version")
            or not pending.owner.binding_current(pending)):
        raise NativeSQLFailure("native_sql_original_writer_or_marker_unavailable")
    _witness(pending, {WITNESS: witness})
    _require_committed_row(use, pending, witness)


def _require_committed_row(use, pending, witness):
    from agent.transcript_repair import transcript_row_snapshot
    from hermes_state_native_schema import require_schema
    with use.pin.db._lock:
        use.pin.book.require_current()
        conn = use.pin.book.connection
        if conn.in_transaction:
            raise NativeSQLFailure("native_sql_original_commit_unavailable")
        require_schema(conn)
        row = conn.execute("SELECT * FROM native_pin_row_links WHERE pin_id=?", (use.pin.pin_id,)).fetchone()
        if row is None or tuple(row) != use.provisional_link:
            raise NativeSQLFailure("native_sql_committed_link_changed")
        actual = conn.execute("SELECT * FROM messages WHERE id=? AND session_id=?",
                              (witness["row_id"], pending.owner.scope["session_id"])).fetchone()
        if (actual is None or transcript_row_snapshot(actual) != witness["row_version"]
                or actual["message_uid"] != pending.message_uid or actual["tool_call_uid"] != pending.uid
                or actual["active"] != 1 or text_digest(actual["content"]) != pending.projection_digest):
            raise NativeSQLFailure("native_sql_committed_row_changed")


def after_consume(pending):
    use = _use(pending)
    if not pending.publication_observed or not pending.owner.binding_current(pending):
        raise NativeSQLFailure("native_sql_original_observation_unavailable")
    with use.pin.book.lock:
        use.pin.book.require_current()
        if use.phase != "checked":
            raise NativeSQLFailure("native_sql_original_observation_unknown")
        use.observation, use.phase = pending.receipt, "observed"


def publish_observation(pending, receipt):
    try:
        return bool(pending.owner.publish(pending, receipt))
    except BaseException as error:
        if pending.native_dispatch_slot is not None:
            fail_sources(((0, pending, pending.message),), error)
        raise


def finish_original_batch(sources):
    for _index, pending, _message in sources:
        use = _use(pending)
        if use.phase != "observed" or use.observation is not pending.receipt:
            raise NativeSQLFailure("native_sql_original_observation_unavailable")
