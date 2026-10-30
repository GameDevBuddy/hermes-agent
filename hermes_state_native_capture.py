"""Read the original local relation in the SAME guarded SessionDB transaction."""
from __future__ import annotations

import json
import time

from agent.tool_commit_rows import MAX_TEXT_BYTES, _row_witness, json_digest, text_digest
from hermes_state_runtime import RuntimeStoreError

MAX_CAPTURE = 262144


def refuse(reason):
    raise RuntimeStoreError(reason)


def _scopes(db, conn, logical, physical):
    scope, current, size = [], physical, 0
    for _ in range(1000):
        if current in scope:
            refuse("native_sql_lineage_cycle")
        row = conn.execute("SELECT * FROM sessions WHERE id=?", (current,)).fetchone()
        if row is None or not isinstance(current, str) or not 0 < len(current.encode()) <= 4096:
            refuse("native_sql_session_missing")
        size += len(current.encode())
        if size > MAX_CAPTURE:
            refuse("native_sql_scope_capacity")
        scope.append(current)
        parent = row["parent_session_id"]
        if not parent or db._is_explicit_fork_child_row(dict(row), include_reset=True):
            break
        parent_row = conn.execute("SELECT end_reason FROM sessions WHERE id=?", (parent,)).fetchone()
        if parent_row is None or parent_row[0] != "compression":
            break
        current = parent
    else:
        refuse("native_sql_scope_capacity")
    if logical not in scope or scope[-1] != db._session_turn_lease_key_on_conn(conn, physical):
        refuse("native_sql_lineage_changed")
    return tuple(scope)


def _identity(pin, conn):
    scope = pin.source_slot.pending.owner.scope
    if scope != json.loads(pin.expected):
        refuse("native_sql_original_scope_changed")
    admission = conn.execute("SELECT * FROM session_admissions WHERE admission_id=?", (scope["admission_id"],)).fetchone()
    epoch = conn.execute("SELECT epoch,instance_id FROM runtime_epoch WHERE singleton=1").fetchone()
    if admission is None or epoch is None or tuple(epoch) != (scope["epoch"], scope["instance_id"]):
        refuse("native_sql_epoch_or_admission_changed")
    logical = admission["target_session_id"]
    logical_row = conn.execute("SELECT * FROM sessions WHERE id=?", (logical,)).fetchone()
    physical_row = conn.execute("SELECT * FROM sessions WHERE id=?", (scope["session_id"],)).fetchone()
    if (logical_row is None or physical_row is None or admission["status"] != "started"
            or admission["owner_epoch"] != scope["epoch"] or admission["generation"] != scope["generation"]
            or logical_row["runtime_generation"] != scope["generation"]
            or logical_row["runtime_revision"] != scope["revision"] or physical_row["ended_at"] is not None):
        refuse("native_sql_original_session_changed")
    lineage = json.loads(admission["lineage_json"])
    if not isinstance(lineage, list) or not lineage or lineage[-1] != scope["session_id"]:
        refuse("native_sql_lineage_changed")
    scopes = _scopes(pin.db, conn, logical, scope["session_id"])
    return scope, admission, logical_row, physical_row, scopes


def _leases(pin, conn, scopes):
    original = pin.source_slot.pending
    turn = conn.execute("SELECT holder,acquired_at,expires_at FROM session_turn_leases WHERE conversation_id=?", (scopes[-1],)).fetchone()
    now = time.time()
    if turn is None or turn["holder"] != original.turn_holder or turn["expires_at"] <= now:
        refuse("native_sql_turn_not_original")
    compression = None
    for sid in scopes:
        row = conn.execute("SELECT holder,acquired_at,expires_at FROM compression_locks WHERE session_id=?", (sid,)).fetchone()
        if sid == pin.source_slot.pending.owner.scope["session_id"]:
            compression = row
        elif row is not None:
            refuse("native_sql_ancestor_compression")
        if sid != scopes[-1] and conn.execute("SELECT 1 FROM session_turn_leases WHERE conversation_id=?", (sid,)).fetchone():
            refuse("native_sql_split_turn")
    if original.compression_holder is None:
        if compression is not None:
            refuse("native_sql_compression_not_absent")
    elif (compression is None or compression["holder"] != original.compression_holder or compression["expires_at"] <= now):
        refuse("native_sql_compression_not_original")
    return {"turn_key": scopes[-1], "turn_holder": turn["holder"], "turn_acquired_at": turn["acquired_at"],
            "compression_holder": compression["holder"] if compression else None,
            "compression_acquired_at": compression["acquired_at"] if compression else None}


def precharge_inspections(pin, conn):
    pending = pin.source_slot.pending
    row = conn.execute("SELECT length(CAST(content AS BLOB)),length(CAST(tool_calls AS BLOB)),"
                       "length(CAST(tool_call_uids AS BLOB)) FROM messages WHERE id=? AND session_id=?",
                       (pending.assistant["row_id"], pending.owner.scope["session_id"])).fetchone()
    if row is None or any(value is not None and not 0 <= value <= MAX_TEXT_BYTES for value in row):
        refuse("native_sql_row_capacity")
    # Four bounded rereads, paid before decoding/copying any row. No refund/replay.
    credit = 16 * sum(value or 0 for value in row) + 65536
    pin.book.charge(credit)
    pin.credit += credit


def _assistant(pin, conn):
    pending = pin.source_slot.pending
    row = conn.execute("SELECT * FROM messages WHERE id=? AND session_id=?",
                       (pending.assistant["row_id"], pending.owner.scope["session_id"])).fetchone()
    witness = _row_witness(pin.db, conn, row, time.time()) if row is not None else None
    keys = ("row_id", "message_uid", "row_version", "calls_digest", "call_uids_digest")
    if (witness is None or witness["role"] != "assistant" or witness["active"] != 1
            or any(witness[key] != pending.assistant[key] for key in keys)):
        refuse("native_sql_original_assistant_changed")
    from agent.message_sanitization import coalesce_tool_call_id
    choices = [call for call in witness["calls"] if coalesce_tool_call_id(call) == pending.call_id]
    if (len(choices) != 1 or witness["call_uids"].get(pending.call_id) != pending.uid
            or choices[0].get("function", {}).get("name") != pending.tool_name
            or json_digest(pending.ref.args) != pending.effective_args_digest):
        refuse("native_sql_original_call_changed")
    return {**{key: witness[key] for key in keys}, "call_id": pending.call_id, "tool_name": pending.tool_name,
            "tool_call_uid": pending.uid, "effective_arguments": pending.effective_args_digest,
            "stored_arguments": json_digest(choices[0]["function"].get("arguments")),
            "stored_calls_utf8": text_digest(row["tool_calls"])}


def capture(pin, conn):
    from gateway.native_source_calls import require_original_source_call
    require_original_source_call(pin.source_slot, pin.source_slot._source_declaration_refs)
    if conn is not pin.book.connection or not conn.in_transaction:
        refuse("native_sql_original_transaction_missing")
    scope, admission, logical, physical, scopes = _identity(pin, conn)
    value = {"schema": "gdb-local-sql-source-pin-v1", "scope": scope, "sessions": list(scopes),
             "logical_session_id": logical["id"], "transcript_session_id": physical["id"],
             "logical_revision": logical["runtime_revision"], "physical_revision": physical["runtime_revision"],
             "physical_generation": physical["runtime_generation"],
             "admission": {key: admission[key] for key in ("admission_id", "principal_id", "request_id", "target_session_id",
                                                          "lineage_json", "payload_digest", "owner_epoch", "generation")},
             "assistant": _assistant(pin, conn), "leases": _leases(pin, conn, scopes)}
    return value, scopes


def require_capture(pin, conn):
    value, scopes = capture(pin, conn)
    if value != pin.capture[0] or scopes != pin.scopes:
        refuse("native_sql_capture_changed")
    row = conn.execute("SELECT capture_json,capture_sha256 FROM native_original_pins WHERE pin_id=?", (pin.pin_id,)).fetchone()
    actual_scopes = tuple(row[0] for row in conn.execute("SELECT session_id FROM native_pin_scopes WHERE pin_id=? ORDER BY session_id", (pin.pin_id,)))
    if row is None or tuple(row) != pin.capture[1:] or actual_scopes != tuple(sorted(scopes)):
        refuse("native_sql_stored_capture_changed")
