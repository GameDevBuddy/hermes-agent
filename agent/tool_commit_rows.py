"""Bounded provisional stored-row witnesses for the existing SessionDB batch writer."""
from __future__ import annotations

import hashlib
import json
import logging
import math
import time

from agent.transcript_repair import transcript_row_snapshot

WITNESS = "_gdb_provisional_tool_commit"
MAX_WITNESSES = MAX_CALLS = 128
MAX_TEXT_BYTES = 8 * 1024 * 1024
MAX_JSON_NODES = 32768
MAX_JSON_DEPTH = 32


def text_digest(value):
    if not isinstance(value, str) or len(value) > MAX_TEXT_BYTES:
        return None
    try:
        raw = value.encode("utf-8", "strict")
    except UnicodeError:
        return None
    if len(raw) > MAX_TEXT_BYTES:
        return None
    return {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def _scalar_cost(item):
    if type(item) is str:
        if len(item) > MAX_TEXT_BYTES:
            return None
        try:
            return len(item.encode("utf-8", "strict"))
        except UnicodeError:
            return None
    if item is None or type(item) is bool:
        return 5
    if type(item) is int:
        return 80 if item.bit_length() <= 256 else None
    if type(item) is float and math.isfinite(item):
        return 32
    return None


def _bounded_json_tree(value):
    stack, seen = [(value, 0)], set()
    nodes, remaining = MAX_JSON_NODES, MAX_TEXT_BYTES
    while stack:
        item, depth = stack.pop()
        nodes -= 1
        if nodes < 0 or depth > MAX_JSON_DEPTH:
            return False
        if type(item) in (dict, list, tuple):
            if id(item) in seen or len(item) > nodes:
                return False
            seen.add(id(item))
            children = list(item) + list(item.values()) if type(item) is dict else item
            if type(item) is dict and any(type(key) is not str for key in item):
                return False
            if len(children) > nodes:
                return False
            stack.extend((child, depth + 1) for child in children)
            remaining -= len(children) * 2
        else:
            cost = _scalar_cost(item)
            if cost is None:
                return False
            remaining -= cost
        if remaining < 0:
            return False
    return True


def json_digest(value):
    if not _bounded_json_tree(value):
        return None
    digest, size = hashlib.sha256(), 0
    encoder = json.JSONEncoder(sort_keys=True, ensure_ascii=False,
                               separators=(",", ":"), allow_nan=False)
    try:
        for chunk in encoder.iterencode(value):
            raw = chunk.encode("utf-8", "strict")
            size += len(raw)
            if size > MAX_TEXT_BYTES:
                return None
            digest.update(raw)
    except (TypeError, ValueError, UnicodeError, RecursionError):
        return None
    return {"bytes": size, "sha256": digest.hexdigest()}


def _definitions(row):
    values = []
    for column, empty in (("tool_calls", []), ("tool_call_uids", {})):
        raw = row[column]
        if raw is not None and text_digest(raw) is None:
            return None
        try:
            values.append(json.loads(raw) if raw else empty)
        except (TypeError, ValueError, RecursionError):
            return None
    calls, uids = values
    if (not isinstance(calls, list) or len(calls) > MAX_CALLS
            or not isinstance(uids, dict) or len(uids) > MAX_CALLS):
        return None
    calls_digest, uids_digest = json_digest(calls), json_digest(uids)
    if calls_digest is None or uids_digest is None:
        return None
    return calls, uids, calls_digest, uids_digest


def _row_witness(db, conn, row, clock):
    definitions = _definitions(row)
    if definitions is None:
        return None
    epoch = conn.execute(
        "SELECT epoch, instance_id FROM runtime_epoch WHERE singleton=1"
    ).fetchone()
    session = conn.execute(
        "SELECT runtime_generation, runtime_revision FROM sessions WHERE id=?",
        (row["session_id"],),
    ).fetchone()
    lease_key = db._session_turn_lease_key_on_conn(conn, row["session_id"])
    lease = conn.execute(
        "SELECT holder, expires_at FROM session_turn_leases WHERE conversation_id=?",
        (lease_key,),
    ).fetchone()
    compression = conn.execute(
        "SELECT holder, expires_at FROM compression_locks WHERE session_id=?",
        (row["session_id"],),
    ).fetchone()
    if epoch is None or session is None or lease is None or lease["expires_at"] <= clock:
        return None
    calls, uids, calls_digest, uids_digest = definitions
    return {
        "row_id": row["id"], "session_id": row["session_id"],
        "role": row["role"], "active": row["active"],
        "message_uid": row["message_uid"], "tool_call_uid": row["tool_call_uid"],
        "tool_call_id": row["tool_call_id"], "tool_name": row["tool_name"],
        "calls": calls, "call_uids": uids, "calls_digest": calls_digest,
        "call_uids_digest": uids_digest,
        "content_digest": text_digest(row["content"]),
        "row_version": transcript_row_snapshot(row),
        "epoch": epoch["epoch"], "instance_id": epoch["instance_id"],
        "generation": session["runtime_generation"],
        "revision": session["runtime_revision"], "turn_holder": lease["holder"],
        "compression_holder": compression["holder"] if compression else None,
        "compression_expires_at": compression["expires_at"] if compression else None,
        "observed_in_transaction_at": clock,
    }


def _capture(db, conn, session_id, messages, requested):
    if not requested or len(requested) > MAX_WITNESSES:
        return
    if any(type(item) is not tuple or len(item) != 2 for item in requested):
        return
    if len({item[0] for item in requested}) != len(requested):
        return
    if any(type(index) is not int or index < 0 or index >= len(messages)
           or (parent_uid is not None and not isinstance(parent_uid, str))
           for index, parent_uid in requested):
        return
    clock = time.time()
    for index, parent_uid in requested:
        message = messages[index]
        row_id = message.get("_row_id")
        if type(row_id) is not int:
            continue
        row = conn.execute(
            "SELECT * FROM messages WHERE session_id=? AND id=? AND active=1",
            (session_id, row_id),
        ).fetchone()
        if row is None or row["role"] not in {"assistant", "tool"}:
            continue
        witness = _row_witness(db, conn, row, clock)
        if witness is not None and row["role"] == "tool":
            parents = conn.execute(
                "SELECT * FROM messages WHERE session_id=? AND message_uid=? "
                "AND role='assistant' AND active=1 LIMIT 2",
                (session_id, parent_uid),
            ).fetchall() if parent_uid else []
            witness["assistant"] = _row_witness(db, conn, parents[0], clock) if len(parents) == 1 else None
        message[WITNESS] = witness


def capture_witnesses(db, conn, session_id, messages, requested):
    for message in messages:
        message.pop(WITNESS, None)
    if not conn.in_transaction:
        raise RuntimeError("Canonical observation requires the original body transaction")
    try:
        _capture(db, conn, session_id, messages, requested)
    except (TypeError, ValueError, UnicodeError, RecursionError):
        for message in messages:
            message.pop(WITNESS, None)
        if not conn.in_transaction:
            raise
        # Only safe data/encoding refusal while the body transaction is intact.
        logging.getLogger(__name__).warning("Canonical tool-row witness unavailable", exc_info=True)
    except Exception:
        # SQLite/storage/unknown failures keep the existing writer's error path.
        for message in messages:
            message.pop(WITNESS, None)
        raise
    if not conn.in_transaction:
        for message in messages:
            message.pop(WITNESS, None)
        raise RuntimeError("Body transaction lost during canonical observation")
