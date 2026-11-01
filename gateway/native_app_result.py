"""Results/declines from the SAME original canonical App socket, never viewers."""
from __future__ import annotations

import json

from gateway.native_source_runtime import refuse

DECLINE = {"code": -32041, "message": "User declined native request"}


def denied_result():
    return json.dumps({"error": "native_request_declined", "isError": True,
                       "message": DECLINE["message"], "replayAllowed": False}, separators=(",", ":"))


def _original_call(connection, frame):
    from gateway import session_server_requests as requests
    from gateway.native_app_call import AppCall
    with requests._lock:
        pending = requests._open.get(frame.get("id"))
    slot = getattr(pending, "native_slot", None)
    call = None if slot is None else slot._source_constructions.get("app_call")
    if type(call) is not AppCall or call.original.connection.connection is not connection:
        return None
    return call


def _decline(call, frame):
    if (set(frame) != {"jsonrpc", "id", "error"} or frame["jsonrpc"] != "2.0"
            or frame["error"] != DECLINE or call.protocol_phase != "awaiting_prepare"
            or call.phases or call.invocation is not None or call.slot._native_pin is not None):
        refuse("native_original_decline_unavailable")
    call.decline_frame = frame
    call.protocol_phase, call.request.native_phase = "declined", "declined"
    call.worker_event.set()


def _native_result(call, frame):
    from agent.tool_commit_rows import MAX_TEXT_BYTES
    if (set(frame) != {"jsonrpc", "id", "result"} or frame["jsonrpc"] != "2.0"
            or call.protocol_phase != "prepared" or call.writer_lease is None):
        refuse("native_original_result_before_preparation")
    result = frame["result"]
    if (type(result) is not dict or set(result) != {"invocationId", "invocationSequence", "rawResult"}
            or (result["invocationId"], result["invocationSequence"]) != call.invocation
            or type(result["invocationSequence"]) is not int or type(result["rawResult"]) is not str
            or len(result["rawResult"]) > MAX_TEXT_BYTES):
        refuse("native_original_result_changed")
    call.writer_lease.require_current()
    runtime = call.original.connection.parent.runtime
    with runtime.lock:
        credit = 4 * len(result["rawResult"]) + 4096
        from gateway.native_source_runtime import MAX_SOURCE_REFERENCE_BYTES
        if runtime.bytes + credit > MAX_SOURCE_REFERENCE_BYTES:
            refuse("native_original_result_capacity")
        runtime.bytes += credit
    if len(result["rawResult"].encode("utf-8")) > MAX_TEXT_BYTES:
        refuse("native_original_result_capacity")
    call.result_frame, call.raw_result = frame, result["rawResult"]
    call.protocol_phase = "result_received"
    call.worker_event.set()


def resolve(connection, frame):
    call = _original_call(connection, frame)
    if call is None:
        return False
    try:
        call.require_current()
        if not call.acknowledged or call.original.connection.transport is not connection.transport:
            refuse("native_original_result_source_unavailable")
        if call.result_frame is not None or call.decline_frame is not None:
            refuse("native_original_result_replay")
        if "error" in frame:
            _decline(call, frame)
        else:
            _native_result(call, frame)
        return True
    except BaseException as error:
        call.failed(error)
        raise
