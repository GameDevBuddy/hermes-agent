"""Couple an authenticated canonical selection to its existing original stdin owner."""
from __future__ import annotations

import asyncio
import json
import re

from gateway.native_app_context_records import (
    _KEY, RECORD_CREDIT, ContextRequest, ContextStage, OriginalAppContext,
    refuse, reserve, source_for,
)
from gateway.session_native_context import NativeSelection, _selected

_NONCE = re.compile(r"[0-9a-f]{64}\Z")


async def stage(connection, ref, params):
    if (type(params) is not dict
            or set(params) != {"nonce", "session_id", "context_id", "snapshot_sha256", "tools"}
            or type(params["nonce"]) is not str or not _NONCE.fullmatch(params["nonce"])):
        refuse("invalid_params")
    if connection._operator is not True or "session:operator" not in connection.actor.capabilities:
        refuse("permission_denied")
    source, original = source_for(connection)
    try:
        context, snapshot, tools, encoded = _selected({k: v for k, v in params.items() if k != "nonce"}, ref)
    except (UnicodeError, ValueError, TypeError, RecursionError):
        refuse("invalid_params")
    authority = connection.authority
    authority.authorize(connection.actor, ref, "session:control", restore_if_missing=False)
    live = authority.sessions.get(ref.session_id)
    selection = NativeSelection(connection, live, ref, authority.instance_id, authority.epoch,
                                context, snapshot, tools, encoded, original)
    if not selection.current():
        refuse("permission_denied")
    with source._state_lock:
        from gateway.native_app_context_construction import begin_context_construction, require_no_context_constructor
        require_no_context_constructor(source)
        slot = (id(connection), ref.session_id)
        if slot in source._app_context_slots:
            refuse("native_original_app_context_capacity_or_reentry")
        reserve(source, source._app_context_stages, params["nonce"], RECORD_CREDIT + len(encoded))
        source._app_context_slots[slot] = params["nonce"]
        attempt = begin_context_construction(source, "stage", params["nonce"], RECORD_CREDIT + len(encoded),
                                             source, source._app_context_stages)
        try:
            with attempt:
                value = attempt.construct(ContextStage, _KEY, source, original, selection, params,
                                          retain=lambda value: source._app_context_stages.__setitem__(params["nonce"], value))
                value.require_current()
        except BaseException as error:
            if source._app_context_error is None:
                source._app_context_error = error
            source.unknown("original-app-context-stage-failed")
            raise
    return {"staged": True, "context_id": context, "nonce": value.nonce}


def claim_from_reader(reader, frame):
    from hermes_owned_control import OriginalControlReader
    if type(reader) is not OriginalControlReader:
        refuse("native_original_app_context_reader_missing")
    reader.require_reading()
    source = reader.source
    with source._state_lock:
        from gateway.native_app_context_construction import begin_context_construction, require_no_context_constructor
        require_no_context_constructor(source)
        reserve(source, source._app_context_requests, frame["requestId"], RECORD_CREDIT)
        attempt = begin_context_construction(source, "request", frame["requestId"], RECORD_CREDIT,
                                             reader, source._app_context_requests)
        try:
            with attempt:
                request = attempt.construct(ContextRequest, _KEY, reader, frame,
                                            retain=lambda value: source._app_context_requests.__setitem__(frame["requestId"], value))
                reader.active = request
                pending = source._app_context_stages.get(frame["nonce"])
                if type(pending) is not ContextStage or pending.request is not None:
                    refuse("native_original_app_context_stage_missing_or_consumed")
                pending.request, request.stage = request, pending  # nonce consumed before loop submission
                if pending.selection.context != frame["contextId"]:
                    refuse()
        except BaseException as error:
            if attempt.value is not None:
                attempt.value.failed(error)
            else:
                source.unknown("original-app-context-bind-failed")
            raise
        return request


async def bind_from_reader(request):
    # Sole caller is the original read_shutdown thread's retained submission.
    if request.task is not None:
        refuse("native_original_app_context_capacity_or_reentry")
    request.task = asyncio.current_task()
    try:
        request.require_current()
        staged, c = request.stage, request.stage.original.connection
        ref = staged.selection.ref
        if ref.session_id in c._gdb_original_app_contexts:
            refuse("native_original_app_context_capacity_or_reentry")
        from gateway.native_app_context_construction import begin_context_construction
        attempt = begin_context_construction(request.source, "association", request.frame["requestId"], 0, request)
        with attempt:
            value = attempt.construct(OriginalAppContext, _KEY, request,
                                      retain=lambda value: setattr(request, "association", value))
            c._gdb_original_app_contexts[ref.session_id] = value
        from gateway.session_authorities import owner_scope
        from gateway.session_native_context import bind_context
        with owner_scope(c.authority):
            request.result = await bind_context(c, ref, {
                "session_id": ref.session_id, "context_id": staged.selection.context,
                "snapshot_sha256": staged.selection.snapshot,
                "tools": json.loads(staged.selection.tools_json),
            })
        request.require_current()
        value.selection = staged.selection.live.event_stream._gdb_native_selection
        if not value.selection.same_selection(staged.selection):
            refuse()
        value.bound = True
        value.require_current(c, ref)
        from hermes_owned_gateway import PROTOCOL
        frame = request.frame
        request.source._emit({"protocol": PROTOCOL, "event": "native_context_bound",
                              "attemptId": frame["attemptId"], "requestId": frame["requestId"],
                              "nonce": frame["nonce"], "contextId": frame["contextId"]})
        request.acknowledged = True
    except BaseException as error:
        request.failed(error)
        raise


def require_context(connection, ref):
    from gateway.session_contract import SessionRef
    if type(ref) is not SessionRef:
        refuse("native_original_app_context_issuer_missing")
    value = connection._gdb_original_app_contexts.get(ref.session_id)
    if type(value) is not OriginalAppContext:
        refuse("native_original_app_context_issuer_missing")
    value.require_current(connection, ref)
    return value
