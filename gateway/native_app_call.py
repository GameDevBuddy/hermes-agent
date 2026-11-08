"""Per-call source authentication on the original socket/pipes, before preparation.

The wire digest correlates bytes already sent by the retained source call. It is
not an Origin, a commit proof, a native permit or evidence of writer exclusion.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time

from hermes_state_runtime import RuntimeStoreError

from gateway.native_source_construction import require_constructed, reserve_construction
from gateway.native_source_runtime import refuse

MAX_CALL_BYTES = 262144


class AppCall:
    def __init__(self, original, request):
        self.original, self.request, self.slot = original, request, original.slot
        self.context = self.wire = self.digest = None
        self.coroutine = self.future = self.task = None
        self.reader = self.frame = self.bind_task = self.bind_coroutine = self.bind_future = None
        self.error = self.traceback = None
        self.sent = self.authenticated = self.acknowledged = False
        self.cancel_requested = False
        self.timeout_requested = False
        self.send_complete = asyncio.Event()
        self.phases, self.phase_credits = {}, {}
        self.protocol_phase = "source"
        self.invocation = self.origin = self.writer_lease = None
        self.initial_proof = self.commit_proof = self.commit_proof_sha256 = None
        self.result_frame = self.raw_result = self.decline_frame = None
        self.worker_event = threading.Event()
        from gateway.session_server_requests import REQUEST_TIMEOUT_S
        self.worker_thread, self.deadline = threading.current_thread(), time.monotonic() + REQUEST_TIMEOUT_S
        self.proof_credit, self.terminal, self.completion = 0, None, None

    def failed(self, error):
        if self.error is None:
            self.error, self.traceback = error, error.__traceback__
        self.request.native_phase = "blocked"
        self.request.event.set()
        self.worker_event.set()
        for phase in self.phases.values():
            self.original.loop.call_soon_threadsafe(phase.completion.set)

    def require_current(self):
        require_constructed(self)
        self.original.require_call()
        if (self.error is not None or self.original.request is not self.request
                or self.slot._native_request_object is not self.request
                or self.request.native_slot is not self.slot):
            refuse("native_original_app_call_changed")
        if self.context is not None:
            from gateway.native_app_context import require_context
            # Construction and loop consumers retain their original loop gate.
            # Only this authenticated original worker may inspect the retained association off-loop.
            try:
                on_loop = asyncio.get_running_loop() is self.original.loop
            except RuntimeError:
                on_loop = False
            worker = self if not on_loop and threading.current_thread() is self.worker_thread else None
            inputs = self.original.connection.connection, self.original.selection.ref
            consumer = self if on_loop else worker
            current = require_context(*inputs) if consumer is None else require_context(*inputs, original_call=consumer)
            if current is not self.context or not current.selection.same_selection(self.original.selection):
                refuse("native_original_app_call_context_changed")
        if self.wire is not None and self.wire != _wire(self.original, self.request):
            refuse("native_original_app_call_arguments_changed")


def _wire(original, request):
    # The bounded, original frozen argument input was charged before this point.
    # Retain the SAME effective dictionary and compare it again after every IO.
    from gateway.native_request_arguments import require_request_parameters
    require_request_parameters(original, request)
    frame = {"jsonrpc": "2.0", "id": original.slot._native_request_candidate_id,
             "method": "native.invoke", "params": request.params}
    pieces, size = [], 0
    for piece in json.JSONEncoder(ensure_ascii=True, separators=(",", ":"), allow_nan=False).iterencode(frame):
        size += len(piece)
        if size > MAX_CALL_BYTES:
            refuse("native_original_app_call_capacity")
        pieces.append(piece)
    return "".join(pieces)


def prepare(original, request):
    original.require_call()
    slot = original.slot
    attempt = reserve_construction(original.connection, slot._source_constructions, "app_call", parent=slot,
                                   book_owner=slot, book_name="_source_constructions")
    with attempt:
        from gateway.native_source_runtime import MAX_SOURCE_REFERENCE_BYTES
        runtime = original.connection.parent.runtime
        with runtime.lock:
            if runtime.bytes + MAX_CALL_BYTES > MAX_SOURCE_REFERENCE_BYTES:
                refuse("native_original_app_call_capacity")
            runtime.bytes += MAX_CALL_BYTES  # retained wire credit, never refunded on failure
        call = attempt.construct(AppCall, original, request)
    return call


async def send(call):
    call.task = asyncio.current_task()
    try:
        call.require_current()
        from gateway.native_app_context import require_context
        call.context = require_context(call.original.connection.connection, call.original.selection.ref, original_call=call)
        call.require_current()
        call.wire = _wire(call.original, call.request)
        call.digest = hashlib.sha256(call.wire.encode("ascii")).hexdigest()
        transport = call.original.connection.transport
        await transport.write_original_call(call)
        call.require_current()
        call.sent = True
    except BaseException as error:
        call.failed(error)
        raise
    finally:
        call.send_complete.set()


def register(slot, request):
    """The original agent worker waits for this source ACK, never a native result."""
    original = slot._source_declaration_refs.original_source_call
    original.require_call()
    from hermes_owned_gateway import OwnedGatewaySource, current
    source = getattr(original.connection.parent.runtime.runner, "_owned_gateway_source", None)
    if type(source) is not OwnedGatewaySource or current(source.contract) is not source:
        request.native_phase = "blocked"
        refuse("native_original_app_call_issuer_missing")  # refusal only; no source ACK/event
    call = prepare(original, request)
    try:
        on_loop = asyncio.get_running_loop() is original.loop
    except RuntimeError:
        on_loop = False
    try:
        if on_loop:
            refuse("native_original_app_call_worker_required")
        call.coroutine = send(call)
        call.future = asyncio.run_coroutine_threadsafe(call.coroutine, original.loop)
        call.future.result()
        # No replay/refund/replacement on a cancelled or failed source attempt.
        while not request.event.wait(0.25):
            original.require_call()
            if source._control_error is not None:
                raise RuntimeStoreError("native_original_app_call_control_unknown") from source._control_error
            if source.phase != "bound" or source._shutdown or source._control_closed:
                refuse("native_original_app_call_control_changed")
        if call.error is not None:
            raise call.error
        if not call.acknowledged:
            refuse("native_original_app_call_ack_missing")
    except BaseException as error:
        call.failed(error)
        raise
    return call


def claim(reader, frame):
    from gateway import session_server_requests as requests
    from hermes_owned_control import OriginalControlReader
    if type(reader) is not OriginalControlReader:
        refuse("native_original_app_call_reader_missing")
    reader.require_reading()
    if reader.active is not None:
        refuse("native_original_app_call_reentry")
    from gateway.native_app_context_records import reserve, RECORD_CREDIT
    with reader.source._state_lock, requests._lock:
        request = requests._open.get(frame["serverRequestId"])
        slot = getattr(request, "native_slot", None)
        call = None if slot is None else slot._source_constructions.get("app_call")
        if type(call) is not AppCall or call.request is not request:
            refuse("native_original_app_call_missing")
        # Consume before task/coroutine/submission callbacks, even on failure.
        if call.frame is not None:
            refuse("native_original_app_call_reentry")
        reserve(reader.source, reader.source._app_context_requests, frame["requestId"], RECORD_CREDIT)
        reader.source._app_context_requests[frame["requestId"]] = call
        call.reader, call.frame, reader.active = reader, frame, call
    return call


async def bind(call):
    if call.bind_task is not None:
        refuse("native_original_app_call_reentry")
    call.bind_task = asyncio.current_task()
    try:
        await call.send_complete.wait()
        call.require_current()
        frame, context = call.frame, call.context
        if (not call.sent or context is None or call.reader.active is not call
                or call.reader is not context.request.reader
                or call.reader.source is not context.stage.source
                or call.reader.source._app_context_requests.get(frame["requestId"]) is not call
                or frame["nonce"] != context.stage.nonce
                or frame["contextId"] != call.original.selection.context
                or frame["wireSha256"] != call.digest
                or frame["serverRequestId"] != call.slot._native_request_candidate_id):
            refuse("native_original_app_call_control_changed")
        call.reader.require_retained()
        call.authenticated = True
        from hermes_owned_gateway import PROTOCOL
        call.reader.source._emit({"protocol": PROTOCOL, "event": "native_call_bound",
                                 "attemptId": frame["attemptId"], "requestId": frame["requestId"],
                                 "nonce": frame["nonce"], "contextId": frame["contextId"],
                                 "serverRequestId": frame["serverRequestId"], "wireSha256": call.digest,
                                 "executionHeld": True})
        call.require_current()
        call.acknowledged = True
        call.protocol_phase = "awaiting_prepare"
        call.request.native_phase = "execution_held"
        call.request.event.set()
    except BaseException as error:
        call.failed(error)
        raise
