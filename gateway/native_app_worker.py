"""Original agent worker owns preparation, canonical save and final private phases."""
from __future__ import annotations

import threading
import time

from gateway.native_source_construction import reserve_construction
from gateway.native_source_runtime import MAX_SOURCE_REFERENCE_BYTES, refuse


def _wait(call, ready):
    while True:
        call.worker_event.clear()
        if call.error is not None:
            raise call.error
        call.require_current()
        reader, source = call.reader, call.reader.source
        reader.require_retained()
        if source._control_error is not None:
            raise source._control_error
        if ready():
            return
        if (not call.cancel_requested and call.protocol_phase in ("awaiting_prepare", "prepared")
                and getattr(call.original.agent, "_interrupt_requested", False)):
            # Use the ordinary request withdrawal event. The App declines an
            # unapproved call or cancels its existing broker invocation; this
            # worker still waits for that real result and saves it normally.
            call.cancel_requested = True
            call.request.events.publish(call.request.session_id, {
                "id": call.slot._native_request_candidate_id,
                "method": "native.invoke", "reason": "interrupted",
            }, event_type="request.cancel")
        if time.monotonic() >= call.deadline:
            refuse("native_original_call_deadline_unknown")
        call.worker_event.wait(min(0.25, call.deadline - time.monotonic()))


def _phase(call, command):
    _wait(call, lambda: _phase_started(call, command))
    phase = call.phases[command]
    phase.require_current()
    if phase.error is not None or phase.completed or phase.command != command:
        refuse("native_original_phase_action_unavailable")
    return phase


def _phase_started(call, command):
    phase = call.phases.get(command)
    return (phase is not None and phase.task is not None and phase.future is not None
            and phase.coroutine is not None and not phase.task.done()
            and phase.task.get_loop() is call.original.loop)


def _prepare(call, phase):
    from gateway.native_app_origin import issue
    from hermes_state_native_lease import NativeWriterLease
    from agent.native_original_use import _acquire_original_native_use
    from gateway.native_app_proof import initial
    from hermes_owned_gateway import MAX_FRAME
    origin = issue(call, phase)
    runtime = call.original.connection.parent.runtime
    with runtime.lock:
        credit = 8 * MAX_FRAME
        if runtime.bytes + credit > MAX_SOURCE_REFERENCE_BYTES:
            refuse("native_original_proof_capacity")
        runtime.bytes += credit
        call.proof_credit = credit
    attempt = reserve_construction(call.original.connection, call.slot._source_constructions, "writer_lease",
                                   parent=origin, book_owner=call.slot, book_name="_source_constructions")

    def retain(value):
        origin.lease = call.writer_lease = value

    with attempt:
        lease = attempt.construct(NativeWriterLease, call.original.db, call, retain=retain)
    lease.acquire(timeout_s=max(0.0, call.deadline - time.monotonic()))
    call.request.native_phase = "invoke"
    use = _acquire_original_native_use(call.slot, call.request)
    origin.pin, origin.use = use.pin, use
    from gateway.native_source_stop import require_native_pin_for_invoke
    require_native_pin_for_invoke(use.pin)
    call.protocol_phase, origin.phase = "prepared", "prepared"
    call.slot._native_write_attempted = True  # Consumed before original preparation publication; never replay.
    initial(origin)
    phase.completed = True
    call.original.loop.call_soon_threadsafe(phase.completion.set)
    return origin


def execute(call):
    if threading.current_thread() is not call.worker_thread:
        refuse("native_original_call_worker_changed")
    try:
        _wait(call, lambda: call.decline_frame is not None or "prepare_native_call" in call.phases)
        if call.decline_frame is not None:
            from gateway.native_app_result import denied_result
            from gateway.native_app_completion import reserve
            reserve(call, call.decline_frame).confirm()
            return denied_result(call.decline_frame)
        origin = _prepare(call, _phase(call, "prepare_native_call"))
        _wait(call, lambda: call.result_frame is not None)
        origin.require_current(owner=True)
        if call.protocol_phase != "result_received":
            refuse("native_original_result_unavailable")
        origin.phase = "result"
        return call.raw_result
    except BaseException as error:
        failed(call, error)
        raise


def prepare_message(use, message, raw_digest):
    from agent.native_original_use import _check_original_native_use_after_io
    from agent.tool_commit_rows import text_digest
    call = use.slot._source_constructions.get("app_call")
    origin = call.origin
    origin.require_current(owner=True)
    if (origin.use is not use or origin.phase != "result" or call.raw_result is None
            or text_digest(call.raw_result) != raw_digest):
        refuse("native_original_result_projection_changed")
    use.slot.projection = message
    _check_original_native_use_after_io(use.slot, use.request, use,
                                        raw_result=call.raw_result, projection=message.get("content"))


def saved(use):
    from gateway.native_app_phases import finish
    from gateway.native_app_proof import after_save
    from gateway.native_source_stop import retire_native_pin
    call = use.slot._source_constructions.get("app_call")
    if call is None or call.origin is None:
        return  # Existing local SQL source fixtures issue no remote authority.
    origin = call.origin
    try:
        origin.require_current(owner=True)
        call.protocol_phase, origin.phase = "committed", "committed"
        after_save(origin)
        ack = _phase(call, "ack_native_call")
        call.protocol_phase, origin.phase = "acknowledged", "acknowledged"
        finish(ack, "native_call_acknowledged")
        complete = _phase(call, "complete_native_call")
        call.protocol_phase, origin.phase = "completed", "completed"
        finish(complete, "native_call_completed")
        release = _phase(call, "release_native_call")
        from gateway.native_app_completion import reserve
        completion = reserve(call, release)  # All terminal allocation precedes SQL release and publication.
        origin.phase = "releasing"
        retire_native_pin(origin.pin)
        origin.lease.release()
        call.protocol_phase, origin.phase = "released", "released"
        finish(release, "native_call_released")
        completion.confirm()
        call.request.native_phase = "released"
    except BaseException as error:
        failed(call, error)
        raise


def failed(call, error):
    if call.origin is not None:
        call.origin.failed(error)
    else:
        call.original.keeper.blocked = True
        call.failed(error)


def require_completed_slot(slot):
    from agent.native_receipt_origin import NativeSlot
    from gateway.native_app_call import AppCall
    from gateway.native_app_completion import OriginalCallCompletion
    book = getattr(slot, "_source_constructions", None)
    if type(slot) is not NativeSlot or type(book) is not dict:
        refuse("native_original_call_completion_missing")
    call = book.get("app_call")
    if type(call) is not AppCall or call.slot is not slot:
        refuse("native_original_call_completion_missing")
    completion = call.completion
    if call.error is not None or type(completion) is not OriginalCallCompletion or completion.call is not call:
        refuse("native_original_call_completion_unknown")
    return completion.require_retained()
