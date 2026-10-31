"""Keep the original request candidate across constructor/association failure; no transport."""
from gateway.native_request_arguments import request_parameters
from gateway.native_source_calls import require_original_source_call
from gateway.native_source_construction import reserve_construction
from gateway.native_source_runtime import refuse


def construct_native_candidate(slot, source):
    original = require_original_source_call(slot, source)
    attempt = reserve_construction(original.connection, slot._source_constructions, "request", parent=slot,
                                   book_owner=slot, book_name="_source_constructions")
    attempt.pair_credit = 2  # Consume the existing pair's metadata BEFORE UUID/Event/association calls.
    try:
        return _construct_candidate(attempt, slot, source, original)
    except BaseException:
        if attempt.value is not None:
            attempt.value.native_phase = "blocked"
        raise


def _construct_candidate(attempt, slot, source, original):
    from gateway import session_server_requests as requests
    with attempt:
        attempt.arguments = (slot, source, original, source.owner, source.live)
        attempt.inputs = _request_inputs(slot, source)
        parameters = request_parameters(original)
        attempt.arguments += (parameters,)
        session_id, events = source.owner.scope["session_id"], source.live.event_stream
        with requests._lock:
            if slot._native_request_candidate_id is not None:
                refuse("native_original_request_reentry")
            candidate = "srq-" + requests.uuid.uuid4().hex
            slot._native_request_candidate_id = candidate
        attempt.require_inputs(_request_inputs(slot, source))
        request = attempt.construct(
            requests._Pending, session_id, events=events,
            method="native.invoke", params=parameters, replay=False, _source_native_slot=slot,
            retain=lambda value: setattr(slot, "_native_request_object", value))
        attempt.require_inputs(_request_inputs(slot, source))
        request.native_slot, request.native_phase, request.native_pair_credit = slot, "invoke", attempt.pair_credit
        with requests._lock:
            if (candidate in requests._open or slot._native_request_candidate_id != candidate
                    or slot._native_request_object is not request):
                refuse("native_original_source_request_changed")
            requests._open[candidate] = request  # Complete initializer only; source association still checked below.
        try:
            original.attach_request(slot, request)
            require_original_source_call(slot, source)
            attempt.require_inputs(_request_inputs(slot, source))
        except BaseException:
            request.native_phase = "blocked"
            raise
    return request


def _request_inputs(slot, source):
    return (slot.pending, slot.pending.ref, source, source.original_source_call, source.owner, source.ref,
            source.owner.scope, source.owner.scope["session_id"], source.retention, source.task, source.loop,
            source.live, source.live.event_stream, source.selection, source.connection, source.transport)
