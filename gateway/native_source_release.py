"""Return nominal source credits only after saved originals are terminal and idle."""
from gateway.native_source_construction import SourceConstruction
from gateway.native_source_runtime import SOURCE_REFERENCE_CREDIT, refuse


def release_saved_call(call):
    completion = call.completion
    runtime = call.original.connection.parent.runtime
    with runtime.lock:
        completion.require_retained()
        if completion.refusal_phase is not None or call.source_refunded:
            return
        if call.reader.active is not None:
            return  # The original phase receiver still uses the live source gates.
        original, slot = call.original, call.slot
        values = [original, slot, *slot._source_constructions.values()]
        attempts = [getattr(value, "_source_construction", None) for value in values]
        if (any(type(attempt) is not SourceConstruction or attempt.runtime is not runtime
                or attempt.credit != SOURCE_REFERENCE_CREDIT or attempt.phase != "ready"
                for attempt in attempts) or len({id(a) for a in attempts}) != len(attempts)):
            refuse("native_original_source_release_changed")
        for attempt in attempts:
            attempt.require_retained()
        assistant_attempt = _assistant_release_attempt(original.assistant, original.keeper, call)
        # No callbacks/IO after validation. Slot custody remains for terminal checks;
        # only the live server index is detached, never history or another budget.
        for attempt in attempts:
            attempt.credit = 0
        original._source_construction.detached = True
        del original.connection.parent.calls[id(slot)]
        runtime.bytes -= len(attempts) * SOURCE_REFERENCE_CREDIT + call.wire_credit + call.proof_credit + call.result_credit
        call.source_refunded = True
        if assistant_attempt is not None:
            del assistant_attempt.book[assistant_attempt.key]
            assistant_attempt.detached, assistant_attempt.credit = True, 0
            runtime.bytes -= SOURCE_REFERENCE_CREDIT


def _assistant_release_attempt(assistant, keeper, current):
    from agent.native_receipt_origin import is_native_tool_name
    from agent.tool_commit_owner import coalesce_tool_call_id
    expected = {coalesce_tool_call_id(item) for item in assistant.calls
                if is_native_tool_name((item.get("function") or {}).get("name", ""))}
    calls = [slot._source_constructions.get("app_call") for slot in keeper.slots.values()]
    calls = [call for call in calls if call is not None and call.original.assistant is assistant]
    if (not expected or {call.original.call_id for call in calls} != expected
            or any(call is not current and not call.source_refunded for call in calls)):
        return  # Includes not-yet-bound, incomplete, failed and refused siblings.
    for call in calls:
        call.completion.require_retained()
    attempt = assistant._source_construction
    attempt.require_retained()
    if attempt.credit != SOURCE_REFERENCE_CREDIT:
        refuse("native_original_source_release_changed")
    return attempt


def release_turn_bridge(keeper):
    attempt = keeper.bridge_construction
    if attempt is None or attempt.source_refunded:
        return
    runtime = attempt.runtime
    if runtime is None:
        return
    with runtime.lock:
        # A bridge is reused by later assistant messages in this same turn.
        # It remains live until the original claim actually exits.
        if (not keeper.completed or keeper.blocked or attempt.error is not None
                or any(not getattr(slot._source_constructions.get("app_call"), "source_refunded", False)
                       for slot in keeper.slots.values())):
            return
        attempt.require_source(terminal=True)
        del runtime.bridges[id(attempt.owner)]
        runtime.bytes -= SOURCE_REFERENCE_CREDIT
        attempt.source_refunded = True
