"""Bind request metadata to original call references; no App/native permission."""
from gateway.native_source_construction import SourceConstruction
from gateway.native_source_runtime import refuse


def request_parameters(original):
    selected = original.selection
    refs = [ref for name, ref in selected.tools if name == original.tool_name]
    if len(refs) != 1:
        refuse("native_original_request_arguments_changed")
    return {
        "session_id": original.owner.scope["session_id"],
        "context_id": selected.context,
        "snapshot_sha256": selected.snapshot,
        "message_uid": original.message_uid,
        "call_uid": original.call_uid,
        "call_id": original.call_id,
        "tool_name": original.tool_name,
        "tool_ref": refs[0],
        "arguments": original.effective_arguments,
    }


def require_request_parameters(original, request):
    parameters = _original_parameters(original, request)
    expected = request_parameters(original)
    if (type(parameters) is not dict or request.params is not parameters
            or len(parameters) != len(expected)):
        refuse("native_original_request_arguments_changed")
    for key, value in expected.items():
        actual = parameters.get(key)
        if key == "arguments":
            if actual is not value:
                refuse("native_original_request_arguments_changed")
        elif type(actual) is not str or actual != value:
            refuse("native_original_request_arguments_changed")


def _original_parameters(original, request):
    attempt = getattr(request, "_source_construction", None)
    if type(attempt) is not SourceConstruction:
        refuse("native_original_request_arguments_changed")
    if (attempt.value is not request or attempt.parent is not original.slot
            or attempt.key != "request" or attempt.phase not in ("validating", "ready")
            or attempt.error is not None or original.slot._native_request_object is not request):
        refuse("native_original_request_arguments_changed")
    attempt.require_retained()
    if (type(attempt.keywords) is not dict or "params" not in attempt.keywords
            or attempt.keywords.get("_source_native_slot") is not original.slot):
        refuse("native_original_request_arguments_changed")
    return attempt.keywords["params"]
