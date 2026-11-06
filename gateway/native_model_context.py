"""Read-only model schemas from the already-bound original context, never call authority.

The initial generation/issuer checks run at the original bind as before. Later
turns reuse these immutable bytes; per-call adoption/generation/issuer checks
remain exclusively on the existing ToolCommitOwner/native execution path.
"""
import hashlib
import json

from agent.native_model_tools import NativeModelTools
from gateway.session_authorities import active_authority
from gateway.session_native_context import NativeSelection, _selected
from hermes_state_runtime import RuntimeStoreError


def snapshot_for_agent(runner, session_id):
    authority = active_authority(runner)
    live = getattr(authority, "sessions", {}).get(session_id)
    if live is None:
        return None, None
    with live.event_stream.lock:
        scope = (authority.profile_id, session_id, authority.instance_id, authority.epoch, id(live))
        selection = getattr(live.event_stream, "_gdb_native_selection", None)
        if selection is None:
            # Canonical live-session identity also scopes an unequipped cache entry:
            # a colliding route key must not carry schemas/profile state from a
            # previous native or ordinary conversation.
            return None, scope
        if (type(selection) is not NativeSelection or selection.live is not live
                or selection.ref.session_id != session_id or selection.connection.authority is not authority
                or selection.ref.profile_id != authority.profile_id or not selection.current()):
            raise RuntimeStoreError("native_model_context_changed")
        _require_bound_original(selection)
        definitions = [{"type": "function", "function": {
            "name": item["name"], "description": item["description"], "parameters": item["input_schema"]}}
            for item in json.loads(selection.tools_json)]
        # Scope the CACHE as well as the data: even identical schemas never carry a
        # native-equipped agent across conversations, live owners or profiles.
        key = (authority.profile_id, session_id, authority.instance_id, authority.epoch,
               id(live), id(selection.connection), id(selection.source_connection),
               selection.context, selection.snapshot, hashlib.sha256(selection.tools_json).hexdigest())
        # The retained descriptors already passed both selection and source JSON
        # bounds. Provider wrappers add nodes, so applying the source-receipt
        # budget again would reject valid selections near its node limit.
        encoded = json.dumps(definitions, sort_keys=True, ensure_ascii=False,
                             separators=(",", ":"), allow_nan=False).encode("utf-8")
        return NativeModelTools(encoded), key


def _require_bound_original(selection):
    # Schema-only observation of the retained association, usable by the existing
    # off-loop agent factory. Do not turn this into a context/call permission API.
    from gateway.native_app_context_records import OriginalAppContext, _retained_source_for, same_originals
    from gateway.native_app_context_construction import ContextConstruction, require_context_constructed
    connection = selection.connection
    value = connection._gdb_original_app_contexts.get(selection.ref.session_id)
    if type(value) is not OriginalAppContext:
        raise RuntimeStoreError("native_original_app_context_issuer_missing")
    stage, request = value.stage, value.request
    for record in (value, stage, request):
        if type(getattr(record, "_context_construction", None)) is not ContextConstruction:
            raise RuntimeStoreError("native_original_app_context_issuer_missing")
        require_context_constructed(record)
    source, original = _retained_source_for(connection)
    request.reader.require_retained()
    if (not value.bound or not request.acknowledged or value.selection is not selection
            or request.association is not value
            or value.error is not None or stage.error is not None or request.error is not None
            or stage.request is not request or request.stage is not stage
            or source._app_context_error is not None or source._control_reader is not request.reader
            or stage.source is not source or request.source is not source
            or stage.original is not original or selection.source_connection is not original
            or stage.contract is not source.contract
            or not same_originals((source.job, source.task, source.loop, source.stdin_owner, source.stdin,
                                   source.stdout_owner, connection.actor, connection.transport),
                                  (stage.job, stage.task, stage.loop, stage.stdin_owner, stage.stdin,
                                   stage.stdout_owner, stage.actor, stage.transport))
            or source._app_context_stages.get(stage.nonce) is not stage
            or source._app_context_slots.get((id(connection), selection.ref.session_id)) != stage.nonce
            or source._app_context_requests.get(request.frame["requestId"]) is not request
            or not selection.same_selection(stage.selection)
            or _selected({k: v for k, v in stage.params.items() if k != "nonce"}, selection.ref)
                != (selection.context, selection.snapshot, selection.tools, selection.tools_json)):
        raise RuntimeStoreError("native_model_context_changed")
