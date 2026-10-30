from __future__ import annotations

from gateway.native_source_runtime import (
    SOURCE_REFERENCE_CREDIT, MAX_SOURCE_REFERENCE_BYTES, OriginalSourceConnection, refuse,
)

_KEY = object()


def _source_digest(runtime, value):
    from agent.tool_commit_rows import MAX_TEXT_BYTES, json_digest
    # Existing json_digest limits stay unchanged. Debit BEFORE its additional
    # string/UTF8/tree inspection; no DB/owner/keeper lock is acquired here.
    credit = 4 * MAX_TEXT_BYTES + 64 * 1024
    with runtime.lock:
        if (runtime.blocked or runtime.retired
                or runtime.bytes + credit > MAX_SOURCE_REFERENCE_BYTES):
            refuse("native_original_source_inspection_capacity")
        runtime.bytes += credit
        try:
            result = json_digest(value)
            if result is None:
                refuse("native_original_source_inspection_unavailable")
        except BaseException:
            runtime.blocked = True
            raise  # failed inspection keeps its nominal debit
        runtime.bytes -= credit  # known success only; reference credits stay
        return result


class OriginalSourceAssistant:
    def __init__(self, key, owner, authority, live, selection, message, witness):
        if key is not _KEY:
            refuse("native_original_source_assistant_factory_missing")
        self.owner, self.agent, self.db = owner, owner.agent(), owner.db
        self.authority, self.live, self.selection = authority, live, selection
        self.connection = selection.source_connection
        self.message, self.witness = message, witness
        self.calls = message["tool_calls"]
        self.message_uid, self.calls_digest = witness["message_uid"], witness["calls_digest"]
        self.credit = SOURCE_REFERENCE_CREDIT

    def require_assistant(self):
        from agent.tool_commit_owner import MESSAGE_UID
        c, a = self.connection, self.authority
        c.require_connection()
        self._require_owner()
        if (c.parent.runtime.assistants.get(id(self.witness)) is not self
                or c.authority is not a or self.selection.source_connection is not c
                or self.selection.connection is not c.connection
                or a.sessions.get(self.owner.scope["session_id"]) is not self.live
                or self.selection.live is not self.live
                or getattr(self.live.event_stream, "_gdb_native_selection", None) is not self.selection
                or self.message.get(MESSAGE_UID) != self.message_uid
                or self.message.get("tool_calls") is not self.calls
                or _source_digest(c.parent.runtime, self.calls) != self.calls_digest
                or self.witness["message_uid"] != self.message_uid
                or self.witness["calls_digest"] != self.calls_digest):
            refuse("native_original_source_assistant_changed")

    def _require_owner(self):
        if (self.agent is None or self.owner.agent() is not self.agent
                or self.owner.db is not self.db or self.agent._session_db is not self.db
                or self.authority.db is not self.db
                or self.agent._canonical_tool_commit_owner is not self.owner):
            refuse("native_original_source_assistant_changed")


class OriginalSourceCall:
    def __init__(self, key, assistant, slot, keeper, model_call, scope_json, assistant_json):
        if key is not _KEY:
            refuse("native_original_source_call_factory_missing")
        self.assistant, self.slot, self.keeper = assistant, slot, keeper
        self.pending, self.ref, self.owner = slot.pending, slot.pending.ref, slot.pending.owner
        self.agent, self.db = assistant.agent, assistant.db
        self.connection, self.selection = assistant.connection, assistant.selection
        self.model_call = model_call
        self.model_arguments = model_call.get("function", {}).get("arguments")
        self.effective_arguments = self.ref.args
        self.message_uid, self.call_uid = assistant.message_uid, self.pending.uid
        self.call_id, self.tool_name, self.task_id = self.ref.call_id, self.ref.name, self.ref.task_id
        self.task, self.loop, self.live = keeper.task, keeper.loop, keeper.live
        self.scope_json, self.assistant_json = scope_json, assistant_json
        self.call_json, self.tool_json, self.arguments_json = (
            slot.call_json, slot.tool_json, slot.arguments_json)
        self.source, self.request = None, None
        self.credit = SOURCE_REFERENCE_CREDIT

    def require_call(self):
        self.assistant.require_assistant()
        parent = self.connection.parent
        pair = parent.calls.get(id(self.slot))
        if pair is None or pair[0] is not self.slot or pair[1] is not self:
            refuse("native_original_source_call_changed")
        self._require_pending()
        self._require_model_call(parent.runtime)
        self._require_keeper(parent.runtime)

    def _require_pending(self):
        p, s = self.pending, self.slot
        if (p is not s.pending or p.owner is not self.owner or p.ref is not self.ref
                or p.native_dispatch_slot is not s or s.bridge.owner is not self.owner
                or s.bridge.connection is not self.connection.connection
                or self.assistant.owner is not self.owner
                or p.assistant["message_uid"] != self.message_uid or p.uid != self.call_uid
                or p.call_id != self.call_id or p.tool_name != self.tool_name or p.task_id != self.task_id
                or self.ref.call_id != self.call_id or self.ref.name != self.tool_name
                or self.ref.task_id != self.task_id or self.ref.args is not self.effective_arguments
                or self.assistant.witness["call_uids"].get(self.call_id) != self.call_uid):
            refuse("native_original_source_call_changed")

    def _require_model_call(self, runtime):
        from agent.tool_commit_owner import coalesce_tool_call_id
        if (_source_digest(runtime, self.ref.args) != self.pending.effective_args_digest
                or coalesce_tool_call_id(self.model_call) != self.call_id
                or self.model_call.get("function", {}).get("name") != self.tool_name
                or self.model_call.get("function", {}).get("arguments") is not self.model_arguments
                or not any(call is self.model_call for call in self.assistant.calls)):
            refuse("native_original_source_call_changed")

    def _require_keeper(self, runtime):
        k, s = self.keeper, self.slot
        if (k.authority is not self.assistant.authority or k.owner is not self.owner
                or k.agent is not self.agent or k.live is not self.live
                or k.task is not self.task or k.loop is not self.loop
                or k.slots.get(id(s)) is not s or self.task.done()
                or self.task.get_loop() is not self.loop
                or self.loop is not runtime.loop
                or self.call_json is not s.call_json or self.tool_json is not s.tool_json
                or self.arguments_json is not s.arguments_json):
            refuse("native_original_source_call_changed")

    def attach_declaration(self, source):
        with self.connection.parent.runtime.lock:
            self.require_call()
            if (self.source is not None or source.original_source_call is not self
                    or source.slot is not self.slot or source.pending is not self.pending
                    or source.ref is not self.ref or source.retention is not self.keeper
                    or source.task is not self.task or source.loop is not self.loop
                    or source.selection is not self.selection
                    or source.scope_json is not self.scope_json
                    or source.assistant_json is not self.assistant_json
                    or source.call_json is not self.call_json
                    or source.tool_json is not self.tool_json
                    or source.arguments_json is not self.arguments_json):
                refuse("native_original_source_declaration_changed")
            self.source = source

    def attach_request(self, slot, request):
        with self.connection.parent.runtime.lock:
            self.require_call()
            if (self.source is None or slot is not self.slot or self.request is not None
                    or slot._source_declaration_refs is not self.source
                    or slot._native_request_object is not request
                    or request.native_slot is not slot or request.native_phase != "invoke"
                    or request.method != "native.invoke" or request.replay is not False
                    or request.native_pair_credit != 2
                    or request.events is not self.live.event_stream):
                refuse("native_original_source_request_changed")
            self.request = request


def capture_original_assistant(authority, session_id, generation, live, owner, message, witness):
    from agent.native_receipt_origin import is_native_tool_name
    from agent.tool_commit_owner import MESSAGE_UID, MAX_CALLS
    from gateway.session_native_context import NativeSelection
    calls = message.get("tool_calls")
    if not isinstance(calls, list) or len(calls) > MAX_CALLS:
        return None
    if not any(isinstance(call, dict)
               and is_native_tool_name((call.get("function") or {}).get("name", "")) for call in calls):
        return None  # ordinary/default assistant, no source allocation
    selection = getattr(live.event_stream, "_gdb_native_selection", None)
    if (not isinstance(selection, NativeSelection)
            or type(selection.source_connection) is not OriginalSourceConnection):
        refuse("native_original_source_assistant_factory_missing")
    r = selection.source_connection.parent.runtime
    with r.lock:
        selection.source_connection.require_connection()
        if (selection.live is not live or selection.connection.authority is not authority
                or authority.sessions.get(session_id) is not live
                or owner.scope["session_id"] != session_id or owner.scope["generation"] != generation
                or owner.staged is not witness or witness.get("role") != "assistant"
                or message.get(MESSAGE_UID) != witness.get("message_uid")
                or _source_digest(r, calls) != witness.get("calls_digest")):
            refuse("native_original_source_assistant_changed")
        r.reserve(r.assistants, id(witness))
        value = OriginalSourceAssistant(_KEY, owner, authority, live, selection, message, witness)
        r.assistants[id(witness)] = value
        value.require_assistant()
        return value


def capture_original_call(slot, keeper, selection, scope_json, assistant_json):
    from agent.tool_commit_owner import coalesce_tool_call_id
    p, owner = slot.pending, slot.pending.owner
    assistant = owner.native_source_assistant
    if type(assistant) is not OriginalSourceAssistant:
        refuse("native_original_source_assistant_missing")
    parent, r = assistant.connection.parent, assistant.connection.parent.runtime
    with r.lock:
        assistant.require_assistant()
        if (assistant.witness is not owner.staged or assistant.selection is not selection
                or keeper.owner is not owner or keeper.agent is not assistant.agent
                or p.native_dispatch_slot is not slot):
            refuse("native_original_source_capture_changed")
        # Charge and retain SAME slot BEFORE keeper registration/object allocation.
        r.reserve(parent.calls, id(slot))
        parent.calls[id(slot)] = (slot, None)
    # Existing lazy owner bind already ran. Reuse existing exact custody method;
    # beforeIO-v6's later SAME-slot invocation remains idempotent custody only.
    keeper.retain_pin_attempt(slot)
    matches = [call for call in assistant.calls if coalesce_tool_call_id(call) == p.call_id]
    if len(matches) != 1:
        refuse("native_original_source_call_occurrence_missing")
    value = OriginalSourceCall(_KEY, assistant, slot, keeper, matches[0], scope_json, assistant_json)
    with r.lock:
        old = parent.calls.get(id(slot))
        if old is None or old[0] is not slot or old[1] is not None:
            refuse("native_original_source_capture_reentry")
        parent.calls[id(slot)] = (slot, value)  # retain even if following check refuses
        value.require_call()
    return value


def require_original_source_call(slot, source, *, request=None, use=None):
    value = getattr(source, "original_source_call", None)
    if type(value) is not OriginalSourceCall:
        refuse("native_original_source_call_missing")
    with value.connection.parent.runtime.lock:
        value.require_call()
        if (value.slot is not slot or value.source is not source
                or slot._source_declaration_refs is not source
                or source.original_source_call is not value
                or source.pending is not value.pending or source.ref is not value.ref):
            refuse("native_original_source_call_changed")
        if request is not None and value.request is not request:
            refuse("native_original_source_request_changed")
        if use is not None and (use.original_source_call is not value
                or use.source is not source or use.slot is not slot
                or use.pending is not value.pending or use.request is not value.request
                or use.pin.source_original_call is not value):
            refuse("native_original_source_use_changed")
    return value
