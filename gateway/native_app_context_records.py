"""Bounded original source/context references, never per-call native authority."""
from __future__ import annotations

import asyncio
import threading

from hermes_state_runtime import RuntimeStoreError

_KEY = object()
MAX_CONTEXTS = 64
MAX_CONTEXT_BYTES = 8 * 1024 * 1024
RECORD_CREDIT = 4096


def refuse(reason="native_original_app_context_changed"):
    raise RuntimeStoreError(reason)


def source_for(connection):
    source, original = _retained_source_for(connection)
    if source.loop_thread is not threading.current_thread() or asyncio.get_running_loop() is not source.loop:
        refuse()
    return source, original


def _retained_source_for(connection):
    from hermes_owned_gateway import OwnedGatewaySource, current
    from gateway.native_source_runtime import require_original_connection
    original = require_original_connection(connection)
    runtime = original.parent.runtime
    source = getattr(runtime.runner, "_owned_gateway_source", None)
    if type(source) is not OwnedGatewaySource or current(source.contract) is not source:
        refuse("native_original_app_context_issuer_missing")
    if (source.runner is not runtime.runner or source.task is not runtime.bootstrap_task
            or source.loop is not runtime.loop or source.phase != "bound"
            or source._shutdown or source._control_closed or not source._ready_emitted
            or source.stdin is not source.stdin_owner.buffer):
        refuse()
    source.job.require_current_member()
    return source, original


def source_for_original_worker(connection, call):
    from gateway.native_app_call import AppCall
    from gateway.native_source_construction import require_constructed
    source, original = _retained_source_for(connection)
    if type(call) is not AppCall:
        refuse("native_original_app_worker_missing")
    require_constructed(call)
    if (call.original.connection is not original or call.original.connection.connection is not connection
            or call.worker_thread is not threading.current_thread() or call.original.loop is not source.loop
            or call.original._source_construction.creator[0] is not call.worker_thread
            or call.original.assistant._source_construction.creator[0] is not call.worker_thread
            or not call.authenticated or not call.acknowledged or call.error is not None
            or call.reader is not source._control_reader or call.reader.source is not source
            or call.context is None or call.context.stage.source is not source
            or call.context.request.reader is not call.reader):
        refuse("native_original_app_worker_changed")
    call.reader.require_retained()
    return source, original


def source_for_original_call(connection, call):
    # Original-loop metadata access may precede the private per-call bind. It
    # cannot issue an Origin; private prepare still requires authenticated ACK.
    from gateway.native_app_call import AppCall
    from gateway.native_source_construction import require_constructed
    source, original = source_for(connection)
    if type(call) is not AppCall:
        refuse("native_original_app_call_missing")
    require_constructed(call)
    call.original.require_call()
    if (call.original.connection is not original or call.original.connection.connection is not connection
            or call.slot._source_constructions.get("app_call") is not call
            or call.original.request is not call.request or call.error is not None
            or call.original._source_construction.creator[0] is not call.worker_thread
            or call.original.assistant._source_construction.creator[0] is not call.worker_thread):
        refuse("native_original_app_call_changed")
    return source, original


def source_for_owner(connection, owner):
    # This grants context metadata access, never a per-call Origin or native permission.
    from agent.tool_commit_owner import ToolCommitOwner
    from gateway.native_source_calls import OriginalSourceAssistant
    source, original = _retained_source_for(connection)
    assistant = getattr(owner, "native_source_assistant", None)
    if type(owner) is not ToolCommitOwner or type(assistant) is not OriginalSourceAssistant:
        refuse("native_original_context_owner_missing")
    assistant.require_assistant()
    if (assistant.owner is not owner or assistant.connection is not original
            or assistant.authority is not connection.authority
            or assistant.db is not owner.db or owner.agent() is not assistant.agent
            or assistant._source_construction.creator[0] is not threading.current_thread()
            or not owner.binding_current(owner.staged)):
        refuse("native_original_context_owner_changed")
    return source, original


def generation(selection):
    row = selection.connection.authority.db.get_session(selection.ref.session_id)
    value = row.get("runtime_generation") if row is not None else None
    if type(value) is not int or value < 0:
        refuse("stale_generation")
    return value


def same_originals(actual, expected):
    return len(actual) == len(expected) and all(left is right for left, right in zip(actual, expected))


def _prune_completed_calls(source, book):
    from gateway.native_app_call import AppCall
    from gateway.native_app_completion import OriginalCallCompletion
    reader = source._control_reader
    # receive() still checks its book cell after the worker confirms completion.
    if reader is None or reader.active is not None:
        return
    for key, call in list(book.items()):
        if (type(call) is not AppCall or call.error is not None
                or type(call.completion) is not OriginalCallCompletion or not call.completion.confirmed):
            continue
        try:
            call.completion.require_retained()
        except RuntimeStoreError:
            continue
        from gateway.native_source_release import release_saved_call, release_turn_bridge
        release_saved_call(call)
        release_turn_bridge(call.original.keeper)
        records = [(key, call)] + [(phase.book_key, phase) for phase in call.phases.values()]
        # Original calls, phases and completions remain in their source/slot custody.
        for record_key, record in records:
            if book.get(record_key) is record:
                del book[record_key]
                source._app_context_credit -= RECORD_CREDIT


def _closed_context_records(source, stage):
    """Validate metadata custody without asking a retired selection to be current."""
    from gateway.native_app_context_construction import ContextConstruction, _payload, _record_refs, _source_refs
    from gateway.native_capture_construction import CaptureConstruction, require_capture_ready
    from gateway.native_source_runtime import OriginalSourceConnection

    if type(stage) is not ContextStage or stage.source is not source or stage.error is not None:
        return
    original = stage.original
    if type(original) is not OriginalSourceConnection:
        return
    c, runtime = original.connection, original.parent.runtime
    capture = getattr(original, "_source_capture_construction", None)
    if (type(capture) is not CaptureConstruction or capture.value is not original
            or runtime.runner is not source.runner or runtime.loop is not source.loop
            or runtime.connections.get(id(c)) is not original
            or c._gdb_original_source_connection is not original
            or c.actor is not original.actor or c.authority is not original.authority
            or c.transport is not original.transport or stage.transport is not original.transport
            or original.transport._ws is not original.ws or original.transport._loop is not source.loop
            or original.transport._closed is not True):
        return
    require_capture_ready(original)
    # A closed socket is not a terminal native call. Even unpublished assistant
    # work and charged construction holes keep their original context custody.
    if (any(call is None or call.connection is original for call in original.parent.calls.values())
            or any(assistant is None or assistant.connection is original for assistant in runtime.assistants.values())
            or any(bridge is None or bridge.source is original for bridge in runtime.bridges.values())):
        return
    request = stage.request
    if (type(request) is not ContextRequest or request.source is not source or request.stage is not stage
            or request.error is not None or not request.acknowledged
            or request.reader is not source._control_reader
            or type(request.task) is not asyncio.Task or not request.task.done() or request.task.cancelled()
            or request.task.get_loop() is not source.loop or request.task.get_coro() is not request.coroutine
            or request.task.exception() is not None or request.future is None or not request.future.done()
            or request.future.cancelled() or request.future.exception() is not None):
        return
    value = request.association
    slot = (id(c), stage.selection.ref.session_id)
    if (type(value) is not OriginalAppContext or value.error is not None or not value.bound
            or value.request is not request or value.stage is not stage or value.selection is None
            or not value.selection.same_selection(stage.selection)
            or c._gdb_original_app_contexts.get(slot[1]) is not value
            or source._app_context_slots.get(slot) != stage.nonce
            or source._app_context_stages.get(stage.nonce) is not stage
            or source._app_context_requests.get(request.frame["requestId"]) is not request):
        return
    records = [("stage", stage.nonce, stage, RECORD_CREDIT + len(stage.selection.tools_json)),
               ("request", request.frame["requestId"], request, RECORD_CREDIT),
               ("association", request.frame["requestId"], value, 0)]
    for kind, key, record, credit in records:
        attempt = getattr(record, "_context_construction", None)
        if (type(attempt) is not ContextConstruction or attempt.source is not source
                or attempt.kind != kind or attempt.key != key or attempt.value is not record
                or attempt.phase != "ready" or attempt.error is not None or attempt.credit != credit
                or type(attempt.arguments) is not tuple
                or not record._context_initialized
                or not same_originals(attempt.source_refs, _source_refs(source))
                or not same_originals(attempt.record_refs, _record_refs(kind, attempt.arguments))
                or attempt.payload != _payload(kind, attempt.arguments)):
            return
        attempt.require_retained()
        if kind == "stage":
            actual = (record.source, record.original, record.selection, record.params)
            expected = attempt.arguments[1:]
        elif kind == "request":
            actual = (record.reader, record.frame, record.source)
            expected = (attempt.arguments[1], attempt.arguments[2], source)
        else:
            actual = (record.request, record.stage)
            expected = (attempt.arguments[1], attempt.stage)
        if not same_originals(actual, expected):
            return
    # An AppCall (including an incomplete one) may still need this association.
    for record in source._app_context_requests.values():
        if record is request:
            continue
        if record is None or getattr(record, "context", None) is value:
            return
    return c, slot, records


def prune_closed_contexts(source):
    # Caller holds the original source state lock, on its original event loop.
    # Do not race reader receive()/publication or an in-progress constructor.
    reader = source._control_reader
    if (source.phase != "bound" or source._shutdown or source._control_closed
            or source._app_context_error is not None or reader is None or reader.active is not None):
        return
    from gateway.native_app_context_construction import require_no_context_constructor
    require_no_context_constructor(source)
    runtime = source.runner._gdb_original_source_runtime
    with runtime.lock:
        for stage in list(source._app_context_stages.values()):
            try:
                retained = _closed_context_records(source, stage)
            except RuntimeStoreError:
                continue  # Changed/failed originals stay charged; never repair or adopt.
            if retained is None:
                continue
            c, slot, records = retained
            # The closed socket's immutable selection is live metadata, not
            # saved chat history. Retiring it permits a fresh bind of that same
            # live session; never replace a different/current selection.
            stream = stage.selection.live.event_stream
            with stream.lock:
                if getattr(stream, "_gdb_native_selection", None) is stage.request.association.selection:
                    del stream._gdb_native_selection
            # All original custody has been checked. No IO, callbacks or await below.
            del c._gdb_original_app_contexts[slot[1]]
            del source._app_context_slots[slot]
            del source._app_context_stages[stage.nonce]
            del source._app_context_requests[stage.request.frame["requestId"]]
            for kind, key, record, credit in records:
                del source._app_context_constructions[(kind, key)]
                source._app_context_credit -= credit


def reserve(source, book, key, credit):
    if book is source._app_context_requests:
        _prune_completed_calls(source, book)
    # A failed construction leaves a charged hole. These are nominal metadata
    # bounds, not allocator, kernel, execution or writer-exclusion evidence.
    if (key in book or len(book) >= MAX_CONTEXTS
            or source._app_context_credit + credit > MAX_CONTEXT_BYTES):
        refuse("native_original_app_context_capacity_or_reentry")
    source._app_context_credit += credit
    book[key] = None


class ContextStage:
    def __init__(self, key, source, original, selection, params):
        self._context_initialized = False
        self.request, self.error = None, None
        if key is not _KEY:
            refuse("native_original_app_context_factory_missing")
        self.source, self.original, self.selection, self.params = source, original, selection, params
        self.nonce = params["nonce"]
        self.contract = source.contract
        self.job, self.task, self.loop = source.job, source.task, source.loop
        self.stdin_owner, self.stdin = source.stdin_owner, source.stdin
        self.stdout_owner, self.actor, self.transport = source.stdout_owner, original.actor, original.transport
        self.generation = generation(selection)
        self._context_initialized = True

    def require_current(self, *, original_call=None, source_owner=None):
        from gateway.native_app_context_construction import require_context_constructed
        require_context_constructed(self)
        s, c, selected = self.source, self.original.connection, self.selection
        if original_call is not None and source_owner is not None:
            refuse("native_original_context_consumer_ambiguous")
        if original_call is not None:
            if s.loop_thread is threading.current_thread():
                source, original = source_for_original_call(c, original_call)
            else:
                source, original = source_for_original_worker(c, original_call)
        elif source_owner is not None:
            source, original = source_for_owner(c, source_owner)
        else:
            source, original = source_for(c)
        if (not same_originals((source, original), (s, self.original))
                or not same_originals((s.job, s.task, s.loop, s.stdin_owner, s.stdin, s.stdout_owner),
                                     (self.job, self.task, self.loop, self.stdin_owner, self.stdin, self.stdout_owner))
                or s.contract is not self.contract or c.actor is not self.actor or c.transport is not self.transport
                or self.error is not None or s._app_context_error is not None
                or not selected.current() or s._app_context_stages.get(self.nonce) is not self
                or s._app_context_slots.get((id(c), selected.ref.session_id)) != self.nonce
                or (original_call is None and source_owner is None and generation(selected) != self.generation)
                or self.params["nonce"] != self.nonce):
            refuse()
        from gateway.session_native_context import _selected
        inputs = {name: value for name, value in self.params.items() if name != "nonce"}
        if _selected(inputs, selected.ref) != (selected.context, selected.snapshot,
                                              selected.tools, selected.tools_json):
            refuse()


class ContextRequest:
    def __init__(self, key, reader, frame):
        self._context_initialized = False
        self.stage = self.task = self.future = self.coroutine = self.association = None
        self.result, self.error, self.acknowledged = None, None, False
        if key is not _KEY:
            refuse("native_original_app_context_factory_missing")
        self.reader, self.frame = reader, frame
        self.source = reader.source
        self._context_initialized = True

    def failed(self, error):
        from gateway.native_app_context_construction import retain_context_failure, original_failure_source
        retain_context_failure(self, error)
        source = original_failure_source(self)
        if self.error is None:
            self.error = error
        stage = getattr(self, "stage", None)
        if stage is not None and stage.error is None:
            stage.error = error
        if source._app_context_error is None:
            source._app_context_error = error
        source.unknown("original-app-context-bind-failed")

    def require_current(self):
        from gateway.native_app_context_construction import require_context_constructed
        require_context_constructed(self)
        self.reader.require_retained()
        s, frame, stage = self.source, self.frame, self.stage
        if (stage is None or s is not stage.source or self.reader.source is not stage.source
                or stage.request is not self or self.error is not None
                or self.reader.active is not self
                or s._app_context_requests.get(frame["requestId"]) is not self
                or stage.nonce != frame["nonce"] or stage.selection.context != frame["contextId"]
                or type(self.task) is not asyncio.Task or self.task.get_loop() is not s.loop
                or self.task.get_coro() is not self.coroutine):
            refuse()
        stage.require_current()


class OriginalAppContext:
    def __init__(self, key, request):
        self._context_initialized = False
        self.error = None
        self.request, self.stage = request, request.stage
        self.selection, self.bound = None, False
        if key is not _KEY or asyncio.current_task() is not request.task:
            refuse("native_original_app_context_factory_missing")
        request.require_current()
        self._context_initialized = True

    def require_current(self, connection, ref, *, original_call=None, source_owner=None):
        from gateway.native_app_context_construction import require_context_constructed
        require_context_constructed(self)
        request, stage = self.request, self.stage
        request.reader.require_retained()
        if original_call is None and source_owner is None:
            stage.require_current()
        else:
            stage.require_current(original_call=original_call, source_owner=source_owner)
        if (request.association is not self or request.error is not None
                or stage.request is not request or stage.original.connection is not connection
                or stage.selection.ref != ref
                or connection._gdb_original_app_contexts.get(ref.session_id) is not self):
            refuse()
        if self.bound:
            if (self.selection is None or not self.selection.same_selection(stage.selection)
                    or stage.selection.live.event_stream._gdb_native_selection is not self.selection):
                refuse()
        elif asyncio.current_task() is not request.task:
            refuse("native_original_app_context_bind_in_progress")


_CONTEXT_CLASSES = {"stage": ContextStage, "request": ContextRequest, "association": OriginalAppContext}
