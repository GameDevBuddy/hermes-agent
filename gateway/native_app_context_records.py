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


def reserve(source, book, key, credit):
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
                or generation(selected) != self.generation or self.params["nonce"] != self.nonce):
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
