"""Finite original context-record construction observations; no new issuer or authentication policy."""
from __future__ import annotations

import threading

from agent.tool_commit_construction import CommitConstruction
from agent.native_receipt_origin import frozen_json
from hermes_constants import hermes_home_key
from hermes_state_runtime import RuntimeStoreError


def require_no_context_constructor(source):
    if any(attempt.phase in ("reserved", "initializing", "constructed", "publishing")
           for attempt in source._app_context_constructions.values()):
        raise RuntimeStoreError("native_original_app_context_capacity_or_reentry")


def _source_refs(source):
    return (source, source.contract, source.job, source.task, source.loop, source.loop_thread,
            source.stdin_owner, source.stdin, source.stdout_owner, source._state_lock,
            source._app_context_stages, source._app_context_slots, source._app_context_requests,
            source._app_context_constructions, getattr(source.stdout_owner, "buffer", None))


def _record_refs(kind, args):
    if kind == "stage":
        _key, _source, original, selection, params = args
        authority = selection.connection.authority
        getter = authority.db.get_session
        return (original, original.parent, original.parent.runtime, original.connection,
                original.actor, original.transport, selection, selection.connection, selection.live,
                selection.ref, authority, authority.db, getattr(getter, "__self__", None),
                getattr(getter, "__func__", getter), params)
    if kind == "request":
        _key, reader, frame = args
        return (reader, reader.source, reader.thread, reader.stdin_owner, reader.stdin, reader.stdout_owner, frame)
    _key, request = args
    return (request, request.reader, request.source, request.stage, request.frame, request.task,
            request.coroutine, request.future, request.stage.original, request.stage.selection)


def _payload(kind, args):
    if kind == "stage":
        return frozen_json(args[-1])
    return frozen_json(args[-1] if kind == "request" else args[-1].frame)


class ContextConstruction(CommitConstruction):
    def __init__(self, source, kind, key, credit, owner, public_book):
        super().__init__(owner, key)
        self.source, self.kind, self.credit = source, kind, credit
        self.owner, self.owner_fields = owner, vars(owner)
        self.book, self.public_book = source._app_context_constructions, public_book
        self.arguments = self.factory = self.initializer = self.allocator = None
        self.observed_result = self.payload = None
        self.source_refs = self.record_refs = ()
        self.creator_thread = self.home = None
        self.stage = None
        self.retainer = None

    def construct(self, factory, *args, retain):
        from gateway.native_app_context_records import _CONTEXT_CLASSES
        if self.phase != "reserved":
            raise RuntimeStoreError("native_original_app_context_capacity_or_reentry")
        self.arguments, self.retainer = args, retain
        self.factory, self.initializer, self.allocator = factory, factory.__init__, factory.__new__
        self.creator_thread, self.home = threading.current_thread(), hermes_home_key()
        self.source_refs, self.record_refs = _source_refs(self.source), _record_refs(self.kind, args)
        self.payload = _payload(self.kind, args)
        self.capture(args, (self.allocator, self.initializer, retain, factory.__setattr__), self.source_refs)
        self.stage = (self.source._app_context_stages.get(args[-1]["nonce"]) if self.kind == "request"
                      else args[-1].stage if self.kind == "association" else None)
        self.phase = "initializing"
        value = (object.__new__(factory) if self.allocator is object.__new__
                 else self.allocator(factory, *args))
        self.observed_result = value
        if (self.allocator is not object.__new__ or factory is not _CONTEXT_CLASSES[self.kind]
                or type(value) is not _CONTEXT_CLASSES[self.kind]):
            raise RuntimeStoreError("native_original_app_context_allocator_changed")
        self.value = value
        value._context_construction = self
        retain(value)  # Same original public record, BEFORE sole initializer/lower callbacks.
        self.initializer(value, *args)
        self.phase = "constructed"
        self.require_inputs()
        return value

    def require_inputs(self):
        from gateway.native_app_context_records import same_originals
        self.require_current_context()
        if (self.creator_thread is not threading.current_thread() or self.home != hermes_home_key()
                or not same_originals(self.source_refs, _source_refs(self.source))
                or not same_originals(self.record_refs, _record_refs(self.kind, self.arguments))
                or self.payload != _payload(self.kind, self.arguments)
                or self.factory.__init__ is not self.initializer or self.factory.__new__ is not self.allocator
                or self.factory.__setattr__ is not self.callbacks[3]):
            raise RuntimeStoreError("native_original_app_context_constructor_inputs_changed")
        self.require_retained()
        if not getattr(self.value, "_context_initialized", False):
            raise RuntimeStoreError("native_original_app_context_constructor_incomplete")
        if self.kind == "stage":
            actual = (self.value.source, self.value.original, self.value.selection, self.value.params)
            expected = self.arguments[1:]
        elif self.kind == "request":
            actual = (self.value.reader, self.value.frame, self.value.source)
            expected = (self.arguments[1], self.arguments[2], self.source)
        else:
            actual = (self.value.request, self.value.stage)
            expected = (self.arguments[1], self.stage)
        if not same_originals(actual, expected):
            raise RuntimeStoreError("native_original_app_context_constructor_output_changed")

    def require_retained(self):
        if (self.source._app_context_constructions is not self.book
                or self.book.get((self.kind, self.key)) is not self
                or vars(self.owner) is not self.owner_fields or self.error is not None):
            raise RuntimeStoreError("native_original_app_context_constructor_displaced")
        if self.value is None:
            return
        if self.kind == "association":
            retained = self.owner.association is self.value
        else:
            current = (self.source._app_context_stages if self.kind == "stage"
                       else self.source._app_context_requests)
            retained = current is self.public_book and current.get(self.key) is self.value
        if not retained:
            raise RuntimeStoreError("native_original_app_context_constructor_displaced")

    def finish(self):
        self.require_inputs()
        if self.kind == "request":
            if (self.owner.active is not self.value or self.value.stage is not self.stage
                    or self.stage is None or self.stage.request is not self.value):
                raise RuntimeStoreError("native_original_app_context_active_publication_changed")
        if self.kind == "association":
            selection = self.stage.selection
            if selection.connection._gdb_original_app_contexts.get(selection.ref.session_id) is not self.value:
                raise RuntimeStoreError("native_original_app_context_association_publication_changed")
        self.phase = "ready"

    def fail(self, error):
        if self.error is None:
            self.error, self.traceback = error, error.__traceback__
        self.phase = "failed"
        if self.value is not None and vars(self.value).get("error") is None:
            vars(self.value)["error"] = self.error
        fields = vars(self.source)
        if fields.get("_app_context_error") is None:
            fields["_app_context_error"] = self.error
            fields["_app_context_traceback"] = self.traceback

    def __enter__(self):
        return self

    def __exit__(self, _kind, error, _traceback):
        if error is not None:
            self.fail(error)
            return False
        try:
            self.finish()
        except BaseException as failure:
            self.fail(failure)
            raise


def begin_context_construction(source, kind, key, credit, owner, public_book=None):
    require_no_context_constructor(source)
    if (kind, key) in source._app_context_constructions:
        raise RuntimeStoreError("native_original_app_context_capacity_or_reentry")
    attempt = ContextConstruction(source, kind, key, credit, owner, public_book)
    source._app_context_constructions[(kind, key)] = attempt
    return attempt


def require_context_constructed(value):
    if not getattr(value, "_context_initialized", False):
        raise RuntimeStoreError("native_original_app_context_constructor_incomplete")
    attempt = getattr(value, "_context_construction", None)
    if attempt is None:
        return  # A direct observation is neither an authenticated context nor native permission.
    if (type(attempt) is not ContextConstruction or attempt.value is not value
            or attempt.phase not in ("constructed", "ready") or attempt.error is not None):
        raise RuntimeStoreError("native_original_app_context_constructor_incomplete")
    attempt.require_retained()  # No new creation-time task/thread predicate on later healthy queries.


def retain_context_failure(value, error):
    attempt = getattr(value, "_context_construction", None)
    if type(attempt) is ContextConstruction:
        attempt.fail(error)
    elif vars(value).get("error") is None:
        vars(value)["error"] = error


def original_failure_source(value):
    attempt = getattr(value, "_context_construction", None)
    return attempt.source if type(attempt) is ContextConstruction else value.source
