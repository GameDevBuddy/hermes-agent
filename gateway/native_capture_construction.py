"""Original source-capture constructor records, not authentication or native authority."""
from __future__ import annotations

import threading

from agent.tool_commit_construction import CommitConstruction
from hermes_constants import hermes_home_key
from hermes_state_runtime import RuntimeStoreError


def _same(before, after):
    return len(before) == len(after) and all(left is right for left, right in zip(before, after))


def _runtime_inputs(args, lock):
    _key, runner, task, loop = args
    return (runner, getattr(runner, "_gdb_original_source_capture_lock", None), lock, task, loop,
            getattr(runner, "session_authorities", None))


def _server_inputs(args, _lock):
    _key, runtime, api, runner, mount, server, listener = args
    return (runtime, runtime.runner, runtime.lock, runtime.registry, runtime.servers,
            api, api.runner, api.web_app, runner, mount, server, listener)


def _connection_inputs(args, _lock):
    _key, parent, connection, ws, transport, task = args
    runtime, authority = parent.runtime, connection.authority
    writer = getattr(authority.db, "_execute_write", None)
    return (parent, runtime, runtime.runner, runtime.lock, runtime.registry, runtime.connections,
            parent.api, parent.mount, parent.server, parent.listener, parent.task, parent.handle,
            connection, authority, connection.actor, ws, transport, task, transport._ws, transport._loop,
            authority.db, getattr(authority.db, "_conn", None), getattr(writer, "__self__", None),
            getattr(writer, "__func__", writer), authority.profile_id, authority.instance_id, authority.epoch)


_INPUTS = {"runtime": _runtime_inputs, "server": _server_inputs, "connection": _connection_inputs}


def remember_failure(owner, error, traceback):
    # Original plain metadata dictionaries avoid a secondary user __setattr__
    # callback replacing the first exception while an assignment is failing.
    fields = vars(owner)
    if fields.get("_gdb_original_source_construction_error") is None:
        fields["_gdb_original_source_construction_error"] = error
        fields["_gdb_original_source_construction_traceback"] = traceback


class CaptureConstruction(CommitConstruction):
    def __init__(self, kind, owner, runner, runtime, book, key, lock):
        super().__init__(owner, key)
        self.kind, self.owner, self.runner, self.runtime = kind, owner, runner, runtime
        self.book, self.lock = book, lock
        self.owner_fields = vars(owner)
        self.arguments = self.factory = self.initializer = self.allocator = None
        self.observed_result = None
        self.creator_thread = self.home = None
        self.inputs_before = ()
        self.retained_book = self.child_book = None

    def prepare(self, args):
        self.arguments = args
        self.creator_thread, self.home = threading.current_thread(), hermes_home_key()
        self.capture(args, (), ())

    def construct(self, factory):
        from gateway.native_source_runtime import _SOURCE_CAPTURE_CLASSES
        expected = _SOURCE_CAPTURE_CLASSES[self.kind]
        if self.phase != "reserved":
            raise RuntimeStoreError("native_original_capture_constructor_reentry")
        self.factory, self.initializer, self.allocator = factory, factory.__init__, factory.__new__
        self.callbacks = (self.allocator, self.initializer, factory.__setattr__)
        self.inputs_before = _INPUTS[self.kind](self.arguments, self.lock)
        self.phase = "initializing"
        value = (object.__new__(factory) if self.allocator is object.__new__
                 else self.allocator(factory, *self.arguments))
        self.observed_result = value
        if self.allocator is not object.__new__ or factory is not expected or type(value) is not expected:
            raise RuntimeStoreError("native_original_capture_allocator_foreign")
        self.value = value
        value._source_capture_construction = self
        self.initializer(value, *self.arguments)
        self.phase = "validating"
        self.require_inputs()
        return value

    def require_inputs(self):
        self.require_current_context()
        if (self.creator_thread is not threading.current_thread() or self.home != hermes_home_key()
                or not _same(self.inputs_before, _INPUTS[self.kind](self.arguments, self.lock))
                or self.factory.__init__ is not self.initializer or self.factory.__new__ is not self.allocator
                or self.factory.__setattr__ is not self.callbacks[2]):
            raise RuntimeStoreError("native_original_capture_constructor_inputs_changed")
        self.require_retained()
        self.require_outputs()

    def require_outputs(self):
        if self.value is None:
            raise RuntimeStoreError("native_original_capture_constructor_incomplete")
        if self.kind == "runtime":
            expected = self.arguments[1:]
            actual = tuple(getattr(self.value, name, None) for name in ("runner", "bootstrap_task", "loop"))
        elif self.kind == "server":
            expected = self.arguments[1:]
            actual = tuple(getattr(self.value, name, None) for name in
                           ("runtime", "api", "runner", "mount", "server", "listener"))
        else:
            _key, parent, connection, ws, transport, task = self.arguments
            expected = (parent, connection, self.inputs_before[13], self.inputs_before[14], ws, transport, task)
            actual = tuple(getattr(self.value, name, None) for name in
                           ("parent", "connection", "authority", "actor", "ws", "transport", "task"))
        if not _same(expected, actual):
            raise RuntimeStoreError("native_original_capture_initializer_association_changed")

    def require_retained(self):
        if vars(self.owner) is not self.owner_fields:
            raise RuntimeStoreError("native_original_capture_constructor_owner_changed")
        if self.kind == "runtime":
            retained = self.owner_fields.get("_gdb_original_source_runtime_construction") is self
        else:
            retained = (self.runtime._capture_constructions is self.retained_book
                        and self.retained_book.get((self.kind, self.key)) is self
                        and self.owner_fields.get("_gdb_original_source_" + self.kind + "_construction") is self
                        and (self.runtime.servers if self.kind == "server" else self.runtime.connections) is self.book)
            if self.child_book is not None:
                retained = retained and self.arguments[1]._capture_constructions is self.child_book \
                    and self.child_book.get((self.kind, self.key)) is self
        if not retained or self.error is not None:
            raise RuntimeStoreError("native_original_capture_constructor_displaced")

    def finish(self):
        self.require_inputs()
        value = self.value
        if value is None or not getattr(value, "_source_capture_initialized", False):
            raise RuntimeStoreError("native_original_capture_constructor_incomplete")
        if self.kind == "runtime":
            runner, task, loop = self.arguments[1:]
            if (runner._gdb_original_source_runtime is not value or value.runner is not runner
                    or value.lock is not self.lock or value.bootstrap_task is not task or value.loop is not loop
                    or runner._gdb_original_source_capture_base_credit != self.credit or value.base_credit != self.credit):
                raise RuntimeStoreError("native_original_capture_runtime_publication_changed")
        elif self.book.get(self.key) is not value:
            raise RuntimeStoreError("native_original_capture_book_publication_changed")
        if self.kind == "connection" and self.owner._gdb_original_source_connection is not value:
            raise RuntimeStoreError("native_original_capture_connection_publication_changed")
        self.phase = "ready"

    def fail(self, error):
        if self.error is None:
            self.error, self.traceback = error, error.__traceback__
        self.phase = "failed"
        remember_failure(self.owner, self.error, self.traceback)
        remember_failure(self.runner, self.error, self.traceback)
        for value in (self.value, self.runtime):
            if value is not None and vars(value).get("construction_error") is None:
                vars(value)["construction_error"] = self.error
        # A failed child does not block unrelated healthy listeners/connections.

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


def begin_runtime_capture(runner, task, loop, lock):
    attempt = CaptureConstruction("runtime", runner, runner, None, None, "runtime", lock)
    vars(runner)["_gdb_original_source_runtime_construction"] = attempt
    try:
        from gateway.native_source_runtime import _KEY
        attempt.prepare((_KEY, runner, task, loop))
        return attempt
    except BaseException as error:
        attempt.fail(error)
        raise


def require_fresh_capture(owner, kind, runtime=None):
    name = "_gdb_original_source_" + kind + "_construction"
    if vars(owner).get(name) is not None:
        reason = ("native_original_source_runner_missing_or_reentry" if kind == "runtime"
                  else "native_original_source_capacity_or_reentry")
        raise RuntimeStoreError(reason)
    if runtime is not None and (kind, id(owner)) in runtime._capture_constructions:
        raise RuntimeStoreError("native_original_source_capacity_or_reentry")


def begin_child_capture(kind, owner, runtime, book, key, args):
    attempt = CaptureConstruction(kind, owner, runtime.runner, runtime, book, key, runtime.lock)
    attempt.retained_book = runtime._capture_constructions
    attempt.retained_book[(kind, key)] = attempt
    if kind == "connection":
        attempt.child_book = args[1]._capture_constructions
        attempt.child_book[(kind, key)] = attempt
    vars(owner)["_gdb_original_source_" + kind + "_construction"] = attempt
    try:
        attempt.prepare(args)
        return attempt
    except BaseException as error:
        attempt.fail(error)
        raise


def require_capture_ready(value):
    if not getattr(value, "_source_capture_initialized", False):
        raise RuntimeStoreError("native_original_capture_constructor_incomplete")
    attempt = getattr(value, "_source_capture_construction", None)
    if attempt is None:
        return  # Direct initialized observations do not establish App/native authority.
    if (type(attempt) is not CaptureConstruction or attempt.value is not value
            or attempt.phase != "ready" or attempt.error is not None):
        raise RuntimeStoreError("native_original_capture_constructor_incomplete")
    attempt.require_retained()  # No creation-time thread/task/context affinity on later queries.


def publish_captured_server(api, value):
    if value is None:
        api._gdb_original_source_server = None
        return
    attempt = getattr(value, "_source_capture_construction", None)
    try:
        if attempt is not None:
            attempt.phase = "publishing"
            attempt.require_inputs()
        api._gdb_original_source_server = value
        if api._gdb_original_source_server is not value:
            raise RuntimeStoreError("native_original_capture_server_publication_changed")
        if attempt is not None:
            attempt.finish()
    except BaseException as error:
        if attempt is not None:
            attempt.fail(error)
        raise
