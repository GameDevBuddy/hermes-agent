"""Before-initializer source custody in existing books; never App/native authority."""
from __future__ import annotations

import asyncio
import contextvars
import threading

from gateway.native_source_runtime import SOURCE_REFERENCE_CREDIT, refuse
from hermes_constants import hermes_home_key


def _creator():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    return threading.current_thread(), loop, asyncio.current_task() if loop is not None else None


def _block(runtime, error):
    # The original exception/traceback anchors partial originals even if a callback
    # replaced a book entry. Never clear it or infer quiescence from a failed build.
    if runtime.construction_error is None:
        runtime.construction_error = error
    runtime.blocked = True


def reserve_construction(connection, book, key, *, parent, book_owner, book_name, pair=None):
    runtime = connection.parent.runtime
    with runtime.lock:
        runtime.reserve(book, key)
        try:
            attempt = SourceConstruction(connection, book, key, parent, book_owner, book_name, pair)
            attempt._retain(attempt)
            return attempt
        except BaseException as error:
            _block(runtime, error)
            raise


class SourceConstruction:
    def __init__(self, connection, book, key, parent, book_owner, book_name, pair):
        self.connection, self.server = connection, connection.parent
        self.runtime, self.parent = self.server.runtime, parent
        self.book, self.key, self.pair = book, key, pair
        self.book_owner, self.book_name = book_owner, book_name
        self.value, self.error, self.phase = None, None, "reserved"
        self.factory = self.initializer = None
        self.arguments, self.keywords = (), {}
        self.inputs = ()
        self.credit, self.pair_credit = SOURCE_REFERENCE_CREDIT, 0
        self.creator = _creator()
        self.home, self.context = hermes_home_key(), contextvars.copy_context()
        self.graph = self._graph()

    def _graph(self):
        r, s, c = self.runtime, self.server, self.connection
        return (r.runner, r.bootstrap_task, r.loop, r.registry, s.api, s.runner, s.mount,
                s.server, s.listener, s.task, s.handle, c.connection, c.authority, c.actor,
                c.ws, c.transport, c.task)

    def _retain(self, value):
        self.book[self.key] = value if self.pair is None else (self.pair, value)

    def __enter__(self):
        try:
            self.require_current()
        except BaseException as error:
            self.fail(error)
            raise
        return self

    def construct(self, factory, *arguments, retain=None, **keywords):
        if self.phase != "reserved" or self.value is not None:
            refuse("native_original_constructor_reentry")
        self.factory, self.initializer = factory, factory.__init__
        self.arguments, self.keywords = arguments, keywords
        self.require_current()
        if factory.__new__ is not object.__new__:
            refuse("native_original_constructor_allocator_changed")
        self.phase = "initializing"
        self.value = object.__new__(factory)
        object.__setattr__(self.value, "_source_construction", self)
        self._retain(self.value)  # Original self exists BEFORE its sole initializer/lower calls.
        if retain is not None:
            retain(self.value)
        self.initializer(self.value, *arguments, **keywords)
        self.phase = "validating"
        self.require_current()
        return self.value

    def require_current(self):
        self.runtime.require_runtime()
        self.connection.require_connection()
        self.require_retained()
        current = contextvars.copy_context()
        if (_creator() != self.creator or hermes_home_key() != self.home
                or len(current) != len(self.context)
                or any(var not in current or current.get(var) is not value
                       for var, value in self.context.items())):
            refuse("native_original_constructor_context_changed")

    def require_retained(self):
        expected = self.value if self.value is not None else self
        entry = self.book.get(self.key)
        retained = entry is expected if self.pair is None else (
            type(entry) is tuple and len(entry) == 2
            and entry[0] is self.pair and entry[1] is expected)
        if (self.error is not None or self.connection.parent is not self.server
                or self.server.runtime is not self.runtime or not retained
                or getattr(self.book_owner, self.book_name) is not self.book
                or any(before is not after for before, after in zip(self.graph, self._graph()))
                or (self.factory is not None and self.factory.__init__ is not self.initializer)):
            refuse("native_original_constructor_association_changed")

    def require_inputs(self, current):
        if (len(self.inputs) != len(current)
                or any(before is not after for before, after in zip(self.inputs, current))):
            refuse("native_original_constructor_inputs_changed")

    def fail(self, error):
        if self.error is None:
            self.error = error
        self.phase = "blocked"
        _block(self.runtime, self.error)

    def __exit__(self, kind, error, traceback):
        if error is not None:
            self.fail(error)
            return
        try:
            if self.phase != "validating" or self.value is None:
                refuse("native_original_constructor_incomplete")
            self.require_current()
        except BaseException as failure:
            self.fail(failure)
            raise
        self.phase = "ready"


def require_constructed(value):
    attempt = getattr(value, "_source_construction", None)
    if (type(attempt) is not SourceConstruction or attempt.value is not value
            or attempt.phase != "ready" or attempt.error is not None):
        refuse("native_original_constructor_incomplete")
    # Consumers may observe from another original thread; the factory's creator
    # check belongs around construction, not a new thread-affinity permission.
    attempt.runtime.require_runtime()
    attempt.require_retained()
