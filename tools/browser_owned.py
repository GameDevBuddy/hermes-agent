"""Original browser resource custody. Local observations confer no native authority."""
from __future__ import annotations

import contextvars
import functools
import threading
import types

from agent.auxiliary_owned import running_loop
from agent.auxiliary_owned_close import _same_callable
from hermes_owned_gateway import OwnedGatewayUnknown, active

CREDIT, MAX_RECORDS = 65536, 2048
_lock = threading.RLock()
_records, _index, _books, _origins, _method_origins = [], {}, {}, {}, {}
_building = contextvars.ContextVar("_original_browser_build", default=None)
_command = contextvars.ContextVar("_original_browser_command", default=None)
_normal = contextvars.ContextVar("_original_browser_normal_close", default=None)


def fail(record, error):
    with record.source._state_lock:
        record.error = record.error or error
        record.source.browser_error = record.source.browser_error or record.error
        record.source.unknown("original-browser-unknown:" + type(record.error).__name__)


def refuse(record, reason):
    error = OwnedGatewayUnknown(reason)
    fail(record, error)
    raise error


class Record:
    def __init__(self, source, kind, factory, parent):
        from hermes_constants import get_hermes_home, hermes_home_key
        self.source, self.kind, self.factory = source, kind, factory
        self.runner, self.runtime, self.job = source.runner, source.runner._gdb_original_source_runtime, source.job
        self.parent = self.original_parent = parent
        self.creator_thread, self.creator_loop = threading.current_thread(), running_loop()
        self.context, self.home = contextvars.copy_context(), get_hermes_home()
        self.home_key = hermes_home_key(self.home)
        self.obj = self.result = self.error = None
        self.children, self.methods, self.references = (), (), ()
        self.arguments = None
        self.attempted = self.done = self.active = False
        self.close_attempted = self.closed = False
        self.close_result = None
        self.finished = threading.Event()
        _origins[self] = (source, self.runner, self.runtime, self.job, parent, self.creator_thread,
                         self.creator_loop, self.context, self.home, self.home_key, kind, factory)

    def require(self):
        try:
            self._require_original()
        except BaseException as error:
            fail(self, error)
            raise

    def _require_original(self):
        from hermes_constants import hermes_home_key
        origin = _origins[self]
        current = (self.source, self.runner, self.runtime, self.job, self.original_parent, self.creator_thread,
                   self.creator_loop, self.context, self.home, self.home_key, self.kind, self.factory)
        if any(before is not after for before, after in zip(origin, current)) or self.parent is not origin[4]:
            refuse(self, "original-browser-association-changed")
        runtime = self.runtime
        if (active(self.runner) is not self.source or self.source.runner is not self.runner
                or self.runner._gdb_original_source_runtime is not runtime or runtime.runner is not self.runner
                or runtime.bootstrap_task is not self.source.task or runtime.bootstrap_task.done()
                or runtime.loop is not self.source.loop or self.source.job is not self.job
                or hermes_home_key() != self.home_key):
            refuse(self, "original-browser-source-runtime-or-profile-changed")
        if self.error is not None or self.source.browser_error is not None:
            raise OwnedGatewayUnknown("original-browser-first-failure-retained")
        self.job.require_current_member()
        if self.original_parent is not None:
            self.original_parent.require()


def reserve(kind, factory, parent=None):
    source = active()
    parent = parent if parent is not None else _building.get()
    if parent is not None:
        parent.require()
        if source is not parent.source:
            refuse(parent, "browser-child-original-source-changed")
    if source is None:
        return None
    with source._state_lock, _lock:
        book = _books.setdefault(source, source.browser_records)
        if book is not source.browser_records or source.browser_closed or len(book) >= MAX_RECORDS:
            raise source.unknown("original-browser-before-work-book-or-capacity")
        if source.browser_closing and _normal.get() is None:
            raise source.unknown("original-browser-work-after-shutdown-admission")
        if not source.browser_closing:
            source.require_child_work()
        record = Record.__new__(Record)
        book.append(record)
        _records.append(record)
        source.browser_credit += CREDIT
        try:
            record.__init__(source, kind, factory, parent)
            record.require()
        except BaseException as error:
            source.browser_error = source.browser_error or error
            source.unknown("original-browser-partial-claim-retained")
            raise
        if parent is not None:
            parent.children += (record,)
        return record


def bind(record, obj):
    record.obj = obj
    with _lock:
        previous = _index.get(id(obj))
        if previous is not None and previous.obj is obj and previous is not record:
            refuse(record, "original-browser-object-already-associated")
        _index[id(obj)] = record


def find(obj):
    with _lock:
        record = _index.get(id(obj))
    return record if record is not None and record.obj is obj else None


def members(source):
    with _lock:
        records = tuple(record for record in _records if getattr(record, "source", None) is source)
        book = _books.get(source, source.browser_records)
        if (source.browser_records is not book or len(book) != len(records)
                or any(a is not b for a, b in zip(book, records))):
            raise source.unknown("original-browser-resource-book-changed")
        for record in records:
            record.require()
            if record.obj is not None and find(record.obj) is not record:
                refuse(record, "original-browser-indexed-resource-omitted")
            if any(child not in records or child.original_parent is not record for child in record.children):
                refuse(record, "original-browser-child-membership-changed")
            if record.original_parent is not None and record not in record.original_parent.children:
                refuse(record, "original-browser-child-omitted-from-parent")
    return records


def require_object(obj, kind=None):
    record = find(obj)
    if record is None:
        if active() is None:
            return None
        record = reserve("unowned", require_object)
        record.result = obj
        record.references = (obj,)
        refuse(record, "browser-original-constructor-unavailable")
    record.require()
    if kind is not None and record.kind != kind:
        refuse(record, "browser-original-resource-kind-changed")
    return record


def remember_methods(record, names):
    record.methods = tuple((name, getattr(record.obj, name)) for name in names)
    _method_origins[record] = record.methods


def require_methods(record):
    record.require()
    if (record.methods is not _method_origins.get(record)
            or any(not _same_method(getattr(record.obj, name), method) for name, method in record.methods)):
        refuse(record, "original-browser-method-changed")


def _same_method(left, right):
    # Built-in bound file methods have no __func__; their equality compares the actual C method and self.
    return (_same_callable(left, right)
            or isinstance(left, types.BuiltinMethodType) and isinstance(right, types.BuiltinMethodType) and left == right)


def construct(kind):
    def decorate(initializer):
        @functools.wraps(initializer)
        def invoke(obj, *args, **kwargs):
            record = reserve(kind, initializer)
            if record is None:
                return initializer(obj, *args, **kwargs)
            bind(record, obj)  # actual partial self, before the initializer can acquire anything
            record.arguments, record.attempted, record.active = (args, kwargs), True, True
            token = _building.set(record)
            try:
                result = initializer(obj, *args, **kwargs)
                record.result = result
                record.require()
                record.done, record.active = True, False
                return result
            except BaseException as error:
                fail(record, error)
                raise
            finally:
                _building.reset(token)
        return invoke
    return decorate


def call(record, callback, *args, **kwargs):
    record.require()
    record.arguments, record.attempted, record.active = (args, kwargs), True, True
    token = _building.set(record)
    try:
        record.result = callback(*args, **kwargs)
        record.require()  # after IO as well as before it
        members(record.source)
        record.done, record.active = True, False
        return record.result
    except BaseException as error:
        fail(record, error)
        raise
    finally:
        _building.reset(token)


def factory(kind, callback, *args, parent=None, **kwargs):
    record = reserve(kind, callback, parent)
    if record is None:
        return callback(*args, **kwargs)
    result = call(record, callback, *args, **kwargs)
    bind(record, result)
    record.require()
    return result


def unsupported(kind, obj=None):
    if active() is not None:
        record = reserve("unsupported", unsupported)
        record.result, record.references = obj, (obj,)
        refuse(record, "browser-normal-lifecycle-unavailable:" + kind)


def protect(obj):
    record = find(obj)
    if record is None and active() is None:
        return None
    return require_object(obj)
