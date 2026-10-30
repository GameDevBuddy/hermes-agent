"""Original managed cron owners. These local records confer no native lifecycle authority."""
from __future__ import annotations

import contextvars
import threading
import types

from agent.auxiliary_owned import running_loop
from agent.auxiliary_owned_close import _same_callable
from hermes_owned_gateway import OwnedGatewayUnknown, active

CREDIT, MAX_RECORDS = 65536, 2048
_lock = threading.RLock()
_records, _books, _origins, _index, _methods = [], {}, {}, {}, {}
_submission = contextvars.ContextVar("_original_cron_submission", default=None)


def fail_source(source, error):
    with source._state_lock:
        if source.cron_error is None:
            source.cron_error = error
        source.unknown("original-cron-unknown:" + type(source.cron_error).__name__)


def fail(record, error):
    if record.error is None:
        record.error = error
    fail_source(record.source, record.error)


def refuse(record, reason):
    error = OwnedGatewayUnknown(reason)
    fail(record, error)
    raise error


class Record:
    def __init__(self, source, kind, factory, parent):
        from hermes_constants import get_hermes_home, hermes_home_key
        self.source, self.kind, self.factory, self.parent = source, kind, factory, parent
        self.runner, self.runtime, self.job = source.runner, source.runner._gdb_original_source_runtime, source.job
        self.bootstrap_task, self.bootstrap_loop = source.task, source.loop
        self.creator_thread, self.creator_loop = threading.current_thread(), running_loop()
        self.context, self.home = contextvars.copy_context(), get_hermes_home()
        self.home_key = hermes_home_key(self.home)
        self.obj = self.result = self.error = None
        self.children, self.references, self.methods = (), (), ()
        self.arguments = None
        self.attempted = self.active = self.done = self.closed = False
        _origins[self] = (source, kind, factory, parent, self.runner, self.runtime, self.job,
                         self.creator_thread, self.creator_loop, self.context, self.home, self.home_key,
                         self.bootstrap_task, self.bootstrap_loop)

    def require(self):
        try:
            self._require_original()
        except BaseException as error:
            fail(self, error)
            raise

    def _require_original(self):
        from hermes_constants import hermes_home_key
        values = (self.source, self.kind, self.factory, self.parent, self.runner, self.runtime, self.job,
                  self.creator_thread, self.creator_loop, self.context, self.home, self.home_key,
                  self.bootstrap_task, self.bootstrap_loop)
        if any(a is not b for a, b in zip(values, _origins[self])):
            refuse(self, "cron-original-source-owner-or-creator-changed")
        source, runtime = self.source, self.runtime
        import hermes_owned_gateway as ownership
        if (ownership._CURRENT is not source or source.runner is not self.runner
                or self.runner._gdb_original_source_runtime is not runtime or runtime.runner is not self.runner
                or runtime.bootstrap_task is not self.bootstrap_task or source.task is not self.bootstrap_task
                or self.bootstrap_task.done() or runtime.loop is not self.bootstrap_loop
                or source.loop is not self.bootstrap_loop or source.job is not self.job
                or hermes_home_key() != self.home_key or source.cron_error is not None):
            refuse(self, "cron-original-runtime-job-or-profile-changed")
        if active(self.runner) is not source:
            refuse(self, "cron-original-current-source-changed")
        self.job.require_current_member()


def reserve(kind, factory, parent=None, *, continuation=False):
    source = active()
    if source is None:
        return None
    with source._state_lock, _lock:
        book = _books.get(source, source.cron_records)
        if book is not source.cron_records or source.cron_closed or len(book) >= MAX_RECORDS:
            raise source.unknown("cron-original-book-or-capacity")
        if source.cron_closing and not continuation:
            raise source.unknown("cron-admission-closed")
        if parent is not None:
            parent.require()
            if parent.source is not source:
                refuse(parent, "cron-child-original-source-changed")
        if not source.cron_closing:
            source.require_child_work()
        try:
            source.cron_credit += CREDIT  # nominal bookkeeping only, not allocator/RSS/native authority
            if _books.setdefault(source, book) is not book:
                raise source.unknown("cron-original-book-publication-changed")
            record = Record.__new__(Record)
            book.append(record)
            _records.append(record)
            record.__init__(source, kind, factory, parent)
            if parent is not None:
                parent.children += (record,)
            record.require()
            return record
        except BaseException as error:
            fail_source(source, error)
            raise


def bind(record, obj):
    record.obj = obj
    previous = _index.get(id(obj))
    if previous is not None and previous.obj is obj and previous is not record:
        refuse(record, "cron-original-object-already-associated")
    _index[id(obj)] = record


def find(obj):
    record = _index.get(id(obj))
    return record if record is not None and record.obj is obj else None


def unsupported(kind, obj=None):
    record = reserve("unsupported", unsupported)
    if record is not None:
        record.result, record.references = obj, (obj,)
        refuse(record, "cron-original-normal-owner-unavailable:" + kind)


def protect(obj, kind=None):
    record = find(obj)
    if record is None:
        unsupported(kind or "foreign-object", obj)
        return None
    record.require()
    if kind is not None and record.kind != kind:
        refuse(record, "cron-original-object-kind-changed")
    return record


def remember_methods(record, names):
    record.methods = tuple((name, getattr(record.obj, name)) for name in names)
    _methods[record] = record.methods


def require_methods(record):
    record.require()
    if (record.methods is not _methods.get(record)
            or any(not same_method(getattr(record.obj, name), method) for name, method in record.methods)):
        refuse(record, "cron-original-method-changed")


def same_method(left, right):
    # Python 3.14 Task C methods have no __func__; equality pins their actual method and self.
    return (_same_callable(left, right)
            or isinstance(left, types.BuiltinMethodType) and isinstance(right, types.BuiltinMethodType) and left == right)


def members(source):
    with source._state_lock, _lock:
        values = tuple(record for record in _records if getattr(record, "source", None) is source)
        if (source.cron_records is not _books.get(source, source.cron_records)
                or len(values) != len(source.cron_records)
                or any(a is not b for a, b in zip(values, source.cron_records))):
            raise source.unknown("cron-original-member-book-changed")
        for record in values:
            record.context.copy().run(record.require)
            if record.obj is not None and find(record.obj) is not record:
                refuse(record, "cron-original-object-omitted-or-replaced")
            if any(child not in values or child.parent is not record for child in record.children):
                refuse(record, "cron-original-child-membership-changed")
            if record.parent is not None and record not in record.parent.children:
                refuse(record, "cron-original-child-omitted")
        return values


def construct(record, cls, args, kwargs):
    record.arguments, record.attempted, record.active = (args, kwargs), True, True
    try:
        if not isinstance(cls, type):
            refuse(record, "cron-opaque-constructor-before-work")
        bind(record, cls.__new__(cls))
        cls.__init__(record.obj, *args, **kwargs)
        record.require()
        record.done, record.active = True, False
        return record.obj
    except BaseException as error:
        fail(record, error)
        raise
