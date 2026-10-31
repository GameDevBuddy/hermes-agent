"""Original local SQL constructor custody; never App/Origin/native authority."""
from __future__ import annotations

import asyncio
import contextvars
import threading

from hermes_constants import hermes_home_key
from hermes_state_native_book import NativeSQLFailure


def _creator():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    return threading.current_thread(), loop, asyncio.current_task() if loop is not None else None


def _source_refs(slot, source, request):
    if source is None:
        return ()
    keeper, original = source.retention, source.original_source_call
    connection = original.connection
    runtime = connection.parent.runtime
    return (slot.pending, slot.pending.owner, slot.pending.ref, slot.bridge, slot._source_declaration_refs,
            source, source.owner, source.agent, source.db, keeper, keeper.task, keeper.loop, keeper.live,
            source.selection, original, original.assistant, original.model_call, source.task, source.loop,
            source.connection, source.authority, source.actor, source.transport, request, connection,
            connection.parent, runtime, runtime.runner, runtime.bootstrap_task, runtime.loop)


class SQLConstruction:
    def __init__(self, db, credit, *, book=None, slot=None, source=None, pin=None, request=None):
        self.db, self.credit, self.book = db, credit, book
        self.slot, self.source, self.pin, self.request = slot, source, pin, request
        self.value = self.foreign_value = self.error = None
        self.phase, self.arguments, self.keywords = "reserved", (), {}
        self.factory = self.initializer = None
        self.connection, self.db_lock, self.writer = db._conn, db._lock, db._execute_write
        self.creator, self.home, self.context = _creator(), hermes_home_key(), contextvars.copy_context()
        self.retention = source.retention if source is not None else None
        self.inputs = _source_refs(slot, source, request)
        self.book_refs = self._book_refs()
        self.pin_refs = self._pin_refs()

    def _book_refs(self):
        b = self.book
        return () if b is None else (b.db, b.connection, b.writer.__self__, b.writer.__func__, b.lock, b.pins, b.uses)

    def _pin_refs(self):
        p = self.pin
        return () if p is None else (p.db, p.book, p.source_slot, p.source_retention,
                                    p.source_original_call, p.expected, p.capture, p.pin_id)

    def require_current(self):
        current, context = self.db._execute_write, contextvars.copy_context()
        expected = self.book if self.book is not None else self.value or self
        if (self.db._conn is not self.connection or self.db._lock is not self.db_lock
                or self.db._native_pin_book is not expected or self.db._native_pin_closing or self.db.read_only
                or self.db._native_pin_init_credit != 4096
                or getattr(current, "__self__", None) is not self.db
                or getattr(current, "__func__", None) is not getattr(self.writer, "__func__", None)):
            raise NativeSQLFailure("native_sql_constructor_database_changed")
        if (_creator() != self.creator or hermes_home_key() != self.home
                or len(context) != len(self.context)
                or any(var not in context or context.get(var) is not value for var, value in self.context.items())):
            raise NativeSQLFailure("native_sql_constructor_context_changed")
        refs = _source_refs(self.slot, self.source, self.request)
        if (len(refs) != len(self.inputs) or any(a is not b for a, b in zip(self.inputs, refs))
                or any(a is not b for a, b in zip(self.book_refs, self._book_refs()))
                or any(a is not b for a, b in zip(self.pin_refs, self._pin_refs()))):
            raise NativeSQLFailure("native_sql_constructor_source_changed")
        self.require_retained()

    def require_retained(self):
        if self.book is None:
            expected = self.value if self.value is not None else self
            if self.db._native_pin_book is not expected:
                raise NativeSQLFailure("native_sql_constructor_book_displaced")
        elif self.pin is None:
            pair = self.book.pins.get(id(self.slot))
            if (type(pair) is not tuple or pair[0] is not self.slot or pair[1] is not self.value
                    or self.slot._native_pin is not self.value
                    or self.retention.pins.get(id(self.slot)) is not self.value):
                raise NativeSQLFailure("native_sql_constructor_pin_displaced")
        else:
            if (self.book.uses.get(self.pin.pin_id) is not self.value or self.pin.active_use is not self.value
                    or self.slot._native_active_use is not self.value or self.request.native_active_use is not self.value
                    or self.retention.active_uses.get(id(self.slot)) is not None
                    or (self.value is not None and self.retention.active_uses.get(id(self.value)) is not self.value)):
                raise NativeSQLFailure("native_sql_constructor_use_displaced")

    def construct(self, factory, expected, retain, validate, *arguments, **keywords):
        self.factory, self.initializer = factory, factory.__init__
        self.arguments, self.keywords = arguments, keywords
        try:
            self.require_current()
            if factory is not expected or factory.__new__ is not object.__new__:
                # Preserve the consumed-hole contract for a first allocator that
                # itself raises. A returned opaque object is held, never initialized/adopted.
                self.foreign_value = factory.__new__(factory, *arguments, **keywords)
                raise NativeSQLFailure("native_sql_constructor_allocator_opaque")
            self.phase = "initializing"
            self.value = object.__new__(factory)
            object.__setattr__(self.value, "_sql_construction", self)
            retain(self.value)
            self.require_current()
            self.initializer(self.value, *arguments, **keywords)
            validate(self.value, self)
            self.require_current()
            self.phase = "ready"
            return self.value
        except BaseException as error:
            self.remember(error)
            self.record_failure(error)
            raise

    def remember(self, error):
        if self.error is None:
            self.error = error
        self.phase = "blocked"

    def record_failure(self, error):
        if self.book is None:
            if self.db._native_pin_init_error is None:
                self.db._native_pin_init_error = type(error).__name__
            if self.db._native_pin_init_exception is None:
                self.db._native_pin_init_exception = error
            if self.value is not None:
                self.value.first_exception = self.db._native_pin_init_exception
            return
        from hermes_state_native_book import block
        pin, use = (self.value, None) if self.pin is None else (self.pin, self.value)
        block(self.book, error, pin, use)


def require_constructed(value):
    attempt = getattr(value, "_sql_construction", None)
    if (type(attempt) is not SQLConstruction or attempt.value is not value
            or attempt.error is not None or attempt.phase != "ready"):
        raise NativeSQLFailure("native_sql_constructor_incomplete")
    attempt.require_retained()


def remember_error(value, error):
    attempt = getattr(value, "_sql_construction", None)
    if type(attempt) is SQLConstruction:
        attempt.remember(error)
        return attempt
    return None


def require_fresh_pin(slot, source):
    if source.db._native_pin_init_error is not None:
        raise NativeSQLFailure("native_sql_book_construction_consumed")
    book = source.db._native_pin_book
    if book is not None:
        require_constructed(book)
        if id(slot) in book.pins:
            raise NativeSQLFailure("native_sql_pin_reentry_or_capacity")


def require_fresh_use(pin):
    require_constructed(pin)
    if pin.active_use is not None or pin.pin_id in pin.book.uses:
        raise NativeSQLFailure("native_original_use_reentry_or_capacity")


def construct_book(db, factory, expected):
    attempt = SQLConstruction(db, 4096)
    db._native_pin_book = attempt
    return attempt.construct(factory, expected, lambda value: setattr(db, "_native_pin_book", value), _book_fields, db)


def _book_fields(value, attempt):
    if (value.db is not attempt.db or value.connection is not attempt.connection
            or value.writer.__self__ is not attempt.db or value.writer.__func__ is not attempt.writer.__func__
            or value.bytes != attempt.credit or value.error is not None):
        raise NativeSQLFailure("native_sql_constructor_book_fields_changed")


def construct_pin(book, slot, source, factory, expected, key):
    attempt = SQLConstruction(source.db, 65536, book=book, slot=slot, source=source)

    def retain(value):
        book.pins[id(slot)] = (slot, value)
        slot._native_pin = value
        source.retention.retain_before_send(slot, value)

    return attempt.construct(factory, expected, retain, _pin_fields, key, source.db,
                             source.owner.scope["admission_id"], source.scope_json, source_slot=slot,
                             source_stamp=source.original_source_call.assistant.witness,
                             source_retention=source.retention, source_original_call=source.original_source_call,
                             _source_book=book)


def _pin_fields(value, attempt):
    if (value.db is not attempt.db or value.book is not attempt.book or value.source_slot is not attempt.slot
            or value.source_retention is not attempt.retention or value.source_original_call is not attempt.source.original_source_call
            or value.source_stamp is not attempt.source.original_source_call.assistant.witness
            or value.expected is not attempt.arguments[3] or value.credit != attempt.credit
            or value.phase != "created" or value.active_use is not None):
        raise NativeSQLFailure("native_sql_constructor_pin_fields_changed")


def construct_use(book, pin, slot, source, request, factory, expected, key):
    attempt = SQLConstruction(pin.db, 81920, book=book, slot=slot, source=source, pin=pin, request=request)

    def retain(value):
        book.uses[pin.pin_id] = value
        pin.active_use = slot._native_active_use = request.native_active_use = value
        source.retention.active_uses[id(value)] = value

    return attempt.construct(factory, expected, retain, _use_fields, key, pin, slot, source, request)


def _use_fields(value, attempt):
    if (value.pin is not attempt.pin or value.slot is not attempt.slot or value.source is not attempt.source
            or value.request is not attempt.request or value.pending is not attempt.slot.pending
            or value.owner is not attempt.source.owner or value.keeper is not attempt.retention
            or value.expected is not attempt.pin_refs[5] or value.capture is not attempt.pin_refs[6]
            or value.call_json is not attempt.slot.call_json or value.original_source_call is not attempt.source.original_source_call
            or value.phase != "staging" or value.credit != attempt.credit or value.io_checked):
        raise NativeSQLFailure("native_sql_constructor_use_fields_changed")
