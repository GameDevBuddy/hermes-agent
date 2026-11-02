"""Retained local source bookkeeping on the original SessionDB, not authority."""
from __future__ import annotations

import threading

from hermes_state_native_schema import MAX_METADATA, MAX_PINS
from hermes_state_runtime import RuntimeStoreError


class NativeSQLFailure(RuntimeStoreError):
    """Unknown native source work must not enter ordinary repair/adoption/retry."""


class NativeSQLBook:
    def __init__(self, db):
        from hermes_state import SessionDB
        writer = db._execute_write
        if (not isinstance(db, SessionDB) or getattr(writer, "__self__", None) is not db
                or getattr(writer, "__func__", None) is not SessionDB._execute_write):
            raise NativeSQLFailure("native_sql_original_writer_unavailable")
        self.db, self.connection, self.writer = db, db._conn, db._execute_write
        self.pins, self.uses = {}, {}
        self.bytes, self.error, self.first_exception = 4096, None, None
        self.lock = threading.RLock()  # Metadata only; never held across SQL or callbacks.

    def charge(self, credit):
        from hermes_state_native_construction import require_constructed
        require_constructed(self)
        with self.lock:
            if self.error is not None or type(credit) is not int or credit <= 0 or self.bytes + credit > MAX_METADATA:
                raise NativeSQLFailure("native_sql_capacity_or_unknown")
            self.bytes += credit

    def require_current(self):
        from hermes_state_native_construction import require_constructed
        require_constructed(self)
        current = self.db._execute_write
        with self.lock:
            if (self.error is not None or self.db._conn is not self.connection or self.db._native_pin_closing
                    or getattr(current, "__self__", None) is not self.db
                    or getattr(current, "__func__", None) is not self.writer.__func__):
                raise NativeSQLFailure("native_sql_original_writer_changed")

    def reserve_pin(self, slot):
        self.require_current()
        if id(slot) in self.pins or len(self.pins) >= MAX_PINS:
            raise NativeSQLFailure("native_sql_pin_reentry_or_capacity")
        self.charge(65536)
        self.pins[id(slot)] = (slot, None)


_BOOK_TYPE = NativeSQLBook


def book_for(db):
    from hermes_state_native_construction import require_constructed
    # A lower initializer callback must refuse BEFORE the original non-reentrant DB lock.
    if db._native_pin_init_consumed:
        if db._native_pin_init_error is not None:
            raise NativeSQLFailure("native_sql_book_construction_consumed")
        require_constructed(db._native_pin_book)
    with db._lock:
        if db._native_pin_closing or db.read_only or db._conn is None:
            raise NativeSQLFailure("native_sql_original_database_unavailable")
        book = db._native_pin_book
        if db._native_pin_init_error is not None:
            raise NativeSQLFailure("native_sql_book_construction_consumed")
        if book is None:
            if db._native_pin_init_consumed:
                raise NativeSQLFailure("native_sql_book_construction_consumed")
            db._native_pin_init_consumed = True
            db._native_pin_init_credit = 4096
            try:
                from hermes_state_native_construction import construct_book
                book = construct_book(db, NativeSQLBook, _BOOK_TYPE)
            except BaseException as error:
                if db._native_pin_init_error is None:
                    db._native_pin_init_error = type(error).__name__
                if db._native_pin_init_exception is None:
                    db._native_pin_init_exception = error
                raise
        book.require_current()
        return book


def block(book, error, pin=None, use=None):
    # A failing original writer ALREADY holds SessionDB's non-reentrant Lock.
    # Never reacquire it here. This separate short metadata lock grants no writer exclusion.
    from hermes_state_native_construction import remember_error
    with book.lock:
        if book.first_exception is None:
            book.first_exception = error
        if book.error is None:
            book.error = (type(error).__name__, getattr(error, "reason", "native_sql_unknown"))
        if pin is not None:
            attempt = remember_error(pin, error)
            pin.phase = "unknown"
            pin.error = getattr(pin, "error", None) or book.error
            retention = attempt.retention if attempt is not None else pin.source_retention
            retention.blocked = True
        if use is not None:
            remember_error(use, error)
            use.phase = "unknown"
            use.error = getattr(use, "error", None) or book.error


def guarded_write(book, callback):
    """Use the original guarded writer once; failures cannot become repair/replay."""
    entered = False

    def once(conn):
        nonlocal entered
        if entered:
            raise NativeSQLFailure("native_sql_body_reentry")
        entered = True
        try:
            book.require_current()
            if conn is not book.connection or not conn.in_transaction:
                raise NativeSQLFailure("native_sql_original_transaction_missing")
            return callback(conn)
        except BaseException as error:
            block(book, error)
            raise NativeSQLFailure(getattr(error, "reason", "native_sql_body_unknown")) from error

    try:
        book.require_current()
        return book.writer(once, patience_s=0)
    except BaseException as error:
        block(book, error)
        raise


def require_no_local_native_hold(db):
    gate = getattr(db, "_native_read_gate", None)
    if gate is not None and gate.lease is not None:
        raise NativeSQLFailure("native_writer_original_database_retirement_held")
    if getattr(db, "_native_pin_init_consumed", False):
        raise NativeSQLFailure("native_sql_original_database_retirement_held")
