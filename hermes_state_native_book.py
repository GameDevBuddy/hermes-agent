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
        self.lock = threading.RLock()  # Metadata only; never held across SQL or callbacks.
        self.pins, self.uses = {}, {}
        self.bytes, self.error = 4096, None

    def charge(self, credit):
        with self.lock:
            if self.error is not None or type(credit) is not int or credit <= 0 or self.bytes + credit > MAX_METADATA:
                raise NativeSQLFailure("native_sql_capacity_or_unknown")
            self.bytes += credit

    def require_current(self):
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


def book_for(db):
    with db._lock:
        if db._native_pin_closing or db.read_only or db._conn is None:
            raise NativeSQLFailure("native_sql_original_database_unavailable")
        book = db._native_pin_book
        if book is None:
            if db._native_pin_init_consumed:
                raise NativeSQLFailure("native_sql_book_construction_consumed")
            db._native_pin_init_consumed = True
            try:
                book = NativeSQLBook(db)
                db._native_pin_book = book
            except BaseException as error:
                db._native_pin_init_error = type(error).__name__
                raise
        book.require_current()
        return book


def block(book, error, pin=None, use=None):
    # A failing original writer ALREADY holds SessionDB's non-reentrant Lock.
    # Never reacquire it here. This separate short metadata lock grants no writer exclusion.
    with book.lock:
        if book.error is None:
            book.error = (type(error).__name__, getattr(error, "reason", "native_sql_unknown"))
        if pin is not None:
            pin.phase = "unknown"
            pin.error = pin.error or book.error
            pin.source_retention.blocked = True
        if use is not None:
            use.phase = "unknown"
            use.error = use.error or book.error


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
    if getattr(db, "_native_pin_init_consumed", False):
        raise NativeSQLFailure("native_sql_original_database_retirement_held")
