"""Original SQLite connection exclusion; this lease alone grants no App authority."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import sqlite3
import threading
import time
import uuid

from hermes_state_native_book import NativeSQLFailure


class NativeReadGate:
    def __init__(self):
        self.condition = threading.Condition()
        self.readers = 0
        self.lease = None


class _ReleasedRead(Exception):
    """A reader's original lease ended before it acquired the database lock; no body ran."""


@dataclass(frozen=True)
class OriginalTransactionCommit:
    lease: object
    connection: object
    callback: object
    result: object
    sequence: int
    thread: object


class NativeWriterLease:
    def __init__(self, db, call):
        self.phase, self.error, self.traceback = "initializing", None, None
        self.io_lock = None
        self.db, self.call, self.connection = db, call, db._conn
        self.owner, self.gate = threading.current_thread(), db._native_read_gate
        self.writer = db._execute_write
        self.lease_id = str(uuid.uuid4())
        self.phase, self.error, self.traceback = "reserved", None, None
        self.writing = self.configuring = False
        self.commits = []
        self.acquisition = None
        self.previous_mode = None
        self.authorizer = self.authorize
        self.io_lock = threading.RLock()

    def failed(self, error):
        if getattr(self, "error", None) is None:
            self.error, self.traceback = error, error.__traceback__
        self.phase = "unknown"

    def require_current(self):
        with self.io_lock:
            self._require_current_locked()

    def _require_current_locked(self):
        current = self.db._execute_write
        if (self.error is not None or self.gate.lease is not self or self.db._conn is not self.connection
                or self.phase not in ("acquiring", "held", "releasing")
                or getattr(current, "__self__", None) is not self.db
                or getattr(current, "__func__", None) is not self.writer.__func__):
            raise NativeSQLFailure("native_writer_exclusion_unknown")
        self.db._raise_if_db_replaced()
        if self.phase == "held" and self.connection.execute("PRAGMA main.locking_mode").fetchone()[0] != "exclusive":
            raise NativeSQLFailure("native_writer_exclusion_changed")

    def require_owner(self):
        if threading.current_thread() is not self.owner:
            raise NativeSQLFailure("native_writer_original_thread_required")
        self.require_current()

    def authorize(self, operation, first, second, _database, _trigger):
        if threading.current_thread() is self.owner and (self.writing or self.configuring):
            return sqlite3.SQLITE_OK
        if operation in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE):
            return sqlite3.SQLITE_OK
        if operation == sqlite3.SQLITE_PRAGMA and second is None and first in (
                "locking_mode", "application_id", "journal_mode", "data_version", "schema_version"):
            return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_DENY

    def acquire(self, *, timeout_s=10.0):
        if self.phase != "reserved":
            raise NativeSQLFailure("native_writer_exclusion_reentry")
        try:
            with self.gate.condition:
                if self.gate.lease is not None:
                    raise NativeSQLFailure("native_writer_exclusion_already_reserved")
                self.gate.lease = self
                self.phase = "acquiring"
                self.gate.condition.notify_all()
                deadline = time.monotonic() + timeout_s
                while self.gate.readers:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise NativeSQLFailure("native_writer_readers_not_drained")
                    self.gate.condition.wait(remaining)
            with self.db._lock, self.io_lock:
                if (self.connection is None or self.connection.in_transaction or self.db.read_only
                        or self.db._conn is not self.connection):
                    raise NativeSQLFailure("native_writer_original_connection_required")
                while self.db._evict_one_idle_read_conn():
                    pass
                self.previous_mode = self.connection.execute("PRAGMA main.locking_mode").fetchone()[0]
                if self.previous_mode != "normal":
                    raise NativeSQLFailure("native_writer_original_mode_required")
                self.connection.set_authorizer(self.authorizer)
                self.configuring = True
                self.connection.execute("PRAGMA main.locking_mode=EXCLUSIVE").fetchone()
                self.configuring = False
            # The existing BEGIN IMMEDIATE/commit producer acquires the actual file lock.
            # A locking_mode value by itself never proves another writer was excluded.
            self.writer(lambda conn: conn.execute("SELECT 1").fetchone()[0], patience_s=0)
            self.acquisition = self.commits[-1]
            self.phase = "held"
            self.require_owner()
            return self
        except BaseException as error:
            self.failed(error)
            raise
        finally:
            self.configuring = False
            with self.gate.condition:
                self.gate.condition.notify_all()

    def exclusion(self):
        self.require_owner()
        if (self.phase != "held" or not self.commits or self.acquisition is not self.commits[0]
                or self.acquisition.connection is not self.connection or self.acquisition.thread is not self.owner):
            raise NativeSQLFailure("native_original_exclusion_acquisition_missing")
        return "sqlite-exclusive-connection"

    @contextmanager
    def read(self):
        if threading.current_thread() is self.owner and self.writing:
            with self.io_lock:
                self.require_current()
                yield self.connection
        else:
            with self.db._lock, self.io_lock:
                if self.phase == "released" and self.error is None and self.gate.lease is not self:
                    raise _ReleasedRead()
                self.require_current()
                yield self.connection

    def committed(self, callback, result):
        self.require_owner()
        if self.connection.in_transaction or not self.writing or len(self.commits) >= 512:
            raise NativeSQLFailure("native_original_commit_unavailable")
        commit = OriginalTransactionCommit(self, self.connection, callback, result,
                                           len(self.commits) + 1, threading.current_thread())
        self.commits.append(commit)
        return commit

    def release(self):
        self.require_owner()
        if self.phase != "held" or self.connection.in_transaction or self.writing:
            raise NativeSQLFailure("native_writer_release_reentry")
        try:
            with self.db._lock, self.io_lock:
                self.phase, self.configuring = "releasing", True
                mode = self.connection.execute("PRAGMA main.locking_mode=NORMAL").fetchone()[0]
                if mode != "normal":
                    raise NativeSQLFailure("native_writer_release_unconfirmed")
            # SQLite requires actual file access after NORMAL before it releases the lock.
            self.writer(lambda conn: conn.execute("SELECT count(*) FROM sqlite_schema").fetchone()[0], patience_s=0)
            with self.db._lock, self.io_lock:
                self.connection.set_authorizer(None)
                self.phase = "released"
                with self.gate.condition:
                    self.gate.lease = None
                    self.gate.condition.notify_all()
        except BaseException as error:
            self.failed(error)
            raise
        finally:
            self.configuring = False


@contextmanager
def read_context(db, ordinary):
    gate = db._native_read_gate
    while True:
        with gate.condition:
            lease = gate.lease
            while lease is not None and lease.phase == "acquiring" and threading.current_thread() is not lease.owner:
                gate.condition.wait()
                lease = gate.lease
            if lease is None:
                gate.readers += 1
        entered = False
        try:
            with (ordinary() if lease is None else lease.read()) as conn:
                entered = True
                yield conn
            return
        except _ReleasedRead:
            if entered:
                raise  # Only resample before a reader body has ever run.
        finally:
            if lease is None:
                with gate.condition:
                    gate.readers -= 1
                    gate.condition.notify_all()


def refresh_turn_lease_from_worker(db, session_id, holder, expires_at):
    """Renew on the exclusive connection; None leaves the ordinary/owner path unchanged.

    The scheduler may update only the holder-qualified expiry, not run an owner
    transcript transaction. A second connection cannot write while this lease
    holds SQLite's exclusive lock.
    """
    lease = db._native_read_gate.lease
    if lease is None or threading.current_thread() is lease.owner:
        return None
    with db._lock:
        # The native call may have released its connection while this tick waited.
        lease = db._native_read_gate.lease
        if lease is None:
            return None
        with lease.io_lock:
            db._raise_if_db_corrupt(storage=True)
            lease.require_current()
            conn, worker = lease.connection, threading.current_thread()

            def authorize(operation, first, second, database, trigger):
                if threading.current_thread() is worker and trigger is None:
                    if operation == sqlite3.SQLITE_TRANSACTION:
                        return sqlite3.SQLITE_OK
                    if (operation == sqlite3.SQLITE_UPDATE and database == "main"
                            and first == "session_turn_leases" and second == "expires_at"):
                        return sqlite3.SQLITE_OK
                return lease.authorize(operation, first, second, database, trigger)

            conn.set_authorizer(authorize)
            try:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conversation_id = db._session_turn_lease_key_on_conn(conn, session_id)
                    renewed = conn.execute(
                        "UPDATE session_turn_leases SET expires_at = ? "
                        "WHERE conversation_id = ? AND holder = ?", (expires_at, conversation_id, holder),
                    ).rowcount > 0
                    conn.commit()
                    return renewed
                except BaseException:
                    conn.rollback()
                    raise
            finally:
                # Expire the cached UPDATE too: SQLite authorizes at prepare time.
                conn.set_authorizer(lease.authorizer)


def require_transaction_entry(db):
    lease = db._native_read_gate.lease
    if lease is not None:
        lease.require_owner()
        if lease.writing:
            raise NativeSQLFailure("native_original_transaction_reentry")


@contextmanager
def transaction_scope(db):
    # Recheck after the original DB lock: acquisition can race a previously ordinary writer.
    require_transaction_entry(db)
    lease = db._native_read_gate.lease
    if lease is None:
        yield None
        return
    with lease.io_lock, _leased_transaction(lease):
        yield lease


@contextmanager
def _leased_transaction(lease):
    lease.require_owner()
    if len(lease.commits) >= 512:
        raise NativeSQLFailure("native_original_commit_capacity")
    lease.writing = True
    try:
        yield lease
    except BaseException as error:
        lease.failed(error)
        if isinstance(error, Exception):
            raise NativeSQLFailure("native_original_transaction_unknown") from error
        raise
    finally:
        lease.writing = False
        # Expire statements prepared while writes were allowed; SQLite authorizes at prepare time.
        lease.connection.set_authorizer(lease.authorizer)
