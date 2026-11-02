"""Original SessionDB transactions on fresh SQLite files, never an App Origin or permit."""
import sqlite3
import threading
from contextlib import contextmanager

import pytest

from hermes_state import SessionDB
from hermes_state_native_book import NativeSQLFailure
from hermes_state_native_lease import NativeWriterLease


# Unknown test connections remain fenced until this isolated test interpreter exits.
# Neither an exception nor the fixture sweep may turn them into a release receipt.
_unknown_databases = []


@pytest.fixture
def db(tmp_path):
    database = SessionDB(tmp_path / "state.db")
    database.create_session("fixture", "cli")
    database._execute_write(lambda conn: conn.execute(
        "CREATE TABLE native_lease_fixture (id INTEGER PRIMARY KEY, value TEXT)"))
    database._execute_write(lambda conn: conn.execute(
        "INSERT INTO native_lease_fixture VALUES (1, 'original')"))
    yield database
    lease = database._native_read_gate.lease
    if lease is not None and lease.phase == "held":
        lease.release()
    if database._native_read_gate.lease is None:
        database.close()
    else:
        _unknown_databases.append(database)


def test_exclusion_survives_original_commits_and_releases_for_other_connections(db):
    original = db._conn
    # Warm the actual read path: pooled under usable WAL, locked otherwise.
    assert db.get_session("fixture")["id"] == "fixture"
    call = object()  # Local exclusion does not manufacture an authenticated App caller.
    lease = NativeWriterLease(db, call).acquire()
    assert lease.connection is original and lease.call is call
    assert db._read_pool.empty()
    other = sqlite3.connect(db.db_path, timeout=0, isolation_level=None)
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            other.execute("SELECT value FROM native_lease_fixture").fetchone()
        db._execute_write(lambda conn: conn.execute(
            "UPDATE native_lease_fixture SET value='committed' WHERE id=1"))
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            other.execute("BEGIN IMMEDIATE")
        with db._read_ctx() as conn:
            assert conn is original
            assert conn.execute("SELECT value FROM native_lease_fixture").fetchone()[0] == "committed"
        lease.release()
        assert db._native_read_gate.lease is None
        assert other.execute("SELECT value FROM native_lease_fixture").fetchone()[0] == "committed"
        other.execute("UPDATE native_lease_fixture SET value='foreign' WHERE id=1")
        assert db._read_one("SELECT value FROM native_lease_fixture")[0] == "foreign"
    finally:
        other.close()


def test_authorizer_refuses_cached_writes_ddl_and_configuration_outside_transaction(db):
    sql = "UPDATE native_lease_fixture SET value=? WHERE id=1"
    db._execute_write(lambda conn: conn.execute(sql, ("before",)))
    lease = NativeWriterLease(db, object()).acquire()
    db._execute_write(lambda conn: conn.execute(sql, ("managed",)))
    for statement, values in (
        (sql, ("unmanaged",)),
        ("CREATE TABLE unowned(value)", ()),
        ("PRAGMA main.locking_mode=NORMAL", ()),
        ("PRAGMA wal_checkpoint", ()),
        ("ATTACH DATABASE ':memory:' AS unowned", ()),
    ):
        with pytest.raises(sqlite3.DatabaseError, match="authorized"):
            db._conn.execute(statement, values)
    assert db._read_one("SELECT value FROM native_lease_fixture")[0] == "managed"
    assert lease.phase == "held" and lease.error is None


def test_foreign_worker_refuses_before_callback_but_reads_use_original_connection(db):
    lease = NativeWriterLease(db, object()).acquire()
    results, callbacks = {}, []

    def worker():
        try:
            db._execute_write(lambda conn: callbacks.append(conn))
        except NativeSQLFailure as error:
            results["error"] = error
        with db._read_ctx() as conn:
            results["connection"] = conn
            results["value"] = conn.execute("SELECT value FROM native_lease_fixture").fetchone()[0]

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(5)
    assert not thread.is_alive()
    assert "native_writer_original_thread_required" in str(results["error"])
    assert callbacks == [] and results["connection"] is lease.connection
    assert results["value"] == "original"
    assert lease.phase == "held" and lease.error is None


def test_original_callback_read_and_reentry_do_not_reacquire_nonreentrant_lock(db):
    lease = NativeWriterLease(db, object()).acquire()
    returned = object()
    seen = []

    def original(conn):
        assert db.get_session("fixture")["id"] == "fixture"
        with pytest.raises(NativeSQLFailure, match="transaction_reentry"):
            db._execute_write(lambda _conn: seen.append("replayed"))
        seen.append(conn)
        return returned

    assert db._execute_write(original) is returned
    commit = lease.commits[-1]
    assert seen == [lease.connection]
    assert commit.callback is original and commit.result is returned
    assert commit.connection is lease.connection and commit.thread is threading.current_thread()
    assert commit.sequence == len(lease.commits)


def test_database_close_refuses_original_active_lease(db):
    lease = NativeWriterLease(db, object()).acquire()
    with pytest.raises(NativeSQLFailure, match="retirement_held"):
        db.close()
    assert db._conn is lease.connection and db._native_read_gate.lease is lease
    assert lease.phase == "held" and db._native_pin_closing is False


def test_failed_original_transaction_retains_first_error_and_never_retries(db):
    lease = NativeWriterLease(db, object()).acquire()
    first = sqlite3.OperationalError("database is locked: original callback failed")
    calls = []

    def original(conn):
        calls.append(conn)
        conn.execute("UPDATE native_lease_fixture SET value='uncertain' WHERE id=1")
        raise first

    with pytest.raises(NativeSQLFailure, match="transaction_unknown") as caught:
        db._execute_write(original, patience_s=30)
    assert caught.value.__cause__ is first
    assert lease.error is first and lease.traceback is first.__traceback__
    assert calls == [lease.connection] and lease.phase == "unknown"
    assert db._native_read_gate.lease is lease
    with pytest.raises(NativeSQLFailure, match="exclusion_unknown"):
        lease.release()
    with pytest.raises(NativeSQLFailure, match="exclusion_unknown"):
        db._execute_write(lambda conn: calls.append(conn))
    with pytest.raises(NativeSQLFailure, match="retirement_held"):
        db.close()
    lease.failed(ValueError("later observation"))
    assert lease.error is first and calls == [lease.connection]


def test_failed_lock_acquisition_retains_same_attempt_without_repair_or_callback_replay(db):
    other = sqlite3.connect(db.db_path, timeout=0, isolation_level=None)
    other.execute("BEGIN IMMEDIATE")
    lease = NativeWriterLease(db, object())
    try:
        with pytest.raises(NativeSQLFailure, match="transaction_unknown"):
            lease.acquire()
        assert lease.phase == "unknown" and lease.commits == []
        assert isinstance(lease.error, sqlite3.OperationalError)
        assert db._native_read_gate.lease is lease
        with pytest.raises(NativeSQLFailure, match="exclusion_reentry"):
            lease.acquire()
    finally:
        other.rollback()
        other.close()


def test_existing_reader_finishes_before_exclusive_acquisition(db):
    entered, finish = threading.Event(), threading.Event()
    reader_connections, observed_phases = [], []
    lease = NativeWriterLease(db, object())

    def reader():
        with db._read_ctx() as conn:
            reader_connections.append(conn)
            entered.set()
            assert finish.wait(5)
            observed_phases.append(lease.phase)
            assert conn.execute("SELECT value FROM native_lease_fixture").fetchone()[0] == "original"

    thread = threading.Thread(target=reader)
    thread.start()

    def drain_after_reservation():
        with db._native_read_gate.condition:
            assert db._native_read_gate.condition.wait_for(lambda: lease.phase == "acquiring", 5)
        finish.set()

    releaser = threading.Thread(target=drain_after_reservation)
    try:
        assert entered.wait(5)
        releaser.start()
        lease.acquire()
        assert lease.phase == "held" and db._native_read_gate.readers == 0
        assert len(reader_connections) == 1 and observed_phases == ["acquiring"]
    finally:
        finish.set()
        thread.join(5)
        releaser.join(5)
    assert not thread.is_alive() and not releaser.is_alive()


def test_reader_selected_before_normal_release_resamples_without_replaying_body(db, monkeypatch):
    lease = NativeWriterLease(db, object()).acquire()
    selected, proceed = threading.Event(), threading.Event()
    original_read, bodies = lease.read, []

    @contextmanager
    def delayed_original_read():
        selected.set()
        assert proceed.wait(5)
        with original_read() as conn:
            yield conn

    monkeypatch.setattr(lease, "read", delayed_original_read)

    def reader():
        with db._read_ctx() as conn:
            bodies.append(conn.execute("SELECT value FROM native_lease_fixture").fetchone()[0])

    thread = threading.Thread(target=reader)
    thread.start()
    try:
        assert selected.wait(5)
        lease.release()
        proceed.set()
        thread.join(5)
    finally:
        proceed.set()
        thread.join(5)
    assert not thread.is_alive() and bodies == ["original"]
