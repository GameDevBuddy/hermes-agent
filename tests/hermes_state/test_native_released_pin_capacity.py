"""Durable native SQL pin charges end at a real release, not at process restart."""
import sqlite3

import pytest

from hermes_state import SessionDB
from hermes_state_native_schema import MAX_METADATA, install_schema, native_schema, require_schema


LEGACY_PIN_CAPACITY = (
    "CREATE TRIGGER native_sql_pin_capacity BEFORE INSERT ON native_original_pins WHEN "
    f"(SELECT COUNT(*) FROM native_original_pins)>={1024} OR "
    "(SELECT COALESCE(SUM(credit_bytes),0) FROM native_original_pins)+NEW.credit_bytes>"
    f"{MAX_METADATA} BEGIN SELECT RAISE(ABORT,'native_sql_retained'); END"
)
PIN_CREDIT = 260672


def _legacy_schema():
    statements = []
    for statement in native_schema():
        statements.append(LEGACY_PIN_CAPACITY if statement.startswith(
            "CREATE TRIGGER native_sql_pin_capacity ") else statement)
    return statements


def _pin_values(index, credit=PIN_CREDIT):
    ident = f"pin-{index}"
    return (ident, "owner", 1, "instance", "logical", "transcript", f"admission-{index}", 1,
            index, f"message-{index}", f"call-{index}", index, "{}", "0" * 64, 1, 1, credit)


def _insert_pin(conn, index, credit=PIN_CREDIT):
    conn.execute("INSERT INTO native_original_pins VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 _pin_values(index, credit))


def _release_pin(conn, index):
    ident = f"pin-{index}"
    conn.execute("INSERT INTO native_pin_active_uses VALUES (?,?,?,?,?,?)",
                 (ident, f"use-{index}", "0" * 64, "{}", "0" * 64, 81920))
    conn.execute("INSERT INTO native_pin_row_links VALUES (?,?,?,?,?,?,?,?,?)",
                 (ident, "transcript", index, f"message-{index}", f"call-{index}", "1",
                  "0" * 64, "0" * 64, "{}"))
    conn.execute("INSERT INTO native_original_releases VALUES (?,?,?,?,?)",
                 (ident, f"invocation-{index}", index, "0" * 64, f"release-{index}"))


def _install(conn, statements):
    for statement in statements:
        conn.execute(statement)


def test_legacy_full_pin_book_admits_300_released_cycles_across_reopen(tmp_path):
    path = tmp_path / "state.db"
    db = SessionDB(path)
    try:
        db.create_session("transcript", "cli")
        db._execute_write(lambda conn: _install(conn, _legacy_schema()))
        for index in range(1, 301):
            if index == 151:
                db.close()
                db = SessionDB(path)
            def cycle(conn, index=index):
                install_schema(conn)
                _insert_pin(conn, index)
                _release_pin(conn, index)
            db._execute_write(cycle)
        with db._read_ctx() as conn:
            require_schema(conn)
            assert conn.execute("SELECT COUNT(*) FROM native_original_pins").fetchone()[0] == 300
            assert conn.execute("SELECT COUNT(*) FROM native_original_releases").fetchone()[0] == 300
            assert conn.execute("SELECT SUM(credit_bytes) FROM native_original_pins").fetchone()[0] == (
                300 * PIN_CREDIT)
    finally:
        db.close()


def test_unreleased_pin_bytes_still_refuse_the_next_pin(tmp_path):
    db = SessionDB(tmp_path / "state.db")
    try:
        db.create_session("transcript", "cli")
        db._execute_write(lambda conn: _install(conn, native_schema()))
        db._execute_write(lambda conn: _insert_pin(conn, 1, MAX_METADATA - 200000))
        with pytest.raises(sqlite3.IntegrityError, match="native_sql_retained"):
            db._execute_write(lambda conn: _insert_pin(conn, 2))
        assert db._read_one("SELECT COUNT(*) FROM native_original_pins")[0] == 1
        assert db._read_one("SELECT COUNT(*) FROM native_original_releases")[0] == 0
    finally:
        db.close()
