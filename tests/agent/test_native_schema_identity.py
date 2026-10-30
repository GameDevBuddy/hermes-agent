"""Point-in-time actual SQLite DDL identity; no continuous/global isolation claim."""
from __future__ import annotations

import sqlite3

import pytest

from agent.native_original_use import reserve_local_sql_pin
from hermes_state_native_schema import native_schema, require_schema
from hermes_state_runtime import RuntimeStoreError
from tests.agent.native_sql_fixture import finish_case, local_case


async def original_target(monkeypatch, request, target):
    case = await local_case(monkeypatch, request, use=False, pin=False)
    scope = case.pending.owner.scope
    if target == "worker":
        case.fixture.write_sql((
            "INSERT INTO worker_executions (execution_id,session_id,kind,owner_epoch,generation,status,adoption_digest) "
            "VALUES (?,?,?,?,?,?,?)",
            ("schema-worker", scope["session_id"], "compute", scope["epoch"], scope["generation"], "running", "source-fixture")))
    case.pin = reserve_local_sql_pin(case.slot)
    return case


def unknown_transition(case, target):
    statements = {
        "admission": ("UPDATE session_admissions SET status='unknown' WHERE admission_id=?", (case.pending.owner.scope["admission_id"],)),
        "worker": ("UPDATE worker_executions SET status='unknown' WHERE execution_id=?", ("schema-worker",)),
    }
    case.fixture.write_sql(statements[target])


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["admission", "worker"])
async def test_installed_generated_ddl_round_trips_exactly_and_allows_unknown(monkeypatch, request, target):
    case = await original_target(monkeypatch, request, target)
    try:
        db = case.fixture._session_db
        with db._lock:
            require_schema(db._conn)
            expected = {statement.split()[2]: statement for statement in native_schema()}
            actual = dict(db._conn.execute("SELECT name,sql FROM sqlite_master WHERE name IN ("
                                          + ",".join("?" for _ in expected) + ")", tuple(expected)))
        # This is live sqlite_master data, proving exact round-trip support on the actual pinned engine.
        assert actual == expected
        unknown_transition(case, target)
    finally:
        await finish_case(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["admission", "worker"])
async def test_changed_quoted_literal_changes_behavior_and_is_refused(monkeypatch, request, target):
    case = await original_target(monkeypatch, request, target)
    try:
        name = "native_sql_" + target + "_update"
        db = case.fixture._session_db
        with db._lock:
            installed = db._conn.execute("SELECT sql FROM sqlite_master WHERE name=?", (name,)).fetchone()[0]
        case.fixture.write_sql(("DROP TRIGGER " + name, ()),
                               (installed.replace("'unknown'", "'UNKNOWN'"), ()))
        # The original schema allows this transition. The altered literal now blocks real DML.
        with pytest.raises(sqlite3.IntegrityError, match="native_sql_retained"):
            unknown_transition(case, target)
        with db._lock:
            with pytest.raises(RuntimeStoreError, match="native_sql_schema_changed"):
                require_schema(db._conn)
    finally:
        await finish_case(case)
