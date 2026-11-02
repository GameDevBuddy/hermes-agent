"""Actual startup recovery consumer over fresh SQLite; no old PID/child is probed or adopted."""
import asyncio
from types import SimpleNamespace as Data

import pytest

from hermes_owned_gateway import OwnedGatewayUnknown
from hermes_state import SessionDB
from gateway import run as canonical, session_authorities
from gateway import run_owned_recovery as recovery
from tests.agent.auxiliary_owned_fixture import bind_source


def setup(monkeypatch, tmp_path, *, row=None, checkpoint=None):
    home = tmp_path / "creator-profile"
    db = SessionDB(home / "state.db")
    if row is not None:
        db._conn.execute("""INSERT INTO async_delegations
            (delegation_id,origin_session,state,dispatched_at,updated_at,event_json,delivery_state)
            VALUES ('fixture','fixture',?,0,0,?,?)""", row)
        db._conn.commit()
    source, runner = bind_source(monkeypatch, tmp_path)
    if checkpoint is not None:
        (home / "processes.json").write_bytes(checkpoint)
    calls = []
    monkeypatch.setattr(canonical, "_hermes_home", home)
    monkeypatch.setattr(session_authorities, "all_authorities", lambda value: [Data(db=db)])
    monkeypatch.setattr(runner, "_start_register_plugins_relay_hooks", lambda: calls.append("hooks"))
    runner.hooks = Data(discover_and_load=lambda: calls.append("discover"))
    monkeypatch.setattr(runner, "_subscribe_plugin_rewire", lambda manager: None)
    monkeypatch.setattr("hermes_cli.plugins.get_plugin_manager", lambda: Data())
    monkeypatch.setattr(runner, "_suspend_stuck_loop_sessions", lambda: 0)

    async def sessions():
        return 0, 0

    monkeypatch.setattr(runner, "_recover_unclean_sessions", sessions)

    def forbidden(*args):
        calls.append("legacy")
        raise AssertionError("managed startup reached old process/delegation recovery")

    from tools.process_registry import process_registry
    monkeypatch.setattr(process_registry, "recover_from_checkpoint", forbidden)
    monkeypatch.setattr("tools.async_delegation.restore_undelivered_completions", forbidden)
    return source, runner, db, home, calls


@pytest.mark.parametrize("checkpoint", [None, b"[]\n"])
def test_empty_reference_data_never_invokes_legacy_recovery(monkeypatch, tmp_path, checkpoint):
    async def scenario():
        source, runner, db, _home, calls = setup(monkeypatch, tmp_path, checkpoint=checkpoint)
        try:
            await runner._start_recover_previous_run()
            assert calls == ["hooks", "discover"] and source.phase == "bound"
        finally:
            db.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("checkpoint", [b'[{"pid":1}]', b"{invalid", b"x" * (recovery.MAX_CHECKPOINT_BYTES + 1)],
                         ids=["retained-work", "invalid-json", "oversized-data"])
def test_old_or_unreadable_process_references_refuse_before_hooks(monkeypatch, tmp_path, checkpoint):
    async def scenario():
        _source, runner, db, home, calls = setup(monkeypatch, tmp_path, checkpoint=checkpoint)
        try:
            with pytest.raises(OwnedGatewayUnknown):
                await runner._start_recover_previous_run()
            assert calls == [] and (home / "processes.json").read_bytes() == checkpoint
        finally:
            db.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("row", [("running", None, "pending"), ("stalling", None, "pending"),
                                  ("completed", "{}", "pending")])
def test_unsettled_delegation_rows_refuse_without_reaping_or_replaying(monkeypatch, tmp_path, row):
    async def scenario():
        _source, runner, db, _home, calls = setup(monkeypatch, tmp_path, row=row)
        try:
            with pytest.raises(OwnedGatewayUnknown, match="delegation-work"):
                await runner._start_recover_previous_run()
            actual = db._read_one("SELECT state,event_json,delivery_state FROM async_delegations")
            assert tuple(actual) == row and calls == []
        finally:
            db.close()

    asyncio.run(scenario())


def test_delivered_delegation_history_is_preserved_without_recovery(monkeypatch, tmp_path):
    async def scenario():
        row = ("completed", "{}", "delivered")
        _source, runner, db, _home, calls = setup(monkeypatch, tmp_path, row=row)
        try:
            await runner._start_recover_previous_run()
            actual = db._read_one("SELECT state,event_json,delivery_state FROM async_delegations")
            assert tuple(actual) == row and calls == ["hooks", "discover"]
        finally:
            db.close()

    asyncio.run(scenario())
