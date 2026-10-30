"""Real source/SQLite observer flow with synthetic sockets, never App/native issuance."""
import asyncio
import json
from types import SimpleNamespace

import pytest

from agent.native_receipt_origin import frozen_json
from agent.native_turn_retention import capture_claim_retention
from gateway.native_source_calls import require_original_source_call
from gateway.session_contract import SessionRef
from gateway.session_native_context import NativeSelection
from gateway import session_server_requests as requests
from hermes_state_runtime import RuntimeStoreError
from tests.agent.canonical_commit_fixture import CALL, SESSION, CommitFixture
from tests.gateway.native_source_fixture import source_graph, finish_source_graph

TOOL_REF = "0" * 26
TOOL = "gdb_native_" + TOOL_REF.lower()


async def call_graph(monkeypatch, tmp_path):
    fixture = CommitFixture(tmp_path)
    graph = await source_graph(monkeypatch, tmp_path, authority=fixture.authority)
    authority, live = fixture.authority, fixture.live
    ref = SessionRef(authority.profile_id, SESSION)
    keeper = capture_claim_retention(authority, ref, live, fixture.claim)
    authority.adopt_agent(SESSION, fixture.claim["generation"], fixture)
    fixture.owner = fixture._canonical_tool_commit_owner
    live.subscribers["source-viewer"] = graph.connection.actor
    selection = NativeSelection(
        graph.connection, live, ref, authority.instance_id, authority.epoch,
        "source-fixture-context", "0" * 64, ((TOOL, TOOL_REF),),
        frozen_json([{"name": TOOL, "tool_ref": TOOL_REF}]),
        graph.connection._gdb_original_source_connection)
    # Source reference fixture data only. bind_context and App issuance stay refused.
    live.event_stream._gdb_native_selection = selection
    message = fixture.stage([{"id": CALL, "type": "function",
                              "function": {"name": TOOL, "arguments": '{"original":1}'}}])
    dispatch = fixture.begin(name=TOOL)
    pending = dispatch.canonical_commit
    assert pending is not None and pending.native_dispatch_slot is not None
    slot = pending.native_dispatch_slot
    source = slot.prepare_source_declaration()
    return SimpleNamespace(fixture=fixture, graph=graph, keeper=keeper, selection=selection,
                           message=message, dispatch=dispatch, pending=pending, slot=slot, source=source)


async def close_graph(case):
    await finish_source_graph(case.graph)
    case.fixture.close()


@pytest.mark.asyncio
async def test_actual_observer_captures_model_and_effective_originals(monkeypatch, tmp_path):
    case = await call_graph(monkeypatch, tmp_path)
    try:
        call = require_original_source_call(case.slot, case.source)
        assert call.assistant.message is case.message
        assert call.model_call is case.message["tool_calls"][0]
        assert call.model_arguments == '{"original":1}'
        assert call.effective_arguments is case.dispatch.args
        assert json.loads(case.source.arguments_json) == {"effective": 2}
        assert call.message_uid == case.message["message_uid"]
        assert call.call_uid == case.pending.uid
        assert case.keeper.slots[id(case.slot)] is case.slot
        assert call.keeper is case.keeper and call.pending is case.pending
        assert case.fixture.owner.native_source_assistant is call.assistant
    finally:
        await close_graph(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", ["message-uid", "call-uid", "effective-object",
                                         "effective-value", "model-call", "model-arguments",
                                         "owner", "keeper", "selection", "server"])
async def test_changed_original_uid_argument_or_object_refuses(monkeypatch, tmp_path, replacement):
    case = await call_graph(monkeypatch, tmp_path)
    original = case.source.original_source_call
    try:
        changes = {
            "message-uid": lambda: case.message.__setitem__("message_uid", "different-message"),
            "call-uid": lambda: setattr(case.pending, "uid", "different-call"),
            "effective-object": lambda: setattr(case.dispatch, "args", dict(case.dispatch.args)),
            "effective-value": lambda: case.dispatch.args.__setitem__("effective", 3),
            "model-call": lambda: case.message["tool_calls"].__setitem__(0, json.loads(json.dumps(case.message["tool_calls"][0]))),
            "model-arguments": lambda: case.message["tool_calls"][0]["function"].__setitem__("arguments", '{"original":2}'),
            "owner": lambda: setattr(case.fixture, "_canonical_tool_commit_owner", object()),
            "keeper": lambda: case.keeper.slots.__setitem__(id(case.slot), object()),
            "selection": lambda: setattr(case.fixture.live.event_stream, "_gdb_native_selection", object()),
            "server": lambda: setattr(case.graph.handle, "server", SimpleNamespace(should_exit=False)),
        }
        changes[replacement]()
        with pytest.raises(RuntimeStoreError):
            require_original_source_call(case.slot, case.source)
        assert original.slot is case.slot and original.pending is case.pending
        assert case.graph.handle.source_owner.calls[id(case.slot)] == (case.slot, original)
        assert case.source.original_source_call is original
    finally:
        await close_graph(case)


@pytest.mark.asyncio
async def test_missing_app_issuer_holds_one_actual_request_and_rejects_replay(monkeypatch, tmp_path):
    case = await call_graph(monkeypatch, tmp_path)
    try:
        with pytest.raises(RuntimeStoreError, match="app_call_issuer_missing"):
            requests.register_native_request(case.slot)
        candidate = case.slot._native_request_candidate_id
        request = case.slot._native_request_object
        assert request is requests._open[candidate]
        assert case.source.original_source_call.request is request
        assert request.native_slot is case.slot and request.native_phase == "blocked"
        assert request.native_pair_credit == 2 and request.replay is False
        assert not request.event.is_set()
        with pytest.raises(RuntimeStoreError, match="request_reentry"):
            requests.register_native_request(case.slot)
        assert case.slot._native_request_candidate_id == candidate
        assert case.slot._native_request_object is request
        assert not requests.resolve({"id": candidate, "result": {"approved": True}})
        assert requests.withdraw(request.events, SESSION, "native.invoke") == 0
        assert requests._open[candidate] is request
    finally:
        await close_graph(case)


@pytest.mark.asyncio
async def test_repeated_declaration_does_not_replace_held_refs(monkeypatch, tmp_path):
    case = await call_graph(monkeypatch, tmp_path)
    try:
        original = case.source.original_source_call
        with pytest.raises(RuntimeStoreError, match="declaration_reentry"):
            case.slot.prepare_source_declaration()
        assert case.slot._source_declaration_refs is case.source
        assert case.keeper.slots[id(case.slot)] is case.slot
        assert case.graph.handle.source_owner.calls[id(case.slot)] == (case.slot, original)
    finally:
        await close_graph(case)


def test_ordinary_writer_still_publishes_observation_without_native_permission(tmp_path):
    fixture = CommitFixture(tmp_path)
    try:
        fixture.stage()
        ref = fixture.begin()
        fixture.prepare(ref)
        assert fixture.flush() is True
        receipt, = fixture.canonical_events()
        assert receipt["native_ack_allowed"] is False and receipt["replay_allowed"] is False
        assert fixture.owner.native_source_assistant is None
        assert ref.canonical_commit.native_dispatch_slot is None
    finally:
        fixture.close()


@pytest.mark.asyncio
async def test_actual_writer_rolls_back_unissued_native_row(monkeypatch, tmp_path):
    case = await call_graph(monkeypatch, tmp_path)
    try:
        before = case.fixture.rows()
        message = {"role": "tool", "name": TOOL, "tool_call_id": CALL,
                   "content": "not an issued native result"}
        with pytest.raises(RuntimeStoreError, match="writer_unbound"):
            case.fixture._session_db.append_messages_batch(
                SESSION, [message],
                turn_lease_holder=case.fixture._active_session_turn_lease_holder,
                tool_commit_source_calls=((0, case.pending, message),))
        assert case.fixture.rows() == before
        assert not case.fixture._session_db._conn.in_transaction
    finally:
        await close_graph(case)


@pytest.mark.parametrize("name", [TOOL, "gdb_native_invalid"])
def test_reserved_native_name_never_falls_through_generic_dispatch(monkeypatch, tmp_path, name):
    from agent import tool_executor as executor
    fixture = CommitFixture(tmp_path)
    try:
        fixture.stage([{"id": CALL, "type": "function",
                        "function": {"name": name, "arguments": "{}"}}])
        ref = executor._ToolCallRef(name, {}, SESSION, CALL, [])
        state = SimpleNamespace(args=ref.args, dispatch_ref=None, data_use=None)
        dispatched = []
        monkeypatch.setattr(executor, "_pre_tool_block", lambda agent, call: (None, call.args))
        monkeypatch.setattr(executor, "_run_with_activity_heartbeat", lambda agent, tool, run: run())
        result = executor._dispatch_authorized_once(
            fixture, state, ref, execute=lambda args: dispatched.append(args),
            scope_block=None, display_index=None, begin_execution=None, authorization_gate=None)
        assert json.loads(result)["error"] == "native_dispatch_unavailable"
        assert dispatched == [] and state.dispatch_ref is ref
    finally:
        fixture.close()
