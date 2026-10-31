"""Real private SQLite/source metadata graph; no context, App issuer or native permit."""
from types import SimpleNamespace

from agent.native_receipt_origin import NativeDispatchBridge, frozen_json
from agent.native_turn_retention import capture_claim_retention
from gateway.session_contract import SessionRef
from gateway.session_native_context import NativeSelection
from tests.agent.canonical_commit_fixture import CALL, SESSION, CommitFixture
from tests.gateway.native_source_fixture import source_graph

TOOL_REF = "0" * 26
TOOL = "gdb_native_" + TOOL_REF


async def sql_call_graph(monkeypatch, directory):
    fixture = CommitFixture(directory)
    graph = await source_graph(monkeypatch, directory, authority=fixture.authority)
    authority, live = fixture.authority, fixture.live
    ref = SessionRef(authority.profile_id, SESSION)
    keeper = capture_claim_retention(authority, ref, live, fixture.claim)
    authority.adopt_agent(SESSION, fixture.claim["generation"], fixture)
    fixture.owner = fixture._canonical_tool_commit_owner
    live.subscribers["source-viewer"] = graph.connection.actor
    selection = NativeSelection(
        graph.connection, live, ref, authority.instance_id, authority.epoch,
        "local-sql-source-fixture", "0" * 64, ((TOOL, TOOL_REF),),
        frozen_json([{"name": TOOL, "tool_ref": TOOL_REF}]),
        graph.connection._gdb_original_source_connection)
    live.event_stream._gdb_native_selection = selection
    message = fixture.stage([{"id": CALL, "type": "function",
                              "function": {"name": TOOL, "arguments": '{"original":1}'}}])
    # Construct observational source metadata directly, without calling or
    # substituting bridge_for_owner/require_original_app_context. This fixture
    # does NOT exercise authenticated context creation or the missing App issuer.
    keeper.bind_owner(fixture.owner, fixture)
    fixture.owner.native_bridge = NativeDispatchBridge(
        graph.connection, fixture.owner, selection.context, selection.snapshot,
        [{"name": TOOL, "tool_ref": TOOL_REF}])
    dispatch = fixture.begin(name=TOOL)
    pending = dispatch.canonical_commit
    assert pending is not None and pending.native_dispatch_slot is not None
    slot = pending.native_dispatch_slot
    source = slot.prepare_source_declaration()
    return SimpleNamespace(fixture=fixture, graph=graph, keeper=keeper, selection=selection,
                           message=message, dispatch=dispatch, pending=pending, slot=slot, source=source)
