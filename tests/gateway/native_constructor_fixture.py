"""Actual source constructors over in-memory store/transport metadata, never App/native rights."""
from contextlib import contextmanager
from types import SimpleNamespace
import weakref

from agent.native_receipt_origin import NativeDispatchBridge, frozen_json
from agent.native_turn_retention import capture_claim_retention
from agent.tool_commit_owner import ToolCommitOwner
from agent.tool_commit_rows import json_digest
from gateway.native_source_calls import capture_original_assistant
from gateway.session_authority import LiveSession
from gateway.session_contract import SessionRef
from gateway.session_native_context import NativeSelection
from tests.gateway.native_source_fixture import source_graph, finish_source_graph

SESSION, CALL, UID = "constructor-source-fixture", "model-call", "retained-call"
TOOL_REF = "0" * 26
TOOL = "gdb_native_" + TOOL_REF


class Store:
    """Only the original owner's lease-read protocol; no connection or real SQLite store."""
    @contextmanager
    def _read_ctx(self):
        yield self

    def _session_turn_lease_key_on_conn(self, conn, session):
        return session

    def execute(self, query, params):
        row = {"holder": "source-turn"} if "session_turn_leases" in query else None
        return SimpleNamespace(fetchone=lambda: row)


class Agent:
    pass


async def constructor_graph(monkeypatch, tmp_path, *, capture_assistant=True, function=None):
    graph = await source_graph(monkeypatch, tmp_path)
    authority = graph.authority
    authority.db = Store()
    live = LiveSession(SimpleNamespace(platform=None), "source-route")
    authority.sessions[SESSION] = live
    live.subscribers["source-viewer"] = graph.connection.actor
    ref = SessionRef(authority.profile_id, SESSION)
    selection = NativeSelection(graph.connection, live, ref, authority.instance_id, authority.epoch,
                                "source-fixture", "0" * 64, ((TOOL, TOOL_REF),),
                                frozen_json([{"name": TOOL, "tool_ref": TOOL_REF}]),
                                graph.connection._gdb_original_source_connection)
    live.event_stream._gdb_native_selection = selection
    agent = Agent()
    agent._session_db, agent.session_id = authority.db, SESSION
    agent._active_session_turn_lease_holder = "source-turn"
    agent._active_compression_lock_holder = None
    scope = {"profile_id": authority.profile_id, "session_id": SESSION,
             "instance_id": authority.instance_id, "epoch": authority.epoch,
             "generation": 1, "revision": 1}
    owner = ToolCommitOwner(agent, scope, lambda: True, lambda value: None)
    agent._canonical_tool_commit_owner = owner
    keeper = capture_claim_retention(authority, ref, live, {"generation": 1})
    keeper.note_owner(owner)
    keeper.bind_owner(owner, agent)
    owner._native_retention_ref = weakref.ref(keeper)
    call = {"id": CALL, "type": "function", "function": function if function is not None else
            {"name": TOOL, "arguments": '{"model":1}'}}
    message = {"role": "assistant", "message_uid": "source-message", "tool_calls": [call]}
    witness = {"role": "assistant", "row_id": 1, "row_version": 1,
               "message_uid": message["message_uid"], "calls": message["tool_calls"],
               "calls_digest": json_digest(message["tool_calls"]), "call_uids": {CALL: UID},
               "call_uids_digest": json_digest({CALL: UID}),
               "turn_holder": "source-turn", "compression_holder": None}
    owner.staged = witness
    bridge = NativeDispatchBridge(graph.connection, owner, selection.context, selection.snapshot,
                                  [{"name": TOOL, "tool_ref": TOOL_REF}])
    owner.native_bridge = bridge
    result = SimpleNamespace(graph=graph, authority=authority, live=live, ref=ref, selection=selection,
                             agent=agent, owner=owner, keeper=keeper, message=message, witness=witness,
                             call=call, bridge=bridge, runtime=graph.runtime)
    if capture_assistant:
        result.assistant = capture_original_assistant(authority, SESSION, 1, live, owner, message, witness)
        owner.native_source_assistant = result.assistant
    return result


def pending_slot(case, ref=None):
    if ref is None:
        ref = SimpleNamespace(name=TOOL, call_id=CALL, task_id=SESSION, args={"effective": 2})
    pending = case.owner.bind(ref)
    assert pending is not None and pending.native_dispatch_slot is not None
    ref.canonical_commit = pending
    return pending.native_dispatch_slot


def original_self(error, initializer):
    traceback = error.__traceback__
    while traceback is not None:
        if traceback.tb_frame.f_code is initializer.__code__:
            return traceback.tb_frame.f_locals["self"]
        traceback = traceback.tb_next
    raise AssertionError("The actual initializer never ran")


async def finish(case):
    await finish_source_graph(case.graph)
