"""Actual lazy owner/bridge constructors, synthetic lower IO; no App context or native rights."""
from contextlib import contextmanager
from types import SimpleNamespace
import threading
import weakref

from agent.native_receipt_origin import NativeDispatchBridge, frozen_json
from agent.native_turn_retention import capture_claim_retention
from agent.tool_commit_owner import ToolCommitOwner, owner_call
from agent.tool_commit_rows import json_digest
from gateway.session_authority import LiveSession
from gateway.session_contract import SessionRef
from gateway.session_native_context import NativeSelection
from tests.gateway.native_constructor_fixture import Agent, Store, SESSION, CALL, UID, TOOL, TOOL_REF
from tests.gateway.native_source_fixture import source_graph, finish_source_graph

RETAINED = []


class BridgeStore(Store):
    def __init__(self):
        self.lock = threading.Lock()

    @contextmanager
    def _read_ctx(self):
        with self.lock:
            yield self


async def bridge_case(monkeypatch, directory, tools=None):
    graph = await source_graph(monkeypatch, directory)
    authority = graph.authority
    authority.db = BridgeStore()
    live = LiveSession(SimpleNamespace(platform=None), "bridge-source-route")
    authority.sessions[SESSION] = live
    live.subscribers["bridge-source-viewer"] = graph.connection.actor
    ref = SessionRef(authority.profile_id, SESSION)
    selection = NativeSelection(graph.connection, live, ref, authority.instance_id, authority.epoch,
                                "bridge-source", "0" * 64, ((TOOL, TOOL_REF),),
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
    owner = ToolCommitOwner(agent, scope, lambda: True, lambda *args: True)
    agent._canonical_tool_commit_owner = owner
    keeper = capture_claim_retention(authority, ref, live, {"generation": 1})
    keeper.note_owner(owner)
    keeper.bind_owner(owner, agent)
    owner._native_retention_ref = weakref.ref(keeper)
    call = {"id": CALL, "type": "function", "function": {"name": TOOL, "arguments": "{}"}}
    owner.staged = {"row_id": 1, "row_version": 1, "message_uid": "bridge-source-message",
                    "calls": [call], "calls_digest": json_digest([call]), "call_uids": {CALL: UID},
                    "call_uids_digest": json_digest({CALL: UID}),
                    "turn_holder": "source-turn", "compression_holder": None}
    items = tools if tools is not None else [{"name": TOOL, "tool_ref": TOOL_REF}]
    # This deliberately configures a LOWER observational constructor factory,
    # not bridge_for_owner and not a substitute authenticated App-context gate.
    owner.native_bridge_factory = lambda actual: NativeDispatchBridge(
        graph.connection, actual, selection.context, selection.snapshot, items)
    result = SimpleNamespace(graph=graph, owner=owner, agent=agent, keeper=keeper,
                             selection=selection, authority=authority, live=live, tools=items)
    RETAINED.append(result)
    return result


def begin(case):
    ref = SimpleNamespace(name=TOOL, call_id=CALL, task_id=SESSION, args={"effective": 2})
    result = owner_call(case.agent, "bind", ref)
    return ref, result


def add_call(case, *, call_id="second-call", uid="second-uid", name=TOOL, new_assistant=False):
    old = case.owner.staged
    call = {"id": call_id, "type": "function", "function": {"name": name, "arguments": "{}"}}
    calls = [call] if new_assistant else old["calls"] + [call]
    uids = {call_id: uid} if new_assistant else {**old["call_uids"], call_id: uid}
    case.owner.staged = {**old, "calls": calls, "call_uids": uids,
                         "calls_digest": json_digest(calls), "call_uids_digest": json_digest(uids)}
    return SimpleNamespace(name=name, call_id=call_id, task_id=SESSION, args={"effective": 3})


async def finish(case):
    await finish_source_graph(case.graph)
