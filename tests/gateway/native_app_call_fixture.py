"""Compose original source call/context/reader/WS consumers over lower in-memory IO."""
import asyncio
import json
import weakref
from types import SimpleNamespace as Data

from agent.native_receipt_origin import NativeDispatchBridge
from agent.native_turn_retention import capture_claim_retention
from agent.tool_commit_owner import ToolCommitOwner
from agent.tool_commit_rows import json_digest
from gateway.native_app_call import prepare, send
from gateway.native_source_calls import capture_original_assistant
from gateway.session_server_requests import retain_native_candidate
from tests.gateway.native_app_context_fixture import Case, line
from tests.gateway.native_constructor_fixture import Agent, Store
from tui_gateway.ws import WSTransport, _authority_connection


class CallCase(Case):
    async def connect(self):
        self.sent = []

        async def send_text(text):
            self.sent.append(text)
            if getattr(self, "send_error", None) is not None:
                raise self.send_error
            if getattr(self, "after_send", None) is not None:
                self.after_send()

        self.ws = Data(scope={"hermes.session_authority": self.authority,
                             "hermes.gdb_original_source_server": self.handle.source_owner}, send_text=send_text)
        self.transport = WSTransport(self.ws, self.loop)
        self.connection = _authority_connection(self.ws, self.transport, {
            "user_id": "context-fixture", "profile_id": self.authority.profile_id,
            "instance_id": self.authority.instance_id, "native_owner": True, "native_bootstrap": True}, True)
        self.live.subscribers["fixture"] = self.connection.actor
        self.connected.set()
        await self.stop.wait()

    async def offer(self, arguments=None):
        try:
            await self._offer(arguments)
        finally:
            self.offered.set()
        await self.stop.wait()  # keep the original modeled agent/keeper task alive

    async def _offer(self, arguments=None):
        # Same actual authority/connection/selection/context as the preceding
        # private bind, plus actual owner/keeper/assistant/slot/call constructors.
        self.authority.db = Store()
        self.authority.db.get_session = lambda sid: self.row if sid == "s" else None
        self.agent = Agent()
        self.agent._session_db, self.agent.session_id = self.authority.db, "s"
        self.agent._active_session_turn_lease_holder = "source-turn"
        self.agent._active_compression_lock_holder = None
        self.selection = self.live.event_stream._gdb_native_selection
        name, tool_ref = self.selection.tools[0]
        scope = {"profile_id": self.authority.profile_id, "session_id": "s",
                 "instance_id": self.authority.instance_id, "epoch": 1, "generation": 1, "revision": 1}
        self.owner = ToolCommitOwner(self.agent, scope, lambda: True, lambda value: None)
        self.agent._canonical_tool_commit_owner = self.owner
        self.keeper = capture_claim_retention(self.authority, self.ref, self.live, {"generation": 1})
        self.keeper.note_owner(self.owner)
        self.keeper.bind_owner(self.owner, self.agent)
        self.owner._native_retention_ref = weakref.ref(self.keeper)
        model_call = {"id": "call-fixture", "type": "function", "function": {"name": name, "arguments": "{}"}}
        message = {"role": "assistant", "message_uid": "message-fixture", "tool_calls": [model_call]}
        witness = {"role": "assistant", "row_id": 1, "row_version": 1,
                   "message_uid": message["message_uid"], "calls": message["tool_calls"],
                   "calls_digest": json_digest(message["tool_calls"]), "call_uids": {"call-fixture": "uid-fixture"},
                   "call_uids_digest": json_digest({"call-fixture": "uid-fixture"}),
                   "turn_holder": "source-turn", "compression_holder": None}
        self.owner.staged = witness
        self.owner.native_bridge = NativeDispatchBridge(self.connection, self.owner,
            self.selection.context, self.selection.snapshot, [{"name": name, "tool_ref": tool_ref}])
        self.owner.native_source_assistant = capture_original_assistant(
            self.authority, "s", 1, self.live, self.owner, message, witness)
        ref = Data(name=name, call_id="call-fixture", task_id="s", args=arguments if arguments is not None else {"unicode": "é🦉"})
        self.pending = self.owner.bind(ref)
        ref.canonical_commit = self.pending
        self.slot = self.pending.native_dispatch_slot
        self.slot.prepare_source_declaration()
        if getattr(self, "use_register", False):
            return
        self.request = retain_native_candidate(self.slot)
        self.original = self.slot._source_declaration_refs.original_source_call
        self.call = prepare(self.original, self.request)
        await send(self.call)

    def call_frame(self, **changes):
        return {**self.frame(), "command": "bind_native_call", "requestId": "01J00000000000000000000003",
                "serverRequestId": self.slot._native_request_candidate_id,
                "wireSha256": self.call.digest, **changes}

    def exchange(self, *, modify=None, frame_changes=None, arguments=None, repeat=False):
        assert self.stage().get("error") is None

        def after_context():
            self.offered = asyncio.Event()
            self.offer_task = self.loop.create_task(self.offer(arguments))
            self.run(self.offered.wait())
            if self.offer_task.done():
                self.run(self.offer_task)
            if modify is not None:
                modify(self)
            frame = self.call_frame(**(frame_changes or {}))
            position = self.input.tell()
            tail = self.input.read()
            self.input.seek(position)
            replay = line({**frame, "requestId": "01J00000000000000000000004"}) if repeat else b""
            self.input.write(line(frame) + replay + tail)
            self.input.seek(position)

        self.read(before_shutdown=after_context)

    def close(self):
        super().close()
        task = getattr(self, "offer_task", None)
        if task is not None and not task.done():
            self.run(task)
