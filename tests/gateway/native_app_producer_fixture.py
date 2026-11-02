"""Real original private consumers and SQLite; lower socket/pipe/Job/provider IO are models."""
from __future__ import annotations

import asyncio
import io
import json
import logging
import threading
from types import SimpleNamespace as Data

import hermes_owned_control as control
from agent.session_persistence import SessionPersistenceMixin
from agent.native_turn_retention import capture_claim_retention
from agent.tool_commit_rows import text_digest
from agent.tool_executor import _ToolCallRef
from gateway.native_app_result import resolve
from hermes_state import SessionDB
from hermes_state_runtime import admit_session_input, begin_runtime_epoch, claim_session_input
from tests.gateway import native_app_context_fixture as context_fixture
from tests.gateway.native_app_call_fixture import CallCase
from tests.gateway.native_app_context_fixture import line

RETAINED = []


class BlockingInput(io.BytesIO):
    def __init__(self):
        super().__init__()
        self.condition, self.eof = threading.Condition(), False

    def readline(self, size=-1):
        with self.condition:
            while True:
                raw = super().readline(size)
                if raw or self.eof:
                    return raw
                self.condition.wait()


class Output(io.BytesIO):
    notify = None

    def write(self, raw):
        written = super().write(raw)
        if self.notify is not None:
            for part in raw.splitlines():
                self.notify(json.loads(part))
        return written


class Agent(SessionPersistenceMixin):
    def __init__(self, db):
        self._session_db, self.session_id, self._session_db_created = db, "s", True
        self._active_session_turn_lease_holder, self._active_compression_lock_holder = "producer-turn", None
        self._active_session_turn_lease_ttl_seconds = 300
        self.messages = []
        self.quiet_mode, self.verbose_logging = True, False
        self._checkpoint_mgr = Data(enabled=False)
        self._incremental_persistence_failed = False

    def flush(self):
        return self._flush_messages_to_session_db(self.messages)


class ProducerCase(CallCase):
    def __init__(self, monkeypatch, tmp_path, *, claim_after_context=False):
        monkeypatch.setattr(context_fixture, "Input", BlockingInput)
        monkeypatch.setattr(context_fixture, "Output", Output)
        super().__init__(monkeypatch, tmp_path)
        # The new fixture uses actual declared test threads and actual cross-thread submission.
        monkeypatch.setattr(control, "threading", threading)
        monkeypatch.setattr(control, "asyncio", asyncio)
        self.notifications = asyncio.Queue()
        self.output.notify = lambda value: self.loop.call_soon_threadsafe(self.notifications.put_nowait, value)
        self.wire_ready, self.worker_done = asyncio.Event(), asyncio.Event()
        self.after_send = self.wire_ready.set
        self.db = SessionDB(tmp_path / "producer-state.db")
        self.db.create_session("s", "test")
        self.epoch = begin_runtime_epoch(self.db, instance_id="context-instance")
        self.claim_after_context = claim_after_context
        self.authority.db, self.authority.epoch = self.db, self.epoch
        if not claim_after_context:
            self._claim_original_turn()
        self.agent, self.worker_error, self.control_error = Agent(self.db), None, None
        self.result = self.message = None
        self.canonical_frames = []
        # Exercise the existing synchronous canonical observer with actual frames.
        # This lower delivery model is not an App socket or a native acknowledgement.
        self.live.event_stream.observers.add(self.canonical_frames.append)
        self.control_thread = threading.Thread(target=self._control, name="source-fixture-original-control")
        self.worker_thread = threading.Thread(target=self._worker, name="source-fixture-original-agent")
        RETAINED.append(self)

    def _claim_original_turn(self):
        admit_session_input(self.db, epoch=self.epoch, principal_id="context-fixture", session_id="s",
                            request_id="producer-input", payload={"text": "declared source fixture"})
        self.claim = claim_session_input(self.db, epoch=self.epoch, session_id="s")
        assert self.db.acquire_session_turn_lease("s", "producer-turn", wait_seconds=0)
        self.live.event_stream.execution = {"authority_epoch": self.epoch,
            "execution_generation": self.claim["generation"], "admission_id": self.claim["admission_id"]}
        self.authority.executing["s"] = self.claim["generation"]

    def append(self, raw):
        with self.input.condition:
            super().append(raw)
            self.input.condition.notify_all()

    def _control(self):
        try:
            self.source.read_shutdown()
        except BaseException as error:
            self.control_error = error
            logging.getLogger(__name__).exception("Original source fixture control failed")

    async def notification(self, event):
        while True:
            try:
                frame = await asyncio.wait_for(self.notifications.get(), 15)
            except TimeoutError:
                if self.worker_error is not None:
                    raise self.worker_error
                if self.control_error is not None:
                    raise self.control_error
                raise
            if frame["event"] == event:
                return frame

    async def _offer(self, arguments=None):
        self.keeper = capture_claim_retention(self.authority, self.ref, self.live, self.claim)
        self.authority.adopt_agent("s", self.claim["generation"], self.agent)
        self.owner = self.agent._canonical_tool_commit_owner
        assert self.owner is not None
        self.keeper.bind_owner(self.owner, self.agent)
        # Model provider/model IO only. The actual original agent worker below stages,
        # writes and binds this same assistant and effective argument object.

    async def offer(self, arguments=None):
        try:
            await self._offer(arguments)
        finally:
            self.offered.set()
        await self.stop.wait()
        if self.worker_error is None and self.call.terminal is not None:
            await self.keeper.wait_claim_exit()

    def _stage_original_assistant(self):
        tool_name = self.params()["tools"][0]["name"]
        self.assistant = {"role": "assistant", "content": "original source request", "tool_calls": [
            {"id": "original-call", "type": "function", "function": {"name": tool_name, "arguments": "{}"}}]}
        self.agent.messages.append(self.assistant)
        self.owner.stage(self.assistant)
        assert self.agent.flush() is True
        self.dispatch = _ToolCallRef(tool_name, {"original": "é🙂"}, "s", "original-call", [])
        self.pending = self.owner.bind(self.dispatch)
        self.dispatch.canonical_commit = self.pending
        self.slot = self.pending.native_dispatch_slot
        assert self.slot is not None

    def _worker(self):
        try:
            self._stage_original_assistant()
            self.result = self.slot.execute()
            self.message = {"role": "tool", "name": self.dispatch.name, "tool_call_id": self.dispatch.call_id,
                            "content": "canonical projection: " + self.result}
            self.owner.prepare(self.pending, self.message, text_digest(self.result))
            self.agent.messages.append(self.message)
            if not self.agent.flush():
                raise AssertionError("Original canonical producer failed: " + str(getattr(self.agent, "_last_persistence_error", "unknown")))
        except BaseException as error:
            self.worker_error = error
            logging.getLogger(__name__).exception("Original source fixture worker failed")
        finally:
            self.loop.call_soon_threadsafe(self.worker_done.set)

    async def _begin(self):
        staged = await self.connection.dispatch({"id": 1, "method": "native.context.stage", "params": self.params()})
        assert staged.get("error") is None
        self.append(line(self.frame()))
        await self.notification("native_context_bound")
        if self.claim_after_context:
            self._claim_original_turn()
        self.offered = asyncio.Event()
        self.offer_task = self.loop.create_task(self.offer())
        await self.offered.wait()
        if self.offer_task.done():
            await self.offer_task
        self.worker_thread.start()
        await asyncio.wait_for(self.wire_ready.wait(), 15)
        self.call = self.slot._source_constructions["app_call"]
        self.request = self.slot._native_request_object
        self.append(line(self.call_frame()))
        await self.notification("native_call_bound")

    def begin(self):
        self.control_thread.start()
        self.run(self._begin())

    def phase(self, command, number, **changes):
        frame = {**self.call_frame(), "command": command,
                 "requestId": "01J00000000000000000000" + f"{number:03d}",
                 "invocationId": "11111111-1111-4111-8111-111111111111", "invocationSequence": 1}
        if command != "prepare_native_call":
            frame["proofSha256"] = self.call.commit_proof_sha256
        frame.update(changes)
        self.append(line(frame))
        return frame

    def decline(self):
        from gateway.native_app_result import DECLINE
        frame = {"jsonrpc": "2.0", "id": self.slot._native_request_candidate_id, "error": dict(DECLINE)}
        async def original_response():
            return resolve(self.connection, frame)
        assert self.run(original_response())
        return frame

    def deliver_result(self, raw="original native result"):
        frame = {"jsonrpc": "2.0", "id": self.slot._native_request_candidate_id, "result": {
            "invocationId": self.call.invocation[0], "invocationSequence": self.call.invocation[1], "rawResult": raw}}
        async def original_response():
            return resolve(self.connection, frame)
        assert self.run(original_response())
        return frame

    def close(self):
        if self.worker_thread.ident is not None:
            self.run(asyncio.wait_for(self.worker_done.wait(), 15))
            self.worker_thread.join(5)
            assert not self.worker_thread.is_alive()
        self.append(line({"protocol": self.frame()["protocol"], "command": "shutdown",
                          "attemptId": self.contract.attempt_id}))
        with self.input.condition:
            self.input.eof = True
            self.input.condition.notify_all()
        self.control_thread.join(5)
        assert not self.control_thread.is_alive()
        self.stop.set()
        if getattr(self, "offer_task", None) is not None:
            self.run(self.offer_task)
        super().close()
        if self.db._native_read_gate.lease is None and not self.worker_error:
            self.db.close()
