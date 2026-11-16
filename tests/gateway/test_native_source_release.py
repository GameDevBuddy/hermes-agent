"""Saved-source accounting uses real producer/SQLite fixtures, not native authority."""
import asyncio

import pytest

from gateway.native_app_context_records import _prune_completed_calls
from gateway.native_app_worker import require_completed_slot
from gateway.native_source_release import release_saved_call
from hermes_state_runtime import admit_session_input, claim_session_input, settle_session_input, RuntimeStoreError
from tests.gateway.native_app_context_fixture import line
from tests.gateway.native_app_producer_fixture import Agent, ProducerCase
from tests.gateway.test_native_app_result_source import ContinuingProducerCase
from tests.waiting import wait_until
from agent.tool_executor import _ToolCallRef
from hermes_state import SessionDB


class TurnProducerCase(ContinuingProducerCase):
    """Separate real claims avoid exercising the unrelated per-turn 128-slot limit."""
    async def offer(self, arguments=None):
        self.turn_commands = asyncio.Queue()
        await self._offer(arguments)
        self.offered.set()
        while True:
            ready = await self.turn_commands.get()
            if self.worker_error is not None:
                return
            await self.keeper.wait_claim_exit()
            if ready is None:
                return
            settle_session_input(self.db, epoch=self.epoch, admission_id=self.claim["admission_id"],
                                 generation=self.claim["generation"], outcome="completed")
            admit_session_input(self.db, epoch=self.epoch, principal_id="context-fixture", session_id="s",
                                request_id=f"source-turn-{self.claim['generation']}", payload={"text": "next turn"})
            self.claim = claim_session_input(self.db, epoch=self.epoch, session_id="s")
            self.live.event_stream.execution = {"authority_epoch": self.epoch,
                "execution_generation": self.claim["generation"], "admission_id": self.claim["admission_id"]}
            self.authority.executing["s"] = self.claim["generation"]
            messages = self.agent.messages
            # This regression owns only the source book. Keep each old SQL book
            # charged and retained; a fresh real handle reads the SAME history.
            self.old_databases = [*getattr(self, "old_databases", []), self.db]
            self.db.close()  # The completed claim has released its actual writer.
            self.db = SessionDB(self.db.db_path)
            self.authority.db = self.db
            self.agent = Agent(self.db)
            self.agent.messages = messages  # Keep the actual transcript, never reset history.
            await self._offer()
            ready.set()

    async def rotate(self):
        ready = asyncio.Event()
        await self.turn_commands.put(ready)
        await asyncio.wait_for(ready.wait(), 15)

    def close(self):
        if getattr(self, "turn_commands", None) is not None:
            self.turn_commands.put_nowait(None)
        super().close()



def finish_saved(case, index=0):
    invocation = f"11111111-1111-4111-8111-{index + 1:012d}"
    case.phase("prepare_native_call", 100 + index * 10 + 2,
               requestId=f"{100000 + index * 10 + 2:026d}",
               invocationId=invocation, invocationSequence=index + 1)
    case.run(case.notification("native_call_prepared"))
    case.deliver_result("saved source result")
    case.run(case.notification("native_call_committed"))
    for offset, (command, event) in enumerate((
        ("ack_native_call", "native_call_acknowledged"),
        ("complete_native_call", "native_call_completed"),
        ("release_native_call", "native_call_released")), 3):
        case.phase(command, 100 + index * 10 + offset, requestId=f"{100000 + index * 10 + offset:026d}",
                   invocationId=invocation, invocationSequence=index + 1)
        case.run(case.notification(event))
    case.run(asyncio.wait_for(case.worker_done.wait(), 15))
    assert case.worker_error is None and case.control_error is None
    assert wait_until(lambda: case.call.reader.active is None, timeout=5)


class SharedAssistantCase(ContinuingProducerCase):
    def _stage_original_assistant(self):
        tool_name = self.params()["tools"][0]["name"]
        index = len(getattr(self, "shared_dispatches", []))
        if index == 0:
            self.shared_dispatches = []
            self.assistant = {"role": "assistant", "content": "two native calls", "tool_calls": [
                {"id": f"shared-{i}", "type": "function", "function": {"name": tool_name, "arguments": "{}"}}
                for i in range(2)]}
            self.agent.messages.append(self.assistant)
            self.owner.stage(self.assistant)
            assert self.agent.flush()
        self.dispatch = _ToolCallRef(tool_name, {}, "s", f"shared-{index}", [])
        self.pending = self.owner.bind(self.dispatch)
        self.dispatch.canonical_commit = self.pending
        self.slot = self.pending.native_dispatch_slot
        self.shared_dispatches.append(self.dispatch)


def test_shared_assistant_and_bridge_stay_live_for_unbound_sibling(monkeypatch, tmp_path):
    case = SharedAssistantCase(monkeypatch, tmp_path)
    try:
        case.begin()
        first = case.call
        assistant, bridge = first.original.assistant, first.slot.bridge
        runtime = first.original.connection.parent.runtime
        finish_saved(case)
        release_saved_call(first)
        assert first.source_refunded
        assert runtime.assistants[id(assistant.witness)] is assistant
        assert runtime.bridges[id(case.owner)].value is bridge
        assistant.require_assistant()
        case.worker_done.clear()
        case.wire_ready.clear()
        case.continue_chat = True
        case.next_request.set()
        case.run(asyncio.wait_for(case.wire_ready.wait(), 15))
        case.call = case.slot._source_constructions["app_call"]
        case.request = case.slot._native_request_object
        case.append(line(case.call_frame(requestId="01J00000000000000000000011")))
        case.run(case.notification("native_call_bound"))
        assert case.call.original.assistant is assistant and case.slot.bridge is bridge
        finish_saved(case, 1)
        release_saved_call(case.call)
        assert not runtime.assistants and not first.original.connection.parent.calls
        assert require_completed_slot(first.slot) is first
        assert require_completed_slot(case.slot) is case.call
    finally:
        case.continue_chat = False
        case.next_request.set()
        case.close()
    assert not runtime.bridges


def test_300_saved_calls_on_same_runtime_return_to_flat_source_book(monkeypatch, tmp_path):
    case = TurnProducerCase(monkeypatch, tmp_path)
    case.request_count = 300
    completed = []
    try:
        case.begin()
        runtime = case.call.original.connection.parent.runtime
        baseline = None
        for index in range(300):
            if index:
                case.run(case.rotate())
                case.worker_done.clear()
                case.wire_ready.clear()
                case.continue_chat = True
                case.next_request.set()
                case.run(asyncio.wait_for(case.wire_ready.wait(), 15))
                case.call = case.slot._source_constructions["app_call"]
                case.request = case.slot._native_request_object
                case.append(line(case.call_frame(requestId=f"01J000000000000000000{index * 10 + 1:05d}")))
                case.run(case.notification("native_call_bound"))
            finish_saved(case, index)
            with case.source._state_lock:
                _prune_completed_calls(case.source, case.source._app_context_requests)
            assert case.call.original.connection.parent.runtime is runtime
            assert case.call.source_refunded
            if baseline is None:
                baseline = runtime.bytes
            assert runtime.bytes == baseline
            assert not runtime.assistants and not case.call.original.connection.parent.calls
            assert len(runtime.bridges) == 1  # Reusable until this original claim exits.
            completed.append(case.call)
            before = runtime.bytes
            release_saved_call(case.call)
            assert runtime.bytes == before
        for call in completed:
            assert require_completed_slot(call.slot) is call
        assert case.db._read_one("SELECT COUNT(*) FROM messages WHERE role='tool'")[0] == 300
    finally:
        case.continue_chat = False
        case.next_request.set()
        case.close()
    assert not runtime.bridges


@pytest.mark.parametrize("state", ["incomplete", "failed", "unknown"])
def test_nonterminal_source_calls_keep_all_credits(monkeypatch, tmp_path, state):
    case = ProducerCase(monkeypatch, tmp_path)
    try:
        case.begin()
        runtime = case.call.original.connection.parent.runtime
        before, calls = runtime.bytes, dict(case.call.original.connection.parent.calls)
        if state != "incomplete":
            case.call.error = RuntimeStoreError(state)
        with case.source._state_lock:
            _prune_completed_calls(case.source, case.source._app_context_requests)
        assert runtime.bytes == before and case.call.original.connection.parent.calls == calls
        assert not case.call.source_refunded
        case.call.error = None
        case.decline()
        case.run(asyncio.wait_for(case.worker_done.wait(), 15))
    finally:
        case.close()


@pytest.mark.parametrize("state", ["active", "unconfirmed", "failed", "changed-terminal", "missing-original"])
def test_saved_source_refund_requires_unchanged_idle_completion(monkeypatch, tmp_path, state):
    case = ProducerCase(monkeypatch, tmp_path)
    try:
        case.begin()
        finish_saved(case)
        call = case.call
        runtime, server = call.original.connection.parent.runtime, call.original.connection.parent
        before, terminal, pair = runtime.bytes, call.terminal, server.calls[id(call.slot)]
        with case.source._state_lock:
            if state == "active":
                call.reader.active = call.phases["release_native_call"]
            elif state == "unconfirmed":
                call.completion.confirmed = False
            elif state == "failed":
                call.error = RuntimeStoreError("saved-but-unknown")
            elif state == "changed-terminal":
                call.terminal = object()
            else:
                del server.calls[id(call.slot)]
            try:
                if state == "active":
                    release_saved_call(call)
                else:
                    with pytest.raises(RuntimeStoreError):
                        release_saved_call(call)
                assert runtime.bytes == before and not call.source_refunded
            finally:
                call.reader.active = None
                call.completion.confirmed = True
                call.error = None
                call.terminal = terminal
                server.calls[id(call.slot)] = pair
            release_saved_call(call)
            assert call.source_refunded and runtime.bytes < before
            after = runtime.bytes
            attempts = [value._source_construction for value in
                        (call.original, call.slot, *call.slot._source_constructions.values())]
            assert all(attempt.credit == 0 for attempt in attempts)
            assert call.wire_credit == 262144 and call.proof_credit == 65536
            assert call.result_credit == 4 * len(call.raw_result) + 4096
            assert before - after == len(attempts) * 4096 + 4096 + call.wire_credit + call.proof_credit + call.result_credit
            release_saved_call(call)
            assert runtime.bytes == after
            assert require_completed_slot(call.slot) is call
            call.terminal = object()
            try:
                with pytest.raises(RuntimeStoreError):
                    require_completed_slot(call.slot)
            finally:
                call.terminal = terminal
    finally:
        case.close()
