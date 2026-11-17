"""One authority's actual claim exits return native-turn capacity; source IO is modeled."""
import asyncio

import pytest

from gateway.session_authority import SessionAuthority
from gateway.native_app_worker import require_completed_slot
from gateway.native_source_stop import reject_native_authority_retirement
from hermes_state_runtime import admit_session_input, claim_session_input, settle_session_input
from tests.gateway.native_app_context_fixture import line
from tests.gateway.test_native_sql_pin_refund import SameDatabaseCase
from tests.gateway.test_native_source_release import finish_saved


class AuthorityTurnsCase(SameDatabaseCase):
    def __init__(self, monkeypatch, tmp_path):
        super().__init__(monkeypatch, tmp_path)
        from gateway import session_finite
        monkeypatch.setattr(session_finite, "execute_finite_admission", self.finite)

    async def finite(self, authority, ref, row):
        assert authority is self.authority and row is self.claim
        await self._offer()
        self.offered.set()
        if getattr(self, "ready", None) is not None:
            self.ready.set()
        self.ready = await self.turn_commands.get()
        return "finished fixture turn"

    async def offer(self, arguments=None):
        self.turn_commands = asyncio.Queue()
        while True:
            response, outcome = await SessionAuthority._execute_claimed(
                self.authority, self.ref, self.live, self.claim)
            assert outcome == "completed", response
            if self.ready is None:
                return
            settle_session_input(self.db, epoch=self.epoch, admission_id=self.claim["admission_id"],
                                 generation=self.claim["generation"], outcome="completed")
            admit_session_input(self.db, epoch=self.epoch, principal_id="context-fixture", session_id="s",
                                request_id=f"turn-refund-{self.claim['generation']}", payload={"text": "next turn"})
            self.claim = claim_session_input(self.db, epoch=self.epoch, session_id="s")
            self.live.event_stream.execution = {"authority_epoch": self.epoch,
                "execution_generation": self.claim["generation"], "admission_id": self.claim["admission_id"]}
            self.authority.executing["s"] = self.claim["generation"]


def test_finished_turns_return_capacity_on_same_authority(monkeypatch, tmp_path):
    import agent.native_turn_retention as retention
    monkeypatch.setattr(retention, "MAX_NATIVE_TURNS", 4, raising=False)
    case = AuthorityTurnsCase(monkeypatch, tmp_path)
    case.request_count = 20
    originals = []
    try:
        case.begin()
        authority, book, db = case.authority, case.authority._native_turn_retention, case.db
        for index in range(20):
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
            originals.append(case.call)
            assert case.authority is authority and case.db is db
            assert authority._native_turn_retention is book
            assert len(book) <= retention.MAX_NATIVE_TURNS
        assert len(book) == 1  # Only the original still-open execution turn.
        for call in originals:
            assert require_completed_slot(call.slot) is call
    finally:
        case.continue_chat = False
        case.next_request.set()
        case.close()
    assert not book
    reject_native_authority_retirement(authority)


@pytest.mark.parametrize("state", ["blocked", "unconfirmed", "failed", "active"])
def test_unfinished_native_turn_remains_charged(monkeypatch, tmp_path, state):
    case = AuthorityTurnsCase(monkeypatch, tmp_path)
    try:
        case.begin()
        finish_saved(case)
        keeper, call = case.keeper, case.call
        if state == "blocked":
            keeper.blocked = True
        elif state == "unconfirmed":
            call.completion.confirmed = False
        elif state == "failed":
            call.error = RuntimeError("unfinished fixture")
        else:
            call.reader.active = call.phases["release_native_call"]
        # Capacity reclamation must not interpret a task cancellation as completion.
        async def attempt():
            task = case.loop.create_task(keeper.wait_claim_exit())
            await asyncio.sleep(0.02)
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        case.run(attempt())
        assert case.authority._native_turn_retention[("s", case.claim["generation"])] is keeper
    finally:
        case.keeper.blocked = False
        case.call.completion.confirmed = True
        case.call.error = None
        case.call.reader.active = None
        case.continue_chat = False
        case.next_request.set()
        case.close()


def test_full_book_refusal_does_not_poison_next_turn(monkeypatch, tmp_path):
    import agent.native_turn_retention as retention
    monkeypatch.setattr(retention, "MAX_NATIVE_TURNS", 1)
    case = AuthorityTurnsCase(monkeypatch, tmp_path)
    case.request_count = 2
    try:
        case.begin()
        first = case.keeper
        # Still-pending native work consumes the only credit; refuse before execution.
        refused = {**case.claim, "generation": case.claim["generation"] + 1,
                   "admission_id": "capacity-refusal"}
        response, outcome = case.run(SessionAuthority._execute_claimed(
            case.authority, case.ref, case.live, refused))
        assert (response, outcome) == ("The admitted turn failed.", "failed")
        assert list(case.authority._native_turn_retention.values()) == [first]
        finish_saved(case)
        case.run(case.rotate())
        assert first.exited and first.completed
        assert list(case.authority._native_turn_retention.values()) == [case.keeper]
        case.worker_done.clear()
        case.wire_ready.clear()
        case.continue_chat = True
        case.next_request.set()
        case.run(asyncio.wait_for(case.wire_ready.wait(), 15))
        case.call = case.slot._source_constructions["app_call"]
        case.request = case.slot._native_request_object
        case.append(line(case.call_frame(requestId="01J00000000000000000000011")))
        case.run(case.notification("native_call_bound"))
        finish_saved(case, 1)
    finally:
        case.continue_chat = False
        case.next_request.set()
        case.close()
    assert not case.authority._native_turn_retention
    reject_native_authority_retirement(case.authority)
