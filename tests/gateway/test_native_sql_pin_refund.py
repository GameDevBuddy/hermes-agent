"""Finished SQL budgets on the unchanged producer database; no native acceptance."""
import asyncio

import pytest

from agent.native_original_use import acquire_local_sql_use, reserve_local_sql_pin
from gateway.native_app_worker import require_completed_slot
from gateway.native_source_release import release_saved_call
from hermes_state_native_book import refund_saved_call, require_no_local_native_hold
from hermes_state_native_construction import require_constructed
from hermes_state_runtime import (RuntimeStoreError, admit_session_input,
                                  claim_session_input, settle_session_input)
from tests.gateway.native_app_context_fixture import line
from tests.gateway.test_native_source_release import TurnProducerCase, finish_saved
from tests.gateway.native_app_producer_fixture import ProducerCase


class IdleReaderCase:
    async def notification(self, event):
        result = await super().notification(event)
        if event == "native_call_released":
            # Publication wakes the consumer before the original phase task's
            # finally block. Keep its actual loop running until it returns idle.
            async def idle():
                while self.call.reader.active is not None:
                    await asyncio.sleep(0.001)
            await asyncio.wait_for(idle(), 5)
        return result


class SavedProducerCase(IdleReaderCase, ProducerCase):
    pass


class SameDatabaseCase(IdleReaderCase, TurnProducerCase):
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
                                request_id=f"sql-turn-{self.claim['generation']}", payload={"text": "next turn"})
            self.claim = claim_session_input(self.db, epoch=self.epoch, session_id="s")
            self.live.event_stream.execution = {"authority_epoch": self.epoch,
                "execution_generation": self.claim["generation"], "admission_id": self.claim["admission_id"]}
            self.authority.executing["s"] = self.claim["generation"]
            await self._offer()
            ready.set()


def test_300_finished_cycles_refund_same_sql_book(monkeypatch, tmp_path):
    # Lower only this test process's in-memory caps; the unchanged producer
    # refuses its fourth Read with these limits. Do not alter production caps.
    import hermes_state_native_book as budgets
    monkeypatch.setattr(budgets, "MAX_PINS", 8)
    monkeypatch.setattr(budgets, "MAX_METADATA", 1024 * 1024)
    case = SameDatabaseCase(monkeypatch, tmp_path)
    case.request_count = 300
    completed = []
    try:
        case.begin()
        db, connection = case.db, case.db._conn
        book = None
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
            pin, use = case.call.origin.pin, case.call.origin.use
            book = book or pin.book
            assert case.db is db and db._conn is connection and pin.book is book
            assert book.bytes == 4096 + pin.credit + use.credit
            assert case.keeper.use_credit == 81920
            release_saved_call(case.call)
            assert book.bytes == 4096 and not book.pins and not book.uses
            assert case.keeper.use_credit == 0
            require_no_local_native_hold(db)
            require_constructed(pin)
            require_constructed(use)
            assert require_completed_slot(case.slot) is case.call
            release_saved_call(case.call)
            refund_saved_call(case.call)
            assert book.bytes == 4096
            with pytest.raises(RuntimeStoreError):
                reserve_local_sql_pin(case.slot)
            with pytest.raises(RuntimeStoreError):
                acquire_local_sql_use(pin, use.request)
            completed.append(case.call)
        for call in completed:
            require_constructed(call.origin.pin)
            require_constructed(call.origin.use)
            assert require_completed_slot(call.slot) is call
        assert db._read_one("SELECT COUNT(*) FROM messages WHERE role='tool'")[0] == 300
    finally:
        case.continue_chat = False
        case.next_request.set()
        case.close()


@pytest.mark.parametrize("state", ["active", "unconfirmed", "failed", "unknown"])
def test_unfinished_sql_charge_is_not_refunded(monkeypatch, tmp_path, state):
    case = SavedProducerCase(monkeypatch, tmp_path)
    try:
        case.begin()
        finish_saved(case)
        call, pin, use = case.call, case.call.origin.pin, case.call.origin.use
        book = pin.book
        before = book.bytes
        if state == "active":
            call.reader.active = call.phases["release_native_call"]
        elif state == "unconfirmed":
            call.completion.confirmed = False
        elif state == "failed":
            call.error = RuntimeStoreError("failed-original")
        else:
            book.error = ("RuntimeStoreError", "unknown-original")
        try:
            if state == "active":
                release_saved_call(call)
            else:
                with pytest.raises(RuntimeStoreError):
                    release_saved_call(call)
            assert book.bytes == before
            assert book.pins[id(case.slot)] == (case.slot, pin)
            assert book.uses[pin.pin_id] is use
            assert not call.source_refunded
        finally:
            call.reader.active = None
            call.completion.confirmed = True
            call.error = None
            book.error = None
        release_saved_call(call)
        assert book.bytes == 4096
        # Detached constructor checks still require the original retained objects.
        case.slot._native_pin = object()
        try:
            with pytest.raises(RuntimeStoreError):
                require_constructed(pin)
        finally:
            case.slot._native_pin = pin
        use.request.native_active_use = object()
        try:
            with pytest.raises(RuntimeStoreError):
                require_constructed(use)
        finally:
            use.request.native_active_use = use
    finally:
        case.close()
