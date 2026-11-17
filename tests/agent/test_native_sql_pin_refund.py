"""Pending and failed real local SQL work cannot acquire terminal refunds."""
import pytest

from agent import native_original_use as native
from hermes_state_native_book import NativeSQLFailure, require_no_local_native_hold
from hermes_state_runtime import RuntimeStoreError
from tests.agent.native_sql_fixture import finish_case, local_case, prepare_result


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["pending", "failed"])
async def test_unfinished_pin_and_use_remain_charged(monkeypatch, request, state):
    case = await local_case(monkeypatch, request)
    try:
        db, book = case.fixture._session_db, case.pin.book
        if state == "failed":
            prepare_result(case)
            def interrupted(*args, **kwargs):
                raise RuntimeError("refund-regression-original-writer-failed")
            monkeypatch.setattr(db, "append_messages_batch", interrupted)
            assert case.fixture.flush() is False
            assert book.error is not None
        before = book.bytes
        for _ in range(2):
            with pytest.raises(NativeSQLFailure, match="retirement_held"):
                require_no_local_native_hold(db)
            with pytest.raises(RuntimeStoreError):
                native.acquire_local_sql_use(case.pin, case.request)
            assert book.bytes == before
            assert book.pins[id(case.slot)] == (case.slot, case.pin)
            assert book.uses[case.pin.pin_id] is case.use
        assert case.pin.credit + case.use.credit + 4096 == before
    finally:
        await finish_case(case)
