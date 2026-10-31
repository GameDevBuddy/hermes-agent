"""Original Book/Pin/Use initializer failures over real fresh SQLite, never native authority."""
import asyncio
import sys

import pytest

from agent import native_original_use as native
from gateway.session_server_requests import retain_native_candidate
from hermes_state_runtime import RuntimeStoreError
import hermes_state_native_book as books
from tests.agent.native_sql_fixture import local_case, finish_case


def original_self(error, initializer):
    traceback = error.__traceback__
    while traceback is not None:
        if traceback.tb_frame.f_code is initializer.__code__:
            return traceback.tb_frame.f_locals["self"]
        traceback = traceback.tb_next
    raise AssertionError("Actual original initializer did not run")


@pytest.mark.asyncio
async def test_constructor_book_keeps_original_self_and_first_lock_error(monkeypatch, request):
    case = await local_case(monkeypatch, request, use=False, pin=False)
    error = RuntimeError("original Book RLock failure")
    db = case.fixture._session_db

    def fail():
        raise error

    try:
        with monkeypatch.context() as lower:
            lower.setattr(books.threading, "RLock", fail)
            with pytest.raises(RuntimeError) as caught:
                books.book_for(db)
        value = original_self(error, books.NativeSQLBook.__init__)
        assert error.__traceback__ is not None
        assert caught.value is error and db._native_pin_book is value
        assert db._native_pin_init_exception is error and db._native_pin_init_credit == 4096
        assert db._native_pin_init_error == "RuntimeError" and db._native_pin_init_consumed is True
        with pytest.raises(books.NativeSQLFailure):
            books.book_for(db)
        with pytest.raises(books.NativeSQLFailure):
            value.require_current()
        assert db._native_pin_book is value and db._native_pin_init_exception is error
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_constructor_pin_keeps_original_self_before_uuid_cancellation(monkeypatch, request):
    case = await local_case(monkeypatch, request, use=False, pin=False)
    error = asyncio.CancelledError("original Pin UUID cancellation")

    def fail():
        raise error

    try:
        with monkeypatch.context() as lower:
            lower.setattr(native.uuid, "uuid4", fail)
            with pytest.raises(asyncio.CancelledError) as caught:
                native.reserve_local_sql_pin(case.slot)
        value = original_self(error, native.NativeRetirementPin.__init__)
        assert error.__traceback__ is not None
        book = case.fixture._session_db._native_pin_book
        assert caught.value is error and book.pins[id(case.slot)] == (case.slot, value)
        assert case.slot._native_pin is value and case.keeper.pins[id(case.slot)] is value
        assert book.first_exception is error and value._sql_construction.error is error
        first, credit = book.error, book.bytes
        assert first == ("CancelledError", "native_sql_unknown") and value.phase == "unknown"
        with pytest.raises(RuntimeStoreError):
            native.reserve_local_sql_pin(case.slot)
        assert book.first_exception is error and book.error is first and book.bytes == credit
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_constructor_use_keeps_actual_self_before_original_retention_getter(monkeypatch, request):
    case = await local_case(monkeypatch, request, use=False)
    error = RuntimeError("original Use source retention getter")
    cls = type(case.source)
    getter = cls.__getattribute__

    def fail(value, name):
        if name == "retention" and sys._getframe(1).f_code is native.NativeOriginalUse.__init__.__code__:
            raise error
        return getter(value, name)

    try:
        candidate = retain_native_candidate(case.slot)
        with monkeypatch.context() as lower:
            lower.setattr(cls, "__getattribute__", fail)
            with pytest.raises(RuntimeError) as caught:
                native.acquire_local_sql_use(case.pin, candidate)
        value = original_self(error, native.NativeOriginalUse.__init__)
        assert error.__traceback__ is not None
        book = case.pin.book
        assert caught.value is error and book.uses[case.pin.pin_id] is value
        assert case.pin.active_use is case.slot._native_active_use is candidate.native_active_use is value
        assert case.keeper.active_uses[id(value)] is value and case.keeper.active_uses[id(case.slot)] is None
        assert book.first_exception is error and value._sql_construction.error is error
        assert value.phase == case.pin.phase == "unknown" and not value.io_checked
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_constructor_book_block_retains_actual_first_exception_and_diagnostic(monkeypatch, request):
    case = await local_case(monkeypatch, request)
    try:
        original, later = RuntimeError("original first error"), ValueError("later error")
        books.block(case.pin.book, original, case.pin, case.use)
        first, credit = case.pin.book.error, case.pin.book.bytes
        books.block(case.pin.book, later, case.pin, case.use)
        assert case.pin.book.first_exception is original
        assert case.pin.book.error is case.pin.error is case.use.error is first
        assert first == ("RuntimeError", "native_sql_unknown") and case.pin.book.bytes == credit
        assert case.pin._sql_construction.error is case.use._sql_construction.error is original
    finally:
        await finish_case(case)
