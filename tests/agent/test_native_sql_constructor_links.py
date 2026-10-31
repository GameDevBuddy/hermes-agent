"""Actual private SQL constructor/consumer links; no native permission or lifetime proof."""
from types import SimpleNamespace
import sys

import pytest

from agent import native_original_use as native
from agent import native_sql_writer as writer
from gateway.native_source_issuance import require_original_app_call
from gateway.session_server_requests import retain_native_candidate
from hermes_state_runtime import RuntimeStoreError
import hermes_state_native_book as books
from hermes_state_native_construction import require_constructed
from tests.agent.native_sql_fixture import local_case, finish_case, scalar


@pytest.mark.asyncio
async def test_constructor_original_book_pin_use_keep_full_links_and_refuse_app_issuer(monkeypatch, request):
    case = await local_case(monkeypatch, request)
    try:
        db, book = case.fixture._session_db, case.pin.book
        assert db._native_pin_init_credit == 4096 and book.bytes >= 4096 + 65536 + native.NATIVE_USE_CREDIT
        for value in (book, case.pin, case.use):
            require_constructed(value)
            attempt = value._sql_construction
            assert attempt.value is value and attempt.error is None and attempt.phase == "ready"
            assert attempt.connection is db._conn and attempt.writer.__self__ is db
            assert attempt.creator[2] is case.keeper.task
        assert case.pin.phase == "reserved" and case.use.phase == "held"
        assert book.first_exception is None and book.error is None
        assert scalar(case, "SELECT COUNT(*) FROM native_pin_active_uses") == 1
        with pytest.raises(RuntimeStoreError, match="app_call_issuer_missing"):
            require_original_app_call(case.source.original_source_call, case.request)
        assert case.slot.origin is None and not case.use.io_checked
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_constructor_unmanaged_database_default_writer_stays_ordinary(monkeypatch, request):
    case = await local_case(monkeypatch, request, use=False, pin=False)
    try:
        db = case.fixture._session_db
        assert db._native_pin_book is None and db._native_pin_init_credit == 0
        observed = []

        def write(conn):
            observed.append(conn is db._conn and conn.in_transaction)
            conn.execute("UPDATE sessions SET source='constructor-default' WHERE id=?", (case.fixture.session_id,))

        db._execute_write(write)
        assert observed == [True]
        assert scalar(case, "SELECT COUNT(*) FROM sessions WHERE source='constructor-default'") == 1
        assert db._native_pin_book is None and db._native_pin_init_exception is None
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_constructor_book_connection_changed_inside_lock_factory_is_held(monkeypatch, request):
    case = await local_case(monkeypatch, request, use=False, pin=False)
    db, observed = case.fixture._session_db, []
    connection = db._conn
    lock = books.threading.RLock

    def changed():
        observed.append(db._native_pin_book)
        db._conn = object()
        return lock()

    try:
        with monkeypatch.context() as lower:
            lower.setattr(books.threading, "RLock", changed)
            with pytest.raises(books.NativeSQLFailure) as caught:
                books.book_for(db)
        value, = observed
        assert value is db._native_pin_book and value.connection is connection
        assert value._sql_construction.error is caught.value and db._native_pin_init_exception is caught.value
        with pytest.raises(books.NativeSQLFailure):
            value.require_current()
        assert value.connection is connection and db._conn is not connection
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_constructor_pin_retainer_failure_keeps_uninitialized_self_and_original_error(monkeypatch, request):
    case = await local_case(monkeypatch, request, use=False, pin=False)
    original = RuntimeError("original before Pin initializer retainer")
    observed = []
    retain = case.keeper.retain_before_send

    def fail(slot, pin):
        retain(slot, pin)
        observed.append(pin)
        assert not hasattr(pin, "pin_id")
        raise original

    try:
        monkeypatch.setattr(case.keeper, "retain_before_send", fail)
        with pytest.raises(RuntimeError) as caught:
            native.reserve_local_sql_pin(case.slot)
        pin, = observed
        book = case.fixture._session_db._native_pin_book
        assert caught.value is original and book.pins[id(case.slot)][1] is pin
        assert pin._sql_construction.error is book.first_exception is original
        assert pin.phase == "unknown" and case.keeper.pins[id(case.slot)] is pin
        writer.fail_sources(((0, case.pending, {}),), ValueError("later writer failure"))
        assert book.first_exception is original and pin._sql_construction.error is original
        with pytest.raises(books.NativeSQLFailure):
            writer.retained_writer_sources(case.fixture, [])
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_constructor_pin_keeper_task_changed_in_uuid_does_not_get_adopted(monkeypatch, request):
    case = await local_case(monkeypatch, request, use=False, pin=False)
    task, uuid = case.keeper.task, native.uuid.uuid4

    def changed():
        case.keeper.task = SimpleNamespace(done=lambda: False, get_loop=lambda: case.keeper.loop)
        return uuid()

    try:
        with monkeypatch.context() as lower:
            lower.setattr(native.uuid, "uuid4", changed)
            with pytest.raises(books.NativeSQLFailure) as caught:
                native.reserve_local_sql_pin(case.slot)
        pin = case.slot._native_pin
        assert pin._sql_construction.inputs[10] is task
        assert pin._sql_construction.error is caught.value and pin.phase == "unknown"
        assert case.fixture._session_db._native_pin_book.first_exception is caught.value
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_constructor_use_capture_replacement_after_initializer_stays_unknown(monkeypatch, request):
    case = await local_case(monkeypatch, request, use=False)
    original, observed = native.NativeOriginalUse.__init__, []
    capture = case.pin.capture

    def changed(value, *args):
        original(value, *args)
        observed.append(value)
        value.pin.capture = (dict(capture[0]), capture[1], capture[2])

    try:
        candidate = retain_native_candidate(case.slot)
        monkeypatch.setattr(native.NativeOriginalUse, "__init__", changed)
        with pytest.raises(books.NativeSQLFailure) as caught:
            native.acquire_local_sql_use(case.pin, candidate)
        value, = observed
        assert case.pin.book.uses[case.pin.pin_id] is value
        assert value.capture is capture and value._sql_construction.pin_refs[6] is capture
        assert value._sql_construction.error is caught.value and value.phase == "unknown"
        assert case.pin.book.first_exception is caught.value
    finally:
        await finish_case(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["book", "pin", "use"])
async def test_constructor_callback_reentry_refuses_before_database_lock(monkeypatch, request, target):
    case = await local_case(monkeypatch, request, use=False, pin=(target == "use"))
    observed = []
    try:
        db = case.fixture._session_db
        if target == "book":
            lower = books.threading.RLock

            def callback():
                with pytest.raises(books.NativeSQLFailure):
                    books.book_for(db)
                observed.append(db._native_pin_book)
                return lower()

            monkeypatch.setattr(books.threading, "RLock", callback)
            value = books.book_for(db)
        elif target == "pin":
            lower = native.uuid.uuid4

            def callback():
                credit = db._native_pin_book.bytes
                with pytest.raises(books.NativeSQLFailure, match="reentry"):
                    native.reserve_local_sql_pin(case.slot)
                assert db._native_pin_book.bytes == credit
                observed.append(case.slot._native_pin)
                return lower()

            monkeypatch.setattr(native.uuid, "uuid4", callback)
            value = native.reserve_local_sql_pin(case.slot)
        else:
            candidate = retain_native_candidate(case.slot)
            cls, lower = type(case.source), type(case.source).__getattribute__

            def callback(source, name):
                if name == "retention" and sys._getframe(1).f_code is native.NativeOriginalUse.__init__.__code__:
                    credit = case.pin.book.bytes
                    with pytest.raises(books.NativeSQLFailure, match="reentry"):
                        native.acquire_local_sql_use(case.pin, candidate)
                    assert case.pin.book.bytes == credit
                    observed.append(case.pin.active_use)
                return lower(source, name)

            monkeypatch.setattr(cls, "__getattribute__", callback)
            value = native.acquire_local_sql_use(case.pin, candidate)
        assert observed == [value] and value._sql_construction.phase == "ready"
        assert value._sql_construction.error is None and db._native_pin_book.first_exception is None
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_constructor_opaque_allocator_return_is_retained_and_writer_refuses(monkeypatch, request):
    case = await local_case(monkeypatch, request, use=False, pin=False)
    foreign, calls = object(), []

    class Opaque:
        def __new__(cls, *args):
            calls.append(True)
            return foreign

        def __init__(self, *args):
            raise AssertionError("Opaque initializer must never run")

    try:
        db = case.fixture._session_db
        monkeypatch.setattr(books, "NativeSQLBook", Opaque)
        with pytest.raises(books.NativeSQLFailure, match="allocator_opaque") as caught:
            books.book_for(db)
        attempt = db._native_pin_book
        assert attempt.value is None and attempt.foreign_value is foreign and attempt.error is caught.value
        assert db._native_pin_init_exception is caught.value and db._native_pin_init_credit == 4096
        with pytest.raises(books.NativeSQLFailure):
            writer.retained_writer_sources(case.fixture, [])
        with pytest.raises(books.NativeSQLFailure, match="consumed"):
            books.book_for(db)
        assert calls == [True] and attempt.foreign_value is foreign
    finally:
        await finish_case(case)
