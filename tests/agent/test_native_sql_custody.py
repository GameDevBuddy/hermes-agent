"""Cooperating writer source evidence only: no App/Origin/native/kernel acceptance."""
from __future__ import annotations

import asyncio
import sqlite3

import pytest

from agent import native_original_use as native
from agent import session_persistence as persistence
from gateway.native_source_issuance import require_original_app_call
from gateway.native_source_stop import require_native_pin_for_invoke, retire_native_pin
from hermes_state_native_book import NativeSQLFailure
from hermes_state_runtime import RuntimeStoreError, begin_runtime_epoch
from tests.agent.native_sql_fixture import finish_case, local_case, prepare_result, scalar


@pytest.mark.asyncio
async def test_original_sql_pin_use_and_genuine_app_boundary(monkeypatch, request):
    case = await local_case(monkeypatch, request)
    try:
        db = case.fixture._session_db
        assert case.pin.book.connection is db._conn
        assert case.pin.source_original_call is case.source.original_source_call
        assert case.use.pin is case.pin and case.request.native_active_use is case.use
        assert case.pin.capture[0]["assistant"]["message_uid"] == case.pending.assistant["message_uid"]
        assert case.pin.capture[0]["assistant"]["tool_call_uid"] == case.pending.uid
        assert scalar(case, "SELECT COUNT(*) FROM native_original_pins") == 1
        assert scalar(case, "SELECT COUNT(*) FROM native_pin_active_uses") == 1
        for operation in (lambda: require_original_app_call(case.source.original_source_call, case.request),
                          lambda: require_native_pin_for_invoke(case.pin), lambda: retire_native_pin(case.pin)):
            with pytest.raises(RuntimeStoreError):
                operation()
        with pytest.raises(NativeSQLFailure, match="retirement_held"):
            db.close()
        assert db._conn is case.pin.book.connection and case.pin.book.error is None
        with pytest.raises(sqlite3.IntegrityError, match="native_sql_retained"):
            begin_runtime_epoch(db, instance_id="same-or-new-does-not-retire")
    finally:
        await finish_case(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("sql", [
    "UPDATE messages SET content='changed' WHERE role='assistant'",
    "DELETE FROM messages WHERE role='assistant'",
    "INSERT OR REPLACE INTO messages SELECT * FROM messages WHERE role='assistant'",
    "UPDATE sessions SET runtime_revision=runtime_revision+1",
    "UPDATE session_admissions SET status='terminal'",
    "UPDATE session_admissions SET payload_digest='different'",
    "DELETE FROM session_turn_leases",
    "INSERT OR REPLACE INTO session_turn_leases SELECT * FROM session_turn_leases",
    "INSERT INTO compression_locks VALUES ('canonical-source-fixture','other',1,99999999999)",
    "DELETE FROM native_original_pins",
    "INSERT OR REPLACE INTO native_pin_active_uses SELECT * FROM native_pin_active_uses",
])
async def test_other_connection_dml_cannot_rewrite_held_local_relation(monkeypatch, request, sql):
    case = await local_case(monkeypatch, request)
    try:
        # An independent real SQLite writer exercises the installed DML triggers.
        with sqlite3.connect(case.fixture._session_db.db_path) as other:
            with pytest.raises(sqlite3.IntegrityError, match="native_sql_retained"):
                other.execute(sql)
        assert case.pin.book.error is None
        native.check_local_sql_after_io(case.use, "raw", "projection")
        assert case.use.io_checked
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_actual_writer_returns_then_markers_and_current_owner_publish(monkeypatch, request):
    case = await local_case(monkeypatch, request)
    try:
        message = prepare_result(case)
        original = case.fixture._session_db.append_messages_batch
        observed = []

        def inspect_original_return(*args, **kwargs):
            result = original(*args, **kwargs)
            observed.append((case.use.observation, case.pending.receipt, message.get("_db_persisted"),
                             case.use.writer_returned, case.use.provisional_link is not None))
            return result

        monkeypatch.setattr(case.fixture._session_db, "append_messages_batch", inspect_original_return)
        assert case.fixture.flush() is True
        assert observed == [(None, None, None, False, True)]
        assert case.use.observation is case.pending.receipt and case.use.phase == "observed"
        receipt = case.pending.receipt
        assert receipt["source_result_utf8"] != receipt["canonical_projection_utf8"]
        assert receipt["tool_call_uid"] == case.pending.uid and receipt["assistant_message_uid"] == case.message["message_uid"]
        assert receipt["native_ack_allowed"] is False and receipt["replay_allowed"] is False
        assert scalar(case, "SELECT row_id FROM native_pin_row_links") == message["_row_id"]
        assert case.fixture.canonical_events() == [receipt]
    finally:
        await finish_case(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["writer-return", "marker-sync", "owner-before", "owner-after", "row-sql", "cancel"])
async def test_failed_original_writer_never_publishes_or_replays(monkeypatch, request, fault):
    case = await local_case(monkeypatch, request)
    try:
        message = prepare_result(case)
        db, entered = case.fixture._session_db, []
        original = db.append_messages_batch

        def wrapper(*args, **kwargs):
            entered.append(True)
            result = original(*args, **kwargs)
            if fault == "writer-return":
                raise RuntimeError("original-return-interrupted")
            if fault == "owner-after":
                case.fixture._canonical_tool_commit_owner = object()
            return result

        def fail_sync(*args):
            raise RuntimeError("marker-sync-interrupted")

        def deny_link(action, table, *unused):
            if action == sqlite3.SQLITE_INSERT and table == "native_pin_row_links":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        monkeypatch.setattr(db, "append_messages_batch", wrapper)
        if fault == "owner-before":
            case.fixture._canonical_tool_commit_owner = object()
        if fault == "marker-sync":
            monkeypatch.setattr(persistence, "sync_flushed_message_markers", fail_sync)
        if fault == "row-sql":
            db._conn.set_authorizer(deny_link)
        if fault == "cancel":
            def cancel_sync(*args):
                raise asyncio.CancelledError("source-only-cancel")
            monkeypatch.setattr(persistence, "sync_flushed_message_markers", cancel_sync)
            with pytest.raises(asyncio.CancelledError):
                case.fixture.flush()
        else:
            assert case.fixture.flush() is False
        first, credit = case.pin.book.error, case.pin.book.bytes
        assert first is not None and case.pin.phase == "unknown"
        assert case.use.observation is None and case.pending.receipt is None
        assert case.fixture.canonical_events() == [] and not message.get("_db_persisted")
        count = len(entered)
        assert case.fixture.flush() is False
        assert len(entered) == count
        assert case.pin.book.error is first and case.pin.book.bytes == credit
        assert db._conn is case.pin.book.connection and not db._conn.in_transaction
        expected_rows = 0 if fault in ("owner-before", "row-sql") else 1
        assert scalar(case, "SELECT COUNT(*) FROM native_pin_row_links") == expected_rows
        assert scalar(case, "SELECT COUNT(*) FROM messages WHERE role='tool'") == expected_rows
    finally:
        await finish_case(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["pin", "use"])
async def test_first_failed_constructor_keeps_hole_credit_and_diagnostic(monkeypatch, request, target):
    case = await local_case(monkeypatch, request, use=False, pin=target == "use")
    try:
        calls = []

        class FailedConstruction:
            def __new__(cls, *args, **kwargs):
                calls.append(True)
                raise RuntimeError("inert-construction-failure")

        if target == "pin":
            monkeypatch.setattr(native, "NativeRetirementPin", FailedConstruction)
            action = lambda: native.reserve_local_sql_pin(case.slot)
        else:
            from gateway.session_server_requests import retain_native_candidate
            candidate = retain_native_candidate(case.slot)
            monkeypatch.setattr(native, "NativeOriginalUse", FailedConstruction)
            action = lambda: native.acquire_local_sql_use(case.pin, candidate)
        with pytest.raises(RuntimeError, match="inert-construction-failure"):
            action()
        book = case.fixture._session_db._native_pin_book
        first, credit = book.error, book.bytes
        assert first is not None and calls == [True]
        with pytest.raises(RuntimeStoreError):
            action()
        assert calls == [True] and book.bytes == credit and book.error is first
        assert book.pins[id(case.slot)][0] is case.slot
        if target == "pin":
            assert book.pins[id(case.slot)][1] is None
        else:
            assert book.uses[case.pin.pin_id] is None and case.request is None
        with pytest.raises(NativeSQLFailure, match="retirement_held"):
            case.fixture._session_db.close()
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_schema_change_after_io_stays_unknown_and_retained(monkeypatch, request):
    case = await local_case(monkeypatch, request)
    try:
        case.fixture.write_sql(("DROP TRIGGER native_sql_message_update", ()))
        with pytest.raises(RuntimeStoreError):
            native.check_local_sql_after_io(case.use, "raw", "projection")
        first = case.pin.book.error
        assert first is not None and case.use.io_checked is False and case.use.observation is None
        with pytest.raises(RuntimeStoreError):
            native.check_local_sql_after_io(case.use, "raw", "projection")
        assert case.pin.book.error is first
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_owner_rechecked_after_real_marker_sync(monkeypatch, request):
    case = await local_case(monkeypatch, request)
    try:
        message = prepare_result(case)
        original = persistence.sync_flushed_message_markers

        def sync_then_replace(*args):
            original(*args)
            case.fixture._canonical_tool_commit_owner = object()

        monkeypatch.setattr(persistence, "sync_flushed_message_markers", sync_then_replace)
        assert case.fixture.flush() is False
        assert message["_db_persisted"] is True
        assert scalar(case, "SELECT COUNT(*) FROM native_pin_row_links") == 1
        assert case.use.observation is None and case.pending.receipt is None
        assert case.fixture.canonical_events() == [] and case.pin.book.error is not None
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_equal_uid_copy_cannot_bypass_original_writer(monkeypatch, request):
    case = await local_case(monkeypatch, request)
    try:
        original = prepare_result(case)
        case.fixture.messages[-1] = dict(original)
        assert case.fixture.flush() is False
        assert scalar(case, "SELECT COUNT(*) FROM messages WHERE role='tool'") == 0
        assert case.pending.message is original and case.use.observation is None
        assert case.pin.book.error is not None
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_expiry_after_io_cannot_reacquire_or_publish(monkeypatch, request):
    from types import SimpleNamespace
    import hermes_state_native_capture as capture
    case = await local_case(monkeypatch, request)
    try:
        expiry = scalar(case, "SELECT expires_at FROM session_turn_leases")
        # Negative clock input only; no positive current/issuer callback or real sleep.
        monkeypatch.setattr(capture, "time", SimpleNamespace(time=lambda: expiry + 1))
        with pytest.raises(RuntimeStoreError):
            native.check_local_sql_after_io(case.use, "raw", "projected")
        assert case.use.io_checked is False and case.use.observation is None
        assert case.pin.book.error is not None and scalar(case, "SELECT COUNT(*) FROM native_pin_active_uses") == 1
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_consumed_book_constructor_cannot_retry_reinitialize_or_close(monkeypatch, request):
    import hermes_state_native_book as books
    from hermes_state import SessionDB
    case = await local_case(monkeypatch, request, use=False, pin=False)
    db, calls = case.fixture._session_db, []
    try:
        class FailedBook:
            def __new__(cls, *args):
                calls.append(True)
                raise RuntimeError("book-construction-failure")

        monkeypatch.setattr(books, "NativeSQLBook", FailedBook)
        with pytest.raises(RuntimeError, match="book-construction-failure"):
            native.reserve_local_sql_pin(case.slot)
        first, connection = db._native_pin_init_error, db._conn
        with pytest.raises(RuntimeStoreError, match="consumed"):
            native.reserve_local_sql_pin(case.slot)
        with pytest.raises(RuntimeError, match="reinitialization_refused"):
            SessionDB.__init__(db, db.db_path)
        with pytest.raises(RuntimeStoreError, match="retirement_held"):
            db.close()
        assert calls == [True] and db._native_pin_init_error is first and db._conn is connection
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_retained_budget_refuses_use_before_construction_or_sql(monkeypatch, request):
    from gateway.session_server_requests import retain_native_candidate
    from hermes_state_native_schema import MAX_METADATA
    case = await local_case(monkeypatch, request, use=False)
    try:
        candidate = retain_native_candidate(case.slot)
        book = case.pin.book
        book.charge(MAX_METADATA - book.bytes - native.NATIVE_USE_CREDIT + 1)
        credit = book.bytes
        with pytest.raises(RuntimeStoreError):
            native.acquire_local_sql_use(case.pin, candidate)
        assert book.bytes == credit and case.pin.active_use is None
        assert case.keeper.pins[id(case.slot)] is case.pin
        assert scalar(case, "SELECT COUNT(*) FROM native_pin_active_uses") == 0
        assert book.error is not None
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_unprepared_native_result_cannot_fall_through_ordinary_writer(monkeypatch, request):
    case = await local_case(monkeypatch, request)
    try:
        case.fixture.messages.append({"role": "tool", "name": case.pending.tool_name,
                                      "tool_call_id": case.pending.call_id, "content": "unprepared result"})
        assert case.fixture.flush() is False
        assert scalar(case, "SELECT COUNT(*) FROM messages WHERE role='tool'") == 0
        assert case.use.observation is None and case.pending.receipt is None
        assert case.pin.book.error is not None
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_post_io_capacity_failure_keeps_first_bounded_result_refs(monkeypatch, request):
    from hermes_state_native_schema import MAX_METADATA
    case = await local_case(monkeypatch, request)
    try:
        raw, projection = "first actual source", "first actual projection"
        book = case.pin.book
        book.charge(MAX_METADATA - book.bytes - 1)
        credit = book.bytes
        with pytest.raises(RuntimeStoreError):
            native.check_local_sql_after_io(case.use, raw, projection)
        first = book.error
        assert case.use.raw_result is raw and case.use.projection is projection
        with pytest.raises(RuntimeStoreError):
            native.check_local_sql_after_io(case.use, "replacement", "replacement")
        assert book.error is first and book.bytes == credit
        assert case.use.raw_result is raw and case.use.projection is projection and not case.use.io_checked
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_schema_tamper_between_commit_and_markers_cannot_publish_stale_witness(monkeypatch, request):
    case = await local_case(monkeypatch, request)
    try:
        prepare_result(case)
        db, original = case.fixture._session_db, case.fixture._session_db.append_messages_batch

        def write_then_tamper(*args, **kwargs):
            result = original(*args, **kwargs)
            case.fixture.write_sql(("DROP TRIGGER native_sql_message_update", ()),
                                   ("UPDATE messages SET content='changed after writer return' WHERE role='tool'", ()))
            return result

        monkeypatch.setattr(db, "append_messages_batch", write_then_tamper)
        assert case.fixture.flush() is False
        assert case.use.observation is None and case.pending.receipt is None
        assert case.fixture.canonical_events() == []
        assert case.pin.book.error[1] == "native_sql_schema_changed"
        assert scalar(case, "SELECT COUNT(*) FROM native_pin_row_links") == 1
    finally:
        await finish_case(case)


@pytest.mark.asyncio
async def test_original_publisher_failure_keeps_its_first_diagnostic(monkeypatch, request):
    case = await local_case(monkeypatch, request)
    try:
        message = prepare_result(case)
        calls = []

        def fail_original_callback(pending, receipt):
            calls.append((pending is case.pending, message["_db_persisted"], receipt["native_ack_allowed"]))
            raise RuntimeError("original-publisher-observation-failure")

        monkeypatch.setattr(case.pending.owner, "publish", fail_original_callback)
        assert case.fixture.flush() is False
        assert calls == [(True, True, False)]
        assert case.pin.book.error[0] == "RuntimeError"
        assert case.use.observation is None and not case.pending.publication_observed
        assert case.fixture.canonical_events() == []
        assert scalar(case, "SELECT COUNT(*) FROM native_pin_row_links") == 1
        first = case.pin.book.error
        with pytest.raises(RuntimeStoreError):
            native.check_local_sql_after_io(case.use, "replacement", "replacement")
        assert case.pin.book.error is first
    finally:
        await finish_case(case)
