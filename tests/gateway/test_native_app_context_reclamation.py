"""Closed socket metadata reclamation through the original stage/bind consumers."""
import asyncio

import pytest

from gateway.native_app_context import require_context
from gateway.native_app_context_records import RECORD_CREDIT
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_app_context_fixture import Case, NONCE


@pytest.fixture
def case(monkeypatch, tmp_path):
    value = Case(monkeypatch, tmp_path)
    yield value
    value.close()


def close_socket(case):
    # Lower WebSocket IO has closed; run the real canonical connection teardown.
    case.transport._closed = True
    case.run(case.connection.close())


def reconnect(case):
    async def open_socket():
        case.connected = asyncio.Event()
        case.ws_task = asyncio.create_task(case.connect())
        await case.connected.wait()
    case.run(open_socket())


def test_same_source_serves_more_than_64_closed_settled_contexts(case):
    assert "result" in case.stage()

    def lifecycle():
        first = case.source._app_context_stages[NONCE]
        for index in range(1, 71):
            old_connection = case.connection
            close_socket(case)
            # Each context belongs to a fresh live session, not an attempted
            # rebind of the prior event stream's immutable native selection.
            from gateway.session_authority import LiveSession
            case.live = LiveSession(None, f"context-route-{index}")
            case.authority.sessions["s"] = case.live
            reconnect(case)
            nonce = f"{index:064x}"
            assert "result" in case.stage(nonce=nonce)
            assert not old_connection._gdb_original_app_contexts
            assert len(case.source._app_context_stages) == 1
            assert len(case.source._app_context_slots) == 1
            assert not case.source._app_context_requests
            assert len(case.source._app_context_constructions) == 1
            stage = case.source._app_context_stages[nonce]
            assert case.source._app_context_credit == RECORD_CREDIT + len(stage.selection.tools_json)
            case.source._control_reader.bind(case.frame(nonce=nonce, requestId=f"{index:026d}"))
            assert len(case.source._app_context_requests) == 1
            assert len(case.source._app_context_constructions) == 3
            assert case.source._app_context_credit == 2 * RECORD_CREDIT + len(stage.selection.tools_json)
            async def check():
                assert require_context(case.connection, case.ref).stage is stage
            case.run(check())
        with pytest.raises(RuntimeStoreError):
            first._context_construction.require_retained()
        credit = case.source._app_context_credit
        assert "error" in case.stage(nonce="f" * 64)  # live slot remains one-shot
        assert case.source._app_context_credit == credit

    case.read(before_shutdown=lifecycle)
    assert len([event for event in case.events() if event.get("event") == "native_context_bound"]) == 71


@pytest.mark.parametrize("state", ["open", "retired_only", "active_reader", "pending_future", "failed", "unknown",
                                   "unacknowledged", "construction_failed", "call_hole", "assistant_hole",
                                   "bridge_hole", "capture_changed", "cancelled_future", "failed_future"])
def test_unsettled_or_open_original_is_preserved(case, state):
    case.stage()

    def preserve():
        source = case.source
        stage = source._app_context_stages[NONCE]
        request = stage.request
        connection = case.connection
        if state == "retired_only":
            case.run(connection.close())
        elif state != "open":
            close_socket(case)
        if state == "active_reader":
            source._control_reader.active = request
        elif state == "pending_future":
            from concurrent.futures import Future
            request.future = Future()
        elif state == "failed":
            request.error = RuntimeStoreError("retained-failure")
        elif state == "unknown":
            source.unknown("retained-unknown")
        elif state == "unacknowledged":
            request.acknowledged = False
        elif state == "construction_failed":
            stage._context_construction.fail(RuntimeStoreError("retained-constructor-failure"))
        elif state == "call_hole":
            stage.original.parent.calls["unfinished"] = None
        elif state == "assistant_hole":
            case.runtime.assistants["unfinished"] = None
        elif state == "bridge_hole":
            case.runtime.bridges["unfinished"] = None
        elif state == "capture_changed":
            connection._gdb_original_source_connection = None
        elif state in ("cancelled_future", "failed_future"):
            from concurrent.futures import Future
            request.future = Future()
            if state == "cancelled_future":
                request.future.cancel()
            else:
                request.future.set_exception(RuntimeStoreError("retained-future-failure"))
        before = (dict(source._app_context_stages), dict(source._app_context_requests),
                  dict(source._app_context_constructions), dict(source._app_context_slots), source._app_context_credit)
        reconnect(case)
        case.stage(nonce="c" * 64)
        assert source._app_context_stages.get(NONCE) is stage
        assert source._app_context_requests.get(request.frame["requestId"]) is request
        assert connection._gdb_original_app_contexts["s"] is request.association
        for key, attempt in before[2].items():
            assert source._app_context_constructions[key] is attempt
        assert source._app_context_slots[(id(connection), "s")] == NONCE
        assert source._app_context_credit >= before[4]
        source._control_reader.active = None

    case.read(before_shutdown=preserve)
    if state == "unknown":
        assert case.source.phase == "unknown"


def test_closed_stage_without_completed_bind_stays_charged(case):
    case.stage()
    stage = case.source._app_context_stages[NONCE]
    credit = case.source._app_context_credit
    close_socket(case)
    reconnect(case)
    assert "result" in case.stage(nonce="c" * 64)
    assert case.source._app_context_stages[NONCE] is stage
    assert stage.request is None
    assert case.source._app_context_credit == credit + RECORD_CREDIT + len(stage.selection.tools_json)


def test_reclamation_preserves_original_event_stream_selection(case):
    case.stage()

    def reclaim():
        original_selection = case.live.event_stream._gdb_native_selection
        close_socket(case)
        reconnect(case)
        assert "result" in case.stage(nonce="c" * 64)
        assert NONCE not in case.source._app_context_stages
        assert case.live.event_stream._gdb_native_selection is original_selection
        assert not original_selection.current()

    case.read(before_shutdown=reclaim)


def test_old_bind_replay_refuses_after_closed_context_reclamation(case):
    case.stage()

    def replay():
        old = case.frame()
        close_socket(case)
        reconnect(case)
        assert "result" in case.stage(nonce="c" * 64)
        assert NONCE not in case.source._app_context_stages
        with pytest.raises(RuntimeStoreError, match="stage_missing_or_consumed"):
            case.source._control_reader.bind(old)
        assert not case.connection._gdb_original_app_contexts

    case.read(before_shutdown=replay)
    assert case.source.phase == "unknown"
