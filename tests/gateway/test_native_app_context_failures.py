"""Sticky consumed requests, original reference replacement and partial-bind failure retention."""
import asyncio
from types import SimpleNamespace as Data

import pytest

import hermes_owned_gateway as owned
from gateway import native_app_context as native
from gateway import session_native_context as selected
from tests.gateway.native_app_context_fixture import Case, NONCE, REQUEST, line


@pytest.fixture
def case(monkeypatch, tmp_path):
    value = Case(monkeypatch, tmp_path)
    yield value
    value.close()


@pytest.mark.parametrize("change", ["generation", "job", "stdin_owner", "stdout_owner", "loop", "task",
                                    "actor", "transport", "session", "retired", "profile"])
def test_changed_originals_after_staging_cannot_bind(case, change):
    case.stage()
    stage = case.source._app_context_stages[NONCE]
    mutations = {
        "generation": lambda: case.row.update(runtime_generation=2),
        "job": lambda: setattr(case.source, "job", object()),
        "loop": lambda: setattr(case.source, "loop", object()),
        "task": lambda: setattr(case.source, "task", object()),
        "stdin_owner": lambda: setattr(case.source, "stdin_owner", Data(buffer=case.input)),
        "stdout_owner": lambda: setattr(case.source, "stdout_owner", Data(buffer=case.output)),
        "actor": lambda: setattr(case.connection, "actor", object()),
        "transport": lambda: setattr(case.connection, "transport", object()),
        "session": lambda: case.authority.sessions.update(s=Data()),
        "retired": lambda: setattr(case.connection._gdb_original_source_connection, "retired", True),
        "profile": lambda: setattr(case.authority, "profile_id", "foreign-profile"),
    }
    mutations[change]()
    with pytest.raises(owned.OwnedGatewayUnknown):
        case.read()
    assert stage.request is case.source._app_context_requests[REQUEST]
    assert stage.request.error is not None and case.source.phase == "unknown"
    assert not stage.request.acknowledged and len(case.events()) == 1
    assert stage.stdin is case.input and stage.stdout_owner is case.streams.stdout


@pytest.mark.parametrize("changes", [{}, {"requestId": "01J00000000000000000000003"}])
def test_request_and_nonce_replay_are_consumed_without_rebinding(case, changes):
    case.stage()
    first = case.frame()
    with pytest.raises(owned.OwnedGatewayUnknown):
        case.read(line(first) + line(case.frame(**changes)))
    original = case.source._app_context_requests[REQUEST]
    assert original.acknowledged and case.source._app_context_stages[NONCE].request is original
    assert len(case.submissions) == 1 and len(case.events()) == 2
    assert case.source.phase == "unknown" and not case.source._control_closed


def test_no_stage_cannot_be_replaced_by_control_metadata(case):
    with pytest.raises(owned.OwnedGatewayUnknown):
        case.read()
    request = case.source._app_context_requests[REQUEST]
    assert request.stage is None and request.error is not None
    assert not case.connection._gdb_original_app_contexts and not case.submissions


def test_failed_stage_construction_retains_original_debit_and_partial_record(case, monkeypatch):
    original_init = native.ContextStage.__init__
    first = RuntimeError("original-stage-construction-failed")

    def fail(self, *args):
        original_init(self, *args)
        raise first

    monkeypatch.setattr(native.ContextStage, "__init__", fail)
    with pytest.raises(RuntimeError) as caught:
        case.stage()
    value = case.source._app_context_stages[NONCE]
    assert caught.value is first and case.source._app_context_error is first
    assert value.source is case.source and value.original.connection is case.connection
    assert case.source._app_context_credit > 4096 and case.source.phase == "unknown"
    assert not case.connection._gdb_original_app_contexts


@pytest.mark.parametrize("failure", ["raise", "cancel", "change_after_await"])
def test_actual_bind_failure_cancellation_and_post_await_change_retain_originals(case, monkeypatch, failure):
    case.stage()
    original = selected.bind_context
    first = RuntimeError("original-partial-bind-failed")

    async def fail(*args):
        result = await original(*args)
        if failure == "raise":
            raise first
        if failure == "cancel":
            asyncio.current_task().cancel()
            await asyncio.Event().wait()
        gate = asyncio.get_running_loop().create_future()
        asyncio.get_running_loop().call_soon(gate.set_result, None)
        await gate
        case.row["runtime_generation"] += 1
        return result

    monkeypatch.setattr(selected, "bind_context", fail)
    with pytest.raises(owned.OwnedGatewayUnknown):
        case.read()
    request = case.source._app_context_requests[REQUEST]
    assert request.stage is case.source._app_context_stages[NONCE]
    assert request.task is case.submissions[0].task and request.task.get_coro() is request.coroutine
    assert request.association is case.connection._gdb_original_app_contexts["s"]
    assert case.live.event_stream._gdb_native_selection.connection is case.connection
    assert request.error is case.source._app_context_error and not request.acknowledged
    if failure == "raise":
        assert request.error is first
    if failure == "cancel":
        assert request.task.cancelled() and isinstance(request.error, asyncio.CancelledError)
    prior = case.source.error
    case.source.unknown("later-failure")
    assert case.source.error == prior and case.source.phase == "unknown" and len(case.events()) == 1


def test_submission_failure_keeps_consumed_nonce_request_and_unstarted_coroutine(case, monkeypatch):
    case.stage()
    first = RuntimeError("original-loop-submission-failed")

    def fail(*args):
        raise first

    monkeypatch.setattr(case, "submit", fail)
    # The lower submission factory was captured in the reader's module namespace.
    monkeypatch.setattr("hermes_owned_control.asyncio.run_coroutine_threadsafe", fail)
    with pytest.raises(owned.OwnedGatewayUnknown):
        case.read()
    request = case.source._app_context_requests[REQUEST]
    assert request.stage.request is request and request.error is first
    assert request.future is request.task is None and request.coroutine is not None
    assert case.source.phase == "unknown" and len(case.events()) == 1


def test_partial_original_stdout_never_turns_into_acknowledged_success(case):
    case.stage()
    case.output.short_next = True
    with pytest.raises(owned.OwnedGatewayUnknown, match="partial-original-control-output"):
        case.read()
    request = case.source._app_context_requests[REQUEST]
    assert not request.acknowledged and request.error is not None
    assert request.association.bound and case.source.phase == "unknown"
    assert request.stage.stdout_owner.buffer is case.output
    assert not case.source._complete_emitted and not case.source._control_closed


def test_stage_capacity_is_checked_before_allocating_another_original_record(case, monkeypatch):
    monkeypatch.setattr("gateway.native_app_context_records.MAX_CONTEXTS", 0)
    reply = case.stage()
    assert reply["error"]["data"]["reason"] == "native_original_app_context_capacity_or_reentry"
    assert not case.source._app_context_stages and case.source._app_context_credit == 0
