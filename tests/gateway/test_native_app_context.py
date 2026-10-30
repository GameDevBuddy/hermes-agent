"""The actual canonical stage + original stdin bind consumers, using only lower IO models."""
from types import SimpleNamespace as Data

import pytest

import hermes_owned_gateway as owned
from gateway.native_source_issuance import require_original_app_context
from gateway.native_source_stop import require_native_pin_for_invoke, retire_native_pin
from gateway.session_contract import SessionRef
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_app_context_fixture import Case, NONCE, REQUEST, line
from tui_gateway.contracts.registry import CANONICAL_METHODS, METHODS


@pytest.fixture
def case(monkeypatch, tmp_path):
    value = Case(monkeypatch, tmp_path)
    yield value
    value.close()


async def context(case, ref=None):
    return require_original_app_context(case.connection, ref or case.ref)


def test_stage_alone_does_not_bind_or_open_native_gates(case):
    response = case.stage()
    assert response == {"jsonrpc": "2.0", "id": 1, "result": {
        "staged": True, "context_id": "context-fixture", "nonce": NONCE}}
    assert "native.context.stage" in CANONICAL_METHODS and "native.context.stage" not in METHODS
    assert not case.connection._gdb_original_app_contexts and not case.source._app_context_requests
    assert not hasattr(case.live.event_stream, "_gdb_native_selection")
    with pytest.raises(RuntimeStoreError, match="issuer_missing"):
        case.run(context(case))
    for operation in (lambda: owned.require_lifecycle_available(case.source),
                      lambda: require_native_pin_for_invoke(object()), lambda: retire_native_pin(object())):
        with pytest.raises((owned.OwnedGatewayUnknown, RuntimeStoreError)):
            operation()


def test_original_reader_binds_same_context_and_emits_only_matching_ack(case):
    case.stage()
    staged = case.source._app_context_stages[NONCE]
    observed = []

    def before_shutdown():
        value = case.run(context(case))
        request = case.source._app_context_requests[REQUEST]
        assert value.request is request and value.stage is staged and value.bound
        assert value.selection is case.live.event_stream._gdb_native_selection
        assert value.selection.same_selection(staged.selection)
        assert staged.original is case.connection._gdb_original_source_connection
        assert staged.original.connection is case.connection and staged.task is case.bootstrap_task
        assert request.reader is case.source._control_reader
        assert request.coroutine is request.task.get_coro() and request.task.get_loop() is case.loop
        assert request.future is case.submissions[0] and request.future.done()
        assert staged.job is case.source.job and staged.stdin_owner is case.streams.stdin
        assert staged.stdout_owner is case.streams.stdout and staged.transport is case.transport
        assert request.result["native_work_admitted"] is False and not request.result["replay_allowed"]
        with pytest.raises(RuntimeStoreError):
            case.run(context(case, SessionRef(case.ref.profile_id, "another-session")))
        observed.append(value)

    case.read(before_shutdown=before_shutdown)
    assert len(observed) == 1
    assert case.events()[1:] == [{"protocol": owned.PROTOCOL, "event": "native_context_bound",
        "attemptId": case.contract.attempt_id, "requestId": REQUEST, "nonce": NONCE,
        "contextId": "context-fixture"}]
    assert case.source._shutdown and case.source._control_closed and not case.source._complete_emitted
    with pytest.raises((RuntimeStoreError, owned.OwnedGatewayUnknown)):
        case.run(context(case))
    with pytest.raises(owned.OwnedGatewayUnknown):
        case.source.read_shutdown()


@pytest.mark.parametrize("changes", [{"unexpected": True}, {"nonce": None}, {"nonce": "A" * 64},
    {"nonce": "a" * 63}, {"tools": []}, {"tools": [{"name": "execute", "tool_ref": "x"}]},
    {"context_id": "bad\ncontext"}, {"snapshot_sha256": True}, {"session_id": 1}])
def test_stage_strict_contract_and_values_refuse_before_consuming_context(case, changes):
    reply = case.stage(**changes)
    assert reply["error"]["code"] == 4001 and reply["error"]["data"]["reason"] == "invalid_params"
    assert not case.source._app_context_stages and not case.connection._gdb_original_app_contexts


def test_native_metadata_does_not_issue_verified_operator(monkeypatch, tmp_path):
    value = Case(monkeypatch, tmp_path, operator=False)
    try:
        assert value.connection.native_owner
        assert value.stage()["error"]["data"]["reason"] == "permission_denied"
        assert not value.source._app_context_stages
    finally:
        value.close()


def test_stage_attempt_is_sticky_per_original_connection_session(case):
    case.stage()
    original = case.source._app_context_stages[NONCE]
    credit = case.source._app_context_credit
    reply = case.stage(nonce="c" * 64)
    assert reply["error"]["data"]["reason"] == "native_original_app_context_capacity_or_reentry"
    assert case.source._app_context_stages == {NONCE: original}
    assert case.source._app_context_credit == credit


@pytest.mark.parametrize("change", ["generation", "actor", "reader", "live", "epoch", "closed"])
def test_bound_context_refuses_when_an_original_or_generation_changes(case, change):
    case.stage()
    mutations = {
        "generation": lambda: case.row.update(runtime_generation=2),
        "actor": lambda: setattr(case.connection, "actor", Data(profile_id=case.ref.profile_id,
                                                               capabilities={"session:operator"})),
        "reader": lambda: setattr(case.source, "_control_reader", object()),
        "live": lambda: case.authority.sessions.update(s=Data()),
        "epoch": lambda: setattr(case.authority, "epoch", 2),
        "closed": lambda: setattr(case.transport, "_closed", True),
    }

    def before_shutdown():
        case.run(context(case))
        mutations[change]()
        with pytest.raises((RuntimeStoreError, owned.OwnedGatewayUnknown)):
            case.run(context(case))

    case.read(before_shutdown=before_shutdown)
    assert len(case.events()) == 2  # only the earlier successful context acknowledgement


@pytest.mark.parametrize("raw", [b'{"protocol":"gdb-owned-gateway-v2","command":"bind_native_context",'
    b'"command":"shutdown","attemptId":"context-source-attempt"}\n',
    b'{"protocol":"gdb-owned-gateway-v2","command":"shutdown","attemptId":true}\n',
    b'not-json\n', b'{}\n', b'"scalar"\n', b'x' * owned.MAX_FRAME + b'\n'])
def test_invalid_decoded_control_frames_never_emit_context_ack(case, raw):
    case.stage()
    with pytest.raises(owned.OwnedGatewayUnknown):
        case.read(raw)
    assert case.source.phase == "unknown" and len(case.events()) == 1
    assert not case.connection._gdb_original_app_contexts


@pytest.mark.parametrize("changes", [{"requestId": "bad"}, {"requestId": "Z" * 26}, {"nonce": "D" * 64},
    {"nonce": None}, {"contextId": "context-fixture\n"}, {"extra": 1},
    {"attemptId": "another-attempt"}, {"command": "unknown"}])
def test_bind_control_schema_and_attempt_are_strict(case, changes):
    case.stage()
    with pytest.raises(owned.OwnedGatewayUnknown):
        case.read(line(case.frame(**changes)))
    assert case.source.phase == "unknown" and len(case.events()) == 1


def test_shutdown_without_context_keeps_original_framing(case):
    case.read(b"")
    assert case.source._control_closed and case.source._shutdown and len(case.events()) == 1
    assert not case.source._app_context_requests
    assert case.stage()["error"]["data"]["reason"] == "native_original_app_context_changed"
