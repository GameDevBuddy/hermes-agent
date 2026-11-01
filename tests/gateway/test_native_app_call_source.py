"""Real paired Python source consumers with modeled IO, never native execution."""
import hashlib
import json

import pytest

from gateway.session_server_requests import resolve
from hermes_state_runtime import RuntimeStoreError
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.gateway.native_app_call_fixture import CallCase


def test_per_call_source_authenticates_exact_wire_and_keeps_execution_held(monkeypatch, tmp_path):
    case = CallCase(monkeypatch, tmp_path)
    try:
        case.exchange()
        call = case.call
        assert call.sent and call.authenticated and call.acknowledged and call.error is None
        assert case.sent == [call.wire]
        assert call.digest == hashlib.sha256(case.sent[0].encode()).hexdigest()
        assert json.loads(call.wire)["params"]["arguments"] == {"unicode": "é🦉"}
        assert case.request.params["arguments"] is case.original.effective_arguments
        assert case.request.native_phase == "execution_held" and case.request.event.is_set()
        assert case.slot._native_pin is None and case.slot.origin is None and case.slot.projection is None
        assert case.events()[-1]["event"] == "native_call_bound"
        assert case.events()[-1]["executionHeld"] is True
        assert not resolve({"id": case.slot._native_request_candidate_id, "result": {"ok": True}})
    finally:
        case.close()


@pytest.mark.parametrize("change", [
    lambda c: setattr(c.original.ref, "args", dict(c.original.effective_arguments)),
    lambda c: c.request.params.__setitem__("arguments", dict(c.original.effective_arguments)),
    lambda c: c.original.effective_arguments.__setitem__("injected", True),
    lambda c: c.request.params.__setitem__("call_uid", "replacement"),
])
def test_per_call_source_refuses_argument_or_request_substitution(monkeypatch, tmp_path, change):
    case = CallCase(monkeypatch, tmp_path)
    try:
        with pytest.raises(OwnedGatewayUnknown) as observed:
            case.exchange(modify=change)
        assert isinstance(observed.value.__cause__, RuntimeStoreError)
        assert not case.call.acknowledged and case.call.error is not None
        assert case.request.native_phase == "blocked"
        assert not any(event["event"] == "native_call_bound" for event in case.events())
        assert case.slot.origin is None and case.slot._native_pin is None
    finally:
        case.close()


@pytest.mark.parametrize("changes", [
    {"wireSha256": "c" * 64}, {"nonce": "c" * 64}, {"contextId": "other"},
    {"serverRequestId": "srq-" + "f" * 32}, {"executionHeld": False},
])
def test_per_call_source_refuses_mismatched_private_frame(monkeypatch, tmp_path, changes):
    case = CallCase(monkeypatch, tmp_path)
    try:
        with pytest.raises(RuntimeError):
            case.exchange(frame_changes=changes)
        assert not case.call.acknowledged
        assert not any(event["event"] == "native_call_bound" for event in case.events())
        assert case.slot.origin is None and case.slot._native_pin is None
    finally:
        case.close()


def test_per_call_source_preserves_first_socket_exception(monkeypatch, tmp_path):
    case = CallCase(monkeypatch, tmp_path)
    original = OSError("original in-memory socket failure")
    case.send_error = original
    try:
        with pytest.raises(OwnedGatewayUnknown) as observed:
            case.exchange()
        assert observed.value.__cause__ is original and case.call.error is original
        traceback = case.call.traceback
        case.call.failed(ValueError("later failure"))
        assert case.call.error is original and case.call.traceback is traceback
        assert not case.call.sent and not case.call.acknowledged
        assert case.slot._source_constructions["app_call"] is case.call
    finally:
        case.close()


def test_per_call_source_original_worker_register_stops_at_execution_hold(monkeypatch, tmp_path):
    import asyncio
    from tests.gateway.native_app_context_fixture import Submission
    from gateway.session_server_requests import register_native_request

    case = CallCase(monkeypatch, tmp_path)
    case.use_register = True

    class SourceSubmission(Submission):
        def result(self, timeout=None):
            value = super().result(timeout)
            case.call = case.slot._source_constructions["app_call"]
            case.request = case.slot._native_request_object
            case.source._control_reader.bind_call(case.call_frame())
            return value

    def worker():
        case.offered = asyncio.Event()
        case.offer_task = case.loop.create_task(case.offer())
        case.run(case.offered.wait())
        monkeypatch.setattr("gateway.native_app_call.asyncio.run_coroutine_threadsafe", SourceSubmission)
        with pytest.raises(RuntimeStoreError, match="native_execution_held"):
            register_native_request(case.slot)
        assert case.call.authenticated and case.call.acknowledged and case.call.error is None
        assert case.request.native_phase == "execution_held"
        assert case.slot._native_pin is None and case.slot._native_active_use is None
        assert case.slot.origin is None and case.slot.projection is None

    try:
        assert case.stage().get("error") is None
        case.read(before_shutdown=worker)
    finally:
        case.close()


def test_per_call_source_refuses_replayed_private_bind_without_second_ack(monkeypatch, tmp_path):
    case = CallCase(monkeypatch, tmp_path)
    try:
        with pytest.raises(OwnedGatewayUnknown, match="reentry") as observed:
            case.exchange(repeat=True)
        assert isinstance(observed.value.__cause__, RuntimeStoreError)
        assert case.call.frame["requestId"] == "01J00000000000000000000003"
        assert len([e for e in case.events() if e["event"] == "native_call_bound"]) == 1
        assert case.slot.origin is None
    finally:
        case.close()


def test_per_call_source_cannot_adopt_a_reader_shaped_object(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from gateway.native_app_call import claim

    case = CallCase(monkeypatch, tmp_path)

    def foreign_reader(value):
        before = value.source._app_context_credit
        fake = SimpleNamespace(source=value.source, active=None, require_reading=lambda: None)
        with pytest.raises(RuntimeStoreError, match="reader_missing"):
            claim(fake, value.call_frame())
        assert value.call.frame is None and value.source._control_reader.active is None
        assert value.source._app_context_credit == before

    try:
        case.exchange(modify=foreign_reader)
        assert case.call.acknowledged and case.call.reader is case.source._control_reader
        assert case.slot.origin is None and case.slot._native_pin is None
    finally:
        case.close()
