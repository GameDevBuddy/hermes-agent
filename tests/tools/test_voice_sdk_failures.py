"""Actual voice consumers retain originals/first failure and refuse replacement/foreign users."""
import asyncio
from types import SimpleNamespace as Data

import pytest

from agent import auxiliary_owned_close as closing
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.tools.voice_sdk_fixture import Case, HTTP, SDK


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["response_factory_error", "enter_error", "iteration_error", "response_close_error",
                                   "client_close_error", "constructor_error"])
async def test_stream_failure_retains_before_factory_record_and_first_outcome(monkeypatch, tmp_path, field):
    case = Case(monkeypatch, tmp_path, managed=True)
    first = RuntimeError("first-voice-failure")
    setattr(case, field, first)
    with pytest.raises(RuntimeError) as caught:
        list(case.stream())
    assert caught.value is first
    record = case.source.auxiliary_clients[0]
    assert record.error is first and case.source.auxiliary_error is first and record.phase == "unknown"
    assert record.voice_owner.first_error is first and case.source.phase == "unknown"
    if field == "constructor_error":
        assert record.voice_owner.partial_client is case.clients[0]
        assert record.voice_owner.inner is case.clients[0]._client
    else:
        assert record.client is case.clients[0] and record.voice_owner.client is record.client
    before = list(case.events)
    with pytest.raises(OwnedGatewayUnknown):
        await closing.close_record(record)
    assert case.events == before


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_transcription_body_failure_or_cancellation_closes_then_remains_unknown(monkeypatch, tmp_path, cancel):
    case = Case(monkeypatch, tmp_path, managed=True)
    first = asyncio.CancelledError("cancelled-body") if cancel else RuntimeError("body-failed")

    def body(client):
        assert case.record().voice_owner.body_active and client is case.clients[0]
        raise first

    if cancel:
        with pytest.raises(asyncio.CancelledError) as caught:
            case.transcribe(body)
        assert caught.value is first
    else:
        assert case.transcribe(body)["error"] == "Transcription failed: body-failed"
    record = case.record()
    assert record.error is first and record.voice_owner.first_outcome is first
    assert case.events.count("client.close") == 1 and case.clients[0].is_closed()
    assert record.phase == "unknown" and not case.source.auxiliary_closed


@pytest.mark.asyncio
async def test_active_stream_cannot_be_settled_or_have_client_closed_by_global_sdk_consumer(monkeypatch, tmp_path):
    case = Case(monkeypatch, tmp_path, managed=True)
    stream = case.stream()
    next(stream)
    record = case.record()
    with pytest.raises(OwnedGatewayUnknown, match="still-active"):
        await closing.close_record(record)
    assert "client.close" not in case.events and record.phase == "unknown"
    with pytest.raises(OwnedGatewayUnknown):
        stream.close()
    assert record.voice_owner.response.context is case.context
    assert not case.clients[0].is_closed() and case.source.phase == "unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["stream", "transcribe"])
async def test_client_close_failure_is_consumed_once_and_never_retried(monkeypatch, tmp_path, mode):
    case = Case(monkeypatch, tmp_path, managed=True)
    first = case.client_close_error = RuntimeError("client-close-failed")
    if mode == "stream":
        with pytest.raises(RuntimeError) as caught:
            list(case.stream())
        assert caught.value is first
    else:
        assert case.transcribe()["error"] == "Transcription failed: client-close-failed"
    record = case.record()
    assert record.close_attempted and record.error is first and case.events.count("client.close") == 1
    with pytest.raises(OwnedGatewayUnknown):
        await closing.close_record(record)
    assert case.events.count("client.close") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["inner", "job", "close_method"])
async def test_changed_original_during_normal_client_close_cannot_publish_settlement(monkeypatch, tmp_path, change):
    case = Case(monkeypatch, tmp_path, managed=True)
    original_inner = []

    def changed():
        client = case.clients[0]
        original_inner.append(client._client)
        actions = {"inner": lambda: setattr(client, "_client", HTTP(case)),
                   "job": lambda: setattr(case.source, "job", Data(require_current_member=lambda: None)),
                   "close_method": lambda: setattr(client, "close", lambda: None)}
        actions[change]()

    case.during_close = changed
    with pytest.raises(OwnedGatewayUnknown):
        list(case.stream())
    record = case.source.auxiliary_clients[0]
    assert record.close_attempted and record.phase == "unknown" and record.inner is original_inner[0]
    assert record.voice_owner.inner is original_inner[0] and not case.source.auxiliary_closed
    assert case.events.count("client.close") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["thread", "loop", "profile"])
async def test_foreign_creator_refuses_before_resumed_audio_iterator_io(monkeypatch, tmp_path, change):
    case = Case(monkeypatch, tmp_path, managed=True)
    stream = case.stream()
    assert next(stream) == case.chunks[0]
    record = case.record()
    from agent import voice_owned as voice
    actions = {"thread": lambda: monkeypatch.setattr(voice, "threading", Data(current_thread=lambda: object())),
               "loop": lambda: monkeypatch.setattr(voice.owned, "running_loop", lambda: object()),
               "profile": lambda: monkeypatch.setattr("hermes_constants.hermes_home_key", lambda *a: "foreign")}
    actions[change]()
    with pytest.raises(OwnedGatewayUnknown):
        next(stream)
    assert record.phase == "unknown" and "client.close" not in case.events


def test_ordinary_body_error_keeps_existing_error_envelope_and_client_finally(monkeypatch, tmp_path):
    case = Case(monkeypatch, tmp_path, managed=False)
    def body(client):
        raise PermissionError("fixture-denied")
    assert case.transcribe(body)["error"] == "Permission denied: fixture.wav"
    assert case.events.count("client.close") == 1


@pytest.mark.asyncio
async def test_stream_cancellation_keeps_first_outcome_after_genuine_normal_closers(monkeypatch, tmp_path):
    case = Case(monkeypatch, tmp_path, managed=True)
    first = case.iteration_error = asyncio.CancelledError("audio-cancelled")
    with pytest.raises(asyncio.CancelledError) as caught:
        list(case.stream())
    assert caught.value is first and case.record().error is first
    assert case.events.count("client.close") == 1 and case.clients[0].is_closed()
    assert case.record().phase == "unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["http_response", "context", "source_task"])
async def test_original_association_changed_during_response_exit_never_closes_sdk(monkeypatch, tmp_path, change):
    case = Case(monkeypatch, tmp_path, managed=True)
    original_response = []

    def changed():
        original_response.append(case.response.http_response)
        actions = {"http_response": lambda: setattr(case.response, "http_response", object()),
                   "context": lambda: setattr(case.context, "_ResponseContextManager__response", object()),
                   "source_task": lambda: setattr(case.source, "task", object())}
        actions[change]()

    case.response_during_close = changed
    with pytest.raises(OwnedGatewayUnknown):
        list(case.stream())
    record = case.source.auxiliary_clients[0]
    assert record.voice_owner.response.http_response is original_response[0]
    assert record.phase == "unknown" and "client.close" not in case.events


@pytest.mark.asyncio
async def test_opaque_audio_iterator_is_retained_and_cannot_be_settlement(monkeypatch, tmp_path):
    from tests.tools.voice_sdk_fixture import Response
    case = Case(monkeypatch, tmp_path, managed=True)
    monkeypatch.setattr(Response, "iter_bytes", lambda self: iter(self.case.chunks))
    with pytest.raises(OwnedGatewayUnknown):
        list(case.stream())
    record = case.record()
    assert record.voice_owner.response.iterator is not None and record.phase == "unknown"
    assert "client.close" not in case.events


@pytest.mark.asyncio
async def test_borrowed_returned_client_cannot_be_adopted_as_a_fresh_voice_owner(monkeypatch, tmp_path):
    case = Case(monkeypatch, tmp_path, managed=True)
    assert case.transcribe()["success"]
    original = case.record()
    client = case.clients[0]
    class Borrowed(SDK):
        def __new__(cls, **kwargs):
            return client
    monkeypatch.setattr("openai.OpenAI", Borrowed)
    assert not case.transcribe()["success"]
    attempted = case.source.auxiliary_clients[-1]
    assert attempted is not original and attempted.error is not None and attempted.phase == "unknown"
    assert attempted.voice_owner.client is client and case.events.count("client.close") == 1


@pytest.mark.asyncio
async def test_returned_active_transcription_user_is_held_before_client_close(monkeypatch, tmp_path):
    case = Case(monkeypatch, tmp_path, managed=True)
    user = iter(["unconsumed"])
    assert not case.transcribe(lambda client: user)["success"]
    record = case.record()
    assert record.voice_owner.body_result is user and record.voice_owner.returned_user is user
    assert record.phase == "unknown" and "client.close" not in case.events


@pytest.mark.asyncio
async def test_body_first_error_survives_a_later_client_close_error(monkeypatch, tmp_path):
    case = Case(monkeypatch, tmp_path, managed=True)
    first = RuntimeError("first-body-error")
    second = case.client_close_error = RuntimeError("second-close-error")
    def body(client):
        raise first
    assert case.transcribe(body)["error"] == "Transcription failed: second-close-error"
    assert case.record().error is first and case.record().voice_owner.close_error is second
    assert case.source.auxiliary_error is first and case.events.count("client.close") == 1


@pytest.mark.asyncio
async def test_changed_original_profile_context_inside_client_closer_stays_unknown(monkeypatch, tmp_path):
    from hermes_constants import set_hermes_home_override
    case = Case(monkeypatch, tmp_path, managed=True)
    case.during_close = lambda: set_hermes_home_override(str(tmp_path / "foreign-close-profile"))
    with pytest.raises(OwnedGatewayUnknown):
        list(case.stream())
    record = case.record()
    assert record.close_attempted and record.phase == "unknown" and not case.source.auxiliary_closed


@pytest.mark.asyncio
async def test_stream_byte_cap_still_closes_active_iterator_response_then_client(monkeypatch, tmp_path):
    case = Case(monkeypatch, tmp_path, managed=True)
    monkeypatch.setattr(case.tts, "_STREAM_SENTENCE_BYTE_CAP", 3)
    assert list(case.stream()) == [case.chunks[0]]
    assert case.events.index("iterator.finished") < case.events.index("response.close") < case.events.index("client.close")
    assert case.record().phase == "closed"


def test_ordinary_early_iterator_close_error_still_exits_response_before_client(monkeypatch, tmp_path):
    from tests.tools.voice_sdk_fixture import Response
    case = Case(monkeypatch, tmp_path, managed=False)
    first = RuntimeError("ordinary-iterator-close-failed")
    def stream(self):
        try:
            yield b"fixture"
        finally:
            raise first
    monkeypatch.setattr(Response, "iter_bytes", stream)
    iterator = case.stream()
    assert next(iterator) == b"fixture"
    with pytest.raises(RuntimeError) as caught:
        iterator.close()
    assert caught.value is first
    assert case.events.index("response.close") < case.events.index("client.close")
    assert case.events.count("client.close") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["before", "during"])
async def test_replaced_original_transport_closer_cannot_become_sdk_settlement(monkeypatch, tmp_path, when):
    case = Case(monkeypatch, tmp_path, managed=True)
    def replace():
        case.clients[0]._client._transport.close = lambda: None
    if when == "before":
        def body(client):
            replace()
            return {"success": True}
        assert not case.transcribe(body)["success"]
        assert "client.close" not in case.events
    else:
        case.during_close = replace
        with pytest.raises(OwnedGatewayUnknown):
            list(case.stream())
        assert case.events.count("client.close") == 1
    record = case.record()
    assert record.phase == "unknown" and record.voice_owner.transport is case.clients[0]._client._transport
    assert record.voice_owner.transport_owners and not case.source.auxiliary_closed
