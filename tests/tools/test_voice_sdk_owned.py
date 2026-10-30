"""Actual fresh OpenAI streaming/transcription consumers with modeled lower resources."""
import pytest

from agent import auxiliary_owned_close as closing
from tests.tools.voice_sdk_fixture import Case


@pytest.mark.parametrize("early", [False, True])
def test_ordinary_stream_closes_response_then_original_client(monkeypatch, tmp_path, early):
    case = Case(monkeypatch, tmp_path, managed=False)
    stream = case.stream(model="fixture-model", voice="fixture-voice", base_url="https://fixture.invalid/v1",
                         instructions="fixture instructions")
    if early:
        assert next(stream) == case.chunks[0]
        stream.close()
    else:
        assert list(stream) == case.chunks
    assert case.streamer.sample_rate == 44100
    assert case.events.count("client.close") == 1
    assert case.events.index("response.close") < case.events.index("client.close")
    assert case.kwargs == {"api_key": "fixture-key", "base_url": "https://fixture.invalid/v1"}
    assert case.speech_kwargs == {"model": "fixture-model", "voice": "fixture-voice", "input": "fixture speech",
                                  "response_format": "pcm", "extra_body": {"instructions": "fixture instructions"}}


@pytest.mark.asyncio
@pytest.mark.parametrize("early", [False, True])
async def test_managed_stream_retains_original_book_and_actual_closure_order(monkeypatch, tmp_path, early):
    case = Case(monkeypatch, tmp_path, managed=True)
    stream = case.stream()
    assert next(stream) == case.chunks[0]
    before = case.before_factory[0]
    assert len(before) == 1 and before[0].phase == "held"
    record = case.record()
    assert record is before[0] and record.source is case.source
    if early:
        stream.close()
    else:
        assert list(stream) == case.chunks[1:]
    assert record.phase == "closed" and record.close_attempted and case.clients[0].is_closed()
    assert record.voice_owner.response.response is case.response
    assert record.voice_owner.response.context is case.context
    assert case.events.index("iterator.finished") < case.events.index("response.close") < case.events.index("client.close")
    await closing.close_record(record)
    assert case.events.count("client.close") == 1


@pytest.mark.asyncio
async def test_managed_transcription_reserves_before_factory_and_closes_before_result(monkeypatch, tmp_path):
    case = Case(monkeypatch, tmp_path, managed=True)
    result = case.transcribe()
    assert result == {"success": True, "transcript": "fixture transcript"}
    record = case.record()
    assert len(case.before_factory[0]) == 1 and case.before_factory[0][0] is record
    assert record.phase == "closed" and record.voice_owner.body_result is result
    assert record.close_attempted and case.clients[0].is_closed()
    assert case.kwargs == {"api_key": "fixture-key", "base_url": "https://fixture.invalid/v1",
                           "timeout": 73, "max_retries": 2}
    await closing.close_record(record)
    assert case.events.count("client.close") == 1


def test_ordinary_transcription_preserves_result_config_and_existing_finally_close(monkeypatch, tmp_path):
    case = Case(monkeypatch, tmp_path, managed=False)
    assert case.transcribe()["transcript"] == "fixture transcript"
    assert case.events.count("client.close") == 1 and case.clients[0].is_closed()
    assert case.kwargs["timeout"] == 73 and case.kwargs["max_retries"] == 2
