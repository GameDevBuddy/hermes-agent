"""Changed shared SDK settlement branch consumes voice users without stealing their closer."""
import json

import pytest

from agent import auxiliary_owned as owned, auxiliary_owned_close as closing
from tests.agent.auxiliary_owned_fixture import bind_source, sdk_through_actual_proxy
from tests.tools.voice_sdk_fixture import Case


@pytest.mark.asyncio
async def test_original_sdk_shutdown_accepts_voice_only_after_real_consumer_closure(monkeypatch, tmp_path):
    case = Case(monkeypatch, tmp_path, managed=True)
    assert case.transcribe()["success"]
    stream = case.stream()
    assert list(stream) == case.chunks
    # Actual original control consumer, not fabricated SDK closed flags.
    raw = json.dumps({"protocol": "gdb-owned-gateway-v2", "command": "shutdown",
                      "attemptId": case.source.contract.attempt_id}).encode() + b"\n"
    case.source.stdin.write(raw)
    case.source.stdin.seek(0)
    case.source.read_shutdown()
    await closing.shutdown_owned(case.runner)
    assert case.source.auxiliary_closed
    assert all(record.phase == "closed" for record in case.source.auxiliary_clients)
    assert case.events.count("client.close") == 2


@pytest.mark.asyncio
async def test_shared_closer_still_runs_non_voice_original_sdk_normal_close(monkeypatch, tmp_path):
    source, _ = bind_source(monkeypatch, tmp_path)
    client = sdk_through_actual_proxy(monkeypatch)
    record = owned.find(client)
    assert record.voice_owner is None and not client.is_closed
    await closing.close_record(record)
    assert record.phase == "closed" and client.is_closed and len(client.calls) == 1
    assert source.auxiliary_error is None
