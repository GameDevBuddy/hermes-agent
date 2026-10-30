"""Real source/runner/task/context with mock SDK objects; no OS Job or model acceptance."""
import asyncio
import io
import threading
from types import SimpleNamespace

import hermes_owned_gateway as ownership
from gateway.native_source_runtime import capture_original_runner
from hermes_constants import set_hermes_home_override
from tests.gateway.native_source_fixture import runner_from_actual_constructor


class JobWitness:
    """Stateful test relationship check, explicitly not a Windows Job implementation."""
    def __init__(self, source):
        self.source, self.revoked, self.calls = source, False, []

    def require_current_member(self):
        if (self.revoked or ownership._CURRENT is not self.source or self.source.job is not self
                or self.source.phase not in ("attached", "bound", "draining")):
            raise ownership.OwnedGatewayUnknown("test-original-source-relationship-revoked")
        self.calls.append(threading.current_thread())


def bind_source(monkeypatch, tmp_path):
    monkeypatch.setattr(ownership.sys, "stdin", SimpleNamespace(buffer=io.BytesIO()))
    source = ownership.OwnedGatewaySource(SimpleNamespace(attempt_id="aux-source"), b"{}")
    source.job, source.phase = JobWitness(source), "attached"
    monkeypatch.setattr(ownership, "_CURRENT", source)
    runner = runner_from_actual_constructor(monkeypatch)
    capture_original_runner(runner, asyncio.current_task(), asyncio.get_running_loop())
    source.bind_runner(runner)
    set_hermes_home_override(str(tmp_path / "creator-profile"))
    return source, runner


def allow_normal_shutdown(source):
    source._shutdown, source._control_closed, source.phase = True, True, "draining"


class MockHTTP:
    def __init__(self):
        self.is_closed, self._mounts = False, {}

    def close(self):
        self.is_closed = True


class MockSDK:
    def __init__(self, **kwargs):
        self._client = kwargs.get("http_client") or MockHTTP()
        self.is_closed, self.calls = False, []
        self.api_key, self.base_url = "fixture", "https://fixture.invalid/v1"

    def close(self):
        from hermes_constants import hermes_home_key
        self.calls.append((asyncio.get_running_loop(), hermes_home_key(), threading.current_thread()))
        self._client.close()
        self.is_closed = True


def sdk_through_actual_proxy(monkeypatch, factory=MockSDK):
    from agent import auxiliary_client as auxiliary
    monkeypatch.setattr(auxiliary, "_load_openai_cls", lambda: factory)
    return auxiliary.OpenAI(api_key="fixture", base_url="https://fixture.invalid/v1")
