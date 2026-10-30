"""Actual AIAgent constructor/SDK creator with mock SDK and HTTP factories only."""
import asyncio
import threading

from agent import agent_init, agent_runtime_helpers, process_bootstrap, served_model
from run_agent import AIAgent
from tests.agent.auxiliary_owned_fixture import MockHTTP, MockSDK, bind_source


def primary_fixture(monkeypatch, tmp_path, *, sdk=MockSDK, initializer=None):
    source, runner = bind_source(monkeypatch, tmp_path)
    runner._agent_cache_lock, runner._agent_cache = threading.RLock(), {}
    monkeypatch.setattr(process_bootstrap, "OpenAI", sdk)
    from agent import auxiliary_client
    monkeypatch.setattr(auxiliary_client, "_load_openai_cls", lambda: sdk)
    monkeypatch.setattr(process_bootstrap, "build_keepalive_http_client", lambda *args, **kwargs: MockHTTP())
    monkeypatch.setattr(agent_runtime_helpers, "_provider_supplied_client", lambda *args: None)
    monkeypatch.setattr(served_model, "install_served_model_capture", lambda *args: None)
    monkeypatch.setattr(agent_init, "init_agent", initializer or minimal_primary_init)
    return source, runner


def minimal_primary_init(agent, **kwargs):
    agent.provider, agent.model = "custom", "fixture-model"
    agent.api_mode, agent.base_url, agent.api_key = "chat_completions", "https://fixture.invalid/v1", "fixture"
    agent.quiet_mode, agent.platform, agent.session_id = True, "fixture", None
    agent._memory_manager = agent._anthropic_client = agent._codex_session = None
    agent._client_lock, agent._session_messages = threading.RLock(), []
    agent._client_kwargs = {"api_key": agent.api_key, "base_url": agent.base_url}
    agent.client = agent._create_openai_client(agent._client_kwargs, reason="agent_init", shared=True)


def construct_actual_agent():
    return AIAgent(api_key="fixture", base_url="https://fixture.invalid/v1", model="fixture-model",
                   quiet_mode=True, skip_memory=True, skip_context_files=True)


async def finish_sdks(source, runner):
    from agent.auxiliary_owned_close import shutdown_owned
    from tests.agent.auxiliary_owned_fixture import allow_normal_shutdown
    allow_normal_shutdown(source)
    await shutdown_owned(runner)
    assert source.auxiliary_closed


class AsyncSDK(MockSDK):
    async def close(self):
        await asyncio.sleep(0)
        super().close()
