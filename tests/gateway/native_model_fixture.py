"""No-network model-advertisement fixture: real factory/init/loader/assembly.

Only client construction, configured tool discovery and metadata IO are replaced.
No assistant result, inference, native call or execution authority is fabricated.
"""
import copy
import logging
import threading
from types import SimpleNamespace as Data

from gateway.config import Platform
from gateway.session_execution import GatewaySessionAgentMixin
from gateway.turn_context import TurnContext
from run_agent import AIAgent

BASE_TOOLS = [{"type": "function", "function": {"name": "configured_read", "description": "Configured ordinary tool",
               "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}}}]
ROUTE = {"model": "schema-fixture-model", "runtime": {
    "provider": "custom", "api_mode": "chat_completions", "base_url": "https://schema-fixture.invalid/v1"}}


class NoClient:
    def close(self):
        pass


def factory(case, monkeypatch):
    from agent import agent_init, model_metadata, context_compressor
    from run_agent import AIAgent
    import model_tools
    import hermes_cli.config as config
    import hermes_cli.left_core_migration as migration
    import hermes_cli.plugins as plugins

    def no_client(agent, *args):
        agent.client = NoClient()
        agent.api_key = None
        agent._client_kwargs = {}
        agent._client_lock = threading.RLock()
        agent._anthropic_client = agent._codex_session = None
        agent._anthropic_api_key, agent._anthropic_base_url, agent._is_anthropic_oauth = "", agent.base_url, False

    monkeypatch.setattr(agent_init, "_build_client", no_client)
    monkeypatch.setattr(agent_init, "_setup_logging", lambda agent: None)
    monkeypatch.setattr(config, "load_config_readonly", lambda *a, **kw: {
        "model": {"context_length": 128000}, "memory": {"enabled": False},
        "agent": {"environment_probe": False}})
    for module in (agent_init, model_metadata):
        monkeypatch.setattr(module, "fetch_model_metadata", lambda *a, **kw: {})
    monkeypatch.setattr(migration, "recover_at_startup", lambda: None)
    monkeypatch.setattr(plugins, "discover_plugins", lambda *a, **kw: None)
    # A deliberately shared configured schema snapshot catches write-through contamination.
    monkeypatch.setattr(model_tools, "get_tool_definitions", lambda **kw: BASE_TOOLS)
    runner = case.runner
    runner._agent_cache_lock, runner._agent_cache = threading.RLock(), {}
    runner._session_db, runner._prefill_messages, runner._service_tier = None, [], None
    runner._adapter_for_source = lambda source: None
    runner._refresh_fallback_model = lambda: None
    runner._apply_fallback_chain_to_agent = lambda *a: None
    runner._enforce_agent_cache_cap = lambda: None
    runner._spawn_release_thread = lambda *a, **kw: None
    turn = GatewaySessionAgentMixin()
    turn._runner = runner
    source = Data(platform=Platform.LOCAL, chat_id="s", user_id="fixture", user_id_alt=None,
                  user_name=None, chat_name=None, chat_type="dm", thread_id=None)
    turn._ctx = TurnContext(source=source, session_id="s", session_key="schema-route", user_config={},
                            enabled_toolsets=["fixture"], disabled_toolsets=None, AIAgent=AIAgent)
    return turn


def resolve(turn):
    return turn._resolve_turn_agent(ROUTE, "cli", "", 2, None, {})


def assemble(agent, messages=None):
    from agent.turn_request_assembly import assemble_api_request
    from agent.turn_context import _reset_per_turn_agent_state
    _reset_per_turn_agent_state(agent)
    history = copy.deepcopy(messages or [{"role": "user", "content": "Inspect the selected asset."}])
    return assemble_api_request(agent, messages=history, current_turn_user_idx=len(history) - 1,
        _ext_prefetch_cache="", _plugin_user_context="", moa_config=None,
        active_system_prompt="Schema component fixture.", original_user_message=history[-1]["content"],
        pending_moa_prepared_request=None, request_logger=logging.getLogger(__name__))
