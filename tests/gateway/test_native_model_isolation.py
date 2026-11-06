"""Focused schema-only isolation/guards. IO/Jobs in Case are models, not native acceptance."""
import copy
import json
from types import SimpleNamespace as Data

import pytest

from gateway.session_authorities import owner_scope
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.test_native_model_advertisement import case, descriptor
from tests.gateway.native_model_fixture import BASE_TOOLS, ROUTE, factory, resolve, assemble


def test_staging_alone_cannot_equip_an_agent_and_binding_rebuilds_its_cache(case, monkeypatch):
    assert case.stage(tools=[descriptor()]).get("result", {}).get("staged") is True
    turn = factory(case, monkeypatch)

    async def before_bind():
        with owner_scope(case.authority):
            agent, reused = resolve(turn)
            assert not reused and agent.tools == BASE_TOOLS
            return agent, assemble(agent)

    first, past_request = case.run(before_bind())
    old_bytes = json.dumps(past_request.tools_for_api, sort_keys=True)

    async def after_bind():
        with owner_scope(case.authority):
            current, reused = resolve(turn)
            assert not reused and current is not first
            assert descriptor()["name"] in current.valid_tool_names
            for _ in range(3):
                again, reused = resolve(turn)
                assert reused and again is current
                assert assemble(again).tools_for_api == current.tools
            assert first.tools == BASE_TOOLS
            assert json.dumps(past_request.tools_for_api, sort_keys=True) == old_bytes

    case.read(before_shutdown=lambda: case.run(after_bind()))


def test_native_equipped_cached_agent_cannot_follow_another_session_or_profile(case, monkeypatch, tmp_path):
    from gateway.session_authority import SessionAuthority, LiveSession
    from tools.registry import registry
    assert case.stage(tools=[descriptor()]).get("result", {}).get("staged") is True
    turn = factory(case, monkeypatch)
    before_registry = tuple(entry.name for entry in registry.get_all_entries())

    async def isolation():
        with owner_scope(case.authority):
            native, _ = resolve(turn)
            request = assemble(native)
            frozen = copy.deepcopy(request.tools_for_api)
            # Force a routing-key collision: the native cache must still miss.
            case.authority.sessions["other"] = LiveSession(None, "other-route")
            turn._ctx.session_id, turn._ctx.source.chat_id = "other", "other"
            ordinary, reused = resolve(turn)
            assert not reused and ordinary is not native and ordinary.tools == BASE_TOOLS
            assert descriptor()["name"] not in ordinary.valid_tool_names
        profile = tmp_path / "another-profile"
        profile.mkdir()
        another = SessionAuthority(case.runner, profile_id=str(profile), instance_id="other-instance",
                                   db=Data(), epoch=1)
        another.sessions["s"] = LiveSession(None, "another-profile-route")
        case.registry.add(profile, another, name="other")
        case.runner.config.multiplex_profiles = True  # Real owner_scope routing, not the single-profile no-op.
        turn._ctx.session_id, turn._ctx.source.chat_id = "s", "s"
        # Use the real routed profile scope with explicit empty fixture secrets;
        # owner_scope's cold-secret hydration would otherwise probe a .env file.
        from gateway.run import _profile_runtime_scope
        with _profile_runtime_scope(profile, prepared_secret_scope={}):
            ordinary_profile, reused = resolve(turn)
            assert not reused and ordinary_profile.tools == BASE_TOOLS
            assert ordinary_profile is not native
        assert native.tools == frozen and request.tools_for_api == frozen
        assert tuple(entry.name for entry in registry.get_all_entries()) == before_registry
        assert BASE_TOOLS == frozen[:1]

    case.read(before_shutdown=lambda: case.run(isolation()))


@pytest.mark.parametrize("change", ["association", "selection_copy", "actor", "epoch", "profile",
                                     "transport_closed", "original_connection", "reader", "acknowledgement"])
def test_changed_bound_original_is_refused_before_cached_agent_reuse(case, monkeypatch, change):
    import hermes_owned_gateway as owned
    assert case.stage(tools=[descriptor()]).get("result", {}).get("staged") is True
    turn = factory(case, monkeypatch)

    async def refusal():
        with owner_scope(case.authority):
            agent, _ = resolve(turn)
            before = copy.deepcopy(assemble(agent).tools_for_api)
            original = case.connection._gdb_original_app_contexts["s"]
            changes = {
                "association": lambda: case.connection._gdb_original_app_contexts.clear(),
                "selection_copy": lambda: setattr(case.live.event_stream, "_gdb_native_selection",
                                                  copy.copy(case.live.event_stream._gdb_native_selection)),
                "actor": lambda: setattr(case.connection, "actor", object()),
                "epoch": lambda: setattr(case.authority, "epoch", 2),
                "profile": lambda: setattr(case.authority, "profile_id", "other-profile"),
                "transport_closed": lambda: setattr(case.transport, "_closed", True),
                "original_connection": lambda: setattr(case.connection, "_gdb_original_source_connection", object()),
                "reader": lambda: setattr(case.source, "_control_reader", object()),
                "acknowledgement": lambda: setattr(original.request, "acknowledged", False),
            }
            changes[change]()
            with pytest.raises((RuntimeStoreError, owned.OwnedGatewayUnknown)):
                resolve(turn)
            assert agent.tools == before  # old schema/history is not rewritten on revocation

    case.read(before_shutdown=lambda: case.run(refusal()))


def test_factory_consumes_bound_schema_data_off_loop_without_native_rights(case, monkeypatch):
    import asyncio
    from agent import tool_executor as executor
    from gateway.native_source_issuance import require_original_app_context
    assert case.stage(tools=[descriptor()]).get("result", {}).get("staged") is True
    turn = factory(case, monkeypatch)

    def build():
        with owner_scope(case.authority):
            agent, _ = resolve(turn)
            assert descriptor()["name"] in agent.valid_tool_names
            return agent, assemble(agent)

    async def request():
        # A later generation reuses schema DATA, never the bind's generation rights.
        case.row["runtime_generation"] += 1
        with owner_scope(case.authority), pytest.raises(RuntimeStoreError):
            require_original_app_context(case.connection, case.ref)
        agent, assembled = await asyncio.to_thread(build)
        assert assembled.tools_for_api[-1]["function"]["parameters"] == descriptor()["input_schema"]
        assert not hasattr(agent, "_canonical_tool_commit_owner")
        ref = executor._ToolCallRef(descriptor()["name"], {}, "s", "fixture-call", [])
        state = Data(args=ref.args, dispatch_ref=None, data_use=None)
        dispatched = []
        monkeypatch.setattr(executor, "_pre_tool_block", lambda agent, call: (None, call.args))
        monkeypatch.setattr(executor, "_run_with_activity_heartbeat", lambda agent, tool, run: run())
        result = executor._dispatch_authorized_once(agent, state, ref,
            execute=lambda args: dispatched.append(args), scope_block=None, display_index=None,
            begin_execution=None, authorization_gate=None)
        assert json.loads(result)["error"] == "native_dispatch_unavailable" and dispatched == []

    case.read(before_shutdown=lambda: case.run(request()))


@pytest.mark.parametrize("mode", ["chat_completions", "codex_responses", "anthropic_messages"])
def test_actual_provider_kwargs_contain_selected_schemas_without_context_authority(case, monkeypatch, mode, tmp_path):
    from agent.chat_completion_helpers import build_api_kwargs
    assert case.stage(tools=[descriptor()]).get("result", {}).get("staged") is True
    turn = factory(case, monkeypatch)
    route = copy.deepcopy(ROUTE)
    route["runtime"]["api_mode"] = mode

    async def wire():
        with owner_scope(case.authority):
            agent, _ = turn._resolve_turn_agent(route, "cli", "", 2, None, {})
            assembled = assemble(agent)
            kwargs = build_api_kwargs(agent, assembled.api_messages, assembled.tools_for_api)
            tools = kwargs["tools"]
            if mode == "chat_completions":
                native = [tool["function"] for tool in tools if tool["function"]["name"].startswith("gdb_native_")]
                schema_key = "parameters"
            else:
                native = [tool for tool in tools if tool["name"].startswith("gdb_native_")]
                schema_key = "input_schema" if mode == "anthropic_messages" else "parameters"
            assert len(native) == 1
            assert native[0]["name"] == descriptor()["name"]
            assert native[0]["description"] == descriptor()["description"]
            assert native[0][schema_key] == descriptor()["input_schema"]
            raw = json.dumps(kwargs, sort_keys=True)
            assert not any(key in raw for key in ("tool_ref", "context_id", "snapshot_sha256", "nonce", "jobHandle"))
            (tmp_path / f"provider-kwargs-{mode}.json").write_text(raw, encoding="utf-8")

    case.read(before_shutdown=lambda: case.run(wire()))
