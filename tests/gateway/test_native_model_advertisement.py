"""Source-only original-bind/schema/request fixtures; no network/native/model execution.

The lower IO/Job objects in Case are models, not actual owner-issuance evidence.
"""
import copy
import json

import pytest

from tests.gateway.native_app_context_fixture import Case, NONCE, TOOL
from tests.gateway.native_model_fixture import BASE_TOOLS, factory, resolve, assemble


SCHEMA = {"type": "object", "properties": {
    "path": {"type": "string", "description": "An asset path", "enum": ["/Game/A", "/Game/B"]},
    "options": {"type": "object", "properties": {"tags": {"type": "array", "items": {"type": "string"}}}},
}, "required": ["path"], "additionalProperties": False}
DESCRIPTION = "Read selected asset facts.\nNo changes — source fixture."


def descriptor(tool=TOOL, description=DESCRIPTION):
    return {"name": "gdb_native_" + tool.lower(), "tool_ref": tool,
            "description": description, "input_schema": copy.deepcopy(SCHEMA)}


@pytest.fixture
def case(monkeypatch, tmp_path):
    value = Case(monkeypatch, tmp_path)
    yield value
    value.close()


def test_original_bind_retains_exact_descriptor_bytes(case):
    tools = [descriptor()]
    response = case.stage(tools=tools)
    assert response.get("result", {}).get("staged") is True, response
    staged = case.source._app_context_stages[NONCE]
    frozen = staged.selection.tools_json
    assert json.loads(frozen) == tools
    tools[0]["description"] = "caller mutated input"
    tools[0]["input_schema"]["properties"]["path"]["enum"].append("/Game/Changed")
    assert json.loads(frozen) == [descriptor()]

    def before_shutdown():
        selected = case.live.event_stream._gdb_native_selection
        assert selected.tools_json == frozen
        assert selected.tools == ((descriptor()["name"], TOOL),)
        assert selected.same_selection(staged.selection)
        assert case.connection._gdb_original_app_contexts["s"].bound
        assert not case.runner.__dict__.get("_agent_cache")

    case.read(before_shutdown=before_shutdown)


def test_bound_descriptors_reach_canonical_agent_and_provider_request(case, monkeypatch):
    from gateway.session_authorities import owner_scope
    from tests.gateway.native_model_fixture import BASE_TOOLS, factory, resolve, assemble

    tools = [descriptor(), descriptor("01J00000000000000000000003", "Second selected read")]
    assert case.stage(tools=tools).get("result", {}).get("staged") is True
    turn = factory(case, monkeypatch)
    observed = []

    async def request():
        with owner_scope(case.authority):
            agent, reused = resolve(turn)
            assert not reused
            assert type(agent).__name__ == "AIAgent"
            assembled = assemble(agent)
            native = [tool for tool in assembled.tools_for_api
                      if tool["function"]["name"].startswith("gdb_native_")]
            expected = [{"type": "function", "function": {
                "name": tool["name"], "description": tool["description"], "parameters": tool["input_schema"]}}
                for tool in tools]
            assert native == expected
            assert agent.tools == BASE_TOOLS + expected
            assert agent.valid_tool_names == {tool["function"]["name"] for tool in BASE_TOOLS + expected}
            encoded = json.dumps({"messages": assembled.api_messages, "tools": assembled.tools_for_api})
            for private in ("tool_ref", "context_id", "snapshot_sha256", "authority_epoch", "nonce", "jobHandle"):
                assert private not in encoded
            assert not hasattr(agent, "_canonical_tool_commit_owner")
            observed.append(assembled)

    case.read(before_shutdown=lambda: case.run(request()))
    assert len(observed) == 1


def test_outbound_schema_mutation_cannot_rewrite_later_requests_or_bound_history(case, monkeypatch):
    from gateway.session_authorities import owner_scope
    tools = [descriptor()]
    assert case.stage(tools=tools).get("result", {}).get("staged") is True
    turn = factory(case, monkeypatch)

    async def requests():
        with owner_scope(case.authority):
            agent, _ = resolve(turn)
            history = [{"role": "user", "content": "Original history", "api_content": "Original wire content"}]
            before_history, before_tools = copy.deepcopy(history), copy.deepcopy(agent.tools)
            first = assemble(agent, history)
            bound = case.live.event_stream._gdb_native_selection.tools_json
            native = first.tools_for_api[-1]["function"]
            native["parameters"]["properties"]["path"]["enum"].append("/Game/ProviderChanged")
            native["description"] = "provider/cache changed request-local data"
            second = assemble(agent, history)
            assert second.tools_for_api == before_tools
            assert agent.tools == before_tools
            assert first.tools_for_api != second.tools_for_api
            assert history == before_history
            assert case.live.event_stream._gdb_native_selection.tools_json == bound
            assert json.loads(bound) == [descriptor()]

    case.read(before_shutdown=lambda: case.run(requests()))


@pytest.mark.parametrize("preserve_prefix", [False, True])
def test_configured_tool_refresh_retains_native_snapshot_and_other_features(case, monkeypatch, preserve_prefix):
    from gateway.session_authorities import owner_scope
    from tools.mcp_tool_agent import refresh_agent_mcp_tools
    import model_tools
    assert case.stage(tools=[descriptor()]).get("result", {}).get("staged") is True
    turn = factory(case, monkeypatch)

    async def refresh():
        with owner_scope(case.authority):
            agent, _ = resolve(turn)
            before = assemble(agent)
            previous = copy.deepcopy(before.tools_for_api)
            extra = {"type": "function", "function": {"name": "configured_extra", "description": "New ordinary feature",
                     "parameters": {"type": "object", "properties": {}}}}
            monkeypatch.setattr(model_tools, "get_tool_definitions", lambda **kw: BASE_TOOLS + [extra])
            added = refresh_agent_mcp_tools(agent, preserve_prefix=preserve_prefix)
            assert "configured_extra" in added
            offered = assemble(agent).tools_for_api
            native = [tool for tool in offered if tool["function"]["name"].startswith("gdb_native_")]
            assert native == previous[-1:]
            assert {"configured_read", "configured_extra", descriptor()["name"]} <= agent.valid_tool_names
            assert before.tools_for_api == previous
            assert BASE_TOOLS == previous[:1]

    case.read(before_shutdown=lambda: case.run(refresh()))


def test_saved_tool_pin_cannot_replace_selected_native_schema_bytes(case, monkeypatch):
    from gateway.session_authorities import owner_scope
    from tools import mcp_tool_agent as snapshots
    assert case.stage(tools=[descriptor()]).get("result", {}).get("staged") is True
    turn = factory(case, monkeypatch)
    monkeypatch.setattr(snapshots, "tool_pin_version", lambda: "component-source-version")

    async def restore():
        with owner_scope(case.authority):
            agent, _ = resolve(turn)
            initial = assemble(agent).tools_for_api
            pin = {"version": "component-source-version", "tools": copy.deepcopy(initial)}
            pin["tools"][0]["function"]["description"] = "An already-sent ordinary tool description"
            pin["tools"][-1]["function"]["description"] = "Unrelated saved native bytes"
            saved = copy.deepcopy(pin)
            snapshots.restore_agent_tool_prefix(agent, pin)
            offered = assemble(agent).tools_for_api
            assert offered[-1] == initial[-1]
            assert offered[0] == pin["tools"][0]
            assert pin == saved

    case.read(before_shutdown=lambda: case.run(restore()))
