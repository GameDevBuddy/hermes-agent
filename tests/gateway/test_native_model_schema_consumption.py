"""Accepted metadata must survive canonical model consumption; no inference/native work."""
import copy
import json
from types import SimpleNamespace

from agent.tool_commit_rows import MAX_JSON_DEPTH, MAX_JSON_NODES, json_digest
from gateway.session_native_context import _selected
from tests.gateway.test_native_model_advertisement import case, descriptor
from tests.gateway.native_model_fixture import BASE_TOOLS, factory, resolve, assemble


def _request(case, monkeypatch, tmp_path, tools, label):
    from gateway.session_authorities import owner_scope
    from agent.chat_completion_helpers import build_api_kwargs
    from hermes_state_runtime import RuntimeStoreError

    staged = case.stage(tools=tools)
    assert staged.get("result", {}).get("staged") is True, staged
    turn = factory(case, monkeypatch)
    expected = [{"type": "function", "function": {
        "name": tool["name"], "description": tool["description"],
        "parameters": tool["input_schema"]}} for tool in tools]
    observed = {"scope": "source-only metadata fixture; not execution authority",
                "label": label, "wire_digest": json_digest(tools),
                "provider_digest_before_consumption": json_digest(expected)}
    (tmp_path / "observation.json").write_text(json.dumps(observed, indent=2), encoding="utf-8")

    async def consume():
        with owner_scope(case.authority):
            try:
                agent, reused = resolve(turn)
            except RuntimeStoreError as error:
                raise AssertionError(f"staged and bound {label} metadata was refused during model consumption: {error}") from error
            assert not reused
            request = assemble(agent)
            wire = build_api_kwargs(agent, request.api_messages, request.tools_for_api)
            assert wire["tools"] == BASE_TOOLS + expected
            assert agent.valid_tool_names == {item["function"]["name"] for item in BASE_TOOLS + expected}
            assert agent._native_model_tools.definitions() == expected
            before = copy.deepcopy(wire)
            wire["tools"][-1]["function"]["parameters"]["request_local_change"] = True
            assert agent._native_model_tools.definitions() == expected
            refreshed = assemble(agent)
            assert refreshed.tools_for_api == before["tools"]
            observed["model_consumed"] = True
            (tmp_path / "observation.json").write_text(json.dumps(observed, indent=2), encoding="utf-8")

    case.read(before_shutdown=lambda: case.run(consume()))


def test_deepest_accepted_schema_reaches_model_request(case, monkeypatch, tmp_path):
    schema, accepted = {"type": "string"}, None
    for _ in range(MAX_JSON_DEPTH + 1):
        tool = descriptor()
        tool["input_schema"] = schema
        if json_digest(case.params(tools=[tool])) is None:
            break
        accepted = tool
        schema = {"type": "object", "additionalProperties": schema}
    assert accepted is not None
    tool = accepted
    _selected({"session_id": "s", "context_id": "boundary", "snapshot_sha256": "a" * 64,
               "tools": [tool]}, SimpleNamespace(session_id="s"))
    _request(case, monkeypatch, tmp_path, [tool], "depth-boundary")


def test_largest_accepted_node_tree_reaches_model_request(case, monkeypatch, tmp_path):
    def tools_for(size):
        tools = [descriptor(f"{index:026d}") for index in range(8)]
        for tool in tools:
            tool["input_schema"] = {}
        tools[0]["input_schema"] = {"type": "object", "properties": {"value": {"enum": list(range(size))}}}
        return tools

    low, high = 0, MAX_JSON_NODES
    while low < high:
        middle = (low + high + 1) // 2
        if json_digest(case.params(tools=tools_for(middle))) is None:
            high = middle - 1
        else:
            low = middle
    tools = tools_for(low)
    assert json_digest(case.params(tools=tools)) is not None
    assert json_digest(case.params(tools=tools_for(low + 1))) is None
    _request(case, monkeypatch, tmp_path, tools, "node-boundary")


def test_maximum_accepted_utf8_bytes_reach_model_request(case, monkeypatch, tmp_path):
    tool = descriptor(description="")
    fixed = len(json.dumps([tool], sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    padding = 1024 * 1024 - fixed
    tool["description"] = "é" * (padding // 2) + "x" * (padding % 2)
    digest = json_digest([tool])
    assert digest is not None and digest["bytes"] == 1024 * 1024
    _request(case, monkeypatch, tmp_path, [tool], "byte-boundary")
