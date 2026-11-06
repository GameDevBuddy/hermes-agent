"""Pure model-cache regressions: actual factory, constructor, loader and provider kwargs.

The ONLY changing source seam is an immutable NativeModelTools snapshot provider.
Its keys/data are fixture metadata, not OriginalContext, control, issuer or native
execution authority. No authenticated/bound flags or agent.tools are assigned.
Forbidden client/discovery/metadata IO is isolated by the existing factory fixture.
"""
import copy
import json
from types import SimpleNamespace as Data

from agent.native_model_tools import NativeModelTools
from agent.native_receipt_origin import frozen_json
from tests.gateway.native_model_fixture import BASE_TOOLS, assemble, factory, resolve
from tests.gateway.native_source_fixture import runner_from_actual_constructor
from tests.gateway.test_native_model_advertisement import descriptor


def _provider_definition():
    tool = descriptor()
    return {"type": "function", "function": {
        "name": tool["name"], "description": tool["description"], "parameters": tool["input_schema"]}}


def _wire(agent):
    from agent.chat_completion_helpers import build_api_kwargs
    request = assemble(agent)
    return build_api_kwargs(agent, request.api_messages, request.tools_for_api)


def _native_definitions(wire):
    return [tool for tool in wire["tools"] if tool["function"]["name"].startswith("gdb_native_")]


def test_later_unequipped_resolution_does_not_reuse_differently_sampled_native_payload(monkeypatch, tmp_path):
    import gateway.native_model_context as context
    from run_agent import AIAgent

    turn = factory(Data(runner=runner_from_actual_constructor(monkeypatch)), monkeypatch)
    native = NativeModelTools(frozen_json([_provider_definition()]))
    unequipped_key = ("pure-model-cache-fixture", "s", "stable-live", "unequipped")
    equipped_key = (*unequipped_key[:-1], "equipped-schema-v1")
    state = {"phase": "construction-transition", "sampled_once": False}
    samples = []

    def snapshot(runner, session_id):
        assert runner is turn._runner and session_id == "s"
        if state["phase"] == "construction-transition" and state["sampled_once"]:
            value = native, equipped_key
        else:
            value = None, unequipped_key
        state["sampled_once"] = True
        samples.append({"phase": state["phase"], "native": value[0] is not None, "key": value[1]})
        return value

    monkeypatch.setattr(context, "snapshot_for_agent", snapshot)
    first, first_reused = resolve(turn)
    assert type(first) is AIAgent and first_reused is False
    assert not hasattr(first, "_canonical_tool_commit_owner")
    first_wire = _wire(first)
    entry = turn._runner._agent_cache[turn._ctx.session_key]
    first_signature = copy.deepcopy(entry[1])
    state["phase"] = "stable-unequipped"
    later, later_reused = resolve(turn)
    assert type(later) is AIAgent
    later_wire = _wire(later)
    observation = {
        "scope": "pure immutable model-data snapshot sequence; no native authority or execution",
        "samples": samples,
        "first_cached_signature": first_signature,
        "first_wire": first_wire,
        "later_wire": later_wire,
        "later_reused": later_reused,
        "same_agent": later is first,
        "later_native_names": [tool["function"]["name"] for tool in _native_definitions(later_wire)],
        "later_valid_tool_names": sorted(later.valid_tool_names),
    }
    (tmp_path / "cache-payload-observation.json").write_text(
        json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")

    assert _native_definitions(later_wire) == [], (
        "stable unequipped snapshot reused differently sampled equipped payload; "
        f"reused={later_reused}, same_agent={later is first}, cached_signature={first_signature!r}, "
        f"native_names={observation['later_native_names']!r}")
    assert later.tools == BASE_TOOLS
    assert descriptor()["name"] not in later.valid_tool_names


def test_same_code_saved_native_pin_does_not_equip_an_ordinary_constructed_agent(monkeypatch, tmp_path):
    from tools import mcp_tool_agent as snapshots
    from tools.registry import registry
    from run_agent import AIAgent

    # No native snapshot provider replacement here: actual snapshot_for_agent sees no
    # serving authority on this ordinary source-fixture runner and returns (None, None).
    turn = factory(Data(runner=runner_from_actual_constructor(monkeypatch)), monkeypatch)
    agent, reused = resolve(turn)
    assert type(agent) is AIAgent and reused is False
    assert agent._native_model_tools is None and agent.tools == BASE_TOOLS
    native_definition = _provider_definition()
    assert registry.get_entry(native_definition["function"]["name"]) is None
    monkeypatch.setattr(snapshots, "tool_pin_version", lambda: "isolated-pin-fixture-version")
    saved = {"version": "isolated-pin-fixture-version", "tools": copy.deepcopy(BASE_TOOLS) + [native_definition]}
    original_saved = copy.deepcopy(saved)
    before = _wire(agent)
    changed = snapshots.restore_agent_tool_prefix(agent, saved)
    after = _wire(agent)
    observation = {"scope": "ordinary constructed agent; same-code unregistered saved native pin",
                   "restore_changed": changed, "before": before, "after": after,
                   "valid_tool_names": sorted(agent.valid_tool_names), "saved_unchanged": saved == original_saved}
    (tmp_path / "unequipped-pin-observation.json").write_text(
        json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
    assert _native_definitions(after) == []
    assert agent.tools == BASE_TOOLS and after == before
    assert descriptor()["name"] not in agent.valid_tool_names
    assert saved == original_saved
