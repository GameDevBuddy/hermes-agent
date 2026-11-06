"""Bounded original-context schema contract; component fixtures, no native acceptance."""
import copy
import json

import pytest

from tests.gateway.test_native_model_advertisement import case, descriptor


def malformed(kind):
    tool = descriptor()
    if kind == "legacy_pair":
        tool.pop("description")
        tool.pop("input_schema")
    elif kind == "missing_description":
        tool.pop("description")
    elif kind == "missing_schema":
        tool.pop("input_schema")
    elif kind == "description_number":
        tool["description"] = 12
    elif kind == "surrogate":
        tool["description"] = "\ud800"
    elif kind == "schema_null":
        tool["input_schema"] = None
    elif kind == "schema_array":
        tool["input_schema"] = []
    elif kind == "schema_scalar":
        tool["input_schema"] = "object"
    elif kind == "nan":
        tool["input_schema"] = {"default": float("nan")}
    elif kind == "unknown_authority":
        tool["approval"] = True
    elif kind == "wrong_alias":
        tool["name"] = "native_read"
    elif kind == "wrong_ref_case":
        tool["tool_ref"] = tool["tool_ref"].lower()
    elif kind == "duplicate":
        return [tool, copy.deepcopy(tool)]
    elif kind == "too_deep":
        schema = {}
        for _ in range(40):
            schema = {"properties": {"child": schema}}
        tool["input_schema"] = schema
    elif kind == "too_many_nodes":
        tool["input_schema"] = {"enum": list(range(40000))}
    return [tool]


@pytest.mark.parametrize("kind", ["legacy_pair", "missing_description", "missing_schema", "description_number",
    "surrogate", "schema_null", "schema_array", "schema_scalar", "nan", "unknown_authority", "wrong_alias",
    "wrong_ref_case", "duplicate", "too_deep", "too_many_nodes"])
def test_invalid_descriptors_are_refused_before_retention_or_original_bind(case, kind):
    reply = case.stage(tools=malformed(kind))
    assert reply["error"]["data"]["reason"] == "invalid_params"
    assert not case.source._app_context_stages and not case.connection._gdb_original_app_contexts
    assert case.source._app_context_credit == 0
    assert not hasattr(case.live.event_stream, "_gdb_native_selection")


@pytest.mark.parametrize("excess", [0, 1])
def test_descriptor_aggregate_limit_is_utf8_bytes_not_characters(case, excess):
    tools = [descriptor()]
    tools[0]["description"] = ""
    fixed = len(json.dumps(tools, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    padding = 1024 * 1024 - fixed + excess
    tools[0]["description"] = "é" * (padding // 2) + "x" * (padding % 2)
    reply = case.stage(tools=tools)
    if excess:
        assert reply["error"]["data"]["reason"] == "invalid_params"
        assert not case.source._app_context_stages and case.source._app_context_credit == 0
    else:
        assert reply["result"]["staged"] is True
        staged, = case.source._app_context_stages.values()
        assert len(staged.selection.tools_json) == 1024 * 1024
        assert json.loads(staged.selection.tools_json) == tools


def test_individually_small_descriptors_cannot_bypass_aggregate_bound(case):
    tools = [descriptor(description="x" * 600000),
             descriptor("01J00000000000000000000003", "y" * 600000)]
    assert case.stage(tools=tools)["error"]["data"]["reason"] == "invalid_params"
    assert case.source._app_context_credit == 0


@pytest.mark.parametrize("count", [0, 512, 513])
def test_selected_tool_count_stays_bounded(case, count):
    tools = [{"name": "gdb_native_" + f"{n:026d}", "tool_ref": f"{n:026d}",
              "description": "", "input_schema": {}} for n in range(count)]
    reply = case.stage(tools=tools)
    if count == 512:
        assert reply["result"]["staged"] is True
        staged, = case.source._app_context_stages.values()
        assert len(staged.selection.tools) == count
    else:
        assert reply["error"]["data"]["reason"] == "invalid_params"
        assert case.source._app_context_credit == 0
