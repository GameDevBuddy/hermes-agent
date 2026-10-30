"""Actual caller byte limits precede full materialization; strict JSON semantics remain unchanged."""
import math

import pytest

from agent import native_receipt_origin as origin
from agent import tool_commit_rows as rows
from hermes_state_runtime import RuntimeStoreError


BOUNDARIES = [
    ("a" * 8190, b'"' + b"a" * 8190 + b'"'),
    ("é" * 4095, b'"' + b"\xc3\xa9" * 4095 + b'"'),
    ("\0" * 1365, b'"' + b"\\u0000" * 1365 + b'"'),
    ({"payload": "a" * 262130}, b'{"payload":"' + b"a" * 262130 + b'"}'),
]


@pytest.mark.parametrize("value,expected", BOUNDARIES, ids=["ascii8192", "utf8_8192", "escape8192", "object262144"])
def test_exact_caller_boundaries_preserve_original_wire_bytes(value, expected):
    assert rows.json_digest(value)["bytes"] == len(expected)
    assert origin.frozen_json(value, len(expected)) == expected
    assert origin.frozen_json(value, len(expected) + 1) == expected


@pytest.mark.parametrize("value,expected", BOUNDARIES, ids=["ascii8192", "utf8_8192", "escape8192", "object262144"])
def test_over_limit_value_never_enters_full_materializing_serializer(monkeypatch, value, expected):
    assert rows.json_digest(value)["bytes"] == len(expected)
    original_dumps, materializations = origin.json.dumps, []

    def observe(*args, **kwargs):
        materializations.append(True)
        return original_dumps(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(origin.json, "dumps", observe)
        with pytest.raises(RuntimeStoreError) as caught:
            origin.frozen_json(value, len(expected) - 1)
    assert caught.value.reason == "native_source_json_unavailable"
    assert materializations == []


@pytest.mark.parametrize("value,expected", [
    (None, b"null"), (False, b"false"), ({}, b"{}"), ((1, 2), b"[1,2]"),
    ({"z": "é", "a": [True, None]}, b'{"a":[true,null],"z":"\xc3\xa9"}'),
    (['"', "\\", "\n"], b'["\\\"","\\\\","\\n"]'),
    (-0.0, b"-0.0"), ((1 << 255), str(1 << 255).encode("ascii")),
])
def test_default_path_keeps_sorted_compact_utf8_and_scalar_semantics(value, expected):
    assert origin.frozen_json(value) == expected


def cycle():
    value = []
    value.append(value)
    return value


def shared_container():
    child = []
    return [child, child]


def excessive_depth():
    value = None
    for _ in range(rows.MAX_JSON_DEPTH + 1):
        value = [value]
    return value


@pytest.mark.parametrize("make", [
    lambda: math.nan, lambda: math.inf, lambda: -math.inf,
    lambda: b"bytes", lambda: object(), lambda: {1: "non-string-key"},
    lambda: "\ud800", lambda: 1 << 256, cycle, shared_container, excessive_depth,
    lambda: [None] * (rows.MAX_JSON_NODES + 1),
])
def test_invalid_values_and_global_tree_bounds_keep_original_error_without_materialization(monkeypatch, make):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid JSON reached the full materializing serializer")

    with monkeypatch.context() as patch:
        patch.setattr(origin.json, "dumps", forbidden)
        with pytest.raises(RuntimeStoreError) as caught:
            origin.frozen_json(make())
    assert caught.value.reason == "native_source_json_unavailable"


def test_larger_caller_limit_does_not_override_global_encoded_byte_bound(monkeypatch):
    monkeypatch.setattr(rows, "MAX_TEXT_BYTES", 32)
    value = "é" * 16  # 32 UTF8 payload bytes plus the JSON quotes
    assert rows.json_digest(value) is None
    with pytest.raises(RuntimeStoreError) as caught:
        origin.frozen_json(value, 1024)
    assert caught.value.reason == "native_source_json_unavailable"
