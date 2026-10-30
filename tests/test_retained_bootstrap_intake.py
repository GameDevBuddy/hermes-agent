"""Bounded original-plan intake; no bootstrap, SDK, process or gateway launch."""
import io
import json
from pathlib import Path

import pytest

import hermes_retained_bootstrap as bootstrap


PLAN_LIMIT = 128 * 1024


def test_plan_reader_does_not_read_beyond_the_declared_budget(tmp_path, monkeypatch):
    path = tmp_path / "plan.json"
    reads = []

    class BudgetedInput(io.BytesIO):
        def read(self, size=-1):
            reads.append(size)
            if not 0 <= size <= PLAN_LIMIT + 1:
                raise AssertionError("plan intake requested an unbounded read")
            return super().read(size)

    stream = BudgetedInput(b" " * (2 * PLAN_LIMIT))
    original_open = Path.open

    def open_plan(self, *args, **kwargs):
        if self == path:
            return stream
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_plan)
    with pytest.raises(bootstrap.BootstrapRefused) as error:
        bootstrap._read_plan(str(path))
    assert error.value.reason == "bootstrap-plan-too-large"
    assert reads == [PLAN_LIMIT + 1]
    assert stream.closed


@pytest.mark.parametrize("extra", [0, 1])
def test_exact_byte_boundary_includes_the_original_encoding(tmp_path, extra):
    path = tmp_path / "plan.json"
    prefix = b'\xef\xbb\xbf{"attemptId":"fixture"}'
    raw = prefix + b" " * (PLAN_LIMIT - len(prefix) + extra)
    path.write_bytes(raw)
    if extra:
        with pytest.raises(bootstrap.BootstrapRefused) as error:
            bootstrap._read_plan(str(path))
        assert error.value.reason == "bootstrap-plan-too-large"
    else:
        original, value = bootstrap._read_plan(str(path))
        assert original == raw
        assert value == {"attemptId": "fixture"}


@pytest.mark.parametrize("raw", [
    b'{"writerExclusion":false,"writerExclusion":true}',
    b'{"attemptId":"first","attempt\\u0049d":"second"}',
    b'{"dependencyPins":{"uv.lock":"first","uv.lock":"second"}}',
])
def test_ambiguous_original_plan_fields_are_refused(tmp_path, raw):
    path = tmp_path / "plan.json"
    path.write_bytes(raw)
    with pytest.raises(bootstrap.BootstrapRefused) as error:
        bootstrap._read_plan(str(path))
    assert error.value.reason == "duplicate-bootstrap-plan-field"


@pytest.mark.parametrize("raw", [
    b'{"attemptId":NaN}',
    b'{"attemptId":Infinity}',
    b'{"attemptId":-Infinity}',
])
def test_non_json_numeric_values_are_refused(tmp_path, raw):
    path = tmp_path / "plan.json"
    path.write_bytes(raw)
    with pytest.raises(bootstrap.BootstrapRefused) as error:
        bootstrap._read_plan(str(path))
    assert error.value.reason == "non-finite-bootstrap-plan-value"


@pytest.mark.parametrize("raw", [b'{"x":"\xff"}', b'{"x":}', b"[1,2]"],
                         ids=["invalid-utf8", "malformed-json", "not-an-object"])
def test_malformed_plan_refuses(tmp_path, raw):
    path = tmp_path / "plan.json"
    path.write_bytes(raw)
    with pytest.raises(bootstrap.BootstrapRefused):
        bootstrap._read_plan(str(path))


def test_missing_plan_refuses_without_selecting_a_contract(tmp_path, monkeypatch):
    monkeypatch.setattr(bootstrap, "_BOUND", None)
    monkeypatch.setattr(bootstrap, "_PLAN_BYTES", None)
    with pytest.raises(bootstrap.BootstrapRefused):
        bootstrap._read_plan(str(tmp_path / "missing.json"))
    assert bootstrap.current() is None
    assert bootstrap._PLAN_BYTES is None


def test_unambiguous_utf8_original_bytes_survive_intake(tmp_path):
    path = tmp_path / "plan.json"
    value = {"attemptId": "Stumpy-\u00e9", "dependencyPins": {"uv.lock": "a" * 64}}
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
    path.write_bytes(raw)
    assert bootstrap._read_plan(str(path)) == (raw, value)
