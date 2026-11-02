"""Negative original-caller checks only; modeled Jobs never admit a runtime."""
import io
from dataclasses import replace
from types import SimpleNamespace as Data

import pytest

import hermes_owned_gateway as owned
import hermes_retained_bootstrap as bootstrap
from gateway import run_owned


@pytest.fixture
def selection(monkeypatch, tmp_path):
    contract = bootstrap.RetainedBootstrap(
        tmp_path / "payload", tmp_path / "home", tmp_path / "environment", tmp_path / "site",
        "payload", None, "f" * 64, "1" * 40, (), "unit-attempt", "pinned-input-observations-v1")
    plan, calls = b"{}", []
    monkeypatch.setattr(bootstrap, "_BOUND", contract)
    monkeypatch.setattr(bootstrap, "_PLAN_BYTES", plan)
    monkeypatch.setattr(owned.sys, "stdin", Data(buffer=io.BytesIO()))
    value = owned.OwnedGatewaySource(contract, plan)
    value.phase, value.job = "attached", Data(require_current_member=lambda: calls.append("model-job"))
    monkeypatch.setattr(owned, "_CURRENT", value)
    entry = owned.prepare_lifecycle_entry(value)  # Metadata custody only, never runtime admission.
    calls.clear()
    return Data(source=value, contract=contract, entry=entry, calls=calls)


def test_fabricated_source_methods_are_never_called_by_runtime_admission():
    calls = []
    fake = Data(contract=object(), require_attached=lambda value: calls.append(value))
    with pytest.raises(owned.OwnedGatewayUnknown, match="not-composed:original-source"):
        owned.require_lifecycle_available(fake)
    assert calls == []


def test_copied_bootstrap_cannot_borrow_original_selection(selection, monkeypatch):
    monkeypatch.setattr(bootstrap, "_BOUND", replace(selection.contract))
    with pytest.raises(owned.OwnedGatewayUnknown, match="original-sealed-bootstrap"):
        owned.require_lifecycle_available(selection.source)
    assert selection.calls == []


def test_legacy_policy_cannot_be_promoted_to_runtime_authority(selection, monkeypatch):
    contract = replace(selection.contract, input_policy=None)
    selection.source.contract = contract
    monkeypatch.setattr(bootstrap, "_BOUND", contract)
    with pytest.raises(owned.OwnedGatewayUnknown, match="observation-policy"):
        owned.require_lifecycle_available(selection.source)
    assert selection.calls == []


def test_replaced_original_plan_refuses_before_any_job_callback(selection, monkeypatch):
    monkeypatch.setattr(bootstrap, "_PLAN_BYTES", b'{"replacement":true}')
    with pytest.raises(owned.OwnedGatewayUnknown, match="original-bootstrap-plan-changed"):
        owned.require_lifecycle_available(selection.source)
    assert selection.calls == []


def test_replaced_private_input_refuses_before_any_job_callback(selection, monkeypatch):
    original = selection.source.stdin
    monkeypatch.setattr(owned.sys, "stdin", Data(buffer=io.BytesIO()))
    with pytest.raises(owned.OwnedGatewayUnknown, match="original-private-streams"):
        owned.require_lifecycle_available(selection.source)
    assert selection.source.stdin is original and selection.calls == []


def test_model_job_never_admits_an_original_metadata_entry(selection):
    with pytest.raises(owned.OwnedGatewayUnknown, match="original-windows-job-association"):
        owned.require_lifecycle_available(selection.source)
    assert selection.calls == [] and selection.source.entry is selection.entry


def test_canonical_stdout_to_stderr_route_keeps_private_output_and_still_refuses_model_job(selection, monkeypatch):
    original_output = selection.source.stdout_owner
    monkeypatch.setattr(owned.sys, "stdout", selection.source.stderr_owner)
    with pytest.raises(owned.OwnedGatewayUnknown, match="original-windows-job-association"):
        owned.require_lifecycle_available(selection.source)
    assert selection.source.stdout_owner is original_output and selection.calls == []


def test_replaced_typed_job_refuses_before_loading_or_querying_native_api(selection):
    from hermes_owned_gateway_windows import JobAssociation
    selection.source.job = object.__new__(JobAssociation)
    selection.source._original_job = object()
    selection.source._attach_attempt = object()
    with pytest.raises(owned.OwnedGatewayUnknown, match="attached-job-was-replaced"):
        owned.require_lifecycle_available(selection.source)
    assert selection.calls == []


def test_mutated_entry_callback_never_runs_or_reaches_job_query(selection):
    calls = []
    selection.entry.main = lambda value: calls.append(value)
    with pytest.raises(owned.OwnedGatewayUnknown, match="not-composed"):
        selection.entry.run()
    assert calls == [] and selection.calls == ["model-job"]


def test_unretained_entry_cannot_borrow_the_original_prepared_entry(selection):
    other = run_owned.ManagedEntry(selection.source)
    selection.calls.clear()
    with pytest.raises(owned.OwnedGatewayUnknown, match="foreign-entry"):
        other.run()
    assert selection.source.entry is selection.entry and other is not selection.entry
    assert selection.calls == ["model-job"]
