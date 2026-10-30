"""Actual schema-two install/current/input observations; no exclusion or runtime permission."""
import json

import pytest

import hermes_retained_bootstrap as policy

pytest_plugins = ("tests.retained_observations_fixture",)


def test_v2_install_preserves_exact_original_observation_contract(observed):
    raw = b"\xef\xbb\xbf" + json.dumps(observed.plan, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    contract = observed.bind(raw)
    assert policy.current() is policy.current_for(observed.root) is contract
    assert contract.input_policy == "pinned-input-observations-v1"
    assert policy.original_plan_bytes(contract) is policy._PLAN_BYTES
    assert policy.original_plan_bytes(contract) == raw
    contract.require_selection(contract.state, observed.environment, observed.site)
    with pytest.raises(AttributeError):
        contract.input_policy = "changed"
    with pytest.raises(policy.BootstrapRefused) as caught:
        observed.bind()
    assert caught.value.reason == "bootstrap-contract-already-selected"


def test_original_plan_is_not_reread_or_replaced_after_observation_binding(observed):
    raw = json.dumps(observed.plan).encode("utf-8")
    contract = observed.bind(raw)
    observed.plan_path.write_bytes(b"different current-point metadata")
    assert policy.original_plan_bytes(contract) == raw
    assert contract.input_policy == "pinned-input-observations-v1"


def test_current_consumer_retains_existing_root_and_home_binding_guards(observed, monkeypatch):
    contract = observed.bind()
    with pytest.raises(policy.BootstrapRefused) as caught:
        policy.current_for(observed.root.parent)
    assert caught.value.reason == "bootstrap-root-changed"
    monkeypatch.setenv("HERMES_HOME", str(observed.root))
    with pytest.raises(policy.BootstrapRefused) as caught:
        policy.current_for(observed.root)
    assert caught.value.reason == "home-was-not-explicitly-bound"
    assert policy.current() is contract
