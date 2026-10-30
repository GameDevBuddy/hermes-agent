"""Exact v2 data shape and bounded input refusals; observations never become runtime authority."""
import json

import pytest

import hermes_retained_bootstrap as policy

pytest_plugins = ("tests.retained_observations_fixture",)


def refused(observed, reason=None, raw=None):
    with pytest.raises(policy.BootstrapRefused) as caught:
        observed.bind(raw)
    if reason is not None:
        assert caught.value.reason == reason
    assert policy.current() is None and policy._PLAN_BYTES is None
    expected = raw if raw is not None else json.dumps(observed.plan).encode("utf-8")
    assert observed.plan_path.read_bytes() == expected


@pytest.mark.parametrize("schema", [True, False, 2.0, "2", None, 0, 3, []])
def test_unknown_or_noninteger_schemas_cannot_fallback(observed, schema):
    observed.plan["schemaVersion"] = schema
    refused(observed, "unsupported-bootstrap-contract")


@pytest.mark.parametrize("field", [
    "schemaVersion", "policy", "root", "home", "environment", "site", "selection", "factsSha256",
    "packageManifestSha256", "sourceRevision", "dependencyPins", "attemptId", "inputPolicy",
])
def test_every_original_v2_field_is_required(observed, field):
    del observed.plan[field]
    refused(observed, "unsupported-bootstrap-contract")


@pytest.mark.parametrize("field,value", [
    ("writerExclusion", False), ("writerExclusion", None), ("input_policy", "pinned-input-observations-v1"),
    ("launchPermission", False), ("unknown", None),
])
def test_mixed_and_extra_contract_fields_are_rejected(observed, field, value):
    observed.plan[field] = value
    refused(observed, "unsupported-bootstrap-contract")


@pytest.mark.parametrize("value", [None, False, 1, {}, "writer-exclusion", "pinned-input-observations-v2"])
def test_exact_observation_policy_is_required(observed, value):
    observed.plan["inputPolicy"] = value
    refused(observed, "required-pinned-input-observations-policy-missing")


@pytest.mark.parametrize("value", [None, False, {}, "other-policy"])
def test_same_retained_policy_is_required(observed, value):
    observed.plan["policy"] = value
    refused(observed, "required-pinned-input-observations-policy-missing")


@pytest.mark.parametrize("value", [False, None, 1, "true"])
def test_legacy_schema_is_not_repaired_or_downgraded(observed, value):
    observed.plan["schemaVersion"] = 1
    del observed.plan["inputPolicy"]
    observed.plan["writerExclusion"] = value
    refused(observed, "required-caller-writer-declaration-missing")


@pytest.mark.parametrize("field,value", [
    ("attemptId", ""), ("attemptId", "a" * 129), ("attemptId", 1), ("sourceRevision", "A" * 40),
    ("sourceRevision", "a" * 39), ("packageManifestSha256", "a" * 63), ("factsSha256", False),
    ("dependencyPins", []), ("selection", "other"), ("root", "relative-path"),
])
def test_existing_typed_binding_and_pin_guards_remain(observed, field, value):
    observed.plan[field] = value
    refused(observed)


@pytest.mark.parametrize("raw,reason", [
    (b'{"schemaVersion":2,"schemaVersion":2}', "duplicate-bootstrap-plan-field"),
    (b'{"inputPolicy":"x","input\\u0050olicy":"y"}', "duplicate-bootstrap-plan-field"),
    (b'{"dependencyPins":{"uv.lock":"x","uv.lock":"y"}}', "duplicate-bootstrap-plan-field"),
    (b'{"schemaVersion":NaN}', "non-finite-bootstrap-plan-value"),
    (b'{"schemaVersion":Infinity}', "non-finite-bootstrap-plan-value"),
    (b'{"schemaVersion":-Infinity}', "non-finite-bootstrap-plan-value"),
    (b'{"x":"\xff"}', "cannot-read-required-json"),
    (b'{"schemaVersion":2,}', "cannot-read-required-json"),
    (b'[]', "required-json-is-not-an-object"),
])
def test_v2_uses_existing_bounded_duplicate_and_json_parser(observed, raw, reason):
    refused(observed, reason, raw)


def test_complete_v2_bytes_remain_bounded_before_parse(observed):
    raw = json.dumps(observed.plan).encode("utf-8") + b" " * (128 * 1024)
    refused(observed, "bootstrap-plan-too-large", raw)
