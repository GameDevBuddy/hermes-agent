"""Bind the implemented normal runtime to its original sealed caller.

These checks admit only the managed runtime body. They issue no App Origin,
native approval or commit proof; those still require the per-call consumers.
Input hashes are point observations, never writer-exclusion authority.
"""
from __future__ import annotations

import hashlib
import sys

import hermes_owned_gateway as owned
import hermes_retained_bootstrap as bootstrap


def _refuse(reason):
    raise owned.OwnedGatewayUnknown("canonical-managed-normal-lifecycle-not-composed:" + reason)


def require_original_entry(source):
    if (type(source) is not owned.OwnedGatewaySource
            or owned.current(source.contract) is not source):
        _refuse("original-source-required")
    contract = source.contract
    if type(contract) is not bootstrap.RetainedBootstrap or bootstrap.current() is not contract:
        _refuse("original-sealed-bootstrap-required")
    if contract.input_policy != "pinned-input-observations-v1":
        _refuse("pinned-input-observation-policy-required")
    from gateway.owned_entry_construction import require_prepared_entry
    entry = require_prepared_entry(source)
    if hashlib.sha256(bootstrap.original_plan_bytes(contract)).hexdigest() != source.plan_sha256:
        _refuse("original-bootstrap-plan-changed")
    if (source.stdin_owner is not sys.stdin or source.stdin is not sys.stdin.buffer
            or (sys.stdout is not source.stdout_owner and sys.stdout is not source.stderr_owner)
            or source.stderr_owner is not sys.stderr):
        _refuse("original-private-streams-required")
    from hermes_owned_gateway_windows import JobAssociation
    if type(source.job) is not JobAssociation:
        _refuse("original-windows-job-association-required")
    if source._attach_attempt is None or source.job is not source._original_job or source._attach_error is not None:
        _refuse("original-attached-job-was-replaced-or-failed")
    source.require_attached(contract)
    contract.require_inputs()
    contract.require_bootstrap_tmp()
    # Reconsume the retained relationships after input observers return. No
    # readiness flag, current-point tuple or file/Job observation becomes proof.
    source.require_attached(contract)
    if require_prepared_entry(source) is not entry or source._startup_error is not None:
        _refuse("original-entry-or-startup-state-changed")
    return entry
