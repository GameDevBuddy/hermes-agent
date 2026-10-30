"""Cooperative held-reference fences; never OS or remote-lifetime proof."""
from __future__ import annotations

from hermes_state_runtime import RuntimeStoreError


def reject_native_runner_stop(runner):
    runtime = getattr(runner, "_gdb_original_source_runtime", None)
    if runtime is None:
        return
    with runtime.lock:
        if any(server.calls for server in runtime.servers.values() if server is not None):
            raise RuntimeStoreError("native_original_source_runner_retirement_held")
    for authority in tuple(runtime.authorities.values()):
        if authority is not None:
            reject_native_authority_retirement(authority)
    with runtime.lock:
        runtime.retired = True


def reject_native_authority_retirement(authority):
    records = getattr(authority, "_native_turn_retention", {})
    if any(record.ever_native for record in records.values()):
        raise RuntimeStoreError("native_original_source_authority_retirement_held")


def retire_native_pin(_pin):
    raise RuntimeStoreError("native_original_pin_retirement_unavailable")


def require_native_pin_for_invoke(_pin):
    raise RuntimeStoreError("native_original_app_call_issuer_missing")
