"""Cooperative held-reference fences; never OS or remote-lifetime proof."""
from __future__ import annotations

from hermes_state_runtime import RuntimeStoreError


def reject_native_runner_stop(runner):
    runtime = getattr(runner, "_gdb_original_source_runtime", None)
    if runtime is None:
        return
    with runtime.lock:
        from gateway.native_app_worker import require_completed_slot
        for server in runtime.servers.values():
            if server is None:
                raise RuntimeStoreError("native_original_source_runner_retirement_held")
            for pair in server.calls.values():
                if type(pair) is not tuple or len(pair) != 2:
                    raise RuntimeStoreError("native_original_source_runner_retirement_held")
                try:
                    call = require_completed_slot(pair[0])
                    if call.original is not pair[1]:
                        raise RuntimeStoreError("native_original_source_runner_retirement_held")
                except RuntimeStoreError as error:
                    raise RuntimeStoreError("native_original_source_runner_retirement_held") from error
    for authority in tuple(runtime.authorities.values()):
        if authority is not None:
            reject_native_authority_retirement(authority)
    with runtime.lock:
        runtime.retired = True


def reject_native_authority_retirement(authority):
    records = getattr(authority, "_native_turn_retention", {})
    if any(record.ever_native and not record.completed for record in records.values()):
        raise RuntimeStoreError("native_original_source_authority_retirement_held")


def retire_native_pin(pin):
    from agent.native_original_use import NativeRetirementPin
    if type(pin) is not NativeRetirementPin:
        raise RuntimeStoreError("native_original_pin_retirement_unavailable")
    from gateway.native_app_release import release
    return release(pin)


def require_native_pin_for_invoke(pin):
    from agent.native_original_use import NativeRetirementPin
    if type(pin) is not NativeRetirementPin:
        raise RuntimeStoreError("native_original_app_call_issuer_missing")
    from gateway.native_source_issuance import require_original_app_call
    origin = require_original_app_call(pin.source_original_call)
    origin.require_current(owner=True)
    if origin.pin is not pin or origin.use is not pin.active_use or pin.phase != "reserved":
        raise RuntimeStoreError("native_original_app_call_issuer_missing")
    origin.lease.exclusion()
    return origin
