"""Original stdin can associate a context; the App-owned call boundary stays closed.

Python reference custody, UIDs, digests, SQL rows and local locks are not an
OriginalCanonicalCall, OriginalCanonicalOrigin, commit proof or native permit.
Root must compose actual App call constructors with the same transport/call.
An original-pipe context association supplies none of those per-call rights.
"""
from __future__ import annotations

from hermes_state_runtime import RuntimeStoreError


def require_original_app_context(connection, ref):
    from gateway.native_source_runtime import require_original_connection
    source = require_original_connection(connection)
    if source.authority.profile_id != ref.profile_id:
        raise RuntimeStoreError("profile_mismatch")
    from gateway.native_app_context import require_context
    return require_context(connection, ref)


def require_original_app_call(source, request=None):
    from gateway.native_source_calls import OriginalSourceCall
    if type(source) is not OriginalSourceCall:
        raise RuntimeStoreError("native_original_source_call_missing")
    source.require_call()
    if request is not None and source.request is not request:
        raise RuntimeStoreError("native_original_source_request_changed")
    raise RuntimeStoreError("native_original_app_call_issuer_missing")
