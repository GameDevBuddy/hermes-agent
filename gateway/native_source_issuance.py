"""The missing App-owned original transport/call boundary is deliberately closed.

Python reference custody, UIDs, digests, SQL rows and local locks are not an
OriginalCanonicalCall, OriginalCanonicalOrigin, commit proof or native permit.
Root must compose those actual App constructors with the same transport/call;
there is intentionally no callback, boolean, wire field or installer here.
"""
from __future__ import annotations

from hermes_state_runtime import RuntimeStoreError


def require_original_app_context(connection, ref):
    from gateway.native_source_runtime import require_original_connection
    source = require_original_connection(connection)
    if source.authority.profile_id != ref.profile_id:
        raise RuntimeStoreError("profile_mismatch")
    raise RuntimeStoreError("native_original_app_context_issuer_missing")


def require_original_app_call(source, request=None):
    from gateway.native_source_calls import OriginalSourceCall
    if type(source) is not OriginalSourceCall:
        raise RuntimeStoreError("native_original_source_call_missing")
    source.require_call()
    if request is not None and source.request is not request:
        raise RuntimeStoreError("native_original_source_request_changed")
    raise RuntimeStoreError("native_original_app_call_issuer_missing")
