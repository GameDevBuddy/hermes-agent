"""Require the retained context or actual authenticated per-call Origin.

Context association, reference books, digests and SQL rows alone supply no
per-call rights. The producer must retain the original App call and prepare.
"""
from __future__ import annotations

from hermes_state_runtime import RuntimeStoreError


def require_original_app_context(connection, ref, *, source_owner=None):
    from gateway.native_source_runtime import require_original_connection
    source = require_original_connection(connection)
    if source.authority.profile_id != ref.profile_id:
        raise RuntimeStoreError("profile_mismatch")
    from gateway.native_app_context import require_context
    return require_context(connection, ref) if source_owner is None else require_context(connection, ref, source_owner=source_owner)


def require_original_app_call(source, request=None):
    from gateway.native_source_calls import OriginalSourceCall
    if type(source) is not OriginalSourceCall:
        raise RuntimeStoreError("native_original_source_call_missing")
    source.require_call()
    if request is not None and source.request is not request:
        raise RuntimeStoreError("native_original_source_request_changed")
    from gateway.native_app_call import AppCall
    from gateway.native_app_origin import OriginalCanonicalOrigin
    call = source.slot._source_constructions.get("app_call")
    origin = None if type(call) is not AppCall else call.origin
    if type(origin) is not OriginalCanonicalOrigin or call.original is not source:
        raise RuntimeStoreError("native_original_app_call_issuer_missing")
    origin.require_current()
    return origin
