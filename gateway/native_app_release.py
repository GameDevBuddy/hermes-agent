"""Retire the original SQL fence only after the original authenticated release action."""
from __future__ import annotations

from gateway.native_source_runtime import refuse


def release(pin):
    from gateway.native_source_issuance import require_original_app_call
    from hermes_state_native_book import guarded_write
    origin = require_original_app_call(pin.source_original_call)
    origin.require_current(owner=True)
    call, use = origin.call, origin.use
    phase = call.phases.get("release_native_call")
    if (origin.pin is not pin or origin.phase != "releasing" or call.protocol_phase != "completed"
            or use is None or use.phase != "observed" or use.observation is not use.pending.receipt
            or use.original_commit is None or call.commit_proof is None
            or phase is None or phase.completed or phase.frame["proofSha256"] != call.commit_proof_sha256):
        refuse("native_original_release_unavailable")
    phase.require_current()
    values = (pin.pin_id, call.invocation[0], call.invocation[1], call.commit_proof_sha256, phase.frame["requestId"])

    def original(conn):
        origin.require_current(owner=True)
        if conn is not origin.lease.connection or not conn.in_transaction:
            refuse("native_original_release_transaction_missing")
        conn.execute("INSERT INTO native_original_releases VALUES (?,?,?,?,?)", values)

    guarded_write(pin.book, original)
    # SQL records observe release; only the authenticated original action advances custody.
    pin.settlement, pin.settlement_sha256 = values, call.commit_proof_sha256
    pin.phase = use.phase = "released"
    return origin
