"""Canonical proof publication from the actual original producer, never a receipt RPC."""
from __future__ import annotations

import hashlib
import json

from gateway.native_source_runtime import refuse


def _publish(origin, phase, canonical, commit):
    from gateway.native_app_phases import event
    origin.require_current(owner=True)
    lease, call = origin.lease, origin.call
    lease.require_owner()
    if (lease.phase != "held" or commit is None or not any(value is commit for value in lease.commits)
            or commit.connection is not lease.connection or commit.thread is not origin.thread
            or lease.connection.in_transaction):
        refuse("native_original_canonical_transaction_missing")
    payload = {"schema": "gdb-canonical-native-commit-v1", "phase": phase,
               "attemptId": origin.frame["attemptId"], "contextId": origin.frame["contextId"],
               "nonce": origin.frame["nonce"], "serverRequestId": origin.frame["serverRequestId"],
               "wireSha256": call.digest, "invocationId": call.invocation[0], "invocationSequence": call.invocation[1],
               "writerLeaseId": lease.lease_id, "writerExclusion": lease.exclusion(), "canonical": canonical}
    wire = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    digest = hashlib.sha256(wire.encode("ascii")).hexdigest()
    if phase == "initial":
        if call.initial_proof is not None:
            refuse("native_original_initial_proof_reentry")
        call.initial_proof = payload
        name = "native_call_prepared"
    else:
        if call.commit_proof is not None or call.initial_proof["writerLeaseId"] != lease.lease_id:
            refuse("native_original_commit_proof_reentry")
        call.commit_proof, call.commit_proof_sha256 = payload, digest
        name = "native_call_committed"
    # The SAME retained prepare request carries initial and after-save producer responses.
    return event(origin.prepare, name, proof=payload, proof_sha256=digest)


def initial(origin):
    origin.require_current(owner=True)
    pin, use = origin.pin, origin.use
    if pin.phase != "reserved" or use.phase != "held" or use.request is not origin.request:
        refuse("native_original_initial_relation_missing")
    origin.lease.require_owner()
    commit = origin.lease.commits[-1]
    canonical = {"sourcePinId": pin.pin_id, "scope": pin.capture[0]["scope"],
                 "assistant": pin.capture[0]["assistant"], "leases": pin.capture[0]["leases"],
                 "transactionSequence": commit.sequence}
    return _publish(origin, "initial", canonical, commit)


def after_save(origin):
    origin.require_current(owner=True)
    use, pending = origin.use, origin.use.pending
    if (use.phase != "observed" or use.observation is not pending.receipt or not pending.publication_observed
            or not use.writer_returned or use.original_commit is None or pending.message is not origin.call.slot.projection):
        refuse("native_original_saved_relation_missing")
    canonical = {"sourcePinId": origin.pin.pin_id, "row": pending.receipt,
                 "transactionSequence": use.original_commit.sequence}
    return _publish(origin, "committed", canonical, use.original_commit)
