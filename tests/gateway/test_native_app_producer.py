"""Original authenticated source phases over modeled IO and actual fresh SQLite, no native dispatch."""
import hashlib
import json
import sqlite3
from copy import copy

import pytest

from gateway.native_app_worker import require_completed_slot
from gateway.native_app_context_records import source_for_original_worker
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_app_producer_fixture import ProducerCase


def test_original_private_prepare_actual_save_ack_completion_and_release(monkeypatch, tmp_path):
    case = ProducerCase(monkeypatch, tmp_path)
    try:
        case.begin()
        assert case.call.authenticated and case.call.origin is None and case.slot._native_pin is None
        assert case.call.original.effective_arguments is case.dispatch.args
        case.phase("prepare_native_call", 5)
        prepared = case.run(case.notification("native_call_prepared"))
        lease = case.call.writer_lease
        assert lease.phase == "held" and case.call.origin.prepare is case.call.phases["prepare_native_call"]
        assert prepared["proof"]["phase"] == "initial"
        encoded = json.dumps(prepared["proof"], sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        assert hashlib.sha256(encoded.encode("ascii")).hexdigest() == prepared["proofSha256"]
        with pytest.raises(RuntimeStoreError, match="worker_changed"):
            source_for_original_worker(case.connection, case.call)
        other = sqlite3.connect(case.db.db_path, timeout=0, isolation_level=None)
        try:
            try:
                other.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as error:
                assert "locked" in str(error)
            else:
                raise AssertionError("Original native preparation did not exclude another SQLite writer")
        finally:
            other.close()
        delivered = case.deliver_result()
        committed = case.run(case.notification("native_call_committed"))
        assert case.call.result_frame is delivered
        assert committed["proof"]["phase"] == "committed"
        assert committed["proof"]["writerLeaseId"] == prepared["proof"]["writerLeaseId"] == lease.lease_id
        assert case.pending.message is case.message and case.slot.projection is case.message
        assert case.pending.receipt is case.slot._native_active_use.observation
        assert case.canonical_frames[-1]["params"]["payload"] == case.pending.receipt
        assert case.slot._native_active_use.original_commit.callback is not None
        for index, (command, event) in enumerate((
            ("ack_native_call", "native_call_acknowledged"),
            ("complete_native_call", "native_call_completed"),
            ("release_native_call", "native_call_released")), 6):
            case.phase(command, index)
            response = case.run(case.notification(event))
            assert response["proofSha256"] == committed["proofSha256"]
        case.run(case.worker_done.wait())
        assert case.worker_error is None and case.control_error is None
        assert lease.phase == "released" and case.db._native_read_gate.lease is None
        assert require_completed_slot(case.slot) is case.call
        with pytest.raises(RuntimeStoreError, match="completion_changed"):
            copy(case.call.completion).require_retained()
        assert case.db._read_one("SELECT content FROM messages WHERE id=?", (case.message["_row_id"],))[0] == case.message["content"]
        assert case.db._read_one("SELECT proof_sha256 FROM native_original_releases")[0] == committed["proofSha256"]
    finally:
        case.close()
    assert case.keeper.completed and case.offer_task.done()
    assert require_completed_slot(case.slot) is case.call
    from gateway.native_source_runtime import retire_original_listener
    parent = case.call.original.connection.parent
    parent.runner._draining = True
    retire_original_listener(parent, request_stop=True)
    assert parent.retiring and parent.runtime.retired
    with pytest.raises(RuntimeStoreError):
        case.call.origin.require_current()


def test_missing_canonical_delivery_preserves_committed_row_and_refuses_proof(monkeypatch, tmp_path):
    case = ProducerCase(monkeypatch, tmp_path)
    try:
        case.begin()
        case.phase("prepare_native_call", 5)
        case.run(case.notification("native_call_prepared"))
        case.live.event_stream.observers.clear()
        original = case.deliver_result()
        case.run(case.worker_done.wait())
        assert case.worker_error is not None
        assert case.call.result_frame is original and case.call.raw_result == "original native result"
        assert case.call.commit_proof is None and case.call.commit_proof_sha256 is None
        assert case.pending.receipt is not None and case.pending.publication_observed is False
        assert case.call.origin.phase == "unknown" and case.call.writer_lease.phase == "unknown"
        assert case.db._native_read_gate.lease is case.call.writer_lease
        assert case.slot._native_pin.phase == "unknown" and case.call.terminal is None
        assert not any(frame["event"] in ("native_call_committed", "native_call_released") for frame in case.events())
    finally:
        case.close()


def test_changed_original_arguments_after_prepare_cannot_receive_result_or_release(monkeypatch, tmp_path):
    case = ProducerCase(monkeypatch, tmp_path)
    try:
        case.begin()
        case.phase("prepare_native_call", 5)
        case.run(case.notification("native_call_prepared"))
        original_arguments = case.dispatch.args
        original_arguments["original"] = "changed after preparation"
        with pytest.raises(RuntimeStoreError):
            case.deliver_result()
        case.run(case.worker_done.wait())
        assert case.worker_error is not None and case.call.error is not None
        assert case.call.original.effective_arguments is original_arguments
        assert case.call.result_frame is None and case.call.commit_proof is None and case.call.terminal is None
        assert case.call.writer_lease.phase == "unknown" and case.db._native_read_gate.lease is case.call.writer_lease
        assert case.slot._native_pin.phase == "unknown" and case.keeper.blocked
        assert not any(frame["event"] in ("native_call_committed", "native_call_released") for frame in case.events())
    finally:
        case.close()


def test_wrong_ack_proof_retains_original_saved_result_and_writer(monkeypatch, tmp_path):
    case = ProducerCase(monkeypatch, tmp_path)
    try:
        case.begin()
        case.phase("prepare_native_call", 5)
        case.run(case.notification("native_call_prepared"))
        original_result = case.deliver_result()
        original_proof = case.run(case.notification("native_call_committed"))
        retained_proof = case.call.commit_proof
        case.phase("ack_native_call", 6, proofSha256="0" * 64)
        case.run(case.worker_done.wait())
        assert case.worker_error is not None and "proof_changed" in str(case.call.error)
        assert case.call.origin.first_error is case.call.error
        assert case.slot._native_pin.book.first_exception is case.call.error
        assert case.agent._last_persistence_error_cause == "native_sql_unknown"
        assert case.call.result_frame is original_result
        assert case.call.commit_proof is retained_proof and retained_proof == original_proof["proof"]
        assert case.call.writer_lease.phase == "unknown" and case.db._native_read_gate.lease is case.call.writer_lease
        assert case.pending.publication_observed and case.call.terminal is None
        assert not any(frame["event"] in ("native_call_acknowledged", "native_call_completed", "native_call_released")
                       for frame in case.events())
    finally:
        case.close()


def test_declined_original_call_saves_denial_without_origin_or_sql_lease(monkeypatch, tmp_path):
    case = ProducerCase(monkeypatch, tmp_path)
    try:
        case.begin()
        original_decline = case.decline()
        case.run(case.worker_done.wait())
        assert case.worker_error is None and case.control_error is None
        assert case.call.decline_frame is original_decline and case.call.terminal is original_decline
        assert case.call.origin is None and case.call.writer_lease is None and case.slot._native_pin is None
        assert case.call.initial_proof is None and case.call.commit_proof is None
        # A declined request keeps the existing unissued ordinary-error row policy.
        # Saving that history row does not mint a native canonical witness or proof.
        assert case.pending.message is None and case.pending.receipt is None
        assert "native_request_declined" in case.message["content"]
        assert case.db._read_one("SELECT content FROM messages WHERE id=?", (case.message["_row_id"],))[0] == case.message["content"]
        assert require_completed_slot(case.slot) is case.call
        assert not any(frame["event"] in ("native_call_prepared", "native_call_committed", "native_call_released")
                       for frame in case.events())
    finally:
        case.close()
    assert case.keeper.completed and case.offer_task.done()


@pytest.mark.parametrize("constructor", ("origin", "lease", "completion"))
def test_partial_producer_constructor_preserves_original_self_inputs_and_first_error(monkeypatch, tmp_path, constructor):
    from gateway.native_app_origin import OriginalCanonicalOrigin
    from gateway.native_app_completion import OriginalCallCompletion
    from hermes_state_native_lease import NativeWriterLease
    factory = {"origin": OriginalCanonicalOrigin, "lease": NativeWriterLease, "completion": OriginalCallCompletion}[constructor]
    first, partials = ValueError("original " + constructor + " initializer failed"), []

    def original_failure(self, *inputs):
        partials.append(self)
        assert self._source_construction.value is self
        assert len(self._source_construction.arguments) == len(inputs)
        assert all(before is after for before, after in zip(self._source_construction.arguments, inputs))
        raise first

    monkeypatch.setattr(factory, "__init__", original_failure)
    case = ProducerCase(monkeypatch, tmp_path)
    try:
        case.begin()
        case.phase("prepare_native_call", 5)
        if constructor == "completion":
            case.run(case.notification("native_call_prepared"))
            case.deliver_result()
            case.run(case.notification("native_call_committed"))
            case.phase("ack_native_call", 6)
            case.run(case.notification("native_call_acknowledged"))
            case.phase("complete_native_call", 7)
            case.run(case.notification("native_call_completed"))
            case.phase("release_native_call", 8)
        case.run(case.worker_done.wait())
        assert len(partials) == 1
        partial, key = partials[0], {"origin": "app_origin", "lease": "writer_lease", "completion": "completion"}[constructor]
        assert case.slot._source_constructions[key] is partial
        assert partial._source_construction.error is first and partial._source_construction.phase == "blocked"
        assert case.worker_error is not None and case.call.error is first
        if constructor != "completion":
            assert case.worker_error is first
        else:
            assert case.agent._last_persistence_error_cause == "native_sql_unknown"
        assert case.call.origin.first_error is first and case.call.origin.traceback is not None
        assert case.keeper.blocked and case.call.terminal is None
        if constructor == "completion":
            assert case.db._native_read_gate.lease is case.call.writer_lease
            assert case.call.writer_lease.phase == "unknown" and case.slot._native_pin.phase == "unknown"
        assert not any(frame["event"] == "native_call_released" for frame in case.events())
    finally:
        case.close()
