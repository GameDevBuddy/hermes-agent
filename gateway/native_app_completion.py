"""Terminal original-call custody for later retirement; never an execution issuer."""
from __future__ import annotations

import threading

from gateway.native_source_construction import SourceConstruction, reserve_construction
from gateway.native_source_runtime import refuse

_KEY = object()


class OriginalCallCompletion:
    def __init__(self, key, call, terminal):
        if key is not _KEY or threading.current_thread() is not call.worker_thread:
            refuse("native_original_completion_issuer_missing")
        self.call, self.slot, self.terminal = call, call.slot, terminal
        self.original, self.context = call.original, call.context
        self.proof, self.proof_sha256, self.invocation = call.commit_proof, call.commit_proof_sha256, call.invocation
        self.origin, self.lease, self.reader = call.origin, call.writer_lease, call.reader
        self.pin, self.use = (None, None) if self.origin is None else (self.origin.pin, self.origin.use)
        self.stdin_owner, self.stdout_owner = call.reader.stdin_owner, call.reader.stdout_owner
        self.confirmed, self.publication = False, None
        self.declined = call.protocol_phase == "declined"
        call.require_current()

    def confirm(self):
        if self.confirmed or threading.current_thread() is not self.call.worker_thread:
            refuse("native_original_completion_reentry")
        self._require_terminal()
        self.confirmed = True
        self.call.terminal = self.terminal

    def _require_terminal(self):
        call = self.call
        if (call.error is not None or call.reader is not self.reader or call.slot is not self.slot
                or call.original is not self.original or call.context is not self.context
                or call.commit_proof is not self.proof or call.commit_proof_sha256 != self.proof_sha256
                or call.invocation is not self.invocation):
            refuse("native_original_completion_changed")
        if self.declined:
            if (call.protocol_phase != "declined" or call.decline_frame is not self.terminal
                    or call.invocation is not None or self.origin is not None or self.lease is not None):
                refuse("native_original_completion_changed")
            return
        release = self.terminal
        if (call.protocol_phase != "released" or release is not call.phases.get("release_native_call")
                or not release.completed or release.error is not None
                or call.origin is not self.origin or self.origin.phase != "released"
                or self.origin.pin is not self.pin or self.origin.use is not self.use
                or call.writer_lease is not self.lease or self.lease.phase != "released" or self.lease.error is not None
                or self.origin.pin.phase != "released" or self.origin.use.phase != "released"):
            refuse("native_original_completion_changed")
        published = release.publications.get("native_call_released")
        if published is None or published["proofSha256"] != call.commit_proof_sha256:
            refuse("native_original_completion_publication_missing")
        if self.publication is None:
            self.publication = published
        elif published is not self.publication:
            refuse("native_original_completion_publication_changed")

    def require_retained(self):
        attempt = getattr(self, "_source_construction", None)
        if (type(attempt) is not SourceConstruction or attempt.value is not self or attempt.error is not None
                or attempt.phase != "ready" or attempt.runtime.blocked or not self.confirmed
                or self.call.completion is not self or self.call.terminal is not self.terminal
                or self.reader.source._control_reader is not self.reader
                or self.reader.stdin_owner is not self.stdin_owner or self.reader.stdout_owner is not self.stdout_owner
                or self.reader.source._control_error is not None):
            refuse("native_original_completion_changed")
        # The original runtime may now be draining/retired. This is a retained
        # terminal record; live execution still requires the ordinary live gates.
        attempt.require_retained()
        self._require_source_retained()
        self._require_terminal()
        return self.call

    def _require_source_retained(self):
        for value in (self.call, self.original, self.slot):
            attempt = getattr(value, "_source_construction", None)
            if (type(attempt) is not SourceConstruction or attempt.value is not value
                    or attempt.phase != "ready" or attempt.error is not None):
                refuse("native_original_completion_source_changed")
            attempt.require_retained()


def reserve(call, terminal):
    slot = call.slot
    attempt = reserve_construction(call.original.connection, slot._source_constructions, "completion",
                                   parent=call, book_owner=slot, book_name="_source_constructions")
    with attempt:
        value = attempt.construct(OriginalCallCompletion, _KEY, call, terminal,
                                  retain=lambda value: setattr(call, "completion", value))
    return value
