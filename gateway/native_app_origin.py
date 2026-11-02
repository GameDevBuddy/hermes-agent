"""Issue only from the authenticated original per-call prepare action and worker."""
from __future__ import annotations

import threading

from gateway.native_source_construction import require_constructed, reserve_construction
from gateway.native_source_runtime import refuse

_KEY = object()


class OriginalCanonicalOrigin:
    def __init__(self, key, call, prepare):
        from gateway.native_app_call import AppCall
        from gateway.native_app_phases import OriginalCallPhase
        if key is not _KEY or type(call) is not AppCall or type(prepare) is not OriginalCallPhase:
            refuse("native_original_origin_issuer_missing")
        self.call, self.prepare, self.thread = call, prepare, threading.current_thread()
        self.request, self.original, self.context = call.request, call.original, call.context
        self.reader, self.frame, self.arguments = call.reader, prepare.frame, call.original.effective_arguments
        self.connection, self.transport = call.original.connection, call.original.connection.transport
        self.lease = self.pin = self.use = None
        self.phase, self.first_error, self.traceback = "preparing", None, None

    def require_current(self, *, owner=False):
        require_constructed(self)
        call = self.call
        call.require_current()
        self.prepare.require_current(active=not self.prepare.completed)
        self.reader.require_retained()
        self._require_call_binding()
        if owner and threading.current_thread() is not self.thread:
            refuse("native_original_origin_worker_changed")
        if self.lease is not None:
            if self.lease is not call.writer_lease or self.lease.call is not call:
                refuse("native_original_origin_writer_changed")
            self.lease.require_current()

    def _require_call_binding(self):
        call = self.call
        if (call.origin is not self or call.slot.origin is not self or self.first_error is not None
                or not call.authenticated or not call.acknowledged or call.context is not self.context
                or call.reader is not self.reader or call.request is not self.request
                or call.original is not self.original or call.original.effective_arguments is not self.arguments
                or self.prepare.call is not call or call.phases.get("prepare_native_call") is not self.prepare
                or self.prepare.frame is not self.frame or self.frame["command"] != "prepare_native_call"
                or self.original.connection is not self.connection or self.connection.transport is not self.transport
                or self.phase not in ("preparing", "prepared", "result", "committed", "acknowledged", "completed", "releasing")):
            refuse("native_original_origin_changed")

    def failed(self, error):
        # Refusal custody uses the original inputs retained before this initializer.
        # A partial self cannot supply execution authority or replace that call.
        call = self._source_construction.arguments[1]
        if getattr(self, "first_error", None) is None:
            original = call.error if call.error is not None else error
            self.first_error, self.traceback = original, original.__traceback__
        self.phase = "unknown"
        call.original.keeper.blocked = True
        lease, pin, use = getattr(self, "lease", None), getattr(self, "pin", None), getattr(self, "use", None)
        if lease is not None:
            lease.failed(self.first_error)
        if pin is not None:
            from hermes_state_native_book import block
            block(pin.book, self.first_error, pin, use)
        call.failed(self.first_error)


def issue(call, prepare):
    call.require_current()
    prepare.require_current()
    if (prepare.command != "prepare_native_call" or prepare.completed or prepare.error is not None
            or call.protocol_phase != "awaiting_prepare" or threading.current_thread() is not call.worker_thread):
        refuse("native_original_origin_prepare_missing")
    slot = call.slot
    attempt = reserve_construction(call.original.connection, slot._source_constructions, "app_origin",
                                   parent=call, book_owner=slot, book_name="_source_constructions")

    def retain(value):
        call.origin = slot.origin = value

    with attempt:
        origin = attempt.construct(OriginalCanonicalOrigin, _KEY, call, prepare, retain=retain)
    origin.require_current(owner=True)
    return origin
