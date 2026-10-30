"""The one existing owned stdin reader, including interleaved context coupling."""
from __future__ import annotations

import asyncio
import re
import threading

from hermes_owned_gateway import MAX_FRAME, OwnedGatewayUnknown, _frame

_KEY = object()
_NONCE = re.compile(r"[0-9a-f]{64}\Z")
_ULID = re.compile(r"[0-9A-HJKMNP-TV-Z]{26}\Z")


class OriginalControlReader:
    def __init__(self, key, source):
        if key is not _KEY:
            raise OwnedGatewayUnknown("original-control-reader-factory-required")
        self.source, self.thread = source, threading.current_thread()
        self.stdin_owner, self.stdin = source.stdin_owner, source.stdin
        self.stdout_owner, self.active = source.stdout_owner, None

    def require_retained(self):
        s = self.source
        if (s._control_reader is not self or s.stdin_owner is not self.stdin_owner
                or s.stdin is not self.stdin or s.stdin_owner.buffer is not self.stdin
                or s.stdout_owner is not self.stdout_owner or s._shutdown or s._control_closed
                or s.phase == "unknown"):
            raise OwnedGatewayUnknown("original-control-reader-changed")

    def require_reading(self):
        self.require_retained()
        if threading.current_thread() is not self.thread or self.thread is self.source.loop_thread:
            raise OwnedGatewayUnknown("original-control-reader-thread-required")

    def bind(self, frame):
        if (set(frame) != {"protocol", "command", "attemptId", "requestId", "nonce", "contextId"}
                or type(frame["requestId"]) is not str or not _ULID.fullmatch(frame["requestId"])
                or type(frame["nonce"]) is not str or not _NONCE.fullmatch(frame["nonce"])
                or type(frame["contextId"]) is not str or not 0 < len(frame["contextId"].encode("utf-8")) <= 256
                or any(ord(ch) < 32 or 127 <= ord(ch) <= 159 for ch in frame["contextId"])):
            raise OwnedGatewayUnknown("invalid-original-context-control-frame")
        from gateway.native_app_context import bind_from_reader, claim_from_reader
        request = claim_from_reader(self, frame)
        try:
            request.coroutine = bind_from_reader(request)
            request.future = asyncio.run_coroutine_threadsafe(request.coroutine, self.source.loop)
            request.future.result()  # no retry, replacement, cancellation or another reader
            self.require_reading()
            if request.error is not None or not request.acknowledged:
                raise OwnedGatewayUnknown("original-context-bind-not-complete")
            self.active = None
        except BaseException as error:
            request.failed(error)
            raise


def read_original_control(source):
    with source._state_lock:
        if source._control_reader_attempt is not None or source._shutdown or source._control_closed:
            raise OwnedGatewayUnknown("unknown-duplicate-or-mismatched-shutdown")
        source._control_reader_attempt = object()
        reader = object.__new__(OriginalControlReader)
        source._control_reader = reader
        OriginalControlReader.__init__(reader, _KEY, source)
    while True:
        frame = _frame(source.stdin)
        if type(frame.get("attemptId")) is not str or frame["attemptId"] != source.contract.attempt_id:
            raise OwnedGatewayUnknown("unknown-duplicate-or-mismatched-shutdown")
        command = frame.get("command")
        if command == "bind_native_context":
            reader.bind(frame)
            continue
        if command != "shutdown" or set(frame) != {"protocol", "command", "attemptId"}:
            raise OwnedGatewayUnknown("unknown-duplicate-or-mismatched-shutdown")
        source._shutdown = True
        # A single matching shutdown is still followed by the original half-close.
        if source.stdin.readline(MAX_FRAME + 1) != b"":
            raise OwnedGatewayUnknown("extra-or-duplicate-original-control-frame")
        source._control_closed = True
        return
