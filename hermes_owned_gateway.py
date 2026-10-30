"""Original-stdin managed foreground association, selected before Hermes imports.

The bootstrap plan and wire metadata supply no authority. A source association
also requires the actual original Job membership and, later, the real canonical
runner/current asyncio task. It grants no native invoke, approval, or pin rights.
"""
from __future__ import annotations

import hashlib
import json
import struct
import sys
import threading

PROTOCOL = "gdb-owned-gateway-v1"
MAX_FRAME = 8192
_CURRENT = None


class OwnedGatewayUnknown(RuntimeError):
    pass


def _pairs(items):
    value = {}
    for key, item in items:
        if key in value:
            raise OwnedGatewayUnknown("duplicate-control-key")
        value[key] = item
    return value


def _frame(stream):
    raw = stream.readline(MAX_FRAME + 1)
    if type(raw) is not bytes or not raw or len(raw) > MAX_FRAME or not raw.endswith(b"\n"):
        raise OwnedGatewayUnknown("original-control-eof-short-or-oversized")
    try:
        value = json.loads(raw.decode("utf-8", "strict"), object_pairs_hook=_pairs)
    except (ValueError, UnicodeError) as error:
        raise OwnedGatewayUnknown("invalid-original-control-json") from error
    if type(value) is not dict or value.get("protocol") != PROTOCOL:
        raise OwnedGatewayUnknown("unknown-original-control-protocol")
    return value


class OwnedGatewaySource:
    def __init__(self, contract, plan_bytes):
        self.contract = contract
        self.plan_sha256 = hashlib.sha256(plan_bytes).hexdigest()
        self.stdin_owner = sys.stdin
        self.stdin = sys.stdin.buffer
        self.stdout_owner, self.stderr_owner = sys.stdout, sys.stderr
        self.job = self.runner = self.task = self.loop = None
        self.phase, self.error = "attaching", None
        self._state_lock = threading.RLock()
        self._attach_attempt = None
        self._shutdown = False
        self._control_closed = False
        self._ready_emitted = self._complete_emitted = False
        self._unknown_hold = threading.Event()
        self.settlement = None
        self.child_transports = []

    def unknown(self, reason):
        with self._state_lock:
            if self.error is None:
                self.error = reason
            self.phase = "unknown"
            return OwnedGatewayUnknown(self.error)

    def attach(self):
        with self._state_lock:
            if self._attach_attempt is not None:
                raise self.unknown("original-attach-already-attempted")
            # This original object's one attempt is consumed even if reading or
            # association fails. It must never accept another control frame.
            self._attach_attempt = object()
            if self is not _CURRENT or self.phase != "attaching" or self.job is not None:
                raise self.unknown("original-attach-state-required")
        try:
            from hermes_owned_gateway_windows import JobAssociation
            frame = _frame(self.stdin)
            if (set(frame) != {"protocol", "command", "attemptId", "planSha256", "jobHandle"}
                    or frame["command"] != "attach" or frame["attemptId"] != self.contract.attempt_id
                    or frame["planSha256"] != self.plan_sha256 or type(frame["jobHandle"]) is not int
                    or not 0 < frame["jobHandle"] < 2 ** (8 * struct.calcsize("P"))):
                raise OwnedGatewayUnknown("original-attach-binding-mismatch")
            with self._state_lock:
                if self is not _CURRENT or self.phase != "attaching" or self.job is not None:
                    raise self.unknown("original-attach-state-required")
                self.job = JobAssociation(frame["jobHandle"])
                if self is not _CURRENT or self.phase != "attaching":
                    raise self.unknown("original-attach-state-required")
                self.phase = "attached"
        except Exception as error:
            raise self.unknown(str(error)) from error

    def require_attached(self, contract):
        if self is not _CURRENT or self.contract is not contract or self.phase != "attached" or self.job is None:
            raise self.unknown("actual-original-source-association-required")
        self.job.require_current_member()

    def require_child_work(self):
        if self is not _CURRENT or self.phase not in ("attached", "bound") or self.job is None:
            raise OwnedGatewayUnknown("original-source-does-not-admit-new-child-work")
        self.job.require_current_member()

    async def retain_unknown(self, reason):
        import asyncio
        self.unknown(reason)
        if self.runner is not None:
            self.runner._draining = True
            descriptor = getattr(self.runner, "session_runtime_descriptor", None)
            if descriptor is not None:
                descriptor.update(state="unavailable", capabilities=[])
        # No finally can release this source's streams, runner, handles or stores.
        await asyncio.Future()

    def hold_unknown(self):
        """Keep the original streams and kernel association; never turn failure into exit."""
        self._unknown_hold.wait()
        raise OwnedGatewayUnknown("unknown-original-source-cannot-be-released")

    def read_shutdown(self):
        try:
            frame = _frame(self.stdin)
            if (self._shutdown or set(frame) != {"protocol", "command", "attemptId"}
                    or frame["command"] != "shutdown" or frame["attemptId"] != self.contract.attempt_id):
                raise OwnedGatewayUnknown("unknown-duplicate-or-mismatched-shutdown")
            self._shutdown = True
            # The original parent half-closes only after its one flushed frame.
            # EOF here closes input; it says nothing about work or process exit.
            if self.stdin.readline(MAX_FRAME + 1) != b"":
                raise OwnedGatewayUnknown("extra-or-duplicate-original-control-frame")
            self._control_closed = True
        except Exception as error:
            raise self.unknown(str(error)) from error

    def bind_runner(self, runner):
        import asyncio
        from gateway.run import GatewayRunner
        self.require_attached(self.contract)
        task, loop = asyncio.current_task(), asyncio.get_running_loop()
        if type(runner) is not GatewayRunner or task is None or self.runner is not None:
            raise self.unknown("canonical-runner-and-original-task-required")
        self.runner, self.task, self.loop = runner, task, loop
        runner._owned_gateway_source = self
        self.phase = "bound"

    def require_bound(self, runner):
        import asyncio
        if (self is not _CURRENT or self.runner is not runner or self.phase not in ("bound", "draining")
                or self.task is not asyncio.current_task() or self.loop is not asyncio.get_running_loop()
                or runner._owned_gateway_source is not self):
            raise self.unknown("original-canonical-lifecycle-task-required")
        self.job.require_current_member()

    def require_shutdown(self, runner):
        self.require_bound(runner)
        if not self._shutdown or not self._control_closed:
            raise self.unknown("matching-shutdown-and-original-input-boundary-required")

    def _emit(self, value):
        raw = json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode("utf-8") + b"\n"
        if len(raw) > MAX_FRAME:
            raise self.unknown("original-control-output-too-large")
        stream = self.stdout_owner.buffer
        if stream.write(raw) != len(raw):
            raise self.unknown("partial-original-control-output")
        stream.flush()

    def ready(self, runner):
        self.require_bound(runner)
        descriptor, listener = runner.session_runtime_descriptor, runner.session_api
        authority = runner.session_authorities.launch
        if (self._ready_emitted or descriptor.get("state") != "ready" or listener.task.done()
                or authority.runner is not runner or descriptor.get("api_origin") != listener.api_origin
                or descriptor.get("authority_epoch") != authority.epoch):
            raise self.unknown("actual-canonical-readiness-required")
        self._emit({"protocol": PROTOCOL, "event": "ready", "attemptId": self.contract.attempt_id,
                    "apiOrigin": listener.api_origin, "instanceId": descriptor["instance_id"],
                    "authorityEpoch": authority.epoch})
        self._ready_emitted = True

    def completed(self, runner, settlement):
        if self.settlement is None:
            raise self.unknown("canonical-managed-normal-lifecycle-not-composed")
        from gateway.run_owned_shutdown import ManagedSettlement
        self.require_shutdown(runner)
        if (self._complete_emitted or self.phase != "draining" or type(settlement) is not ManagedSettlement
                or settlement is not self.settlement):
            raise self.unknown("original-managed-settlement-required")
        settlement.require_settled(self, runner)
        self.job.require_only_current_process()
        self._emit({"protocol": PROTOCOL, "event": "shutdown_complete",
                    "attemptId": self.contract.attempt_id})
        self._complete_emitted = True
        self.phase = "closed"


def attach(contract, plan_bytes):
    global _CURRENT
    if _CURRENT is not None:
        raise OwnedGatewayUnknown("original-source-already-selected")
    import hermes_retained_bootstrap as bootstrap
    if contract is not bootstrap.current() or plan_bytes != bootstrap.original_plan_bytes(contract):
        raise OwnedGatewayUnknown("original-installed-bootstrap-required")
    _CURRENT = value = OwnedGatewaySource(contract, plan_bytes)
    value.attach()
    return value


def current(contract):
    if _CURRENT is None or _CURRENT.contract is not contract:
        raise OwnedGatewayUnknown("actual-original-source-association-required")
    return _CURRENT


def run_foreground(source):
    try:
        source.require_attached(source.contract)
        from gateway.run_owned import main
        return main(source)
    except BaseException as error:
        source.unknown(str(error))
        source.hold_unknown()


def require_lifecycle_available(source):
    """The unavailable body must refuse before bootstrap can start any workers."""
    source.require_attached(source.contract)
    raise OwnedGatewayUnknown("canonical-managed-normal-lifecycle-not-composed")


def active(runner=None):
    source = _CURRENT
    if source is None:
        return None
    if runner is not None and source.runner is not runner:
        raise OwnedGatewayUnknown("different-canonical-runner")
    if source.job is None or source.phase == "unknown":
        raise OwnedGatewayUnknown("original-source-custody-unknown")
    source.job.require_current_member()
    return source
