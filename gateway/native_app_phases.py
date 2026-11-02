"""One-shot commands from the original App-owned private reader.

Phase custody is not a SQL commit proof. The actual original worker/writer
must complete the corresponding action before its event is published.
"""
from __future__ import annotations

import asyncio
import json
import re

from gateway.native_source_runtime import refuse

COMMANDS = ("prepare_native_call", "ack_native_call", "complete_native_call", "release_native_call")
COMMON = frozenset(("protocol", "command", "attemptId", "requestId", "nonce", "contextId",
                    "serverRequestId", "wireSha256", "invocationId", "invocationSequence"))
EXPECTED = dict(zip(COMMANDS, ("awaiting_prepare", "committed", "acknowledged", "completed")))
UUID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")


def frozen_frame(frame):
    return json.dumps(frame, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


class OriginalCallPhase:
    def __init__(self, call, reader, frame):
        self.call, self.reader, self.frame = call, reader, frame
        self.book_key, self.credit = frame["requestId"], None
        self.wire, self.command = frozen_frame(frame), frame["command"]
        self.coroutine = self.future = self.task = None
        self.error = self.traceback = None
        self.completed = False
        self.publications = {}
        self.completion = asyncio.Event()

    def require_current(self, *, active=True):
        self.reader.require_retained()
        self.call.require_current()
        if (self.call.phases.get(self.command) is not self or active and self.reader.active is not self
                or self.reader.source._app_context_requests.get(self.book_key) is not self
                or self.wire != frozen_frame(self.frame)):
            refuse("native_original_phase_displaced")

    def failed(self, error):
        if self.error is None:
            self.error, self.traceback = error, error.__traceback__
        self.call.failed(error)


def validate_frame(frame):
    from hermes_owned_control import _NONCE, _ULID
    command = frame.get("command")
    fields = COMMON if command == "prepare_native_call" else COMMON | {"proofSha256"}
    if (command not in COMMANDS or set(frame) != fields
            or type(frame.get("requestId")) is not str or not _ULID.fullmatch(frame["requestId"])
            or type(frame.get("nonce")) is not str or not _NONCE.fullmatch(frame["nonce"])
            or type(frame.get("wireSha256")) is not str or not _NONCE.fullmatch(frame["wireSha256"])
            or type(frame.get("invocationId")) is not str or not UUID.fullmatch(frame["invocationId"])
            or type(frame.get("invocationSequence")) is not int or not 0 < frame["invocationSequence"] < 2**64):
        refuse("native_original_phase_invalid")
    if command != "prepare_native_call" and (type(frame["proofSha256"]) is not str
                                             or not _NONCE.fullmatch(frame["proofSha256"])):
        refuse("native_original_phase_invalid")


def _require_source(call, reader, frame):
    context = call.context
    if (not call.acknowledged or call.error is not None or reader.active is not None or reader is not call.reader
            or frame["attemptId"] != reader.source.contract.attempt_id
            or frame["nonce"] != context.stage.nonce or frame["contextId"] != call.original.selection.context
            or frame["wireSha256"] != call.digest or frame["serverRequestId"] != call.slot._native_request_candidate_id):
        refuse("native_original_phase_source_changed")


def _reserve(call, reader, frame):
    from gateway.native_app_context_records import RECORD_CREDIT, reserve
    from gateway.native_app_phase_credit import original_followup, reserve_followups
    command = frame["command"]
    with reader.source._state_lock:
        if command in call.phases or call.protocol_phase != EXPECTED[command]:
            refuse("native_original_phase_reentry_or_order")
        pair = frame["invocationId"], frame["invocationSequence"]
        if command == "prepare_native_call":
            last = reader.source._native_invocation_sequences.get(frame["nonce"], 0)
            if pair[0] in reader.source._native_invocations or pair[1] <= last:
                refuse("native_original_invocation_replay")
            reserve(reader.source, reader.source._app_context_requests, frame["requestId"], RECORD_CREDIT)
            credit = None
        elif pair != call.invocation or frame["proofSha256"] != call.commit_proof_sha256:
            refuse("native_original_phase_invocation_or_proof_changed")
        else:
            credit = original_followup(call, reader, frame)
        phase = OriginalCallPhase(call, reader, frame)
        if credit is not None:
            phase.book_key, phase.credit, credit.phase = credit.key, credit, phase
        call.phases[command] = phase
        reader.source._app_context_requests[phase.book_key] = phase
        reader.active = phase
        if command == "prepare_native_call":
            reader.source._native_invocations[pair[0]] = call
            reader.source._native_invocation_sequences[frame["nonce"]] = pair[1]
            call.invocation = pair
            reserve_followups(call, reader, frame)
    return phase


def claim(reader, frame):
    from gateway import session_server_requests as requests
    from gateway.native_app_call import AppCall
    from hermes_owned_control import OriginalControlReader
    if type(reader) is not OriginalControlReader:
        refuse("native_original_phase_reader_missing")
    reader.require_reading()
    validate_frame(frame)
    with requests._lock:
        pending = requests._open.get(frame.get("serverRequestId"))
    slot = getattr(pending, "native_slot", None)
    call = None if slot is None else slot._source_constructions.get("app_call")
    if type(call) is not AppCall:
        refuse("native_original_phase_call_missing")
    _require_source(call, reader, frame)
    try:
        return _reserve(call, reader, frame)
    except BaseException as error:
        call.failed(error)
        raise


async def receive(phase):
    if phase.task is not None:
        refuse("native_original_phase_reentry_or_order")
    phase.task = asyncio.current_task()
    try:
        phase.require_current()
        phase.call.worker_event.set()
        await phase.completion.wait()
        if phase.error is not None:
            raise phase.error
        phase.require_current()
        if not phase.completed:
            refuse("native_original_phase_action_missing")
    except BaseException as error:
        phase.failed(error)
        raise


def event(phase, name, *, proof=None, proof_sha256=None):
    phase.require_current(active=not phase.completed)
    if name in phase.publications:
        refuse("native_original_phase_publication_reentry")
    payload = {key: value for key, value in phase.frame.items() if key != "command"}
    payload["event"] = name
    if proof_sha256 is not None:
        payload["proofSha256"] = proof_sha256
    if proof is not None:
        payload["proof"] = proof
    phase.publications[name] = payload
    phase.reader.source._emit(payload)
    return payload


def finish(phase, event_name):
    event(phase, event_name)
    phase.completed = True
    phase.call.original.loop.call_soon_threadsafe(phase.completion.set)
