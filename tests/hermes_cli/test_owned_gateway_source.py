"""Source-control unit tests with fake query results; no Windows APIs or gateway run.

These tests do not establish kernel custody, native authority, runtime readiness,
normal resource settlement or a working managed gateway. Its entry still refuses.
"""
from __future__ import annotations

import hashlib
import io
import json
from types import SimpleNamespace

import pytest

import hermes_owned_gateway as subject
import hermes_owned_gateway_windows as windows
import hermes_retained_bootstrap as bootstrap


class QueryAPI:
    def __init__(self):
        self.rights, self.limits, self.member, self.processes = 4, 0, True, 1
        self.queries = []

    def access(self, handle):
        self.queries.append(("access", handle))
        return self.rights

    def limit_flags(self, handle):
        self.queries.append(("limits", handle))
        return self.limits

    def is_current_member(self, handle):
        self.queries.append(("member", handle))
        return self.member

    def active_processes(self, handle):
        self.queries.append(("processes", handle))
        return self.processes


def line(value):
    return json.dumps(value, separators=(",", ":")).encode() + b"\n"


class RecordingInput(io.BytesIO):
    def __init__(self, raw):
        super().__init__(raw)
        self.read_calls = 0

    def readline(self, size=-1):
        self.read_calls += 1
        return super().readline(size)


@pytest.fixture
def owned(monkeypatch):
    plan = b'{"owned":"exact original plan bytes"}\n'
    contract = SimpleNamespace(attempt_id="owned-attempt")
    api = QueryAPI()
    output, errors = io.BytesIO(), io.BytesIO()
    monkeypatch.setattr(subject, "_CURRENT", None)
    monkeypatch.setattr(bootstrap, "_BOUND", contract)
    monkeypatch.setattr(bootstrap, "_PLAN_BYTES", plan)
    monkeypatch.setattr(windows, "_load_api", lambda: api)
    streams = SimpleNamespace(stdin=None, stdout=io.TextIOWrapper(output, encoding="utf-8", write_through=True),
                              stderr=io.TextIOWrapper(errors, encoding="utf-8", write_through=True))
    monkeypatch.setattr(subject, "sys", streams)
    frame = {"protocol": subject.PROTOCOL, "command": "attach", "attemptId": contract.attempt_id,
             "planSha256": hashlib.sha256(plan).hexdigest(), "jobHandle": 41}

    def select(raw=None):
        owner = io.TextIOWrapper(RecordingInput(line(frame) if raw is None else raw), encoding="utf-8")
        streams.stdin = owner
        return subject.attach(contract, plan)

    return SimpleNamespace(plan=plan, contract=contract, api=api, output=output,
                           frame=frame, select=select, monkeypatch=monkeypatch)


def test_original_input_and_query_api_are_retained(owned):
    source = owned.select()
    assert source.stdin_owner is subject.sys.stdin
    assert source.stdin is subject.sys.stdin.buffer
    assert source.stdout_owner.buffer is owned.output
    assert source.job._api is owned.api
    assert owned.api.queries == [("access", 41), ("limits", 41), ("member", 41)]
    source.require_attached(owned.contract)
    assert len(owned.api.queries) == 6  # association is checked again, not copied into a grant
    assert not owned.output.getvalue()


@pytest.mark.parametrize("field,value", [
    ("attemptId", "other-attempt"), ("planSha256", "0" * 64),
    ("jobHandle", True), ("jobHandle", 0), ("jobHandle", -1), ("jobHandle", 2**65),
])
def test_attach_metadata_never_supplies_membership(owned, field, value):
    owned.frame[field] = value
    with pytest.raises(subject.OwnedGatewayUnknown, match="binding-mismatch"):
        owned.select()
    assert subject._CURRENT.phase == "unknown"
    assert not owned.api.queries and not owned.output.getvalue()


@pytest.mark.parametrize("raw", [b"", b"{}", b"x" * 8193 + b"\n", b"\xff\n",
    b'{"protocol":"gdb-owned-gateway-v1","protocol":"gdb-owned-gateway-v1"}\n'],
    ids=["eof", "short", "oversized", "invalid-utf8", "duplicate-key"])
def test_short_duplicate_and_invalid_control_is_unknown(owned, raw):
    with pytest.raises(subject.OwnedGatewayUnknown):
        owned.select(raw)
    assert subject._CURRENT.phase == "unknown"
    assert not owned.api.queries and not owned.output.getvalue()


@pytest.mark.parametrize("field,value,reason", [
    ("rights", 0x001F003F, "not-query-only"),
    ("limits", 0x2000, "kill-on-close"), ("limits", 0x800, "breakaway"),
    ("limits", 0x1000, "breakaway"), ("member", False, "not-in-original-job"),
])
def test_query_results_must_establish_the_specific_association(owned, field, value, reason):
    setattr(owned.api, field, value)
    with pytest.raises(subject.OwnedGatewayUnknown, match=reason):
        owned.select()
    assert subject._CURRENT.phase == "unknown"
    assert not owned.output.getvalue()


def test_plan_alone_and_second_attach_are_not_a_source(owned):
    with pytest.raises(subject.OwnedGatewayUnknown, match="association-required"):
        subject.current(owned.contract)
    source = owned.select()
    with pytest.raises(subject.OwnedGatewayUnknown, match="already-selected"):
        subject.attach(owned.contract, owned.plan)
    assert subject.current(owned.contract) is source


@pytest.mark.parametrize("first", ["success", "invalid-binding", "query-failure", "unknown"])
def test_same_source_cannot_repeat_its_attach_attempt(owned, first):
    constructions = []
    association = windows.JobAssociation

    def associate(handle):
        constructions.append(handle)
        return association(handle)

    owned.monkeypatch.setattr(windows, "JobAssociation", associate)
    initial = dict(owned.frame)
    replay = dict(owned.frame, jobHandle=99)
    if first == "invalid-binding":
        initial["attemptId"] = "other-attempt"
    elif first == "query-failure":
        owned.api.member = False
    if first in ("invalid-binding", "query-failure"):
        with pytest.raises(subject.OwnedGatewayUnknown):
            owned.select(line(initial) + line(replay))
        source = subject.current(owned.contract)
        owned.api.member = True
    else:
        source = owned.select(line(initial) + line(replay))
        if first == "unknown":
            source.unknown("first-source-failure")
    original_job, original_input = source.job, source.stdin
    original_owner = source.stdin_owner
    reads, queries, constructed = original_input.read_calls, tuple(owned.api.queries), tuple(constructions)
    first_error = source.error
    with pytest.raises(subject.OwnedGatewayUnknown) as error:
        source.attach()
    assert str(error.value) == (first_error if first_error is not None else "original-attach-already-attempted")
    assert source.phase == "unknown"
    assert source.error == str(error.value)
    assert source.job is original_job and source.stdin is original_input
    assert source.stdin_owner is original_owner and not original_owner.closed
    assert original_input.read_calls == reads == 1
    assert tuple(owned.api.queries) == queries
    assert tuple(constructions) == constructed
    assert not owned.output.getvalue()


@pytest.mark.parametrize("invalid", ["different-current", "phase", "existing-job"])
def test_first_attach_claim_checks_original_object_state_before_io(owned, invalid):
    original = owned.select(line(owned.frame) + line(dict(owned.frame, jobHandle=99)))
    candidate = subject.OwnedGatewaySource(owned.contract, owned.plan)
    if invalid != "different-current":
        owned.monkeypatch.setattr(subject, "_CURRENT", candidate)
    if invalid == "phase":
        candidate.phase = "bound"
    elif invalid == "existing-job":
        candidate.job = object()
    job, queries = candidate.job, tuple(owned.api.queries)
    with pytest.raises(subject.OwnedGatewayUnknown, match="original-attach-state-required"):
        candidate.attach()
    assert candidate.phase == "unknown" and candidate.job is job
    assert candidate.stdin.read_calls == 1
    assert tuple(owned.api.queries) == queries
    assert original.job is not None


def test_first_unknown_diagnostic_survives_later_guard_failures(owned):
    source = owned.select()
    job = source.job
    first = source.unknown("first-source-failure")
    later = source.unknown("later-source-failure")
    assert str(first) == str(later) == source.error == "first-source-failure"
    with pytest.raises(subject.OwnedGatewayUnknown, match="first-source-failure"):
        source.require_attached(owned.contract)
    assert source.phase == "unknown" and source.error == "first-source-failure"
    assert source.job is job and source.stdin.read_calls == 1


def test_attach_attempt_is_consumed_before_the_first_input_read(owned):
    readline = RecordingInput.readline
    reentered = []

    def reenter(stream, size=-1):
        if not reentered:
            reentered.append(subject._CURRENT)
            with pytest.raises(subject.OwnedGatewayUnknown, match="already-attempted"):
                subject._CURRENT.attach()
        return readline(stream, size)

    owned.monkeypatch.setattr(RecordingInput, "readline", reenter)
    with pytest.raises(subject.OwnedGatewayUnknown, match="already-attempted"):
        owned.select(line(owned.frame) + line(dict(owned.frame, jobHandle=99)))
    source = subject.current(owned.contract)
    assert reentered == [source] and source.stdin.read_calls == 1
    assert source.job is None and not owned.api.queries
    assert source.phase == "unknown" and source.error == "original-attach-already-attempted"


def test_shutdown_boundary_is_input_closure_only(owned):
    shutdown = {"protocol": subject.PROTOCOL, "command": "shutdown", "attemptId": owned.contract.attempt_id}
    source = owned.select(line(owned.frame) + line(shutdown))
    source.read_shutdown()
    assert source._shutdown and source._control_closed
    assert source.phase == "attached" and not owned.output.getvalue()
    with pytest.raises(subject.OwnedGatewayUnknown, match="not-composed"):
        source.completed(object(), object())
    assert source.phase == "unknown" and not owned.output.getvalue()


@pytest.mark.parametrize("tail", [b"", b"{}", line({"protocol": subject.PROTOCOL, "command": "shutdown",
                                                   "attemptId": "wrong"})], ids=["eof", "short", "mismatch"])
def test_missing_short_or_mismatched_shutdown_retains_unknown(owned, tail):
    source = owned.select(line(owned.frame) + tail)
    with pytest.raises(subject.OwnedGatewayUnknown):
        source.read_shutdown()
    assert source.phase == "unknown" and not source._control_closed
    assert source.job is not None and source.stdin_owner is subject.sys.stdin
    assert not owned.output.getvalue()


def test_duplicate_shutdown_before_half_close_is_unknown(owned):
    shutdown = line({"protocol": subject.PROTOCOL, "command": "shutdown", "attemptId": owned.contract.attempt_id})
    source = owned.select(line(owned.frame) + shutdown + shutdown)
    with pytest.raises(subject.OwnedGatewayUnknown, match="extra-or-duplicate"):
        source.read_shutdown()
    assert source.phase == "unknown" and not source._control_closed
    assert not owned.output.getvalue()


@pytest.mark.parametrize("count", [0, 2])
def test_job_accounting_does_not_guess_zero_or_ignore_children(owned, count):
    source = owned.select()
    owned.api.processes = count
    with pytest.raises(windows.JobAssociationUnknown, match="other-or-unknown"):
        source.job.require_only_current_process()


def test_uncomposed_canonical_route_refuses_before_constructing_gateway(owned):
    from gateway.run_owned import main
    source = owned.select()
    with pytest.raises(subject.OwnedGatewayUnknown, match="not-composed"):
        main(source)
    assert not owned.output.getvalue()
