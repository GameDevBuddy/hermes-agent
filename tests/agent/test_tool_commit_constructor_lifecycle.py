"""Original optional observer custody and refusal; no product/native/GUI lifetime."""
import contextvars
import sys

import pytest

from agent import tool_commit_owner as commits
from agent import tool_commit_construction as construction
from agent.tool_commit_rows import MAX_WITNESSES
from tests.agent.canonical_commit_fixture import CALL, SESSION
from tests.agent.tool_commit_constructor_fixture import original_self, source_case


def test_failed_owner_retry_retains_first_error_and_new_agent_still_installs(monkeypatch, request):
    case = source_case(request)
    error, original = RuntimeError("original weakref lower"), commits.weakref.ref
    reached = []

    def lower(value, *args, **kwargs):
        if sys._getframe(1).f_code is commits.ToolCommitOwner.__init__.__code__:
            reached.append(value)
            raise error
        return original(value, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(commits.weakref, "ref", lower)
        case.authority.adopt_agent(SESSION, case.claim["generation"], case)
        case.authority.adopt_agent(SESSION, case.claim["generation"], case)
    group = case.authority._canonical_tool_commit_attempts[(SESSION, case.claim["generation"], id(case))]
    attempt = group[-1]
    assert len(reached) == 1 and attempt.error is error and attempt.value.first_exception is error
    assert attempt.value is original_self(error, commits.ToolCommitOwner.__init__)
    class Replacement:
        pass

    replacement = Replacement()
    replacement._session_db, replacement.session_id = case._session_db, SESSION
    replacement._active_session_turn_lease_holder = case._active_session_turn_lease_holder
    replacement._active_compression_lock_holder = None
    case.authority.adopt_agent(SESSION, case.claim["generation"], replacement)
    assert replacement._canonical_tool_commit_owner.usable() is True
    assert attempt.error is error and attempt.phase == "failed"


def test_owner_partial_all_optional_consumers_refuse_without_attribute_masking(monkeypatch, request):
    case = source_case(request)
    original, error = commits.threading.RLock, RuntimeError("owner lock lower")

    def lower(*args, **kwargs):
        if sys._getframe(1).f_code is commits.ToolCommitOwner.__init__.__code__:
            raise error
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(commits.threading, "RLock", lower)
        case.authority.adopt_agent(SESSION, case.claim["generation"], case)
    value = original_self(error, commits.ToolCommitOwner.__init__)
    assert value.usable() is False and value.binding_current({}) is False
    assert value.bind(object()) is None and value.watch({}, object()) is None
    assert value.stage({}) is None and value.indices([{}]) == () and value.writer_sources([{}]) == ()
    assert value.consume([{}], [{}]) is None and value.prepare(object(), {}, {}) is None
    assert commits.owner_call(case, "bind", object(), default="ordinary") == "ordinary"
    assert value.first_exception is error


def test_reentrant_install_is_refused_before_nonreentrant_database_lock(monkeypatch, request):
    case = source_case(request)
    original, observed = commits.threading.RLock, []

    def lower(*args, **kwargs):
        if sys._getframe(1).f_code is commits.ToolCommitOwner.__init__.__code__:
            with case._session_db._lock:
                case.authority.adopt_agent(SESSION, case.claim["generation"], case)
                observed.append(case._canonical_tool_commit_owner)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(commits.threading, "RLock", lower)
        case.authority.adopt_agent(SESSION, case.claim["generation"], case)
    assert observed == [None] and case._canonical_tool_commit_owner.usable() is True
    group = case.authority._canonical_tool_commit_attempts[(SESSION, case.claim["generation"], id(case))]
    assert len(group) == 2 and group[-1].phase == "ready"


@pytest.mark.parametrize("field", ["context", "callback", "database"])
def test_pending_constructor_displacement_is_owned_and_cannot_retry(monkeypatch, request, field):
    case = source_case(request)
    case.stage()
    original, changed = commits.deepcopy, []
    marker = contextvars.ContextVar("canonical-constructor-source-context")
    marker.set("original")
    original_db = case._session_db

    def lower(value, *args, **kwargs):
        if sys._getframe(1).f_code is commits.PendingToolCommit.__init__.__code__ and not changed:
            changed.append(True)
            if field == "context":
                marker.set("displaced")
            elif field == "callback":
                case.owner.publish = lambda *args: None
            else:
                case._session_db = object()
        return original(value, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(commits, "deepcopy", lower)
        ref = case.begin()
    attempt = case.owner._pending_constructions[case.owner.staged["call_uids"][CALL]]
    assert ref.canonical_commit is None and attempt.phase == "failed"
    assert attempt.error is case.owner.first_exception and attempt.traceback is not None
    assert attempt.value in case.owner.pending and attempt.value.prepare({}, {}) is None
    assert attempt.value.consume({}) is None
    case._session_db = original_db  # Restore only this new fixture, never a historical root.
    assert case.owner.bind(ref) is None and not case.canonical_events()


def test_pending_reentry_refuses_before_database_lock_and_other_uid_still_works(monkeypatch, request):
    case = source_case(request)
    case.stage()
    original, observed = commits.deepcopy, []

    def lower(value, *args, **kwargs):
        if sys._getframe(1).f_code is commits.PendingToolCommit.__init__.__code__:
            with case._session_db._lock:
                observed.append(case.begin().canonical_commit)
        return original(value, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(commits, "deepcopy", lower)
        ref = case.begin()
    assert observed and all(value is None for value in observed)
    first = ref.canonical_commit
    assert first is not None and first.owner is case.owner
    case.stage()
    second = case.begin().canonical_commit
    assert second is not None and second.uid != first.uid
    assert case.owner._pending_construction_credit == 2 * construction.CONSTRUCTION_CREDIT


def test_failed_pending_uid_is_consumed_but_new_assistant_uid_can_commit(monkeypatch, request):
    case = source_case(request)
    case.stage()
    error, original = RuntimeError("original pending copy"), commits.deepcopy

    def lower(value, *args, **kwargs):
        if sys._getframe(1).f_code is commits.PendingToolCommit.__init__.__code__:
            raise error
        return original(value, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(commits, "deepcopy", lower)
        first = case.begin()
    partial = original_self(error, commits.PendingToolCommit.__init__)
    with case._session_db._lock:
        assert case.owner.bind(first) is None
    case.stage()
    second = case.begin()
    assert second.canonical_commit is not None and second.canonical_commit.uid != partial.uid
    case.prepare(second)
    assert case.flush() is True and len(case.canonical_events()) == 1
    assert case.owner.first_exception is error and len(case.owner.pending) == 2


def test_same_adoption_reinstall_credit_is_finite_without_refunding_healthy_attempts(request):
    case = source_case(request)
    for _ in range(MAX_WITNESSES - 1):
        case.authority.adopt_agent(SESSION, case.claim["generation"], case)
        assert case._canonical_tool_commit_owner.usable() is True
    group = case.authority._canonical_tool_commit_attempts[(SESSION, case.claim["generation"], id(case))]
    original = case._canonical_tool_commit_owner
    case.authority.adopt_agent(SESSION, case.claim["generation"], case)
    assert len(group) == MAX_WITNESSES and case._canonical_tool_commit_owner is original
    assert sum(attempt.credit for attempt in group) == MAX_WITNESSES * construction.CONSTRUCTION_CREDIT


def test_direct_successful_constructor_shape_remains_optional_observer_only(request):
    case = source_case(request)
    value = commits.ToolCommitOwner(case, case.owner.scope, case.owner.validate, case.owner.publish)
    assert construction.construction_ready(value) is True
    assert value.usable() is False and value._pending_construction_credit == 0
    assert case.owner.usable() is True and not case.canonical_events()


def test_original_owner_database_getter_failure_keeps_actual_partial_and_first_error(monkeypatch, request):
    case = source_case(request)
    original, error = type(case).__getattribute__, RuntimeError("original owner DB getter")

    def lower(self, name):
        if (name == "_session_db"
                and sys._getframe(1).f_code is commits.ToolCommitOwner.__init__.__code__):
            raise error
        return original(self, name)

    with monkeypatch.context() as patch:
        patch.setattr(type(case), "__getattribute__", lower)
        case.authority.adopt_agent(SESSION, case.claim["generation"], case)
    attempt = case._canonical_tool_commit_owner_attempt
    assert attempt.error is error and attempt.value is original_self(error, commits.ToolCommitOwner.__init__)
    assert attempt.value.first_exception is error and attempt.value.usable() is False


@pytest.mark.parametrize("target", ["owner", "pending"])
def test_original_base_exception_propagates_with_parent_custody(monkeypatch, request, target):
    case = source_case(request)
    error, original = BaseException("source constructor abort"), commits.deepcopy
    initializer = commits.ToolCommitOwner.__init__ if target == "owner" else commits.PendingToolCommit.__init__
    case.stage()

    def lower(value, *args, **kwargs):
        if sys._getframe(1).f_code is initializer.__code__:
            raise error
        return original(value, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(commits, "deepcopy", lower)
        with pytest.raises(BaseException) as raised:
            if target == "owner":
                case.authority.adopt_agent(SESSION, case.claim["generation"], case)
            else:
                case.begin()
    attempt = (case._canonical_tool_commit_owner_attempt if target == "owner"
               else case.owner._pending_constructions[case.owner.staged["call_uids"][CALL]])
    assert raised.value is error and attempt.error is error
    assert attempt.value is original_self(error, initializer) and attempt.traceback is not None


def test_pending_uid_capacity_is_consumed_before_allocating_another_original(request):
    case = source_case(request)
    case.stage([{"id": f"original-{index}", "type": "function",
                 "function": {"name": "fixture_echo", "arguments": "{}"}}
                for index in range(MAX_WITNESSES)])
    for index in range(MAX_WITNESSES):
        assert case.begin(call_id=f"original-{index}").canonical_commit is not None
    case.stage()
    assert case.begin().canonical_commit is None
    assert len(case.owner.pending) == len(case.owner._pending_constructions) == MAX_WITNESSES
    assert case.owner._pending_construction_credit == MAX_WITNESSES * construction.CONSTRUCTION_CREDIT


@pytest.mark.parametrize("missing", ["database", "live"])
def test_ineligible_adoption_preserves_silent_optional_default(request, caplog, missing):
    case = source_case(request)

    class WithoutDatabase:
        session_id = SESSION

    agent = WithoutDatabase() if missing == "database" else case
    if missing == "live":
        case.authority.sessions.pop(SESSION)
    case.authority.adopt_agent(SESSION, case.claim["generation"], agent)
    assert agent._canonical_tool_commit_owner is None
    assert agent._canonical_tool_commit_owner_attempt.phase == "empty"
    assert commits.owner_call(agent, "bind", object(), default="ordinary") == "ordinary"
    assert "Canonical tool commit observer not installed" not in caplog.text
