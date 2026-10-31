"""Real optional-observer install/bind failures; source references are never App/native authority."""
import sys
from copy import copy as shallow_copy

import pytest

from agent import tool_commit_owner as commits
from tests.agent.canonical_commit_fixture import CALL, SESSION
from tests.agent.tool_commit_constructor_fixture import source_case, original_self


@pytest.mark.parametrize("lower", ["lock", "scope-copy"])
def test_original_owner_partial_is_retained_by_adoption_before_lower_failure(monkeypatch, request, caplog, lower):
    case = source_case(request)
    error = RuntimeError("original optional owner " + lower)
    original_lock, original_copy = commits.threading.RLock, commits.deepcopy

    def lock(*args, **kwargs):
        if sys._getframe(1).f_code is commits.ToolCommitOwner.__init__.__code__:
            raise error
        return original_lock(*args, **kwargs)

    def copy(value, *args, **kwargs):
        if sys._getframe(1).f_code is commits.ToolCommitOwner.__init__.__code__:
            raise error
        return original_copy(value, *args, **kwargs)

    with monkeypatch.context() as lower_patch:
        if lower == "lock":
            lower_patch.setattr(commits.threading, "RLock", lock)
        else:
            lower_patch.setattr(commits, "deepcopy", copy)
        case.authority.adopt_agent(SESSION, case.claim["generation"], case)
    value = original_self(error, commits.ToolCommitOwner.__init__)
    group = case.authority._canonical_tool_commit_attempts[(SESSION, case.claim["generation"], id(case))]
    attempt = group[-1]
    assert attempt.value is value and attempt.error is error and attempt.credit > 0
    assert case._canonical_tool_commit_owner is None
    assert "Canonical tool commit observer not installed" in caplog.text
    assert value.usable() is False and error.__traceback__ is not None


def test_original_pending_partial_is_retained_before_deepcopy_and_default_is_preserved(monkeypatch, request, caplog):
    case = source_case(request)
    case.stage()
    uid = case.owner.staged["call_uids"][CALL]
    error, original = RuntimeError("original pending field deepcopy"), commits.deepcopy

    def copy(value, *args, **kwargs):
        if sys._getframe(1).f_code is commits.PendingToolCommit.__init__.__code__:
            raise error
        return original(value, *args, **kwargs)

    with monkeypatch.context() as lower:
        lower.setattr(commits, "deepcopy", copy)
        ref = case.begin()
    value = original_self(error, commits.PendingToolCommit.__init__)
    assert any(pending is value for pending in case.owner.pending)
    attempt = case.owner._pending_constructions[uid]
    assert attempt.value is value and attempt.error is error and attempt.credit > 0
    assert case.owner.first_exception is error and error.__traceback__ is not None
    assert ref.canonical_commit is None and "Canonical tool-row witness withheld" in caplog.text
    assert case.owner.bind(ref) is None and case.canonical_events() == []


@pytest.mark.parametrize("target", ["live", "staged"])
def test_equal_but_different_original_parent_is_refused_before_publication(monkeypatch, request, target):
    case = source_case(request)
    original, changed = commits.deepcopy, []
    initializer = commits.ToolCommitOwner.__init__ if target == "live" else commits.PendingToolCommit.__init__
    case.stage()

    def lower(value, *args, **kwargs):
        if sys._getframe(1).f_code is initializer.__code__ and not changed:
            if target == "live":
                replacement = shallow_copy(case.live)
                assert replacement == case.live and replacement is not case.live
                case.authority.sessions[SESSION] = replacement
            else:
                replacement = case.owner.staged.copy()
                assert replacement == case.owner.staged and replacement is not case.owner.staged
                case.owner.staged = replacement
            changed.append(replacement)
        return original(value, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(commits, "deepcopy", lower)
        if target == "live":
            case.authority.adopt_agent(SESSION, case.claim["generation"], case)
            attempt = case._canonical_tool_commit_owner_attempt
            assert case._canonical_tool_commit_owner is None
        else:
            ref = case.begin()
            attempt = case.owner._pending_constructions[case.owner.staged["call_uids"][CALL]]
            assert ref.canonical_commit is None
    assert changed and attempt.phase == "failed" and attempt.error is not None
    assert attempt.value is not None and not case.canonical_events()
