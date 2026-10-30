"""Synthetic allocation/publication failures at actual browser reservation consumers; no exhaustion claim."""
import weakref
from types import SimpleNamespace

import pytest

from tests.tools.browser_owned_fixture import session_record

pytest_plugins = ("tests.tools.browser_owned_fixture",)
pytestmark = pytest.mark.asyncio


class SyntheticAllocationFailure(MemoryError):
    """A deliberate ordinary exception, not a real allocator or RSS experiment."""


class FailedAppend(list):
    def __init__(self, failure, insert):
        super().__init__()
        self.failure, self.insert, self.calls, self.partial = failure, insert, 0, None

    def append(self, item):
        self.calls += 1
        if self.calls == 1:
            self.partial = weakref.ref(item)
            if self.insert:
                super().append(item)
            raise self.failure
        return super().append(item)


class FailedPublication(dict):
    def __init__(self, failure, insert):
        super().__init__()
        self.failure, self.insert, self.calls = failure, insert, 0

    def setdefault(self, key, value):
        self.calls += 1
        if self.calls == 1:
            if self.insert:
                super().setdefault(key, value)
            raise self.failure
        return super().setdefault(key, value)


def attempt_again(fixture):
    # Same source, same real admission API; reserve must refuse before a second allocation.
    from hermes_owned_gateway import OwnedGatewayUnknown
    try:
        fixture.owned.reserve("session", fixture.sessions.get_session_info)
    except (OwnedGatewayUnknown, SyntheticAllocationFailure) as error:
        return error
    return None


def require_first_failure(fixture, failure, error, credit):
    from hermes_owned_gateway import OwnedGatewayUnknown
    first, charged, phase = fixture.source.browser_error, fixture.source.browser_credit, fixture.source.phase
    second = attempt_again(fixture)
    assert error is failure and first is failure
    assert charged == credit and phase == "unknown"
    assert isinstance(second, OwnedGatewayUnknown)
    assert fixture.source.browser_error is failure and fixture.source.browser_credit == credit
    assert not fixture.evidence.processes and not fixture.evidence.workers


@pytest.mark.parametrize("partial", [False, True])
async def test_first_constructor_failure_consumes_credit_and_latches_before_reentry(browser_fixture, monkeypatch, partial):
    f = browser_fixture
    f.activate()
    failure = SyntheticAllocationFailure("synthetic first __new__ failure")
    seen = SimpleNamespace(calls=0, credit=[], partial=None)

    def allocate(cls):
        seen.calls += 1
        seen.credit.append(f.source.browser_credit)
        if seen.calls == 1:
            actual_partial = object.__new__(cls) if partial else None
            seen.partial = weakref.ref(actual_partial) if partial else None
            raise failure
        return object.__new__(cls)

    monkeypatch.setattr(f.owned.Record, "__new__", staticmethod(allocate))
    with pytest.raises(SyntheticAllocationFailure) as caught:
        f.sessions.get_session_info("actor")
    require_first_failure(f, failure, caught.value, f.owned.CREDIT)
    assert seen.calls == 1 and seen.credit == [f.owned.CREDIT]
    assert not f.source.browser_records and not f.owned._records
    if partial:
        assert seen.partial() is not None
        assert failure.__traceback__.tb_next is not None


@pytest.mark.parametrize("target", ["source-book", "global-records"])
@pytest.mark.parametrize("insert", [False, True])
async def test_first_append_failure_retains_partial_insertion_and_credit(browser_fixture, monkeypatch, target, insert):
    f = browser_fixture
    f.activate()
    failure = SyntheticAllocationFailure("synthetic original list append failure")
    original = FailedAppend(failure, insert)
    if target == "source-book":
        f.source.browser_records = original
    else:
        monkeypatch.setattr(f.owned, "_records", original)
    with pytest.raises(SyntheticAllocationFailure) as caught:
        f.sessions.get_session_info("actor")
    require_first_failure(f, failure, caught.value, f.owned.CREDIT)
    record = original.partial()
    assert original.calls == 1 and record is not None
    assert len(original) == int(insert)
    if insert:
        assert original[0] is record
    assert f.owned._books[f.source] is f.source.browser_records
    if target == "global-records":
        assert len(f.source.browser_records) == 1 and f.source.browser_records[0] is record
    else:
        assert not f.owned._records


@pytest.mark.parametrize("insert", [False, True])
async def test_first_book_publication_failure_is_charged_and_sticky(browser_fixture, monkeypatch, insert):
    f = browser_fixture
    f.activate()
    failure = SyntheticAllocationFailure("synthetic original book publication failure")
    books = FailedPublication(failure, insert)
    monkeypatch.setattr(f.owned, "_books", books)
    with pytest.raises(SyntheticAllocationFailure) as caught:
        f.sessions.get_session_info("actor")
    require_first_failure(f, failure, caught.value, f.owned.CREDIT)
    assert books.calls == 1 and len(books) == int(insert)
    if insert:
        assert books[f.source] is f.source.browser_records
    assert not f.source.browser_records and not f.owned._records


async def test_initializer_failure_keeps_original_published_partial_and_exception(browser_fixture, monkeypatch):
    f = browser_fixture
    f.activate()
    failure = SyntheticAllocationFailure("synthetic initializer failure")
    observed = []

    def initialize(record, *args):
        observed.append((weakref.ref(record), f.source.browser_credit))
        raise failure

    monkeypatch.setattr(f.owned.Record, "__init__", initialize)
    with pytest.raises(SyntheticAllocationFailure) as caught:
        f.sessions.get_session_info("actor")
    require_first_failure(f, failure, caught.value, f.owned.CREDIT)
    assert len(observed) == 1 and observed[0][1] == f.owned.CREDIT
    assert f.source.browser_records[0] is f.owned._records[0] is observed[0][0]()


async def test_parent_publication_failure_keeps_original_child_and_consumed_credit(browser_fixture, monkeypatch):
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = browser_fixture
    parent = session_record(f)
    prior_credit, prior_records = f.source.browser_credit, len(f.source.browser_records)
    failure = SyntheticAllocationFailure("synthetic parent tuple publication failure")

    class FailedChildren(tuple):
        def __add__(self, value):
            raise failure

    original = FailedChildren(parent.children)
    parent.children = original
    with pytest.raises(SyntheticAllocationFailure) as caught:
        f.owned.reserve("child", f.sessions.get_session_info, parent)
    assert caught.value is failure and f.source.browser_error is failure
    assert f.source.browser_credit == prior_credit + f.owned.CREDIT
    assert len(f.source.browser_records) == prior_records + 1
    child = f.source.browser_records[-1]
    assert f.owned._records[-1] is child and child.original_parent is parent and parent.children is original
    with pytest.raises(OwnedGatewayUnknown):
        f.owned.reserve("child", f.sessions.get_session_info, parent)
    assert f.source.browser_error is failure and len(f.source.browser_records) == prior_records + 1
