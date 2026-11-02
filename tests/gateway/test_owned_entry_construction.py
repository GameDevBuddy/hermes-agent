"""Actual original entry/consumer over modeled Job/input; no startup or native IO."""
import io
from types import SimpleNamespace

import pytest

import hermes_owned_gateway as owned
from gateway import run_owned
from gateway.owned_entry_construction import require_prepared_entry


@pytest.fixture
def source(monkeypatch):
    monkeypatch.setattr(owned.sys, "stdin", SimpleNamespace(buffer=io.BytesIO()))
    value = owned.OwnedGatewaySource(SimpleNamespace(attempt_id="entry-fixture"), b"{}")
    value.job = SimpleNamespace(require_current_member=lambda: None)
    value.phase = "attached"
    monkeypatch.setattr(owned, "_CURRENT", value)
    return value


def test_fabricated_source_methods_cannot_allocate_an_entry(monkeypatch):
    calls = []
    fake = SimpleNamespace(contract=object(), require_attached=lambda value: calls.append(value))
    monkeypatch.setattr(run_owned.ManagedEntry, "__new__", staticmethod(lambda cls: calls.append(cls)))
    with pytest.raises(owned.OwnedGatewayUnknown, match="actual-original-source"):
        owned.prepare_lifecycle_entry(fake)
    assert calls == []


def test_original_entry_is_published_but_still_cannot_start_workers(source, monkeypatch):
    entry = owned.prepare_lifecycle_entry(source)
    assert type(entry) is run_owned.ManagedEntry
    assert source.entry is source._entry_original is source._entry_allocator_result is entry
    assert source._entry_inputs is source and source._entry_state == "prepared"
    assert source._entry_credit == 4096 and source._entry_error is None
    assert require_prepared_entry(source) is entry
    calls = []
    monkeypatch.setattr(run_owned.asyncio, "new_event_loop", lambda: calls.append("loop"))
    with pytest.raises(owned.OwnedGatewayUnknown, match="not-composed"):
        run_owned.main(source)
    assert calls == []
    with pytest.raises(owned.OwnedGatewayUnknown, match="already-attempted"):
        owned.prepare_lifecycle_entry(source)
    assert source.entry is entry and source._entry_error is None


def test_partial_initializer_and_first_error_survive_publication_displacement(source, monkeypatch):
    first = ValueError("original entry initializer failure")
    calls, displaced = [], object()

    def fail(value, original):
        assert original._entry_original is value and original._entry_allocator_result is value
        calls.append(value)
        value.source = original
        original.entry = displaced
        raise first

    monkeypatch.setattr(run_owned.ManagedEntry, "__init__", fail)
    with pytest.raises(owned.OwnedGatewayUnknown, match="construction-failed"):
        owned.prepare_lifecycle_entry(source)
    assert source._entry_original is calls[0] and source.entry is displaced
    assert source._entry_error is first and source._entry_traceback is first.__traceback__
    assert source._entry_state == "failed" and source._entry_credit == 4096
    with pytest.raises(owned.OwnedGatewayUnknown, match="already-attempted"):
        owned.prepare_lifecycle_entry(source)
    assert len(calls) == 1 and source._entry_error is first


def test_opaque_allocator_return_is_retained_without_initialization_or_adoption(source, monkeypatch):
    opaque, initialized = object(), []
    monkeypatch.setattr(run_owned.ManagedEntry, "__new__", staticmethod(lambda cls: opaque))
    monkeypatch.setattr(run_owned.ManagedEntry, "__init__", lambda *args: initialized.append(args))
    with pytest.raises(owned.OwnedGatewayUnknown, match="construction-failed"):
        owned.prepare_lifecycle_entry(source)
    assert source._entry_original is source._entry_allocator_result is opaque
    assert initialized == [] and source.entry is None and source._entry_state == "failed"
    with pytest.raises(owned.OwnedGatewayUnknown, match="not-composed"):
        require_prepared_entry(source)


def test_allocator_exception_keeps_inputs_credit_and_no_invented_partial_self(source, monkeypatch):
    first = RuntimeError("original allocator failure")

    def fail(_cls):
        raise first

    monkeypatch.setattr(run_owned.ManagedEntry, "__new__", staticmethod(fail))
    with pytest.raises(owned.OwnedGatewayUnknown, match="construction-failed"):
        owned.prepare_lifecycle_entry(source)
    assert source._entry_inputs is source and source._entry_credit == 4096
    assert source._entry_error is first and source._entry_traceback is first.__traceback__
    assert source._entry_original is source._entry_allocator_result is None
    assert source.entry is None


def test_swallowed_initializer_reentry_remains_the_first_failure(source, monkeypatch):
    original = run_owned.ManagedEntry.__init__
    calls, failures = [], []

    def reenter(value, original_source):
        calls.append(value)
        try:
            owned.prepare_lifecycle_entry(original_source)
        except owned.OwnedGatewayUnknown as error:
            failures.append(error)
        original(value, original_source)

    monkeypatch.setattr(run_owned.ManagedEntry, "__init__", reenter)
    with pytest.raises(owned.OwnedGatewayUnknown, match="construction-failed"):
        owned.prepare_lifecycle_entry(source)
    assert len(calls) == len(failures) == 1
    assert source._entry_original is calls[0] and source._entry_error is failures[0]
    assert source._entry_traceback is failures[0].__traceback__
    assert source._entry_state == "failed" and source.entry is None


def test_actual_foreground_consumer_refuses_a_changed_callback_before_running_it(source, monkeypatch):
    entry = owned.prepare_lifecycle_entry(source)
    calls = []
    entry.main = lambda value: calls.append(value)

    class Held(Exception):
        pass

    def hold():
        raise Held()

    monkeypatch.setattr(source, "hold_unknown", hold)
    with pytest.raises(Held):
        owned.run_foreground(source)
    assert calls == [] and source.phase == "unknown"
    assert source._entry_original is entry and source._entry_allocator_result is entry


def test_swallowed_allocator_reentry_refuses_before_the_original_initializer(source, monkeypatch):
    values, initialized, failures = [], [], []

    def create(cls):
        try:
            owned.prepare_lifecycle_entry(source)
        except owned.OwnedGatewayUnknown as error:
            failures.append(error)
        value = object.__new__(cls)
        values.append(value)
        return value

    monkeypatch.setattr(run_owned.ManagedEntry, "__new__", staticmethod(create))
    monkeypatch.setattr(run_owned.ManagedEntry, "__init__", lambda *args: initialized.append(args))
    with pytest.raises(owned.OwnedGatewayUnknown, match="construction-failed"):
        owned.prepare_lifecycle_entry(source)
    assert len(values) == len(failures) == 1
    assert source._entry_original is source._entry_allocator_result is values[0]
    assert source._entry_error is failures[0] and source._entry_credit == 4096
    assert initialized == [] and source.entry is None
