"""Actual startup constructor types with inert initializers; no gateway or loop IO."""
import asyncio
import io
from types import SimpleNamespace as Data

import pytest

import hermes_owned_gateway as owned
from gateway import owned_startup_construction as construction
from gateway.run import GatewayRunner
from gateway.run_owned_shutdown import ManagedSettlement


@pytest.fixture
def source(monkeypatch):
    monkeypatch.setattr(owned.sys, "stdin", Data(buffer=io.BytesIO()))
    value = owned.OwnedGatewaySource(Data(attempt_id="startup-fixture"), b"{}")
    value.job = Data(require_current_member=lambda: None)
    value.phase = "attached"
    monkeypatch.setattr(owned, "_CURRENT", value)
    owned.prepare_lifecycle_entry(value)
    return value


def test_unprepared_source_refuses_before_import_or_allocator(source, monkeypatch):
    source.entry = None
    called = []
    monkeypatch.setattr(GatewayRunner, "__new__", staticmethod(lambda cls: called.append(cls)))
    with pytest.raises(owned.OwnedGatewayUnknown, match="not-composed"):
        construction.construct_runner(source, Data())
    assert called == [] and source._startup_credit == 0
    assert source._original_runner_construction is None


def test_actual_runner_partial_self_and_first_error_survive_record_displacement(source, monkeypatch):
    config, first, seen = Data(), ValueError("runner initializer failure"), []
    displaced = object()

    def fail(value, original_config):
        record = source._original_runner_construction
        assert record["original"] is record["allocator_result"] is value
        assert record["inputs"][0] is original_config is config
        seen.append(value)
        source._runner_construction = displaced
        value.config = config
        raise first

    monkeypatch.setattr(GatewayRunner, "__init__", fail)
    with pytest.raises(ValueError) as caught:
        construction.construct_runner(source, config)
    assert caught.value is source._startup_error is first
    record = source._original_runner_construction
    assert record["original"] is seen[0] and record["state"] == "failed"
    assert record["error"] is first and record["traceback"] is source._startup_traceback
    assert source._runner_construction is displaced and source.runner is None
    with pytest.raises(owned.OwnedGatewayUnknown, match="already-attempted"):
        construction.construct_runner(source, config)
    assert len(seen) == 1 and source._startup_credit == 4096


def test_opaque_runner_allocator_result_is_retained_and_never_initialized(source, monkeypatch):
    opaque, called = object(), []
    monkeypatch.setattr(GatewayRunner, "__new__", staticmethod(lambda cls, *args: opaque))
    monkeypatch.setattr(GatewayRunner, "__init__", lambda *args: called.append(args))
    with pytest.raises(owned.OwnedGatewayUnknown, match="foreign-or-preowned"):
        construction.construct_runner(source, Data())
    record = source._original_runner_construction
    assert record["original"] is record["allocator_result"] is opaque
    assert called == [] and source.runner is None and record["state"] == "failed"


def test_swallowed_allocator_reentry_keeps_the_first_error_and_spent_credit(source, monkeypatch):
    config, failures, values, initialized = Data(), [], [], []

    def create(cls, original_config):
        try:
            construction.construct_runner(source, original_config)
        except owned.OwnedGatewayUnknown as error:
            failures.append(error)
        value = object.__new__(cls)
        values.append(value)
        return value

    monkeypatch.setattr(GatewayRunner, "__new__", staticmethod(create))
    monkeypatch.setattr(GatewayRunner, "__init__", lambda value, cfg: initialized.append(value))
    with pytest.raises(owned.OwnedGatewayUnknown) as caught:
        construction.construct_runner(source, config)
    assert len(failures) == len(values) == 1
    assert caught.value is source._startup_error is failures[0]
    assert source._original_runner_construction["original"] is values[0]
    assert source._startup_credit == 4096 and source.runner is None
    assert initialized == []


def test_prepared_runner_consumer_refuses_replacement_without_adopting_it(source, monkeypatch):
    monkeypatch.setattr(GatewayRunner, "__init__", lambda value, config: setattr(value, "config", config))
    runner = construction.construct_runner(source, Data())
    assert construction.require_original(source, "runner", runner) is runner
    replacement = object.__new__(GatewayRunner)
    with pytest.raises(owned.OwnedGatewayUnknown, match="changed"):
        construction.require_original(source, "runner", replacement)
    assert source._original_runner_construction["original"] is runner


def test_loop_policy_refuses_before_foreign_factory_but_keeps_credit(source, monkeypatch):
    called = []
    monkeypatch.setattr(asyncio, "get_event_loop_policy", lambda: Data(new_event_loop=lambda: called.append(1)))
    with pytest.raises(owned.OwnedGatewayUnknown, match="noncanonical-event-loop-policy"):
        construction.construct_loop(source)
    assert called == [] and source.loop_driver is None and source._startup_credit == 4096
    assert source._original_loop_construction["allocator_result"] is None


def test_actual_loop_partial_initializer_is_retained_without_cleanup_or_retry(source, monkeypatch):
    _policy, cls = construction._loop_factory()
    first, seen = ValueError("loop initializer failure"), []

    def fail(loop):
        loop._closed = True  # Inert fixture; no stdlib loop resources were allocated.
        assert source._original_loop_construction["original"] is loop
        seen.append(loop)
        raise first

    monkeypatch.setattr(cls, "__init__", fail)
    with pytest.raises(ValueError) as caught:
        construction.construct_loop(source)
    assert caught.value is source._startup_error is first
    assert source._original_loop_construction["original"] is seen[0]
    assert source.loop_driver is None and source._startup_credit == 4096
    with pytest.raises(owned.OwnedGatewayUnknown, match="already-attempted"):
        construction.construct_loop(source)
    assert len(seen) == 1


def test_actual_settlement_retains_bound_inputs_before_its_failing_initializer(source, monkeypatch):
    monkeypatch.setattr(GatewayRunner, "__init__", lambda value, config: setattr(value, "config", config))
    first, seen = ValueError("settlement initializer failure"), []

    def fail(value, original_source, original_runner):
        record = source._original_settlement_construction
        assert record["original"] is value and record["inputs"] == (original_source, original_runner)
        seen.append(value)
        value.source, value.runner = original_source, original_runner
        raise first

    async def scenario():
        runner = construction.construct_runner(source, Data())
        source.bind_runner(runner)
        monkeypatch.setattr(ManagedSettlement, "__init__", fail)
        with pytest.raises(ValueError) as caught:
            construction.construct_settlement(source, runner)
        assert caught.value is source._startup_error is first
        assert source._original_settlement_construction["original"] is seen[0]
        assert seen[0].source is source and seen[0].runner is runner
        assert source.settlement is None and source._startup_credit == 8192

    asyncio.run(scenario())


def test_actual_settlement_consumer_refuses_displaced_publication(source, monkeypatch):
    monkeypatch.setattr(GatewayRunner, "__init__", lambda value, config: setattr(value, "config", config))

    async def scenario():
        runner = construction.construct_runner(source, Data())
        source.bind_runner(runner)
        settlement = construction.construct_settlement(source, runner)
        settlement.require_task()
        assert settlement.source is source and settlement.runner is runner
        assert source.settlement is source._original_settlement_construction["original"] is settlement
        source.settlement = object()
        with pytest.raises(owned.OwnedGatewayUnknown, match="publication-changed"):
            settlement.require_task()
        assert source._original_settlement_construction["original"] is settlement
        assert not settlement.finished and source._startup_credit == 8192

    asyncio.run(scenario())
