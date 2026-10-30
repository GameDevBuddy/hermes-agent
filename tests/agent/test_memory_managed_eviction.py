"""Managed memory eviction must use the same normal path as final shutdown."""
import io
from types import SimpleNamespace

import pytest

import hermes_owned_gateway as ownership
from agent.memory_manager import MemoryManager
from agent.memory_provider import MemoryProvider
from hermes_constants import reset_hermes_home_override, set_hermes_home_override


class Provider(MemoryProvider):
    name = "managed-source-fixture"

    def __init__(self):
        self.closed = 0

    def is_available(self):
        return True

    def initialize(self, session_id, **kwargs):
        pass

    def get_tool_schemas(self):
        return []

    def shutdown_owned(self):
        self.closed += 1


@pytest.fixture
def managed(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    monkeypatch.setattr(ownership.sys, "stdin", SimpleNamespace(buffer=io.BytesIO()))
    source = ownership.OwnedGatewaySource(SimpleNamespace(attempt_id="memory-route-fixture"), b"{}")
    source.job, source.phase = SimpleNamespace(require_current_member=lambda: None), "bound"
    monkeypatch.setattr(ownership, "_CURRENT", source)
    manager, provider = MemoryManager(), Provider()
    manager.add_provider(provider)
    manager.initialize_all("managed-source-fixture")
    try:
        yield manager, provider, source
    finally:
        reset_hermes_home_override(token)


def test_original_source_is_captured_by_the_actual_memory_constructor(managed):
    manager, _provider, source = managed
    assert getattr(manager, "_owned_source", None) is source


def test_managed_eviction_never_enters_bounded_default_drain(managed, monkeypatch):
    manager, provider, _source = managed

    def forbidden():
        raise AssertionError("managed memory entered legacy bounded drain")

    monkeypatch.setattr(manager, "_drain_sync_executor", forbidden)
    manager.shutdown_all()
    assert provider.closed == 1


def test_completed_managed_eviction_is_rechecked_without_replay(managed):
    manager, provider, _source = managed
    manager.shutdown_all()
    manager.shutdown_all()
    assert provider.closed == 1


def test_changed_original_provider_blocks_rechecked_managed_close(managed):
    manager, _provider, _source = managed
    manager.shutdown_all()
    manager._providers.append(Provider())
    with pytest.raises(RuntimeError, match="changed"):
        manager.shutdown_all()


def test_failed_managed_close_retains_manager_on_original_source(managed, monkeypatch):
    manager, _provider, source = managed

    def fail():
        raise ValueError("managed-provider-close-failed")

    monkeypatch.setattr(manager._providers[0], "shutdown_owned", fail)
    with pytest.raises(ValueError, match="managed-provider-close-failed"):
        manager.shutdown_all()
    assert any(value is manager for value in source.unresolved_resource_refs)
    assert source.phase == "unknown"
