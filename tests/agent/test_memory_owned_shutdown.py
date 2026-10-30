"""Owned memory settlement uses private fixtures and source-test executor work only."""
from concurrent.futures import Future, ThreadPoolExecutor
import contextvars
import threading
from types import SimpleNamespace

import pytest

from agent.memory_manager import MemoryManager
from agent.memory_provider import MemoryProvider
from hermes_constants import reset_hermes_home_override, set_hermes_home_override


class Provider(MemoryProvider):
    name = "owned-source-fixture"

    def __init__(self):
        self.calls = []
        self.on_close = lambda: None

    def is_available(self):
        return True

    def initialize(self, session_id, **kwargs):
        self.session_id = session_id

    def get_tool_schemas(self):
        return []

    def shutdown_owned(self):
        self.calls.append("close")
        self.on_close()


@pytest.fixture
def memory(tmp_path):
    token = set_hermes_home_override(tmp_path)
    manager, provider = MemoryManager(), Provider()
    manager.add_provider(provider)
    manager.initialize_all("source-fixture")
    try:
        yield manager, provider
    finally:
        reset_hermes_home_override(token)


def test_successful_owned_memory_close_cannot_be_replayed(memory):
    manager, provider = memory
    manager.shutdown_owned()
    with pytest.raises(RuntimeError, match="already-attempted"):
        manager.shutdown_owned()
    assert provider.calls == ["close"]


def test_failed_provider_keeps_first_error_and_does_not_retry(memory):
    manager, provider = memory
    failure = ValueError("provider-close-failed")

    def fail():
        raise failure

    provider.on_close = fail
    with pytest.raises(ValueError, match="provider-close-failed"):
        manager.shutdown_owned()
    assert manager._owned_shutdown_error is failure
    assert manager._owned_shutdown_refs[2][0] is provider
    with pytest.raises(RuntimeError, match="already-attempted"):
        manager.shutdown_owned()
    assert provider.calls == ["close"]
    assert manager.providers == [provider]


def test_foreign_profile_cannot_reach_executor_or_provider(memory, tmp_path):
    manager, provider = memory
    calls = []
    manager._sync_executor = SimpleNamespace(shutdown=lambda **kw: calls.append(kw))
    token = set_hermes_home_override(tmp_path / "different")
    try:
        with pytest.raises(RuntimeError, match="caller-refused"):
            manager.shutdown_owned()
    finally:
        reset_hermes_home_override(token)
    assert calls == [] and provider.calls == []


def test_empty_provider_list_still_requires_the_original_profile(memory, tmp_path):
    manager, provider = memory
    manager._providers.clear()
    calls = []
    manager._sync_executor = SimpleNamespace(shutdown=lambda **kw: calls.append(kw))
    token = set_hermes_home_override(tmp_path / "different")
    try:
        with pytest.raises(RuntimeError, match="caller-refused"):
            manager.shutdown_owned()
    finally:
        reset_hermes_home_override(token)
    assert calls == [] and provider.calls == []


def test_failed_original_write_is_not_replaced_with_a_closed_provider(memory):
    manager, provider = memory
    future = Future()
    future.set_exception(ValueError("original-write-failed"))
    manager._background_futures[future] = "write"
    with pytest.raises(ValueError, match="original-write-failed"):
        manager.shutdown_owned()
    assert provider.calls == []
    assert future in manager._background_futures


def test_original_executor_write_finishes_before_provider_close(memory):
    manager, provider = memory
    release, started, closing = threading.Event(), threading.Event(), threading.Event()
    order, errors = [], []
    executor = ThreadPoolExecutor(max_workers=1)
    original_shutdown = executor.shutdown

    def finish_write():
        started.set()
        release.wait()
        order.append("write")

    def shutdown(**kwargs):
        closing.set()
        return original_shutdown(**kwargs)

    executor.shutdown = shutdown
    manager._sync_executor = executor
    future = executor.submit(finish_write)
    manager._background_futures[future] = "write"
    provider.on_close = lambda: order.append("provider")
    context = contextvars.copy_context()

    def close():
        try:
            context.run(manager.shutdown_owned)
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=close)
    assert started.wait(timeout=10)
    thread.start()
    try:
        assert closing.wait(timeout=10)
        assert provider.calls == []
        release.set()
        thread.join()
        assert order == ["write", "provider"] and errors == []
        assert manager._sync_executor is executor
        assert all(not worker.is_alive() for worker in executor._threads)
    finally:
        release.set()
        thread.join()
        original_shutdown(wait=True)


def test_provider_replacement_during_close_is_not_accepted(memory):
    manager, provider = memory
    replacement = Provider()
    provider.on_close = lambda: manager._providers.append(replacement)
    with pytest.raises(RuntimeError, match="provider.*changed"):
        manager.shutdown_owned()
    assert manager.providers == [provider, replacement]
    assert replacement.calls == []


def test_executor_replacement_is_not_adopted_for_normal_completion(memory):
    manager, provider = memory
    replacement = SimpleNamespace()

    def replace(**kwargs):
        manager._sync_executor = replacement

    original = SimpleNamespace(shutdown=replace)
    manager._sync_executor = original
    with pytest.raises(RuntimeError, match="executor.*changed"):
        manager.shutdown_owned()
    assert manager._sync_executor is replacement
    assert provider.calls == []


def test_prior_bounded_shutdown_cannot_be_adopted_as_owned_completion(memory):
    manager, provider = memory
    manager._shutting_down = True
    with pytest.raises(RuntimeError, match="prior-memory-shutdown"):
        manager.shutdown_owned()
    assert provider.calls == []
