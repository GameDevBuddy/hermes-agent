"""Sticky original browser failures and post-IO association failures; no browser process is run."""
import pytest

from hermes_owned_gateway import OwnedGatewayUnknown
from tests.tools.browser_owned_fixture import session_record, finish

pytest_plugins = ("tests.tools.browser_owned_fixture",)
from tests.tools.browser_owned_fakes import Process

pytestmark = pytest.mark.asyncio


async def test_partial_process_and_first_exception_are_retained_without_publication(browser_fixture, monkeypatch):
    f = browser_fixture
    failure = OSError("mock partial constructor")

    def fail(process):
        assert f.owned.find(process).obj is process
        raise failure

    monkeypatch.setattr(Process, "on_init", fail)
    with pytest.raises((OSError, OwnedGatewayUnknown)):
        session_record(f)
    record = next(record for record in f.source.browser_records if record.kind == "process")
    assert record.obj is f.evidence.processes[0] and record.error is failure
    assert f.source.browser_error is failure and not f.sessions._active_sessions
    with pytest.raises(OwnedGatewayUnknown):
        await finish(f)
    assert record.obj.waits == []


async def test_startup_deadline_does_not_abandon_kill_or_forget_original_process(browser_fixture, monkeypatch):
    f = browser_fixture
    monkeypatch.setattr(f.launcher, "_wait_for_devtools", lambda *args: (None, "mock deadline"))
    with pytest.raises(OwnedGatewayUnknown):
        session_record(f)
    record = next(record for record in f.source.browser_records if record.kind == "process")
    assert record.obj is f.evidence.processes[0] and record.obj.waits == []
    assert record.original_parent.log.obj.closed is False
    assert not f.source.browser_closed


@pytest.mark.parametrize("fault", ["job", "runtime", "profile", "parent", "creator", "connection"])
async def test_after_send_revalidates_actual_source_and_members_before_process_wait(browser_fixture, fault):
    f = browser_fixture
    record = session_record(f)
    supervisor = record.supervisor

    def change(message):
        changes = {"job": lambda: setattr(f.source.job, "revoked", True),
                   "runtime": lambda: setattr(f.runner, "_gdb_original_source_runtime", object()),
                   "profile": lambda: setattr(record, "home_key", "foreign-profile"),
                   "parent": lambda: setattr(supervisor, "parent", None),
                   "creator": lambda: setattr(supervisor, "creator_thread", object()),
                   "connection": lambda: setattr(supervisor.obj, "_ws", object())}
        changes[fault]()

    supervisor.connection.obj.on_send = change
    with pytest.raises((OwnedGatewayUnknown, RuntimeError)):
        await finish(f)
    assert record.browser.process.obj.waits == []
    assert not f.source.browser_closed and f.source.browser_error is not None


async def test_original_wait_failure_is_not_retried_or_replaced_by_empty_cache(browser_fixture, monkeypatch):
    f = browser_fixture
    record = session_record(f)
    failure = OSError("mock original wait failed")

    def fail_wait(process):
        raise failure

    monkeypatch.setattr(Process, "on_wait", fail_wait)
    with pytest.raises(OSError):
        await finish(f)
    assert f.source.browser_error is failure
    f.sessions._active_sessions.clear()
    with pytest.raises(OwnedGatewayUnknown):
        await finish(f)
    assert record.browser.process.obj.waits == [None] and not record.browser.launcher.log.obj.closed


@pytest.mark.parametrize("consumer", ["forget-session", "forget-launcher", "force", "supervisor-pop", "alias"])
async def test_cache_eviction_alias_and_force_consumers_retain_original_refs(browser_fixture, consumer):
    from tools import browser_session_lifecycle as lifecycle
    f = browser_fixture
    record = session_record(f)
    actions = {"forget-session": lambda: lifecycle._forget_session_tracking("actor", session=True),
               "forget-launcher": lambda: f.launcher._forget("actor", record.browser.obj),
               "force": lambda: lifecycle._force_reap("actor"),
               "supervisor-pop": lambda: f.registry._pop("actor"),
               "alias": lambda: f.registry.get_or_start("alias", record.obj["cdp_url"])}
    with pytest.raises(OwnedGatewayUnknown):
        actions[consumer]()
    assert f.sessions._active_sessions["actor"] is record.obj
    assert f.launcher._procs["actor"] is record.browser.process.obj
    assert f.registry.get("actor") is record.supervisor.obj
    assert record.browser.process.obj.waits == []
