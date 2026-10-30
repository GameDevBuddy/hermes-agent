"""Original browser callers and normal closers with mock external graphs only."""
import asyncio

import pytest

from tests.tools.browser_owned_fixture import session_record, finish

pytest_plugins = ("tests.tools.browser_owned_fixture",)

pytestmark = pytest.mark.asyncio


async def test_session_publication_and_normal_gateway_closer_keep_original_resources(browser_fixture):
    f = browser_fixture
    record = session_record(f)
    info, process, log = record.obj, record.browser.process.obj, record.browser.launcher.log.obj
    assert record.source is f.source and record.original_parent is None
    assert record.supervisor.obj is f.registry.get("actor")
    assert process is f.launcher._procs["actor"] and not log.closed
    await finish(f)
    assert f.source.browser_closed and record.closed
    assert [item["method"] for item in record.supervisor.connection.obj.sent] == ["Browser.close"]
    assert process.waits == [None] and log.closed
    assert f.sessions._active_sessions["actor"] is info
    assert record.browser.obj is f.launcher._browsers["actor"]
    assert record.supervisor.obj is f.registry.get("actor")
    assert f.source.browser_snapshot.records == tuple(f.source.browser_records)


@pytest.mark.parametrize("route", ["single", "release", "idle", "all", "atexit", "launcher"])
async def test_real_cleanup_routes_use_same_original_normal_closer(browser_fixture, route):
    from tools import browser_session_lifecycle as lifecycle
    f = browser_fixture
    record = session_record(f)
    functions = {"single": lambda: lifecycle.cleanup_single_session("actor"),
                 "release": lambda: lifecycle.release_session_backend("actor", record.obj),
                 "idle": lambda: lifecycle._cleanup_one_inactive("actor", 1000),
                 "all": lifecycle.cleanup_all_browsers,
                 "atexit": f.sessions._atexit_cleanup,
                 "launcher": lambda: f.launcher.stop_browser("actor")}
    await asyncio.to_thread(functions[route])
    assert record.closed and record.browser.process.obj.waits == [None]
    await finish(f)
    assert f.source.browser_closed
    assert record.browser.process.obj.waits == [None]


async def test_empty_session_cache_does_not_hide_original_lifetime_from_gateway_close(browser_fixture):
    f = browser_fixture
    record = session_record(f)
    f.sessions._active_sessions.clear()  # loss is simulated; the independent real source book must still close it
    await finish(f)
    assert record.closed and record.browser.process.obj.waits == [None]
    assert f.source.browser_closed


async def test_actual_original_members_are_revalidated_after_success(browser_fixture):
    from tools.browser_owned_close import require_book
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = browser_fixture
    record = session_record(f)
    await finish(f)
    snapshot = f.source.browser_snapshot
    record.children = ()
    with pytest.raises(OwnedGatewayUnknown):
        require_book(snapshot)
    assert f.source.browser_error is not None or f.source.phase == "unknown"


async def test_original_launcher_claim_precedes_partial_popen_initializer(browser_fixture, monkeypatch):
    from tests.tools.browser_owned_fakes import Process
    f = browser_fixture
    f.activate()
    seen = []

    def observe(process):
        record = f.owned.find(process)
        assert record is not None and record.obj is process and record.attempted
        assert record in f.source.browser_records and record.original_parent.kind == "launcher"
        seen.append(record)

    monkeypatch.setattr(Process, "on_init", observe)
    record = session_record(f)
    assert seen == [record.browser.process]
    await finish(f)


async def test_native_lifecycle_gate_is_still_an_unconditional_refusal(browser_fixture):
    from hermes_owned_gateway import require_lifecycle_available, OwnedGatewayUnknown
    f = browser_fixture
    source = f.activate()
    source.phase = "attached"
    with pytest.raises(OwnedGatewayUnknown):
        require_lifecycle_available(source)
    assert not f.evidence.processes
