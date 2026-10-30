"""Opaque graphs refuse before external work; unmanaged paths preserve their existing entry behavior."""
import pytest

from hermes_owned_gateway import OwnedGatewayUnknown
from tests.tools.browser_owned_fixture import session_record, finish

pytest_plugins = ("tests.tools.browser_owned_fixture",)

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("setting,value", [("cloud_provider", "browserbase"), ("cloud_provider", "auto"),
                                            ("use_real_profile", True), ("engine", "lightpanda"),
                                            ("user_data_dir", "mock-shared-profile"), ("headed", True),
                                            ("cdp_url", "https://mock.invalid")])
async def test_unsupported_graph_retains_claim_and_refuses_before_process_factory(browser_fixture, monkeypatch, setting, value):
    from tools import browser_session_cloud as cloud
    f = browser_fixture
    config = {"cloud_provider": "local", setting: value}
    monkeypatch.setattr(cloud, "browser_section", lambda: config)
    with pytest.raises(OwnedGatewayUnknown):
        session_record(f)
    assert f.source.browser_records and f.source.browser_error is not None
    assert f.evidence.processes == []


async def test_pre_source_cached_session_is_retained_and_never_adopted(browser_fixture):
    f = browser_fixture
    f.activate()
    foreign = {"cdp_url": "ws://mock.invalid", "features": {"local": True}}
    f.sessions._active_sessions["actor"] = foreign
    with pytest.raises(OwnedGatewayUnknown):
        f.sessions.get_session_info("actor")
    assert f.sessions._active_sessions["actor"] is foreign
    assert f.source.browser_records[-1].references == (foreign,)
    assert not f.evidence.processes


async def test_opaque_harness_daemon_refuses_before_its_popen(browser_fixture):
    from tools.browser_owned_commands import daemon_guard
    f = browser_fixture
    record = session_record(f)
    before = tuple(f.evidence.processes)
    with pytest.raises(OwnedGatewayUnknown):
        daemon_guard(("mock-daemon",), {"BU_CDP_URL": record.obj["cdp_url"]})
    assert tuple(f.evidence.processes) == before
    assert f.source.browser_records[-1].kind == "opaque-harness"


async def test_unmanaged_launcher_uses_original_default_entry(browser_fixture, monkeypatch):
    import hermes_owned_gateway as ownership
    f = browser_fixture
    monkeypatch.setattr(ownership, "_CURRENT", None)
    browser, error = f.launcher.launch_chromium("plain")
    assert error is None and browser is f.launcher._browsers["plain"]
    assert f.owned.find(browser) is None
    assert f.evidence.processes[0].kwargs["stdin"] is f.launcher.subprocess.DEVNULL


async def test_method_replacement_after_ready_refuses_before_close_io(browser_fixture):
    f = browser_fixture
    record = session_record(f)
    record.supervisor.connection.obj.send = lambda raw: None
    with pytest.raises(OwnedGatewayUnknown):
        await finish(f)
    assert record.browser.process.obj.waits == []


async def test_foreign_worker_loop_is_not_accepted_as_the_original_cdp_loop(browser_fixture, monkeypatch):
    from tools import browser_owned_cdp as protocol
    f = browser_fixture
    record = session_record(f)
    monkeypatch.setattr(protocol, "_running_loop", lambda: object())
    with pytest.raises(OwnedGatewayUnknown):
        await record.supervisor.obj._cdp("Page.enable")
    assert record.supervisor.connection.obj.sent == []
