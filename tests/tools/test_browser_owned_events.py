"""Actual frame/dialog event consumers with mocked browser workers and timer factories."""
import pytest

from tests.tools.browser_owned_fixture import session_record, finish

pytest_plugins = ("tests.tools.browser_owned_fixture",)
pytestmark = pytest.mark.asyncio


async def test_actual_frame_event_retains_original_domain_task_before_cdp_calls(browser_fixture):
    f = browser_fixture
    record = session_record(f)
    supervisor = record.supervisor.obj
    await supervisor._on_target_attached({"sessionId": "child", "targetInfo": {
        "type": "iframe", "targetId": "frame", "url": "https://example.invalid"}})
    event = next(item for item in f.source.browser_records if item.kind == "cdp-event")
    assert event.original_parent is record.supervisor and event.active and event.obj is not None
    await event.obj
    assert event.done and not event.active and event.references[0] is event.coro
    assert any(message.get("sessionId") == "child" for message in record.supervisor.connection.obj.sent)
    await finish(f)
    assert event in f.source.browser_snapshot.records and f.source.browser_closed


async def test_actual_dialog_retirement_cancels_only_original_timer_normally(browser_fixture):
    f = browser_fixture
    record = session_record(f)
    supervisor = record.supervisor.obj
    supervisor._admit_dialog(type="alert", message="fixture", default_prompt="", session_id="mock-page", frame_id=None)
    handle = supervisor._dialog_watchdogs["d-1"]
    timer = f.owned.find(handle)
    assert timer.kind == "cdp-timer" and timer.original_parent is record.supervisor and timer.active
    assert handle.delay == supervisor.dialog_timeout_s and not handle.cancelled()
    await supervisor._on_dialog_closed({}, "mock-page")
    assert handle.cancelled() and timer.done and timer.closed and not timer.active
    await finish(f)
    assert f.source.browser_closed and timer in f.source.browser_snapshot.records


async def test_actual_watchdog_callback_retains_child_task_and_normal_cdp_response(browser_fixture):
    f = browser_fixture
    record = session_record(f)
    supervisor = record.supervisor.obj
    supervisor._admit_dialog(type="alert", message="fixture", default_prompt="", session_id="mock-page", frame_id=None)
    handle = supervisor._dialog_watchdogs["d-1"]
    timer = f.owned.find(handle)
    task = handle.fire()  # mocked lower scheduler fires the original product callback
    assert timer.fired and timer.child is task and f.owned.find(task).original_parent is record.supervisor
    await task
    assert timer.done and timer.closed and not timer.active and handle.cancelled()
    assert record.supervisor.connection.obj.sent[0]["method"] == "Page.handleJavaScriptDialog"
    await finish(f)
    assert f.source.browser_closed


async def test_pending_dialog_refuses_shutdown_before_any_browser_close_or_process_wait(browser_fixture):
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = browser_fixture
    record = session_record(f)
    supervisor = record.supervisor.obj
    supervisor._admit_dialog(type="alert", message="fixture", default_prompt="", session_id="mock-page", frame_id=None)
    original = supervisor._dialog_watchdogs["d-1"]
    with pytest.raises(OwnedGatewayUnknown):
        await finish(f)
    assert not original.cancelled() and supervisor._dialog_watchdogs["d-1"] is original
    assert not record.supervisor.connection.obj.sent and not record.browser.process.obj.waits
