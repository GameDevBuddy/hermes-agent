"""Actual captured-page and helper pipe consumers; all external owners are in-memory models."""
import asyncio
import subprocess

import pytest

from tests.tools.browser_owned_fixture import session_record, finish

pytest_plugins = ("tests.tools.browser_owned_fixture",)

pytestmark = pytest.mark.asyncio


async def test_actual_captured_page_call_keeps_original_connection_and_reply(browser_fixture):
    f = browser_fixture
    session = session_record(f)
    capture = await asyncio.to_thread(f.registry.capture, "actor")
    reply = await asyncio.to_thread(capture.call, "Runtime.evaluate", {"expression": "7"},
                                    session_id=capture.page_session_id)
    assert reply == {"id": 1, "result": {}}
    record = f.owned.find(capture)
    assert record.owner is session.supervisor
    assert record.references[3] is session.supervisor.connection.obj
    assert capture._ws.sent[0]["sessionId"] == capture.page_session_id
    await finish(f)
    assert f.source.browser_closed and record in f.source.browser_snapshot.records


async def test_captured_page_refuses_callback_retarget_before_original_send(browser_fixture):
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = browser_fixture
    session = session_record(f)
    capture = await asyncio.to_thread(f.registry.capture, "actor")
    original = capture._ws
    with pytest.raises(OwnedGatewayUnknown):
        await asyncio.to_thread(capture.call, "Runtime.evaluate", {"expression": "7"},
                                before_send=lambda: setattr(capture, "_ws", object()))
    assert not original.sent and not session.browser.process.obj.waits
    assert f.owned.find(capture).references[3] is original
    with pytest.raises(OwnedGatewayUnknown):
        await finish(f)


async def test_actual_page_call_retains_exact_send_failure_without_replacement(browser_fixture):
    f = browser_fixture
    session = session_record(f)
    capture = await asyncio.to_thread(f.registry.capture, "actor")
    original = capture._ws
    failure = OSError("synthetic send failure")

    def fail_send(message):
        raise failure

    original.on_send = fail_send
    with pytest.raises(OSError) as caught:
        await asyncio.to_thread(capture.call, "Runtime.evaluate", {"expression": "7"})
    assert caught.value is failure and f.source.browser_error is failure
    assert f.owned.find(capture).references[3] is original
    assert len(original.sent) == 1 and not session.browser.process.obj.waits


def invoke_helper(f, timeout=23):
    # The command scope is a test caller; the process, pipe and completion consumer is the
    # actual product function. The opaque browser harness itself remains unsupported.
    from tools import browser_owned_commands as commands
    from tools.browser_use_cli import _run_cli_killing_process_group

    @commands.entry
    def caller():
        f.sessions.get_session_info("actor")
        return _run_cli_killing_process_group(["mock-helper-not-executed"], "print(7)", {"PATH": "mock"}, timeout)

    return caller()


async def test_actual_helper_waits_original_pipes_eof_and_joins_original_workers(browser_fixture):
    f = browser_fixture
    session_record(f)
    result = invoke_helper(f)
    process = f.evidence.processes[-1]
    record = f.owned.find(process)
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0 and result.stdout == "mock stdout\n" and result.stderr == "mock stderr\n"
    assert process.waits == [23] and process.stdin.writes == ["print(7)"]
    assert all(pipe.closed for pipe in (process.stdin, process.stdout, process.stderr))
    assert record.stdout.eof and record.stderr.eof and record.wait_done
    assert all(pipe.worker.obj.joins == [None] for pipe in (record.stdin, record.stdout, record.stderr))
    await finish(f)
    assert f.source.browser_closed and record in f.source.browser_snapshot.records


async def test_actual_helper_wait_failure_is_retained_once_with_original_pipes(browser_fixture, monkeypatch):
    from tests.tools.browser_owned_fakes import Process
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = browser_fixture
    session_record(f)
    failure = subprocess.TimeoutExpired("mock-helper-not-executed", 23)

    def fail_wait(process):
        raise failure

    monkeypatch.setattr(Process, "on_wait", fail_wait)
    with pytest.raises(subprocess.TimeoutExpired) as caught:
        invoke_helper(f)
    process = f.evidence.processes[-1]
    record = f.owned.find(process)
    assert caught.value is failure and f.source.browser_error is failure
    assert process.waits == [23] and record.wait_attempted and not record.wait_done
    assert record.pipes == (process.stdin, process.stdout, process.stderr)
    with pytest.raises(OwnedGatewayUnknown):
        await finish(f)
    assert process.waits == [23]


@pytest.mark.parametrize("value", [None, "", "none"])
async def test_ambiguous_provider_defaults_refuse_before_external_factory(browser_fixture, monkeypatch, value):
    from tools import browser_session_cloud as cloud
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = browser_fixture
    monkeypatch.setattr(cloud, "browser_section", lambda: {"cloud_provider": value})
    with pytest.raises(OwnedGatewayUnknown):
        session_record(f)
    assert not f.evidence.processes
