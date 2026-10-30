"""Use the pinned SDK's real messages with in-memory process/pipe doubles only."""
import asyncio
import io
import sys
from types import SimpleNamespace

import anyio
from anyio.abc import ByteReceiveStream
from mcp import StdioServerParameters, types
from mcp.client import stdio as sdk
from mcp.shared.message import SessionMessage
import pytest

import hermes_owned_gateway as ownership
from tools.mcp_tool_owned import stdio_client


class Stdout(ByteReceiveStream):
    def __init__(self, process, chunks):
        self.process, self.chunks = process, list(chunks)

    async def receive(self, max_bytes=65536):
        if self.chunks:
            return self.chunks.pop(0)
        await self.process.eof.wait()
        self.process.events.append("stdout-eof")
        raise anyio.EndOfStream

    async def aclose(self):
        self.process.events.append("stdout-close")


class Stdin:
    def __init__(self, process):
        self.process = process

    async def send(self, data):
        self.process.sent.append(data)

    async def aclose(self):
        self.process.events.append("stdin-close")
        self.process.eof.set()


class Process:
    def __init__(self, chunks=(), *, exit_code=0, wait_error=None):
        self.events, self.sent = [], []
        self.returncode, self.exit_code, self.wait_error = None, exit_code, wait_error
        self.eof = anyio.Event()
        self.stdout, self.stdin = Stdout(self, chunks), Stdin(self)

    async def wait(self):
        self.events.append("wait")
        if self.wait_error is not None:
            raise self.wait_error
        self.returncode = self.exit_code
        return self.exit_code

    async def aclose(self):
        self.events.append("process-close")

    def kill(self):
        raise AssertionError("source fixture reached forced process termination")

    def terminate(self):
        raise AssertionError("source fixture reached process signaling")


@pytest.fixture
def harness(monkeypatch):
    monkeypatch.setattr(ownership.sys, "stdin", SimpleNamespace(buffer=io.BytesIO()))
    source = ownership.OwnedGatewaySource(SimpleNamespace(attempt_id="stdio-source-fixture"), b"{}")
    source.job, source.phase = SimpleNamespace(require_current_member=lambda: None), "bound"
    monkeypatch.setattr(ownership, "_CURRENT", source)
    value = SimpleNamespace(source=source, process=None, sender=None, holds=[])

    async def factory(**kwargs):
        return value.process

    async def hold(self, reason):
        # Observe the actual hold boundary without hanging a unit-test worker.
        self.unknown(reason)
        value.holds.append((reason, tuple(self.child_transports), tuple(value.process.events)
                            if value.process is not None else ()))
        if value.process is not None:
            value.process.eof.set()
        if value.sender is not None:
            await value.sender.aclose()

    monkeypatch.setattr(sdk, "_create_platform_compatible_process", factory)
    monkeypatch.setattr(ownership.OwnedGatewaySource, "retain_unknown", hold)
    return value


def parameters():
    return StdioServerParameters(command=sys.executable, args=["never-launched-source-fixture"],
                                 encoding_error_handler="replace")


def test_actual_sdk_message_roundtrip_and_original_pipe_exit_order(harness):
    async def scenario():
        process = harness.process = Process([b'{"jsonrpc":"2.0","id":7,"result":{}}\n'])
        async with stdio_client(parameters(), errlog=None) as (incoming, outgoing):
            harness.sender = outgoing
            assert harness.source.child_transports == [process]
            message = await incoming.receive()
            assert message.message.id == 7
            await outgoing.send(SessionMessage(types.JSONRPCResponse(jsonrpc="2.0", id=8, result={})))
        assert process.sent and process.sent[0].endswith(b"\n")
        assert process.events.index("stdin-close") < process.events.index("wait")
        assert process.events.index("wait") < process.events.index("process-close")
        assert "stdout-eof" in process.events
        assert harness.source.child_transports == [] and harness.holds == []

    anyio.run(scenario)


def test_unread_stdout_is_drained_after_receiver_closes(harness):
    async def scenario():
        process = harness.process = Process([b'{"jsonrpc":"2.0","id":7,"result":{}}\n'] * 4)
        async with stdio_client(parameters(), errlog=None) as (_incoming, outgoing):
            harness.sender = outgoing
        assert process.stdout.chunks == []
        assert "stdout-eof" in process.events and "process-close" in process.events

    anyio.run(scenario)


def test_unknown_exit_result_cannot_release_original_process(harness):
    async def scenario():
        process = harness.process = Process(exit_code=None)
        with pytest.raises(ExceptionGroup):
            async with stdio_client(parameters(), errlog=None) as (_incoming, outgoing):
                harness.sender = outgoing
        assert harness.source.phase == "unknown" and harness.holds
        assert harness.source.child_transports == [process]
        assert "process-close" not in process.events

    anyio.run(scenario)


def test_wait_failure_holds_first_process_instead_of_allowing_retry(harness):
    async def scenario():
        process = harness.process = Process(wait_error=ValueError("original-wait-failed"))
        with pytest.raises(ExceptionGroup):
            async with stdio_client(parameters(), errlog=None) as (_incoming, outgoing):
                harness.sender = outgoing
        assert harness.source.phase == "unknown" and harness.holds
        assert harness.source.child_transports == [process]
        assert "process-close" not in process.events

    anyio.run(scenario)


def test_caller_cancellation_does_not_close_or_release_the_original_child(harness):
    async def scenario():
        process = harness.process = Process()
        with pytest.raises(asyncio.CancelledError):
            async with stdio_client(parameters(), errlog=None) as (_incoming, outgoing):
                harness.sender = outgoing
                raise asyncio.CancelledError("cancelled-source-fixture-caller")
        assert harness.source.phase == "unknown" and harness.holds
        assert harness.source.child_transports == [process]
        assert "stdin-close" not in process.events and "process-close" not in process.events

    anyio.run(scenario)


def test_constructor_failure_consumes_source_availability_without_second_spawn(harness, monkeypatch):
    calls = []

    async def fail(**kwargs):
        calls.append(kwargs)
        raise OSError("source-fixture-constructor-refusal")

    monkeypatch.setattr(sdk, "_create_platform_compatible_process", fail)

    async def scenario():
        with pytest.raises(OSError, match="source-fixture-constructor-refusal"):
            async with stdio_client(parameters(), errlog=None):
                raise AssertionError("constructor failure entered the client")
        with pytest.raises(ownership.OwnedGatewayUnknown):
            async with stdio_client(parameters(), errlog=None):
                raise AssertionError("unknown source created another child")
        assert len(calls) == 1 and harness.holds

    anyio.run(scenario)
