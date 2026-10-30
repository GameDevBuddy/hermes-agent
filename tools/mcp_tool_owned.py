"""Normal MCP teardown for the coupled source; legacy MCP teardown is unchanged.

The SDK's default stdio context escalates to process termination. This consumer
uses its pinned process constructor and message types, but closes stdin and waits
for the actual process and both original pipe workers without a kill deadline.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager


@asynccontextmanager
async def stdio_client(parameters, errlog):
    import anyio
    from anyio.streams.text import TextReceiveStream
    from mcp.client import stdio as sdk
    from mcp.shared.message import SessionMessage
    from mcp import types
    from hermes_owned_gateway import active

    source = active()
    source.require_child_work()
    command = await sdk._get_executable_command(parameters.command)
    process = await sdk._create_platform_compatible_process(
        command=command, args=parameters.args, env=sdk.get_default_environment() | (parameters.env or {}),
        errlog=errlog, cwd=parameters.cwd)
    source.child_transports.append(process)
    incoming_send, incoming = anyio.create_memory_object_stream(0)
    outgoing, outgoing_receive = anyio.create_memory_object_stream(0)
    reader_done, writer_done = anyio.Event(), anyio.Event()

    async def read():
        pending, discard = "", False
        try:
            text = TextReceiveStream(process.stdout, encoding=parameters.encoding,
                                     errors=parameters.encoding_error_handler)
            async for chunk in text:
                lines = (pending + chunk).split("\n")
                pending = lines.pop()
                for line in lines:
                    try:
                        message = SessionMessage(types.jsonrpc_message_adapter.validate_json(line, by_name=False))
                    except ValueError as error:
                        message = error
                    if not discard:
                        try:
                            await incoming_send.send(message)
                        except (anyio.ClosedResourceError, anyio.BrokenResourceError):
                            discard = True  # keep draining the actual child's stdout until EOF
        finally:
            await incoming_send.aclose()
            reader_done.set()

    async def write():
        try:
            async with outgoing_receive:
                async for message in outgoing_receive:
                    raw = message.message.model_dump_json(by_alias=True, exclude_unset=True) + "\n"
                    await process.stdin.send(raw.encode(parameters.encoding, parameters.encoding_error_handler))
        finally:
            writer_done.set()

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(read)
        tasks.start_soon(write)
        try:
            yield incoming, outgoing
        finally:
            await incoming.aclose()
            await outgoing.aclose()
            await writer_done.wait()
            await process.stdin.aclose()
            await process.wait()
            await reader_done.wait()
    await process.aclose()
    source.child_transports.remove(process)


async def shutdown_server(server):
    """Keep the original task/connection until its own context really finishes."""
    pending = list(server._pending_refresh_tasks)
    if pending:
        await asyncio.shield(asyncio.gather(*pending))
    server._shutdown_event.set()
    server._reconnect_event.set()
    if server._task is not None:
        await asyncio.shield(server._task)
    if any(not task.done() or task.cancelled() for task in server._pending_refresh_tasks):
        raise RuntimeError("MCP refresh work did not finish normally")
    server._deregister_tools()
    server.session = None


def shutdown_all():
    """Called off the gateway loop; stop the real MCP loop only after its tasks finish."""
    from tools import mcp_tool as origin
    from tools.mcp_tool_common import _core
    from hermes_owned_gateway import OwnedGatewayUnknown
    with _core._lock:
        loop, thread = origin._mcp_loop, origin._mcp_thread
        servers = dict(_core._servers)
        if _core._server_connecting:
            raise OwnedGatewayUnknown("MCP connection work is still unresolved")
    if loop is None:
        if servers or thread is not None:
            raise OwnedGatewayUnknown("MCP loop ownership is inconsistent")
        return

    async def settle():
        await asyncio.gather(*(shutdown_server(server) for server in servers.values()))
        others = asyncio.all_tasks() - {asyncio.current_task()}
        if others:
            await asyncio.gather(*others)
        await loop.shutdown_asyncgens()
        await loop.shutdown_default_executor()

    if not loop.is_running() or thread is None:
        raise OwnedGatewayUnknown("MCP loop cannot finish normally")
    asyncio.run_coroutine_threadsafe(settle(), loop).result()
    loop.call_soon_threadsafe(loop.stop)
    thread.join()
    loop.close()
    with _core._lock:
        if any(_core._servers.get(key) is not value for key, value in servers.items()):
            raise OwnedGatewayUnknown("MCP registry changed during normal shutdown")
        for key in servers:
            del _core._servers[key]
        origin._mcp_loop = origin._mcp_thread = None
    from tools.mcp_tool_config import _close_mcp_stderr_logs
    _close_mcp_stderr_logs(scope=None)
