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
    from hermes_owned_gateway import OwnedGatewayUnknown, active
    if server._owned_shutdown_attempted:
        raise OwnedGatewayUnknown("owned-MCP-shutdown-already-attempted")
    server._owned_shutdown_attempted = True
    source = server._owned_source
    try:
        if source is None or active() is not source or server not in source.mcp_contexts:
            raise OwnedGatewayUnknown("original-MCP-constructor-source-required")
        pending = list(server._pending_refresh_tasks)
        server._owned_shutdown_pending = tuple(pending)
        server._owned_shutdown_task = server._task
        original = pending + ([server._owned_shutdown_task] if server._owned_shutdown_task is not None else [])
        if any(task.get_loop() is not asyncio.get_running_loop() for task in original):
            raise OwnedGatewayUnknown("original-MCP-task-loop-required")
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
        server._owned_shutdown_completed = True
    except BaseException as error:
        server._owned_shutdown_error = error
        if source is not None:
            source.unknown(f"normal-MCP-shutdown-incomplete:{error}")
        raise


def require_server_settled(server, source):
    """Recheck the same locally closed work; never replay a resource callback."""
    from hermes_owned_gateway import OwnedGatewayUnknown, active
    if (active() is not source or server._owned_source is not source
            or server not in source.mcp_contexts or not server._owned_shutdown_attempted
            or not server._owned_shutdown_completed or server._owned_shutdown_error is not None
            or server.session is not None or server._task is not server._owned_shutdown_task):
        raise OwnedGatewayUnknown("completed-MCP-work-changed")
    pending = server._owned_shutdown_pending
    if pending is None or any(task not in pending for task in server._pending_refresh_tasks):
        raise OwnedGatewayUnknown("completed-MCP-refresh-work-changed")
    original = (*pending, *((server._owned_shutdown_task,) if server._owned_shutdown_task is not None else ()))
    if any(not task.done() or task.cancelled() or task.exception() is not None for task in original):
        raise OwnedGatewayUnknown("original-MCP-work-not-normally-finished")


def shutdown_all():
    """Called off the gateway loop; stop the real MCP loop only after its tasks finish."""
    from tools import mcp_tool as origin
    from tools.mcp_tool_common import _core
    from hermes_owned_gateway import OwnedGatewayUnknown
    from hermes_owned_gateway import active
    source = active()
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
        tasks = []
        for server in servers.values():
            context = source.mcp_contexts.get(server)
            if context is None:
                raise OwnedGatewayUnknown("original-MCP-constructor-context-missing")
            if server._owned_shutdown_completed:
                require_server_settled(server, source)
                continue
            tasks.append(context.run(asyncio.create_task, shutdown_server(server)))
        await asyncio.gather(*tasks)
        for server in servers.values():
            require_server_settled(server, source)
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


async def run_stdio_owned(server, config):
    from tools.mcp_tool_common import _core
    from tools import mcp_tool_config as configuration
    from tools import mcp_tool_registration as registration
    from tools.mcp_tool_transport import _connect_inputs
    source = server._owned_source
    source.require_child_work()
    if not _core._ensure_mcp_sdk():
        raise RuntimeError("declared MCP SDK unavailable")
    inputs, _reason = _connect_inputs(server.name, config)
    server._resolved_identity = registration._identity_digest(inputs)
    command, environment, cwd = inputs
    command, arguments = await _core._preflight_stdio_command(server.name, command, config.get("args", []))
    parameters = _core.StdioServerParameters(command=command, args=arguments, env=environment or None,
                                            cwd=cwd, encoding_error_handler="replace")
    configuration._write_stderr_log_header(server.name)
    stderr = configuration._StderrTee(configuration._get_mcp_stderr_log())
    source.stderr_transports.append(stderr)
    try:
        async with stdio_client(parameters, errlog=stderr.sink) as (read, write):
            async with _core.ClientSession(read, write, **server._session_kwargs()) as session:
                timeout = float(config.get("connect_timeout", _core._DEFAULT_CONNECT_TIMEOUT))
                return await server._serve_session(session, timeout, mark_lifecycle=True)
    finally:
        # The exact stderr sink/reader created above, not an orphan-PID sweep.
        await asyncio.to_thread(stderr.close, timeout=None)
        if stderr._reader.is_alive():
            raise RuntimeError("original-MCP-stderr-reader-still-live")
        source.stderr_transports.remove(stderr)
