"""Real source capture initializers with synthetic lower IO; no service/bootstrap/native acceptance."""
from types import SimpleNamespace
import asyncio

from gateway import native_source_runtime as source
from tests.gateway.native_source_fixture import source_graph, finish_source_graph, runner_from_actual_constructor

RETAINED = []


async def capture_case(monkeypatch, directory, kind):
    if kind == "runtime":
        runner = runner_from_actual_constructor(monkeypatch)
        case = SimpleNamespace(kind=kind, runner=runner, graph=None,
                               task=asyncio.current_task(), loop=asyncio.get_running_loop())
        case.invoke = lambda: source.capture_original_runner(runner, case.task, case.loop)
        case.initializer = source.OriginalSourceRuntime.__init__
        case.owner = runner
    else:
        graph = await source_graph(monkeypatch, directory)
        case = SimpleNamespace(kind=kind, runner=graph.runner, graph=graph)
        if kind == "server":
            case.mount, case.server, case.listener = object(), SimpleNamespace(should_exit=False), object()
            case.api = SimpleNamespace(runner=graph.runner, web_app=case.mount, _gdb_original_source_server=None)
            case.invoke = lambda: source.capture_original_server(
                case.api, case.runner, case.mount, case.server, case.listener)
            case.initializer, case.owner = source.OriginalSourceServer.__init__, case.api
            case.public_book, case.key = graph.runtime.servers, id(case.api)
        else:
            class Connection:
                pass

            connection = case.connection = Connection()
            connection.authority, connection.actor = graph.authority, graph.connection.actor
            connection.transport, connection._gdb_original_source_connection = graph.transport, None
            case.invoke = lambda: source.capture_original_connection(
                graph.handle.source_owner, connection, graph.ws, graph.transport, asyncio.current_task())
            case.initializer, case.owner = source.OriginalSourceConnection.__init__, connection
            case.public_book, case.key = graph.runtime.connections, id(connection)
    RETAINED.append(case)
    return case


def original_self(error, initializer):
    trace = error.__traceback__
    while trace is not None:
        if trace.tb_frame.f_code is initializer.__code__:
            return trace.tb_frame.f_locals["self"]
        trace = trace.tb_next
    raise AssertionError("Real original source initializer did not run")


async def finish(case):
    if case.graph is not None:
        await finish_source_graph(case.graph)
