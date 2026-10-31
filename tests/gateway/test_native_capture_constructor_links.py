"""Source capture associations and ordinary/default compatibility; no App/native/bootstrap acceptance."""
import asyncio
import sys
from types import SimpleNamespace

import pytest

from gateway import native_source_runtime as source
from gateway.run_api import GatewayRuntimeAPI
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_capture_constructor_fixture import capture_case, original_self, finish
from tests.gateway.native_source_fixture import source_graph, finish_source_graph


def attempt_for(case):
    return (case.runner._gdb_original_source_runtime_construction if case.kind == "runtime"
            else case.graph.runtime._capture_constructions[(case.kind, case.key)])


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["runtime", "server", "connection"])
@pytest.mark.parametrize("allocation", ["opaque", "raised"])
async def test_capture_allocators_consume_once_and_do_not_adopt_or_initialize(monkeypatch, tmp_path, kind, allocation):
    case = await capture_case(monkeypatch, tmp_path, kind)
    cls = {"runtime": source.OriginalSourceRuntime, "server": source.OriginalSourceServer,
           "connection": source.OriginalSourceConnection}[kind]
    opaque, error, reached = object(), RuntimeError("original allocator no self"), []

    def allocate(actual, *args):
        credit = case.runner._gdb_original_source_capture_base_credit if kind == "runtime" else case.public_book[case.key]
        reached.append((actual, credit))
        if allocation == "raised":
            raise error
        return opaque

    try:
        with monkeypatch.context() as patch:
            patch.setattr(cls, "__new__", staticmethod(allocate))
            with pytest.raises(RuntimeError if allocation == "raised" else RuntimeStoreError):
                case.invoke()
        attempt = attempt_for(case)
        assert attempt.value is None and attempt.phase == "failed" and len(reached) == 1
        assert reached[0][1] == (source.SOURCE_REFERENCE_CREDIT if kind == "runtime" else None)
        assert attempt.observed_result is (None if allocation == "raised" else opaque)
        if allocation == "raised":
            assert attempt.error is error and attempt.traceback is not None
        with pytest.raises(RuntimeStoreError):
            case.invoke()
        assert len(reached) == 1 and attempt_for(case) is attempt
        if kind == "runtime":
            assert case.runner._gdb_original_source_runtime is None
        else:
            assert case.public_book[case.key] is None
    finally:
        await finish(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["runtime", "server", "connection"])
async def test_real_constructor_reentry_is_refused_without_new_debit_or_lock(monkeypatch, tmp_path, kind):
    case = await capture_case(monkeypatch, tmp_path, kind)
    cls = type(case.runner) if kind == "runtime" else (source.OriginalSourceServer if kind == "server"
                                                      else type(case.connection))
    original_get, original_set, observed = cls.__getattribute__, cls.__setattr__, []
    attr = "_gdb_original_source_capture_lock" if kind == "runtime" else "actor"

    def nested():
        with pytest.raises(RuntimeStoreError):
            case.invoke()
        observed.append(True)

    def read(self, name):
        if name == attr and sys._getframe(1).f_code is case.initializer.__code__:
            nested()
        return original_get(self, name)

    def assign(self, name, value):
        original_set(self, name, value)
        if name == "_source_capture_initialized" and value is True:
            nested()

    try:
        with monkeypatch.context() as patch:
            patch.setattr(cls, "__setattr__" if kind == "server" else "__getattribute__",
                          assign if kind == "server" else read)
            result = case.invoke()
        attempt = attempt_for(case)
        assert observed and attempt.error is None and attempt.phase == "ready"
        assert attempt.value is (case.runner._gdb_original_source_runtime if kind == "runtime" else result)
    finally:
        await finish(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["runtime", "server", "connection"])
async def test_initializer_displacements_keep_original_inputs_and_failed_self(monkeypatch, tmp_path, kind):
    case = await capture_case(monkeypatch, tmp_path, kind)
    target = {"runtime": source.OriginalSourceRuntime, "server": source.OriginalSourceServer,
              "connection": type(case.connection) if kind == "connection" else object}[kind]
    original_set, original_get = target.__setattr__, target.__getattribute__
    replacement = object()

    def assign(self, name, value):
        original_set(self, name, value)
        if name == "_source_capture_initialized" and value is True:
            if kind == "runtime":
                case.runner._gdb_original_source_capture_lock = replacement
            else:
                original_set(self, "mount", replacement)

    def read(self, name):
        if name == "actor" and sys._getframe(1).f_code is case.initializer.__code__:
            return replacement  # Original input is restored on later ordinary reads; output must still refuse.
        return original_get(self, name)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(target, "__getattribute__" if kind == "connection" else "__setattr__",
                          read if kind == "connection" else assign)
            with pytest.raises(RuntimeStoreError):
                case.invoke()
        attempt = attempt_for(case)
        assert attempt.value is not None and attempt.error is not None and attempt.phase == "failed"
        assert attempt.value._source_capture_initialized is True
        with pytest.raises(RuntimeStoreError):
            case.invoke()
        assert attempt_for(case) is attempt
    finally:
        await finish(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["runtime", "connection"])
async def test_original_final_publication_setter_error_retains_value_and_first_trace(monkeypatch, tmp_path, kind):
    case = await capture_case(monkeypatch, tmp_path, kind)
    cls = type(case.runner) if kind == "runtime" else type(case.connection)
    attr = "_gdb_original_source_runtime" if kind == "runtime" else "_gdb_original_source_connection"
    setter, error, values = cls.__setattr__, RuntimeError("original source publication error"), []

    def publish(self, name, value):
        if name == attr and value is not None:
            values.append(value)
            raise error
        setter(self, name, value)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(cls, "__setattr__", publish)
            with pytest.raises(RuntimeError) as caught:
                case.invoke()
        attempt = attempt_for(case)
        assert caught.value is error and values == [attempt.value]
        assert attempt.error is error and attempt.traceback is not None
        if kind == "runtime":
            assert case.runner._gdb_original_source_runtime is None
        else:
            assert case.public_book[case.key] is attempt.value
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_real_api_publication_failure_keeps_original_server_and_api(monkeypatch, tmp_path):
    case = await capture_case(monkeypatch, tmp_path, "connection")
    setter, error = GatewayRuntimeAPI.__setattr__, RuntimeError("actual API source publication setter")

    def publish(self, name, value):
        if name == "_gdb_original_source_server" and value is not None:
            raise error
        setter(self, name, value)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(GatewayRuntimeAPI, "__setattr__", publish)
            with pytest.raises(RuntimeError) as caught:
                GatewayRuntimeAPI(None, case.runner, object(),
                                  source_server=SimpleNamespace(should_exit=False), source_listener=object())
        api = original_self(error, GatewayRuntimeAPI.__init__)
        attempt = case.graph.runtime._capture_constructions[("server", id(api))]
        assert caught.value is error and attempt.error is error and attempt.value.api is api
        assert attempt.value._source_capture_initialized is True and api._gdb_original_source_server is None
        assert case.graph.runtime.servers[id(api)] is attempt.value
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_healthy_queries_from_another_task_and_ordinary_missing_defaults(monkeypatch, tmp_path):
    case = await capture_case(monkeypatch, tmp_path, "connection")
    try:
        result = case.invoke()

        async def query():
            return source.require_original_connection(case.connection)

        assert await asyncio.create_task(query()) is result
        assert source.capture_original_server(object(), SimpleNamespace(), object(), object(), object()) is None
        assert source.capture_original_connection(None, object(), object(), object(), None) is None
        assert source.attach_original_registry(SimpleNamespace(), object()) is None
    finally:
        await finish(case)


@pytest.mark.asyncio
async def test_failed_child_keeps_hole_but_other_original_connection_remains_healthy(monkeypatch, tmp_path):
    case = await capture_case(monkeypatch, tmp_path, "connection")
    setter, error = source.OriginalSourceConnection.__setattr__, RuntimeError("new child assignment model")

    def assign(self, name, value):
        if name == "transport" and self.connection is case.connection:
            raise error
        setter(self, name, value)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(source.OriginalSourceConnection, "__setattr__", assign)
            with pytest.raises(RuntimeError):
                case.invoke()
        assert source.require_original_connection(case.graph.connection).connection is case.graph.connection
        assert case.public_book[case.key] is None and case.graph.runtime.blocked is False
        assert case.graph.runtime.construction_error is error
    finally:
        await finish(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["runtime", "server", "connection"])
async def test_changed_allocator_cannot_reinitialize_an_existing_original_self(monkeypatch, tmp_path, kind):
    case = await capture_case(monkeypatch, tmp_path, kind)
    foreign = await source_graph(monkeypatch, tmp_path / "separate-original-source")
    value = {"runtime": foreign.runtime, "server": foreign.handle.source_owner,
             "connection": foreign.connection._gdb_original_source_connection}[kind]
    cls, fields, previous = type(value), vars(value).copy(), value._source_capture_construction

    def allocate(actual, *args):
        return value

    try:
        with monkeypatch.context() as patch:
            patch.setattr(cls, "__new__", staticmethod(allocate))
            with pytest.raises(RuntimeStoreError):
                case.invoke()
        attempt = attempt_for(case)
        assert attempt.value is None and attempt.observed_result is value and attempt.phase == "failed"
        assert vars(value) == fields and value._source_capture_construction is previous
        assert previous.phase == "ready" and previous.error is None
        assert source.require_original_connection(foreign.connection).connection is foreign.connection
    finally:
        await finish_source_graph(foreign)
        await finish(case)
