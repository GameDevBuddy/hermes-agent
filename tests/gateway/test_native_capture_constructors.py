"""Genuine real-source initializer failures and retained partial self, not allocator-only substitutes."""
import sys

import pytest

from gateway import native_source_runtime as source
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_capture_constructor_fixture import capture_case, original_self, finish


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["runtime", "server", "connection"])
async def test_real_source_initializer_partial_and_first_error_retained(monkeypatch, tmp_path, kind):
    case = await capture_case(monkeypatch, tmp_path, kind)
    error = RuntimeError("real source " + kind + " lower failure")
    cls = type(case.runner) if kind == "runtime" else (source.OriginalSourceServer if kind == "server"
                                                      else type(case.connection))
    attribute = "_gdb_original_source_capture_lock" if kind == "runtime" else "actor"
    getter, setter = cls.__getattribute__, cls.__setattr__

    def read(self, name):
        if name == attribute and sys._getframe(1).f_code is case.initializer.__code__:
            raise error
        return getter(self, name)

    def assign(self, name, value):
        if name == "mount" and sys._getframe(1).f_code is case.initializer.__code__:
            raise error  # Honest model of real initializer assignment failure; no product allocation claim.
        setter(self, name, value)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(cls, "__setattr__" if kind == "server" else "__getattribute__",
                          assign if kind == "server" else read)
            with pytest.raises(RuntimeError) as caught:
                case.invoke()
        value = original_self(error, case.initializer)
        attempt = (case.runner._gdb_original_source_runtime_construction if kind == "runtime"
                   else case.graph.runtime._capture_constructions[(kind, case.key)])
        assert caught.value is error and attempt.error is error and attempt.traceback is not None
        assert attempt.value is value and attempt.credit == source.SOURCE_REFERENCE_CREDIT
        if kind == "runtime":
            assert case.runner._gdb_original_source_runtime is None
            assert case.runner._gdb_original_source_capture_base_credit == source.SOURCE_REFERENCE_CREDIT
        else:
            assert case.public_book[case.key] is None
            assert case.graph.runtime.construction_error is error
        with pytest.raises(RuntimeStoreError):
            if kind == "runtime":
                value.reserve({}, "partial")
            elif kind == "server":
                value.require_server()
            else:
                value.require_connection()
        assert attempt.error is error
    finally:
        await finish(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["server", "connection"])
async def test_real_initializer_cannot_displace_its_original_owner_record(monkeypatch, tmp_path, kind):
    case = await capture_case(monkeypatch, tmp_path, kind)
    cls = source.OriginalSourceServer if kind == "server" else source.OriginalSourceConnection
    setter, original = cls.__setattr__, []
    name = "_gdb_original_source_" + kind + "_construction"

    def assign(self, field, value):
        setter(self, field, value)
        if field == "_source_capture_initialized" and value is True:
            original.append(vars(case.owner)[name])
            vars(case.owner)[name] = object()

    try:
        with monkeypatch.context() as patch:
            patch.setattr(cls, "__setattr__", assign)
            with pytest.raises(RuntimeStoreError):
                case.invoke()
        attempt = case.graph.runtime._capture_constructions[(kind, case.key)]
        assert original == [attempt] and attempt.value is not None and attempt.phase == "failed"
        assert attempt.error is case.graph.runtime.construction_error
        assert case.public_book[case.key] is None
    finally:
        await finish(case)
