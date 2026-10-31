"""Actual bridge partial-self/error custody through lazy owner.bind; not App/native acceptance."""
import sys

import pytest

from agent import native_receipt_origin as origin
from tests.agent.native_bridge_constructor_fixture import bridge_case, begin, finish
from tests.gateway.native_constructor_fixture import original_self


@pytest.mark.asyncio
@pytest.mark.parametrize("lower", ["lock", "tools"])
async def test_actual_bridge_partial_and_first_error_survive_lazy_factory(monkeypatch, tmp_path, caplog, lower):
    error = RuntimeError("actual bridge RLock") if lower == "lock" else ValueError("actual bridge tools getter")

    class Item:
        def __getitem__(self, key):
            raise error

    case = await bridge_case(monkeypatch, tmp_path, [Item()] if lower == "tools" else None)
    initializer, original_lock = origin.NativeDispatchBridge.__init__, origin.threading.RLock

    def lock(*args, **kwargs):
        if sys._getframe(1).f_code is initializer.__code__:
            raise error
        return original_lock(*args, **kwargs)

    try:
        with monkeypatch.context() as patch:
            if lower == "lock":
                patch.setattr(origin.threading, "RLock", lock)
            ref, pending = begin(case)
        value = original_self(error, initializer)
        attempt = case.owner._native_bridge_construction
        assert attempt.value is value and attempt.error is error and attempt.traceback is not None
        assert attempt.credit > 0 and case.keeper.bridge_construction is attempt
        assert case.graph.runtime.bridges[id(case.owner)] is attempt
        assert case.owner.native_bridge is None and error.__traceback__ is not None
        if lower == "lock":
            assert pending is None and "Canonical tool-row witness withheld" in caplog.text
        else:
            assert pending.ref is ref and pending.native_origin_error == "native_original_owner_unavailable"
    finally:
        await finish(case)
