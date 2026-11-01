"""Narrow source-only lower-fixture adapter for one genuinely affected old case.

Keep the complete existing assertion body and production source constructors.
Its former CommitFixture opens SQLite before those assertions. Here the existing
constructor_graph supplies modeled lease reads instead; the global pre-import
SDK/network/process/thread/SQLite refusals remain installed and unchanged.
"""
from types import SimpleNamespace

import pytest


@pytest.fixture(autouse=True)
def original_missing_owner_source_fixture(request, monkeypatch):
    if request.node.nodeid != "tests/agent/test_native_source_calls.py::test_missing_app_issuer_holds_one_actual_request_and_rejects_replay":
        return
    from tests.agent import test_native_source_calls as target
    from tests.gateway.native_constructor_fixture import constructor_graph, pending_slot

    async def modeled_lower_graph(patch, path):
        case = await constructor_graph(patch, path)
        slot = pending_slot(case)
        source = slot.prepare_source_declaration()
        return SimpleNamespace(graph=case.graph, slot=slot, source=source,
                               fixture=SimpleNamespace(close=lambda: None))

    monkeypatch.setattr(target, "call_graph", modeled_lower_graph)
