"""Real SQLite/source custody with synthetic transports, no App issuer or native effect."""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

from agent.native_original_use import acquire_local_sql_use, reserve_local_sql_pin
from gateway.session_server_requests import retain_native_candidate
from tests.agent.test_native_source_calls import call_graph
from tests.gateway.native_source_fixture import finish_source_graph

# Keep test data/handles strongly retained until this source-test process ends.
# Fixtures never retire/close a held original DB or delete their retained files.
RETAINED = []


async def local_case(monkeypatch, request, *, use=True, pin=True):
    if sys.prefix == sys.base_prefix:
        raise RuntimeError("SQL source fixtures require a fresh declared PM test environment")
    root = Path(sys.prefix).resolve().parent / "sqlite-fixtures"
    name = hashlib.sha256(request.node.nodeid.encode()).hexdigest()[:24]
    directory = root / name
    directory.mkdir(parents=True, exist_ok=False)
    case = await call_graph(monkeypatch, directory)
    RETAINED.append(case)
    case.pin = reserve_local_sql_pin(case.slot) if pin else None
    case.request = retain_native_candidate(case.slot) if use else None
    case.use = acquire_local_sql_use(case.pin, case.request) if use else None
    return case


async def finish_case(case):
    # Ends only the synthetic listener task. Original SQL and source custody stay retained.
    await finish_source_graph(case.graph)


def scalar(case, sql, params=()):
    with case.fixture._session_db._lock:
        return case.fixture._session_db._conn.execute(sql, params).fetchone()[0]


def prepare_result(case):
    from agent.native_original_use import check_local_sql_after_io
    check_local_sql_after_io(case.use, "actual raw source result", "different persisted projection")
    return case.fixture.prepare(case.dispatch, content=case.use.projection, raw=case.use.raw_result)
