"""Inert source data and mocked dashboard IO, not runner/Job/native admission."""
import asyncio
import io
import threading
from types import SimpleNamespace as Data
from unittest.mock import AsyncMock, Mock

import hermes_owned_gateway as owned
from gateway.run_api import GatewayDashboardMount, GatewayRuntimeAPI


def graph(monkeypatch):
    output = io.BytesIO()
    streams = Data(stdin=Data(buffer=io.BytesIO()), stdout=Data(buffer=output),
                   stderr=Data(buffer=io.BytesIO()))
    monkeypatch.setattr(owned, "sys", streams)
    source = owned.OwnedGatewaySource(Data(attempt_id="fixture-transport"), b"fixture-plan")
    monkeypatch.setattr(owned, "_CURRENT", source)
    # Deliberate hand-built fixture state: no GatewayRunner/Job/service created.
    runner = Data(_owned_gateway_source=source)
    authority = Data(runner=runner, epoch=3)
    runner.session_authorities, runner.session_authority = Data(launch=authority), authority
    mount = GatewayDashboardMount(runner, Data())
    api = GatewayRuntimeAPI(None, runner, mount)
    listener = Data(api_origin="http://127.0.0.1:8834", server=Data(config=Data(loaded_app=api)),
                    task=Data(done=Mock(return_value=False)), socket=object(), app=mount,
                    source_owner=object())
    descriptor = {"state": "ready", "instance_id": "fixture-instance", "authority_epoch": 3,
                  "api_origin": listener.api_origin}
    runner.session_api, runner.session_runtime_descriptor = listener, descriptor
    source.runner, source.task, source.loop = runner, asyncio.current_task(), asyncio.get_running_loop()
    source.loop_thread, source.phase = threading.current_thread(), "bound"
    source.job = Data(require_current_member=Mock())
    token = "fixture" + "x" * 36
    web = Data(_SESSION_TOKEN=token, app=Data(state=Data(gateway_runner=runner,
        session_authority=authority, withhold_session_token=True)))

    async def mount_existing_dashboard():
        mount.web, mount._lifespan = web, object()
        return web

    ensure = AsyncMock(side_effect=mount_existing_dashboard)
    monkeypatch.setattr(mount, "ensure", ensure)
    return Data(source=source, runner=runner, authority=authority, mount=mount, api=api,
                listener=listener, descriptor=descriptor, web=web, output=output,
                token=token, ensure=ensure, mount_existing_dashboard=mount_existing_dashboard)
