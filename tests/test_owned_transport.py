"""Original dashboard credential carrier: inert objects, no network/SDK/OS APIs."""
import asyncio
import json
from types import SimpleNamespace as Data

import pytest

import hermes_owned_gateway as owned
import hermes_owned_transport as transport
from tests.owned_transport_fixture import graph


@pytest.mark.asyncio
async def test_original_dashboard_token_uses_only_original_private_control_output(monkeypatch):
    g = graph(monkeypatch)
    await g.source.prepare_transport(g.runner)
    record = g.source._transport_record
    assert record.web is g.web and record.mount is g.mount and record.api is g.api
    assert record.owner_task is asyncio.current_task()
    assert g.source._transport_credit == 4096 and g.source._transport_attempt is not None
    assert g.source._transport_error is None and record.prepared
    g.source.ready(g.runner)
    frame = json.loads(g.output.getvalue())
    assert frame["sessionToken"] == g.token
    assert frame["protocol"] == owned.PROTOCOL == "gdb-owned-gateway-v2"
    assert frame["instanceId"] == g.descriptor["instance_id"]
    assert set(frame) == {"protocol", "event", "attemptId", "apiOrigin", "instanceId",
                          "authorityEpoch", "sessionToken"}
    g.ensure.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_transport_credit_is_consumed_before_failed_original_record_factory(monkeypatch):
    g = graph(monkeypatch)
    original = RuntimeError("fixture-record-construction")
    calls = []

    def fail(source, runner):
        calls.append((source, runner, source._transport_credit, source._transport_attempt))
        raise original

    monkeypatch.setattr(transport, "TransportRecord", fail)
    with pytest.raises(owned.OwnedGatewayUnknown):
        await g.source.prepare_transport(g.runner)
    assert calls[0][:3] == (g.source, g.runner, 4096) and calls[0][3] is not None
    assert g.source._transport_record is None and g.source._transport_error is original
    with pytest.raises(owned.OwnedGatewayUnknown):
        await g.source.prepare_transport(g.runner)
    assert len(calls) == 1 and g.source._transport_credit == 4096
    g.ensure.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_dashboard_await_retains_first_exact_error_and_record(monkeypatch):
    g = graph(monkeypatch)
    original = OSError("fixture-normal-dashboard-failure")
    g.ensure.side_effect = original
    with pytest.raises(owned.OwnedGatewayUnknown):
        await g.source.prepare_transport(g.runner)
    record = g.source._transport_record
    assert g.source._transport_error is original and record.mount is g.mount
    assert not record.prepared and record.listener is g.listener
    first = g.source.error
    with pytest.raises(owned.OwnedGatewayUnknown):
        await g.source.prepare_transport(g.runner)
    assert g.source.error == first and g.source._transport_record is record
    g.ensure.assert_awaited_once_with()


def mutate(g, target):
    actions = {
        "listener": lambda: setattr(g.runner, "session_api", Data(**vars(g.listener))),
        "descriptor": lambda: setattr(g.runner, "session_runtime_descriptor", dict(g.descriptor)),
        "authority": lambda: setattr(g.runner.session_authorities, "launch", Data(**vars(g.authority))),
        "primary": lambda: setattr(g.runner, "session_authority", Data(**vars(g.authority))),
        "epoch": lambda: setattr(g.authority, "epoch", 4),
        "instance": lambda: g.descriptor.update(instance_id="changed-instance"),
        "origin": lambda: setattr(g.listener, "api_origin", "http://127.0.0.1:8835"),
        "server": lambda: setattr(g.listener, "server", object()),
        "socket": lambda: setattr(g.listener, "socket", object()),
        "api": lambda: setattr(g.listener.server.config, "loaded_app", object()),
        "listener-task": lambda: setattr(g.listener, "task", Data(done=lambda: False)),
        "creator-task": lambda: setattr(g.source, "task", object()),
        "creator-thread": lambda: setattr(g.source, "loop_thread", object()),
        "contract": lambda: setattr(g.source, "contract", Data(**vars(g.source.contract))),
        "record": lambda: setattr(g.source, "_transport_record", object()),
    }
    actions[target]()


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["listener", "descriptor", "authority", "primary", "epoch",
    "instance", "origin", "server", "socket", "api", "listener-task", "creator-task",
    "creator-thread", "contract", "record"])
async def test_original_associations_are_rechecked_after_dashboard_io(monkeypatch, target):
    g = graph(monkeypatch)

    async def changed_after_io():
        web = await g.mount_existing_dashboard()
        mutate(g, target)
        return web

    g.ensure.side_effect = changed_after_io
    with pytest.raises(owned.OwnedGatewayUnknown):
        await g.source.prepare_transport(g.runner)
    assert g.source.phase == "unknown" and not g.source._transport_original_record.prepared
    assert g.source._transport_original_record.mount is g.mount
    assert g.source._transport_error is not None and not g.output.getvalue()


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["credential", "public-token", "dashboard", "dashboard-owner"])
async def test_ready_never_exports_changed_or_public_dashboard_binding(monkeypatch, target):
    g = graph(monkeypatch)
    await g.source.prepare_transport(g.runner)
    if target == "credential": g.web._SESSION_TOKEN = "changed" + "y" * 36
    elif target == "public-token": g.web.app.state.withhold_session_token = False
    elif target == "dashboard": g.mount.web = Data(**vars(g.web))
    else: g.web.app.state.gateway_runner = object()
    with pytest.raises(owned.OwnedGatewayUnknown):
        g.source.ready(g.runner)
    assert not g.output.getvalue() and g.source.phase == "unknown"


@pytest.mark.asyncio
async def test_same_source_transport_success_cannot_be_prepared_twice(monkeypatch):
    g = graph(monkeypatch)
    await g.source.prepare_transport(g.runner)
    record = g.source._transport_record
    with pytest.raises(owned.OwnedGatewayUnknown):
        await g.source.prepare_transport(g.runner)
    assert g.source._transport_record is record and g.source._transport_credit == 4096
    g.ensure.assert_awaited_once_with()
