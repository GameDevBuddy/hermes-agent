"""Existing dashboard credential on the original owned pipe, not native proof.

No new authentication system or credential is created here. The canonical
dashboard still generates/checks its normal session token. These original-object
and point-in-time checks do not prove remote/native authority or kernel closure.
"""
from __future__ import annotations

import asyncio
import re
import threading


class TransportRecord:
    __slots__ = ("api", "authority", "contract", "credential", "descriptor", "epoch", "instance",
                 "listener", "listener_task", "loop", "mount", "origin", "owner_task", "prepared",
                 "primary", "runner", "server", "socket", "source", "source_owner", "thread", "web")

    def __init__(self, source, runner):
        source.require_bound(runner)
        self.source, self.runner, self.contract = source, runner, source.contract
        self.listener, self.descriptor = runner.session_api, runner.session_runtime_descriptor
        self.authority, self.primary = runner.session_authorities.launch, runner.session_authority
        self.instance, self.epoch = self.descriptor["instance_id"], self.authority.epoch
        self.origin = self.listener.api_origin
        self.server, self.listener_task = self.listener.server, self.listener.task
        self.socket, self.mount = self.listener.socket, self.listener.app
        self.api, self.source_owner = self.server.config.loaded_app, self.listener.source_owner
        self.loop, self.owner_task = asyncio.get_running_loop(), asyncio.current_task()
        self.thread = threading.current_thread()
        self.web = self.credential = None
        self.prepared = False

    def require_current(self):
        from gateway.run_api import GatewayDashboardMount, GatewayRuntimeAPI
        source, runner = self.source, self.runner
        source.require_bound(runner)
        if (source._transport_record is not self or source._transport_original_record is not self
                or source._transport_credit != 4096
                or source._transport_error is not None or source.contract is not self.contract
                or source.phase != "bound" or source.task is not self.owner_task
                or source.loop_thread is not self.thread
                or asyncio.current_task() is not self.owner_task
                or asyncio.get_running_loop() is not self.loop
                or threading.current_thread() is not self.thread):
            raise source.unknown("original-transport-owner-changed")
        if (runner.session_api is not self.listener
                or runner.session_runtime_descriptor is not self.descriptor
                or runner.session_authorities.launch is not self.authority
                or runner.session_authority is not self.primary
                or self.authority.runner is not runner or self.authority.epoch != self.epoch
                or self.descriptor.get("authority_epoch") != self.epoch
                or self.descriptor.get("instance_id") != self.instance
                or self.descriptor.get("state") != "ready"
                or self.descriptor.get("api_origin") != self.origin):
            raise source.unknown("original-transport-session-changed")
        if (self.listener.server is not self.server or self.listener.task is not self.listener_task
                or self.listener.socket is not self.socket or self.listener.app is not self.mount
                or self.listener.source_owner is not self.source_owner
                or self.listener.api_origin != self.origin or self.listener_task.done()
                or self.server.config.loaded_app is not self.api
                or type(self.api) is not GatewayRuntimeAPI or self.api.runner is not runner
                or self.api.web_app is not self.mount
                or type(self.mount) is not GatewayDashboardMount or self.mount.runner is not runner
                or self.mount._closed):
            raise source.unknown("original-transport-api-changed")
        if self.web is not None:
            self.require_dashboard()

    def require_dashboard(self):
        web = self.web
        if (self.mount.web is not web or self.mount._lifespan is None
                or web.app.state.gateway_runner is not self.runner
                or web.app.state.session_authority is not self.primary
                or web.app.state.withhold_session_token is not True
                or web._SESSION_TOKEN is not self.credential
                or type(self.credential) is not str
                or re.fullmatch(r"[A-Za-z0-9_-]{43}", self.credential) is None):
            raise self.source.unknown("original-private-dashboard-binding-changed")

    async def prepare(self):
        self.require_current()
        self.web = await self.mount.ensure()
        # Retain the actual returned object before any post-IO check. An ensure
        # failure/partial constructor is retained by the source's exact traceback.
        self.credential = self.web._SESSION_TOKEN
        self.require_current()
        self.prepared = True

    def session_token(self):
        self.require_current()
        if not self.prepared:
            raise self.source.unknown("original-private-dashboard-not-prepared")
        return self.credential
