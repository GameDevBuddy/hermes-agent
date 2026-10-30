"""Actual source/transport/context consumers; all lower IO and Job queries are models."""
import asyncio
from concurrent.futures import Future
import hashlib
import io
import json
from types import SimpleNamespace as Data

import hermes_owned_control as control
import hermes_owned_gateway as owned
import hermes_owned_gateway_windows as windows
import hermes_retained_bootstrap as bootstrap
from gateway.native_source_runtime import (
    attach_original_authority, attach_original_listener_task, attach_original_registry, capture_original_runner,
)
from gateway.run_api import GatewayAPIHandle, GatewayDashboardMount, GatewayRuntimeAPI
from gateway.session_authorities import SessionAuthorities
from gateway.session_authority import LiveSession, SessionAuthority
from gateway.session_contract import SessionRef
from hermes_constants import hermes_home_key
from tests.gateway.native_source_fixture import runner_from_actual_constructor
from tests.hermes_cli.test_owned_gateway_source import QueryAPI
from tui_gateway.ws import _authority_connection

NONCE = "a" * 64
REQUEST = "01J00000000000000000000001"
TOOL = "01J00000000000000000000002"


def line(value):
    return json.dumps(value, separators=(",", ":")).encode() + b"\n"


class Input(io.BytesIO):
    hook = None

    def readline(self, size=-1):
        if self.hook is not None:
            callback, self.hook = self.hook, None
            callback()
        return super().readline(size)


class Output(io.BytesIO):
    short_next = False

    def write(self, raw):
        if self.short_next:
            self.short_next = False
            return super().write(raw[:-1])
        return super().write(raw)


class Submission(Future):
    """Model only the cross-thread submission; the original coroutine/task/loop run normally."""
    def __init__(self, coroutine, loop):
        super().__init__()
        self.loop, self.task = loop, loop.create_task(coroutine)

    def result(self, timeout=None):
        if not self.done():
            try:
                self.set_result(self.loop.run_until_complete(self.task))
            except BaseException as error:
                self.set_exception(error)
        return super().result(timeout)


class Case:
    def __init__(self, monkeypatch, tmp_path, *, operator=True):
        self.patch, self.home = monkeypatch, tmp_path
        self.operator = operator
        self.loop = asyncio.new_event_loop()  # ordinary test driver, no product workers
        self.job_api, self.input, self.output = QueryAPI(), Input(), Output()
        self.streams = Data(stdin=Data(buffer=self.input), stdout=Data(buffer=self.output),
                            stderr=Data(buffer=io.BytesIO()))
        self.contract, self.plan = Data(attempt_id="context-source-attempt"), b"context-source-plan"
        monkeypatch.setattr(owned, "sys", self.streams)
        monkeypatch.setattr(owned, "_CURRENT", None)
        monkeypatch.setattr(bootstrap, "_BOUND", self.contract)
        monkeypatch.setattr(bootstrap, "_PLAN_BYTES", self.plan)
        monkeypatch.setattr(windows, "_load_api", lambda: self.job_api)
        self.append(line({"protocol": owned.PROTOCOL, "command": "attach", "attemptId": self.contract.attempt_id,
                          "planSha256": hashlib.sha256(self.plan).hexdigest(), "jobHandle": 41}))
        self.source = owned.attach(self.contract, self.plan)
        self.reader_thread = object()
        monkeypatch.setattr(control, "threading", Data(current_thread=lambda: self.reader_thread))
        monkeypatch.setattr(control, "asyncio", Data(run_coroutine_threadsafe=self.submit))
        self.initialized, self.stop = asyncio.Event(), asyncio.Event()
        self.submissions = []
        self.bootstrap_task = self.loop.create_task(self.build())
        self.run(self.initialized.wait())
        if self.bootstrap_task.done():
            self.run(self.bootstrap_task)

    def run(self, coroutine):
        return self.loop.run_until_complete(coroutine)

    def append(self, raw):
        position = self.input.tell()
        self.input.seek(0, io.SEEK_END)
        self.input.write(raw)
        self.input.seek(position)

    def submit(self, coroutine, loop):
        assert loop is self.loop
        value = Submission(coroutine, loop)
        self.submissions.append(value)
        return value

    async def build(self):
        try:
            await self.build_originals()
        except BaseException:
            self.initialized.set()
            raise

    async def build_originals(self):
        self.runner = runner_from_actual_constructor(self.patch)
        capture_original_runner(self.runner, asyncio.current_task(), self.loop)
        self.source.bind_runner(self.runner)
        self.runtime = self.runner._gdb_original_source_runtime
        self.registry = self.runner.session_authorities = SessionAuthorities(str(self.home))
        attach_original_registry(self.runner, self.registry)
        self.row = {"runtime_generation": 1, "runtime_revision": 1}
        self.db = Data(get_session=lambda sid: self.row if sid == "s" else None)
        self.authority = SessionAuthority(self.runner, profile_id=str(self.home), instance_id="context-instance",
                                          db=self.db, epoch=1)
        self.runner.session_authority = self.authority
        self.registry.add(self.home, None)
        self.registry.replace(self.home, self.authority)
        attach_original_authority(self.runner, self.authority, hermes_home_key(self.home))
        self.mount = GatewayDashboardMount(self.runner, Data())
        self.server = Data(should_exit=False)
        self.api = GatewayRuntimeAPI(None, self.runner, self.mount,
                                     source_server=self.server, source_listener=object())
        self.api_task = asyncio.create_task(self.stop.wait())
        self.server.config = Data(loaded_app=self.api)
        self.handle = GatewayAPIHandle("http://127.0.0.1:1", self.server, self.api_task, self.mount,
                                       self.api._gdb_original_source_server.listener,
                                       self.api._gdb_original_source_server)
        attach_original_listener_task(self.handle.source_owner, self.handle, self.api_task)
        self.runner.session_api = self.handle
        self.runner.session_runtime_descriptor = {"state": "ready", "instance_id": "context-instance",
            "authority_epoch": 1, "api_origin": self.handle.api_origin}
        self.web = Data(_SESSION_TOKEN="f" * 43, app=Data(state=Data(gateway_runner=self.runner,
                        session_authority=self.authority, withhold_session_token=True)))

        async def ensure():
            self.mount.web, self.mount._lifespan = self.web, object()
            return self.web

        self.patch.setattr(self.mount, "ensure", ensure)  # lower dashboard IO, not a context issuer
        await self.source.prepare_transport(self.runner)
        self.source.ready(self.runner)
        self.live = LiveSession(None, "context-route")
        self.authority.sessions["s"] = self.live
        self.ref = SessionRef(self.authority.profile_id, "s")
        self.connected = asyncio.Event()
        self.ws_task = asyncio.create_task(self.connect())
        await self.connected.wait()
        self.initialized.set()
        await self.stop.wait()
        await self.ws_task
        await self.api_task

    async def connect(self):
        self.ws = Data(scope={"hermes.session_authority": self.authority,
                              "hermes.gdb_original_source_server": self.handle.source_owner})
        self.transport = Data(_ws=self.ws, _loop=self.loop, _closed=False)
        self.connection = _authority_connection(self.ws, self.transport, {
            "user_id": "context-fixture", "profile_id": self.authority.profile_id,
            "instance_id": self.authority.instance_id, "native_owner": True,
            "native_bootstrap": True}, self.operator)
        self.live.subscribers["fixture"] = self.connection.actor
        self.connected.set()
        await self.stop.wait()

    def params(self, **changes):
        return {"nonce": NONCE, "session_id": "s", "context_id": "context-fixture",
                "snapshot_sha256": "b" * 64,
                "tools": [{"name": "gdb_native_" + TOOL.lower(), "tool_ref": TOOL}], **changes}

    def stage(self, **changes):
        return self.run(self.connection.dispatch({"id": 1, "method": "native.context.stage",
                                                  "params": self.params(**changes)}))

    def frame(self, **changes):
        return {"protocol": owned.PROTOCOL, "command": "bind_native_context",
                "attemptId": self.contract.attempt_id, "requestId": REQUEST,
                "nonce": NONCE, "contextId": "context-fixture", **changes}

    def read(self, frames=None, *, before_shutdown=None):
        raw = line(self.frame()) if frames is None else frames
        self.append(raw + line({"protocol": owned.PROTOCOL, "command": "shutdown",
                               "attemptId": self.contract.attempt_id}))
        if before_shutdown is not None:
            def first():
                self.input.hook = before_shutdown
            self.input.hook = first
        self.source.read_shutdown()

    def events(self):
        return [json.loads(raw) for raw in self.output.getvalue().splitlines()]

    def close(self):
        self.stop.set()
        self.run(self.bootstrap_task)
        for submission in self.submissions:
            if not submission.task.done():
                raise AssertionError("fixture left an original test task pending")
        for record in self.source._app_context_requests.values():
            coroutine = getattr(record, "coroutine", None)
            if coroutine is not None and getattr(record, "task", None) is None:
                coroutine.close()  # disposable unstarted fixture coroutine, never product cleanup
        self.loop.close()
