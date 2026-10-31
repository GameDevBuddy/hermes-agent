"""Original reference custody only; no native authority or admission."""
from __future__ import annotations

import asyncio

from hermes_state_runtime import RuntimeStoreError

_KEY = object()
SOURCE_REFERENCE_CREDIT = 4096
MAX_SOURCE_REFERENCE_BYTES = 64 * 1024 * 1024
MAX_SOURCE_REFERENCES_PER_KIND = 1024


def refuse(reason="native_original_source_changed"):
    raise RuntimeStoreError(reason)


class OriginalSourceRuntime:
    def __init__(self, key, runner, bootstrap_task, loop):
        capture_lock = getattr(runner, "_gdb_original_source_capture_lock", None)
        base_credit = getattr(runner, "_gdb_original_source_capture_base_credit", None)
        if (key is not _KEY or capture_lock is None
                or base_credit != SOURCE_REFERENCE_CREDIT
                or runner._gdb_original_source_runtime is not None):
            refuse("native_original_source_factory_missing")
        # Sole factory already owns this ORIGINAL runner lock and has consumed
        # its base debit BEFORE calling OriginalSourceRuntime (including __new__).
        self.runner, self.bootstrap_task, self.loop = runner, bootstrap_task, loop
        self.lock = capture_lock
        self.base_credit = base_credit
        self.registry = None
        self.authorities = {}
        self.servers = {}
        self.connections = {}
        self.assistants = {}
        self.bridges = {}
        self.bytes = base_credit  # carry the already-consumed base term once
        self.retired = False
        self.blocked = False
        self.construction_error = None

    def reserve(self, book, key):
        # Caller owns this lock. A charged hole is retained, never adopted/refunded.
        if (self.retired or self.blocked or key in book
                or len(book) >= MAX_SOURCE_REFERENCES_PER_KIND
                or self.bytes + SOURCE_REFERENCE_CREDIT > MAX_SOURCE_REFERENCE_BYTES):
            refuse("native_original_source_capacity_or_reentry")
        self.bytes += SOURCE_REFERENCE_CREDIT
        book[key] = None

    def require_runtime(self):
        # Same ORIGINAL capture/metadata lock. Reentrant existing source callers
        # keep their lock order; this body performs no DB/IO/await/transport work.
        with self.lock:
            if (self.retired or self.blocked
                    or getattr(self.runner, "_gdb_original_source_capture_lock", None) is not self.lock
                    or getattr(self.runner, "_gdb_original_source_capture_base_credit", None)
                        != SOURCE_REFERENCE_CREDIT
                    or self.base_credit != SOURCE_REFERENCE_CREDIT
                    or self.bytes < self.base_credit
                    or getattr(self.runner, "_gdb_original_source_runtime", None) is not self
                    or getattr(self.runner, "session_authorities", None) is not self.registry
                    or self.registry is None or self.bootstrap_task.done()
                    or self.bootstrap_task.get_loop() is not self.loop
                    or getattr(self.runner, "_draining", False)):
                refuse()
        # Account fields/pointers/task observations are not OS/remote authority.

    def require_authority(self, authority):
        self.require_runtime()
        key = getattr(authority, "_gdb_original_source_registry_key", None)
        if (authority.runner is not self.runner
                or getattr(authority, "_gdb_original_source_runtime", None) is not self
                or self.authorities.get(id(authority)) is not authority
                or self.registry._by_key.get(key) is not authority
                or getattr(authority, "retiring", False)
                or getattr(authority, "retired", False)
                or getattr(authority, "_native_retirement_claimed", False)):
            refuse("native_original_source_authority_changed")


class OriginalSourceServer:
    def __init__(self, key, runtime, api, runner, mount, server, listener):
        if key is not _KEY:
            refuse("native_original_source_factory_missing")
        self.runtime, self.api, self.runner = runtime, api, runner
        self.mount, self.server, self.listener = mount, server, listener
        self.task, self.handle = None, None
        self.calls = {}
        self.retiring = False

    def require_server(self):
        r = self.runtime
        r.require_runtime()
        h = self.handle
        if (self.retiring or r.servers.get(id(self.api)) is not self
                or self.api._gdb_original_source_server is not self
                or self.api.runner is not r.runner or self.runner is not r.runner
                or self.api.web_app is not self.mount
                or h is None or self.task is None or self.task.done()
                or self.task.get_loop() is not r.loop
                or getattr(r.runner, "session_api", None) is not h
                or h.source_owner is not self or h.server is not self.server
                or h.app is not self.mount or h.socket is not self.listener
                or h.task is not self.task or self.server.should_exit):
            refuse("native_original_source_server_changed")


class OriginalSourceConnection:
    def __init__(self, key, parent, connection, ws, transport, task):
        if key is not _KEY:
            refuse("native_original_source_factory_missing")
        self.parent, self.connection = parent, connection
        self.authority, self.actor = connection.authority, connection.actor
        self.ws, self.transport, self.task = ws, transport, task
        self.retired = False

    def require_connection(self):
        r, c = self.parent.runtime, self.connection
        self.parent.require_server()
        r.require_authority(self.authority)
        if (self.retired or r.connections.get(id(c)) is not self
                or getattr(c, "_gdb_original_source_connection", None) is not self
                or c.authority is not self.authority or c.actor is not self.actor
                or c.transport is not self.transport
                or self.authority.events.get(self.actor.transport_id) is not self.transport
                or self.transport._ws is not self.ws
                or self.transport._loop is not r.loop or self.transport._closed
                or self.task is None or self.task.done() or self.task.get_loop() is not r.loop):
            refuse("native_original_source_connection_changed")


def capture_original_runner(runner, bootstrap_task, loop):
    # Sole caller remains immediately after the actual canonical constructor.
    from gateway.run import GatewayRunner
    if not isinstance(runner, GatewayRunner):
        refuse("native_original_source_runner_missing_or_reentry")
    if (not hasattr(runner, "_gdb_original_source_capture_lock")
            or not hasattr(runner, "_gdb_original_source_capture_base_credit")
            or not hasattr(runner, "_gdb_original_source_runtime")):
        refuse("native_original_source_runner_constructor_missing")
    capture_lock = runner._gdb_original_source_capture_lock
    if capture_lock is None:
        refuse("native_original_source_runner_constructor_missing")
    with capture_lock:
        # The SAME one integer is both first-attempt latch and consumed base
        # credit: 0 is untouched, 4096 is permanently claimed. No two-store
        # attempted/credit window, fresh lock, optional default or adoption.
        if (runner._gdb_original_source_capture_base_credit != 0
                or runner._gdb_original_source_runtime is not None):
            refuse("native_original_source_runner_missing_or_reentry")
        runner._gdb_original_source_capture_base_credit = SOURCE_REFERENCE_CREDIT
        # All following validation/allocation failures retain this debit. A
        # concurrent or later factory call sees nonzero even when runtime=None.
        try:
            if (not isinstance(bootstrap_task, asyncio.Task)
                    or bootstrap_task.get_loop() is not loop or bootstrap_task.done()):
                refuse("native_original_source_runner_missing_or_reentry")
            value = OriginalSourceRuntime(_KEY, runner, bootstrap_task, loop)
            runner._gdb_original_source_runtime = value  # publish only complete record
        except BaseException:
            runner._gdb_original_source_runtime = None
            raise  # NEVER clear/reset/refund the original consumed base credit


def attach_original_registry(runner, registry):
    r = getattr(runner, "_gdb_original_source_runtime", None)
    if type(r) is not OriginalSourceRuntime:
        return  # ordinary other constructor path has no native source issuer
    with r.lock:
        if r.registry is not None or runner.session_authorities is not registry:
            refuse()
        r.registry = registry


def attach_original_authority(runner, authority, registry_key):
    r = getattr(runner, "_gdb_original_source_runtime", None)
    if type(r) is not OriginalSourceRuntime:
        return
    with r.lock:
        r.require_runtime()
        if (authority.runner is not runner
                or r.registry._by_key.get(registry_key) is not authority
                or authority._gdb_original_source_runtime is not None):
            refuse()
        r.reserve(r.authorities, id(authority))
        r.authorities[id(authority)] = authority
        authority._gdb_original_source_registry_key = registry_key
        authority._gdb_original_source_runtime = r


def capture_original_server(api, runner, mount, server, listener):
    r = getattr(runner, "_gdb_original_source_runtime", None)
    if type(r) is not OriginalSourceRuntime:
        return None
    with r.lock:
        r.require_runtime()
        r.reserve(r.servers, id(api))
        value = OriginalSourceServer(_KEY, r, api, runner, mount, server, listener)
        r.servers[id(api)] = value
        return value


def attach_original_listener_task(parent, handle, task):
    if parent is None:
        return
    with parent.runtime.lock:
        if (parent.task is not None or parent.handle is not None
                or handle.source_owner is not parent or handle.task is not task
                or handle.server is not parent.server or handle.app is not parent.mount
                or handle.socket is not parent.listener or task.get_loop() is not parent.runtime.loop):
            refuse()
        parent.task, parent.handle = task, handle


def capture_original_connection(parent, connection, ws, transport, task):
    if type(parent) is not OriginalSourceServer:
        return None
    r = parent.runtime
    with r.lock:
        parent.require_server()
        r.require_authority(connection.authority)
        if (connection._gdb_original_source_connection is not None
                or connection.transport is not transport or transport._ws is not ws
                or transport._loop is not r.loop or task is None
                or task.get_loop() is not r.loop or task.done()):
            refuse()
        r.reserve(r.connections, id(connection))
        value = OriginalSourceConnection(_KEY, parent, connection, ws, transport, task)
        r.connections[id(connection)] = value
        connection._gdb_original_source_connection = value
        return value


def require_original_connection(connection):
    value = getattr(connection, "_gdb_original_source_connection", None)
    if type(value) is not OriginalSourceConnection:
        refuse("native_original_source_connection_missing")
    with value.parent.runtime.lock:
        value.require_connection()
    return value


def retire_original_connection(connection):
    value = getattr(connection, "_gdb_original_source_connection", None)
    if type(value) is OriginalSourceConnection:
        with value.parent.runtime.lock:
            value.retired = True  # no release, deletion, adoption or completion


def retire_original_listener(parent, *, request_stop):
    if parent is None:
        return
    with parent.runtime.lock:
        parent.retiring = True
        held = bool(parent.calls)  # includes a retained charged construction hole
    if request_stop and held:
        from gateway.native_source_stop import reject_native_runner_stop
        reject_native_runner_stop(parent.runner)
        # A pending source capture can precede keeper fence registration. Even if
        # the old no-fence ordinary branch returned, this source capture is held.
        refuse("native_original_source_listener_retirement_held")
