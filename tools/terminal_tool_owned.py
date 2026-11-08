"""Original local terminal source custody; object/API observations grant no native authority."""
from __future__ import annotations

import contextvars
import functools
import threading

from agent.auxiliary_owned import running_loop
from hermes_owned_gateway import OwnedGatewayUnknown, active

# Fixed before-work nominal accounting, not a bound on process/pipe/kernel heap.
CREDIT = 65536
MAX_RECORDS, MAX_PROCESS_RECORDS = 512, 4096
_lock = threading.RLock()
_index, _sources = {}, []
_claims = 0
_building = contextvars.ContextVar("_owned_local_environment_build", default=None)
_command = contextvars.ContextVar("_owned_local_terminal_command", default=None)
_normal_cleanup = contextvars.ContextVar("_owned_local_terminal_cleanup", default=None)


def fail(record, error):
    with record.source._state_lock:
        record.error = record.error or error
        record.source.terminal_error = record.source.terminal_error or record.error
        record.source.unknown("original-local-terminal-unknown:" + type(record.error).__name__)


def refuse(record, reason):
    error = OwnedGatewayUnknown(reason)
    fail(record, error)
    raise error


class Record:
    def __init__(self, source, kind, factory, parent):
        from hermes_constants import get_hermes_home, hermes_home_key
        self.source, self.kind, self.factory = source, kind, factory
        self.runner, self.runtime, self.job = source.runner, source.runner._gdb_original_source_runtime, source.job
        self.parent = self.original_parent = parent
        self.creator_thread, self.creator_loop = threading.current_thread(), running_loop()
        self.context, self.home = contextvars.copy_context(), get_hermes_home()
        self.home_key = hermes_home_key(self.home)
        self.obj = self.result = self.error = None
        self.children = ()
        self.attempted = self.done = self.active = False
        self.require()

    def require(self):
        try:
            self._require_original()
        except BaseException as error:
            fail(self, error)
            raise

    def _require_original(self):
        from hermes_constants import hermes_home_key
        runtime = self.runtime
        if (active(self.runner) is not self.source or self.source.runner is not self.runner
                or self.runner._gdb_original_source_runtime is not runtime or runtime.runner is not self.runner
                or runtime.bootstrap_task is not self.source.task or runtime.bootstrap_task.done()
                or runtime.loop is not self.source.loop or self.source.job is not self.job
                or self.parent is not self.original_parent or hermes_home_key() != self.home_key):
            refuse(self, "original-terminal-source-context-or-parent-changed")
        if self.error is not None:
            raise OwnedGatewayUnknown("original-terminal-first-failure-retained")
        self.job.require_current_member()
        if self.original_parent is not None:
            self.original_parent.require()


def reserve(kind, factory, parent=None):
    source = active()
    if parent is not None:
        parent.require()
        if source is not parent.source:
            refuse(parent, "terminal-child-original-source-changed-before-claim")
    if source is None:
        return None
    source.require_child_work()
    global _claims
    with source._state_lock, _lock:
        if source.terminal_closing or len(source.terminal_records) >= MAX_RECORDS or _claims >= MAX_PROCESS_RECORDS:
            raise source.unknown("original-terminal-before-work-capacity-or-shutdown")
        source.terminal_credit += CREDIT
        _claims += 1
        slot = len(source.terminal_records)
        source.terminal_records.append(None)
        _sources.append(source)
        try:
            record = Record(source, kind, factory, parent)
        except BaseException as error:
            source.terminal_error = source.terminal_error or error
            source.unknown("original-terminal-partial-claim-retained")
            raise
        source.terminal_records[slot] = record
        if parent is not None:
            parent.children += (record,)
        return record


def bind(record, obj):
    record.obj = obj
    with _lock:
        if id(obj) in _index:
            refuse(record, "original-terminal-object-already-recorded")
        _index[id(obj)] = record


def find(obj):
    with _lock:
        record = _index.get(id(obj))
    return record if record is not None and record.obj is obj else None


def require_env(env):
    record = find(env)
    if record is None:
        if active() is None:
            return None
        record = reserve("unowned-environment", require_env)
        record.result = env
        record.hold_marker_error = None
        try:
            env._gdb_terminal_hold = record
        except (AttributeError, TypeError) as error:
            record.hold_marker_error = error
        refuse(record, "terminal-original-environment-constructor-unavailable")
    record.require()
    if record.kind != "environment" or env._gdb_terminal_owner is not record:
        refuse(record, "original-terminal-environment-association-changed")
    from agent.auxiliary_owned_close import _same_callable
    for name, method in record.methods:
        if not _same_callable(getattr(env, name), method):
            refuse(record, "original-terminal-environment-method-changed:" + name)
    return record


def environment_factory(factory):
    @functools.wraps(factory)
    def invoke(env_type, *args, **kwargs):
        record = reserve("environment", factory)
        if record is None:
            return factory(env_type, *args, **kwargs)
        if env_type != "local":
            refuse(record, "managed-terminal-backend-normal-lifecycle-unavailable:" + env_type)
        token = _building.set(record)
        record.attempted = True
        try:
            result = factory(env_type, *args, **kwargs)
            record.result = result
            if record.obj is not result or not record.done:
                refuse(record, "terminal-factory-did-not-return-original-local-constructor")
            require_env(result)
            return result
        except BaseException as error:
            fail(record, error)
            raise
        finally:
            _building.reset(token)
    return invoke


def local_initializer(initializer):
    @functools.wraps(initializer)
    def invoke(self, *args, **kwargs):
        record = _building.get() or reserve("environment", initializer)
        if record is None:
            return initializer(self, *args, **kwargs)
        from tools.environments.local import LocalEnvironment
        if type(self) is not LocalEnvironment or record.obj is not None:
            refuse(record, "original-local-initializer-reentry-or-opaque-subclass")
        bind(record, self)
        self._gdb_terminal_owner = record
        record.methods = tuple((name, getattr(self, name)) for name in (
            "execute", "init_session", "_run_bash", "_wait_for_process", "cleanup"))
        record.cleanup_attempted = record.cleanup_done = False
        record.cleanup_paths = None
        token = _building.set(record)
        record.attempted = True
        try:
            initializer(self, *args, **kwargs)
            require_env(self)
            record.cleanup_paths = (self._snapshot_path, self._cwd_file)
            record.done = True
        except BaseException as error:
            fail(record, error)
            raise
        finally:
            _building.reset(token)
    return invoke


def execute(method):
    @functools.wraps(method)
    def invoke(env, *args, **kwargs):
        parent = require_env(env)
        if parent is None:
            return method(env, *args, **kwargs)
        record = reserve("command", method, parent)
        record.obj, record.arguments = env, (args, kwargs)
        record.active = record.attempted = True
        token = _command.set(record)
        try:
            record.result = method(env, *args, **kwargs)
            from tools.terminal_tool_owned_workers import finish_command
            finish_command(record)
            require_env(env)
            record.require()
            record.done = True
            return record.result
        except BaseException as error:
            fail(record, error)
            raise
        finally:
            record.active = False
            _command.reset(token)
    return invoke


def protect_legacy(env):
    if env is None:
        return False
    record = find(env)
    if record is not None:
        return _normal_cleanup.get() is not record
    original = getattr(env, "_gdb_terminal_owner", None)
    if isinstance(original, Record) and original.obj is env:
        return True  # A stale destructor may hold; this marker never authorizes normal closure/adoption.
    held = getattr(env, "_gdb_terminal_hold", None)
    if isinstance(held, Record) and held.result is env:
        return True  # Unknown original objects also stay held; no new source is adopted by destruction.
    if active() is not None:
        require_env(env)
    return False


def prevent_force(env, proc=None):
    record = find(proc) or find(env)
    if record is not None:
        refuse(record, "original-terminal-active-work-requires-natural-completion")
    if active() is not None:
        require_env(env)


def guard_global_force(registry, condition):
    with condition:
        originals = tuple((env, proc) for env, proc in registry.values() if find(proc) is not None)
    for env, proc in originals:
        prevent_force(env, proc)


def unsupported(reason):
    record = reserve("unsupported-terminal", unsupported)
    if record is not None:
        refuse(record, "managed-terminal-original-normal-lifecycle-unavailable:" + reason)
