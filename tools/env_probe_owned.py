"""Original scoped toolchain-probe work, never native or authentication authority.

One already-accepted probe owns at most five original diagnostic commands and
their original readers. A deadline only retains an unknown lifetime: no kill,
replacement reader, retry, cleanup funnel or inferred descendant completion.
"""
from __future__ import annotations

import threading
import contextvars
from pathlib import Path

from hermes_owned_gateway import OwnedGatewayUnknown, active

_thread_type = threading.Thread
_current_thread = threading.current_thread
COMMANDS = 5
CREDIT = 1024 * 1024  # Nominal retained references/output, not allocator/RSS proof.


def fail(source, error):
    with source._state_lock:
        if source._environment_probe_error is None:
            source._environment_probe_error = error
        if source._environment_probe_traceback is None:
            source._environment_probe_traceback = source._environment_probe_error.__traceback__
        source._environment_probe_state = "failed"
        source.unknown("original-environment-probe-unknown")
    return source._environment_probe_error


def construct(owner, node, cls, args=(), kwargs=None):
    if node:
        raise OwnedGatewayUnknown("original-probe-constructor-already-attempted")
    kwargs = {} if kwargs is None else kwargs
    node.update(type=cls, creator=cls.__new__, initializer=cls.__init__, args=args, kwargs=kwargs,
                original=None, allocator_result=None, error=None, traceback=None)
    try:
        creator, initializer = node["creator"], node["initializer"]
        value = creator(cls) if creator is object.__new__ else creator(cls, *args, **kwargs)
        node["original"] = node["allocator_result"] = value
        if owner.source._environment_probe_error is not None:
            raise owner.source._environment_probe_error
        if type(value) is not cls or vars(value):
            raise OwnedGatewayUnknown("foreign-or-preowned-probe-allocation")
        initializer(value, *args, **kwargs)
        if (node["original"] is not value or node["allocator_result"] is not value
                or cls.__new__ is not creator or cls.__init__ is not initializer
                or owner.source._environment_probe_error is not None):
            raise OwnedGatewayUnknown("original-probe-construction-displaced")
        return value
    except BaseException as error:
        first = fail(owner.source, error)
        node["error"], node["traceback"] = first, owner.source._environment_probe_traceback
        raise


def require_node(owner, node):
    owner.require()
    cls, value = node["type"], node["original"]
    if (type(value) is not cls or node["allocator_result"] is not value or node["error"] is not None
            or cls.__new__ is not node["creator"] or cls.__init__ is not node["initializer"]):
        raise OwnedGatewayUnknown("original-probe-construction-changed")
    return value


def require_thread(owner, node):
    from agent.auxiliary_owned_close import _same_callable
    thread = require_node(owner, node)
    if any(not _same_callable(actual, original) for actual, original in zip(
            (thread.start, thread.join, thread.is_alive), node["methods"])):
        raise OwnedGatewayUnknown("original-probe-thread-methods-changed")
    return thread


def original_owner(source):
    owner = source.environment_probe
    callbacks = source._environment_probe_constructor
    if (type(owner) is not ProbeOwner or source._original_environment_probe is not owner
            or source._environment_probe_credit != CREDIT or type(callbacks) is not tuple
            or len(callbacks) != 2 or callbacks[0] is not ProbeOwner.__new__
            or callbacks[1] is not ProbeOwner.__init__):
        raise OwnedGatewayUnknown("original-probe-construction-required")
    return owner


class ProbeOwner:
    def __init__(self, source, module):
        from hermes_constants import get_hermes_home, hermes_home_key
        from tools.environments.local import build_subprocess_env
        self.source, self.module = source, module
        self.runner, self.job = source.runner, source.job
        self.runtime = self.runner._gdb_original_source_runtime
        self.registry = self.runtime.registry
        self.home, self.home_key = get_hermes_home(), hermes_home_key()
        self.context = contextvars.copy_context()
        self.generation, self.worker = module._PROBE_GEN, module._probe_worker
        self.builder = module._build_probe_line
        self.nodes = tuple({} for _ in range(1 + 3 * COMMANDS))
        self.commands, self.next_command = [], 0
        self.command_active = False
        self.finished = self.closed = self.join_attempted = False
        self.thread = None
        self.runtime.require_runtime()  # New batch requires the original initialized runtime.
        self.require()
        self.environment = build_subprocess_env(strip_launch_profile=True)
        self.cwd = str(Path(self.home))

    def require(self):
        from gateway.native_capture_construction import require_capture_ready
        from hermes_constants import hermes_home_key
        source = self.source
        if (original_owner(source) is not self or active(self.runner) is not source
                or source._original_environment_probe is not self
                or source.job is not self.job or source.runner is not self.runner
                or self.runner._gdb_original_source_runtime is not self.runtime
                or self.runtime.registry is not self.registry or self.runner.session_authorities is not self.registry
                or self.runtime.bootstrap_task is not source.task or self.runtime.loop is not source.loop
                or source.phase not in ("bound", "draining") or hermes_home_key() != self.home_key
                or source._environment_probe_error is not None):
            raise OwnedGatewayUnknown("original-probe-source-context-changed")
        # An already-accepted diagnostic batch may finish while native admission
        # is draining. Check retained construction, never weaken the native gate.
        require_capture_ready(self.runtime)
        self.job.require_current_member()

    def thread_factory(self, node, **kwargs):
        return construct(self, node, _thread_type, kwargs=kwargs)

    def start_thread(self, node, target, *, name, args=()):
        from agent.memory_provider import spawn_context_thread
        from gateway.runtime_ownership import process_ownership
        thread = spawn_context_thread(target, name=name, args=args,
            thread_factory=lambda **kwargs: self.thread_factory(node, **kwargs))
        node["methods"] = (thread.start, thread.join, thread.is_alive)
        self.require()
        process_ownership.start_writer(thread)
        require_thread(self, node)
        return thread

    def run_worker(self):
        try:
            self.require()
            if (_current_thread() is not self.thread or self.module._PROBE_THREAD is not self.thread
                    or self.module._PROBE_GEN != self.generation or self.module._probe_worker is not self.worker
                    or self.module._build_probe_line is not self.builder):
                raise OwnedGatewayUnknown("original-probe-worker-publication-changed")
            self.worker(self.generation)
            self.require()
            if not self.module._PROBE_DONE.is_set():
                raise OwnedGatewayUnknown("original-probe-worker-did-not-publish")
            self.finished = True
        except BaseException as error:
            fail(self.source, error)
            raise

    def run_command(self, cmd, timeout):
        self.require()
        if (_current_thread() is not self.thread or self.finished or self.command_active
                or self.next_command >= COMMANDS):
            raise OwnedGatewayUnknown("original-probe-worker-or-command-credit-required")
        slot = self.next_command
        self.next_command += 1
        self.command_active = True
        record = {"argv": cmd, "timeout": timeout, "slot": slot, "process": self.nodes[1 + 3 * slot],
                  "readers": self.nodes[2 + 3 * slot:4 + 3 * slot], "eof": [False, False],
                  "output": [bytearray(), bytearray()], "exit": None, "finished": False}
        self.commands.append(record)  # Keep inputs/credit before the actual process constructor.
        from tools.env_probe_owned_io import run_command
        result = run_command(self, record)
        self.command_active = False
        return result


def ensure_started(module, source):
    owner = source.environment_probe
    if owner is not None:
        if source._environment_probe_state == "initializing":
            raise OwnedGatewayUnknown("original-probe-construction-already-initializing")
        owner = original_owner(source)
        owner.require()
        if (owner.module is not module or module._PROBE_THREAD is not owner.thread
                or module._PROBE_GEN != owner.generation):
            raise OwnedGatewayUnknown("original-probe-cache-displaced")
        if not owner.finished and not owner.thread.is_alive():
            raise OwnedGatewayUnknown("original-probe-worker-ended-without-completion")
        return
    source.require_child_work()
    if (source.phase != "bound" or source.runner is None or source._environment_probe_credit
            or source._original_environment_probe is not None or module._PROBE_THREAD is not None
            or module._PROBE_DONE.is_set()):
        raise OwnedGatewayUnknown("original-probe-batch-unavailable-or-preowned")
    source._environment_probe_credit = CREDIT
    source._environment_probe_state = "initializing"
    source._environment_probe_constructor = (ProbeOwner.__new__, ProbeOwner.__init__)
    try:
        if ProbeOwner.__new__ is not object.__new__:
            raise OwnedGatewayUnknown("foreign-probe-owner-allocator")
        owner = object.__new__(ProbeOwner)
        source.environment_probe = source._original_environment_probe = owner
        ProbeOwner.__init__(owner, source, module)
        # Publish this exact original thread BEFORE its start callback can run.
        from agent.memory_provider import spawn_context_thread
        node = owner.nodes[0]
        thread = spawn_context_thread(owner.run_worker, name="env-probe",
            thread_factory=lambda **kwargs: owner.thread_factory(node, **kwargs))
        owner.thread = module._PROBE_THREAD = thread
        node["methods"] = (thread.start, thread.join, thread.is_alive)
        source._environment_probe_state = "prepared"  # Construction, never startup permission.
        from gateway.runtime_ownership import process_ownership
        process_ownership.start_writer(thread)
        require_thread(owner, node)
    except BaseException as error:
        fail(source, error)
        raise


def shutdown(module, source):
    if source.phase != "draining" or not source._shutdown or not source._control_closed:
        raise OwnedGatewayUnknown("original-probe-shutdown-boundary-required")
    owner = source.environment_probe
    if owner is None:
        if module._PROBE_THREAD is not None:
            raise OwnedGatewayUnknown("foreign-probe-worker-cannot-be-adopted")
        return
    owner = original_owner(source)
    if owner.module is not module:
        raise OwnedGatewayUnknown("original-probe-module-changed")
    return owner.context.run(_shutdown, owner)


def _shutdown(owner):
    owner.require()
    if owner.closed:
        raise OwnedGatewayUnknown("original-probe-close-already-attempted")
    if owner.join_attempted:
        raise OwnedGatewayUnknown("original-probe-join-attempt-retained")
    owner.join_attempted = True
    require_thread(owner, owner.nodes[0])
    owner.nodes[0]["methods"][1]()
    require_thread(owner, owner.nodes[0])
    if (owner.module._PROBE_THREAD is not owner.thread or owner.module._PROBE_GEN != owner.generation
            or owner.thread.is_alive() or not owner.finished
            or any(not record["finished"] for record in owner.commands)):
        raise OwnedGatewayUnknown("original-probe-work-not-normally-settled")
    owner.closed = True
    owner.source._environment_probe_state = "closed"
