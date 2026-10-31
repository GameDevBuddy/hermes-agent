"""Finite source-only optional-observer construction custody; never native admission."""
from __future__ import annotations

import asyncio
import contextvars
from copy import deepcopy
import threading

from agent.tool_commit_rows import MAX_WITNESSES, json_digest
from hermes_state_runtime import RuntimeStoreError

CONSTRUCTION_CREDIT = 4096  # Nominal accounting, not a Python heap-size claim.


class CommitConstruction:
    def __init__(self, parent, key):
        self.parent, self.key = parent, key
        self.value = self.error = self.traceback = None
        self.phase, self.credit = "reserved", CONSTRUCTION_CREDIT
        self.inputs = self.callbacks = self.original = None
        self.thread = self.context = self.loop = self.task = None

    def capture(self, inputs, callbacks, original):
        self.inputs, self.callbacks, self.original = inputs, callbacks, original
        self.thread, self.context = threading.get_ident(), contextvars.copy_context()
        try:
            self.loop = asyncio.get_running_loop()
            self.task = asyncio.current_task()
        except RuntimeError:
            pass

    def fail(self, error):
        if self.error is None:
            self.error, self.traceback = error, error.__traceback__
        if self.value is not None and getattr(self.value, "first_exception", None) is None:
            self.value.first_exception = error
        self.phase = "failed"

    def require_current_context(self):
        if threading.get_ident() != self.thread:
            raise RuntimeStoreError("canonical_constructor_thread_displaced")
        current = dict(contextvars.copy_context().items())
        original = dict(self.context.items())
        if (current.keys() != original.keys()
                or any(current[key] is not value for key, value in original.items())):
            raise RuntimeStoreError("canonical_constructor_context_displaced")
        try:
            loop, task = asyncio.get_running_loop(), asyncio.current_task()
        except RuntimeError:
            loop = task = None
        if loop is not self.loop or task is not self.task:
            raise RuntimeStoreError("canonical_constructor_task_displaced")


def construction_ready(value):
    if not getattr(value, "_initialized", False):
        return False
    attempt = getattr(value, "_commit_construction", None)
    return attempt is None or (attempt.value is value and attempt.phase == "ready")


def pending_unavailable(owner, ref):
    attempt = getattr(owner, "_pending_constructing", None)
    if attempt is not None and attempt.phase == "constructing":
        return True
    staged = owner.staged
    uid = staged.get("call_uids", {}).get(ref.call_id) if isinstance(staged, dict) else None
    return uid in owner._pending_constructions


def owner_graph(authority, session_id, generation, agent):
    live = authority.sessions.get(session_id)
    db = authority.db
    writer = getattr(db, "_execute_write", None)
    return (live, authority.db, getattr(db, "_conn", None),
            getattr(writer, "__self__", None), getattr(writer, "__func__", writer),
            authority.profile_id, authority.instance_id, authority.epoch,
            authority.adopted.get(session_id), authority.executing.get(session_id),
            getattr(agent, "_session_db", None), getattr(agent, "session_id", None),
            getattr(agent, "_active_session_turn_lease_holder", None),
            getattr(agent, "_active_compression_lock_holder", None),
            live.event_stream if live is not None else None,
            live.event_stream.execution.copy() if live is not None else None,
            authority.runner, getattr(authority, "_gdb_original_source_runtime", None),
            authority._native_turn_retention,
            authority._native_turn_retention.get((session_id, generation)),
            live.source if live is not None else None, live.task if live is not None else None,
            live.event_stream.lock if live is not None else None,
            authority._canonical_tool_commit_attempts)


def same_graph(current, original, identity_indices):
    return (all(current[index] is original[index] for index in identity_indices)
            and current == original)


def same_owner_graph(attempt):
    authority, session_id, generation, agent = attempt.inputs
    current = owner_graph(authority, session_id, generation, agent)
    return (same_graph(current, attempt.original, (0, 1, 2, 3, 4, 10, 14, 16, 17, 18, 19, 20, 21, 22, 23))
            and current[8][1] is attempt.original[8][1]
            and authority._canonical_tool_commit_attempts[attempt.key][-1] is attempt)


def begin_owner_install(authority, session_id, generation, agent):
    key = (session_id, generation, id(agent))
    group = authority._canonical_tool_commit_attempts.setdefault(key, [])
    if group and (group[-1] is None or group[-1].phase in ("reserved", "constructing", "failed")):
        raise RuntimeStoreError("canonical_owner_construction_already_consumed")
    if len(group) >= MAX_WITNESSES:
        raise RuntimeStoreError("canonical_owner_construction_budget_exhausted")
    group.append(None)  # Consume this adoption's finite credit before allocation.
    attempt = CommitConstruction(authority, key)
    group[-1] = attempt
    agent._canonical_tool_commit_owner_attempt = attempt
    return attempt


def capture_owner_install(attempt, authority, session_id, generation, agent):
    attempt.phase = "constructing"
    attempt.capture((authority, session_id, generation, agent), (),
                    owner_graph(authority, session_id, generation, agent))


def construct_owner(attempt, cls, agent, scope, validate, publish):
    attempt.callbacks = (validate, publish)
    attempt.owner_inputs = (agent, scope, tuple(scope.items()), validate, publish)
    value = object.__new__(cls)
    attempt.value = value
    value._commit_construction = attempt
    cls.__init__(value, agent, scope, validate, publish)
    attempt.require_current_context()
    if (value.agent() is not agent or value.db is not attempt.original[1]
            or tuple(scope.items()) != attempt.owner_inputs[2]
            or value.scope != scope or value.validate is not validate or value.publish is not publish):
        raise RuntimeStoreError("canonical_owner_constructor_inputs_displaced")
    return value


def finish_owner_install(attempt, value):
    agent = attempt.inputs[3]
    attempt.require_current_context()
    if (not same_owner_graph(attempt)
            or agent._canonical_tool_commit_owner_attempt is not attempt
            or agent._canonical_tool_commit_owner is not None or value is not attempt.value):
        raise RuntimeStoreError("canonical_owner_constructor_parent_displaced")
    if value is None:
        attempt.phase = "empty"
        return
    if not value._initialized:
        raise RuntimeStoreError("canonical_owner_constructor_incomplete")
    agent._canonical_tool_commit_owner = value
    attempt.require_current_context()
    if agent._canonical_tool_commit_owner is not value or not same_owner_graph(attempt):
        raise RuntimeStoreError("canonical_owner_constructor_publication_displaced")
    attempt.phase = "ready"


def pending_graph(owner):
    agent = owner.agent()
    writer = getattr(owner.db, "_execute_write", None)
    return (agent, owner.db, getattr(owner.db, "_conn", None),
            getattr(writer, "__self__", None), getattr(writer, "__func__", writer),
            agent._session_db, getattr(agent, "_canonical_tool_commit_owner", None),
            owner.scope.copy(), owner.validate, owner.publish, owner.staged,
            getattr(agent, "_active_session_turn_lease_holder", None),
            getattr(agent, "_active_compression_lock_holder", None),
            owner.lock, owner._native_retention_ref, owner.native_bridge, owner.native_source_assistant,
            owner.native_bridge_factory, owner.native_source_message_factory,
            owner._pending_constructions, owner.pending)


def construct_pending(owner, cls, assistant, uid, ref, args_digest):
    # The caller already holds the original owner's RLock. Reentry is refused
    # before any policy/DB read, including while a non-reentrant DB lock is held.
    if uid in owner._pending_constructions or len(owner._pending_constructions) >= MAX_WITNESSES:
        return None
    owner._pending_construction_credit += CONSTRUCTION_CREDIT
    owner._pending_constructions[uid] = None
    attempt = CommitConstruction(owner, uid)
    owner._pending_constructions[uid] = owner._pending_constructing = attempt
    try:
        attempt.phase = "constructing"
        attempt.capture((owner, assistant, uid, ref, args_digest),
                        (owner.validate, owner.publish), pending_graph(owner))
        keys = ("row_id", "message_uid", "row_version", "calls_digest", "call_uids_digest",
                "turn_holder", "compression_holder")
        attempt.assistant_fields = deepcopy({key: assistant[key] for key in keys})
        attempt.ref_fields = (ref.call_id, ref.name, ref.task_id)
        value = object.__new__(cls)
        attempt.value = value
        owner.pending.append(value)
        value._commit_construction = attempt
        cls.__init__(value, owner, assistant, uid, ref, args_digest)
        finish_pending(attempt)
        return value
    except BaseException as error:
        attempt.fail(error)
        if owner.first_exception is None:
            owner.first_exception = error
        raise


def finish_pending(attempt):
    owner, assistant, uid, ref, args_digest = attempt.inputs
    value = attempt.value
    attempt.require_current_context()
    if (not same_graph(pending_graph(owner), attempt.original,
                       (0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 13, 14, 15, 16, 17, 18, 19, 20))
            or owner._pending_constructions.get(uid) is not attempt
            or owner._pending_constructing is not attempt
            or not any(pending is value for pending in owner.pending)
            or value.owner is not owner or value.uid != uid or value.ref is not ref
            or value.assistant != attempt.assistant_fields
            or any(assistant[key] != expected for key, expected in attempt.assistant_fields.items())
            or (ref.call_id, ref.name, ref.task_id) != attempt.ref_fields
            or (value.call_id, value.tool_name, value.task_id) != attempt.ref_fields
            or value.effective_args_digest is not args_digest or json_digest(ref.args) != args_digest
            or not value._initialized):
        raise RuntimeStoreError("canonical_pending_constructor_parent_displaced")
    attempt.phase = "ready"
