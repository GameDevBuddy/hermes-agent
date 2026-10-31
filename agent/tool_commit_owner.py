"""Original live canonical call binding to the existing batch writer's successful return."""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
import logging
import threading
import weakref

from agent.message_metadata import MESSAGE_UID, TOOL_CALL_UID, stamp_message_uid
from agent.message_sanitization import coalesce_tool_call_id
from agent.tool_commit_rows import WITNESS, MAX_WITNESSES, MAX_CALLS, json_digest, text_digest
from agent.tool_commit_construction import construction_ready, construct_pending, pending_unavailable

logger = logging.getLogger(__name__)


class PendingToolCommit:
    def __init__(self, owner, assistant, uid, ref, args_digest):
        self._initialized = False
        self.owner = owner
        self.uid = uid
        self.ref = ref
        self.call_id, self.tool_name, self.task_id = ref.call_id, ref.name, ref.task_id
        self.effective_args_digest = args_digest
        self.source_digest = None
        self.projection_digest = None
        self.message = None
        self.message_uid = None
        self.turn_holder = assistant["turn_holder"]
        self.compression_holder = assistant["compression_holder"]
        self.receipt = None
        self.publication_observed = False
        self.native_dispatch_slot = None
        self.native_origin_error = None
        keys = ("row_id", "message_uid", "row_version", "calls_digest", "call_uids_digest",
                "turn_holder", "compression_holder")
        self.assistant = {key: deepcopy(assistant[key]) for key in keys}
        self._initialized = True

    def prepare(self, message, raw_digest):
        if not construction_ready(self):
            return
        if self.native_dispatch_slot is not None or self.native_origin_error is not None:
            from agent.native_sql_writer import prepare_local_message
            return prepare_local_message(self, message, raw_digest)
        projection = text_digest(message.get("content"))
        if raw_digest is None or projection is None or self.effective_args_digest is None:
            return
        if self.message is not None:
            return
        stamp_message_uid(message)
        self.message_uid = message[MESSAGE_UID]
        message[TOOL_CALL_UID] = self.uid
        self.source_digest = deepcopy(raw_digest)
        self.projection_digest = projection
        self.message = message
        self.owner.watch(message, self)

    def consume(self, witness):
        if not construction_ready(self):
            return
        if self.receipt is not None or self.message is None:
            return
        if self.native_dispatch_slot is not None:
            from agent.native_sql_writer import before_consume, fail_sources
            try:
                before_consume(self, witness)
            except BaseException as error:
                fail_sources(((0, self, self.message),), error)
                raise
        parent = witness.get("assistant")
        keys = ("row_id", "message_uid", "row_version", "calls_digest", "call_uids_digest")
        if not isinstance(parent, dict) or any(parent.get(key) != self.assistant[key] for key in keys):
            return
        if (parent.get("calls_digest") is None or parent.get("call_uids_digest") is None
                or witness.get("role") != "tool" or witness.get("tool_call_uid") != self.uid
                or witness.get("message_uid") != self.message_uid
                or witness.get("tool_call_id") != self.call_id
                or witness.get("tool_name") != self.tool_name
                or witness.get("content_digest") != self.projection_digest):
            return
        self.receipt = {
            **self.owner.scope,
            "schema": "gdb-canonical-tool-row-commit-v1",
            "assistant_message_uid": self.assistant["message_uid"],
            "assistant_row_id": self.assistant["row_id"],
            "tool_call_uid": self.uid, "provider_call_id": self.call_id,
            "tool_name": self.tool_name, "effective_args_digest": self.effective_args_digest,
            "tool_result_message_uid": witness["message_uid"],
            "tool_result_row_id": witness["row_id"],
            "source_result_utf8": self.source_digest,
            "canonical_projection_utf8": self.projection_digest,
            "canonical_row_version": witness["row_version"],
            "observed_in_transaction_at": witness["observed_in_transaction_at"],
            "native_ack_allowed": False, "replay_allowed": False,
        }
        from agent.native_sql_writer import publish_observation
        self.publication_observed = publish_observation(self, deepcopy(self.receipt))
        if self.native_dispatch_slot is not None:
            from agent.native_sql_writer import after_consume, fail_sources
            try:
                after_consume(self)
            except BaseException as error:
                fail_sources(((0, self, self.message),), error)
                raise


class ToolCommitOwner:
    def __init__(self, agent, scope, validate, publish):
        self._initialized = False
        self._pending_constructions = {}
        self._pending_constructing = None
        self._pending_construction_credit = 0
        self.first_exception = None
        self._native_bridge_construction = None
        self._native_bridge_constructor_phase = "fresh"
        self._native_bridge_constructor_credit = 0
        self._native_bridge_constructor_error = self._native_bridge_constructor_traceback = None
        self.agent = weakref.ref(agent)
        self.db = agent._session_db
        self.scope = deepcopy(scope)
        self.validate = validate
        self.publish = publish
        self.lock = threading.RLock()
        self.watched = {}
        self.pending = []
        self.staged = None
        self.native_bridge = self.native_bridge_factory = None
        self.native_source_assistant = self.native_source_message_factory = None
        self._native_retention_ref = None
        self._initialized = True

    def usable(self):
        if not construction_ready(self):
            return False
        agent = self.agent()
        return (agent is not None and agent._session_db is self.db
                and getattr(agent, "_canonical_tool_commit_owner", None) is self
                and agent.session_id == self.scope["session_id"] and self.validate())

    def binding_current(self, frozen):
        if (not self.usable()
                or isinstance(frozen, PendingToolCommit) and not construction_ready(frozen)):
            return False
        agent = self.agent()
        if isinstance(frozen, dict):
            holder, compression = frozen["turn_holder"], frozen["compression_holder"]
        else:
            holder, compression = frozen.turn_holder, frozen.compression_holder
        if (not isinstance(holder, str) or not holder
                or getattr(agent, "_active_session_turn_lease_holder", None) != holder
                or getattr(agent, "_active_compression_lock_holder", None) != compression):
            return False
        with self.db._read_ctx() as conn:
            lease_key = self.db._session_turn_lease_key_on_conn(conn, self.scope["session_id"])
            lease = conn.execute(
                "SELECT holder, expires_at FROM session_turn_leases WHERE conversation_id=?",
                (lease_key,),
            ).fetchone()
            comp = conn.execute(
                "SELECT holder, expires_at FROM compression_locks WHERE session_id=?",
                (self.scope["session_id"],),
            ).fetchone()
        # Compare original holders; expiry does not mint a replacement holder.
        # The actual append retains its existing same-holder expiry-renewal behavior.
        return (lease is not None and lease["holder"] == holder
                and (comp["holder"] if comp is not None else None) == compression)

    def watch(self, message, pending):
        if not construction_ready(self):
            return
        with self.lock:
            if len(self.watched) < MAX_WITNESSES and self.usable():
                self.watched[id(message)] = (message, pending)

    def stage(self, message):
        if not construction_ready(self):
            return
        with self.lock:
            self.staged = None
            self.native_source_assistant = None
            if not self.usable():
                return
            calls = message.get("tool_calls") or []
            if not isinstance(calls, list) or len(calls) > MAX_CALLS:
                return
            definitions = json_digest(calls)
            if definitions is None:
                return
            agent = self.agent()
            holder = getattr(agent, "_active_session_turn_lease_holder", None)
            if not isinstance(holder, str) or not holder:
                return
            stamp_message_uid(message)
            frozen = {
                "calls_digest": definitions, "message_uid": message[MESSAGE_UID],
                "turn_holder": holder,
                "compression_holder": getattr(agent, "_active_compression_lock_holder", None),
            }
            if self.binding_current(frozen):
                self.watch(message, frozen)

    def indices(self, messages):
        if not construction_ready(self):
            return ()
        with self.lock:
            if not self.usable():
                return ()
            return tuple(
                (index, pending.assistant["message_uid"] if isinstance(pending, PendingToolCommit) else None)
                for index, message in enumerate(messages)
                if (entry := self.watched.get(id(message))) is not None and entry[0] is message
                for pending in (entry[1],)
                if self.binding_current(pending)
            )

    def writer_sources(self, messages):
        if not construction_ready(self):
            return ()
        with self.lock:
            return tuple((index, entry[1], message)
                         for index, message in enumerate(messages)
                         if (entry := self.watched.get(id(message))) is not None
                         and entry[0] is message and isinstance(entry[1], PendingToolCommit)
                         and construction_ready(entry[1])
                         and entry[1].native_dispatch_slot is not None)

    def _matches(self, witness, pending):
        if not isinstance(witness, dict) or not self.binding_current(pending):
            return False
        if isinstance(pending, dict):
            holder, compression = pending["turn_holder"], pending["compression_holder"]
        else:
            holder, compression = pending.turn_holder, pending.compression_holder
        keys = ("session_id", "instance_id", "epoch", "generation", "revision")
        return (witness.get("active") == 1
                and all(witness.get(key) == self.scope[key] for key in keys)
                and witness.get("turn_holder") == holder
                and witness.get("compression_holder") == compression)

    def consume(self, messages, rows):
        if not construction_ready(self):
            return
        with self.lock:
            if not self.usable():
                return
            for message, row in zip(messages, rows):
                watched = self.watched.get(id(message))
                witness = row.get(WITNESS)
                if watched is None or watched[0] is not message:
                    continue
                pending = watched[1]
                if not self._matches(witness, pending):
                    continue
                if isinstance(pending, dict):
                    if (pending["calls_digest"] is not None and witness.get("calls_digest") is not None
                            and witness.get("call_uids_digest") is not None
                            and witness.get("role") == "assistant"
                            and witness.get("message_uid") == pending["message_uid"]
                            and witness.get("calls_digest") == pending["calls_digest"]):
                        self.staged = deepcopy(witness)
                        self.native_source_assistant = None
                        if self.native_source_message_factory is not None:
                            self.native_source_assistant = self.native_source_message_factory(
                                self, message, self.staged)
                else:
                    pending.consume(witness)
                self.watched.pop(id(message), None)

    def prepare(self, pending, message, raw_digest):
        if not construction_ready(self) or not construction_ready(pending):
            return
        with self.lock:
            if (pending.owner is self and pending in self.pending
                    and self.binding_current(pending)):
                pending.prepare(message, raw_digest)

    def bind(self, ref):
        from agent.native_bridge_construction import bridge_in_progress
        if not construction_ready(self) or bridge_in_progress(self) or pending_unavailable(self, ref):
            return None
        with self.lock:
            if (self.staged is None or len(self.pending) >= MAX_WITNESSES
                    or not self.binding_current(self.staged)
                    or self.staged.get("calls_digest") is None
                    or self.staged.get("call_uids_digest") is None):
                return None
            args_digest = json_digest(ref.args)
            if args_digest is None:
                return None
            calls = self.staged["calls"]
            if not isinstance(calls, list) or len(calls) > MAX_CALLS:
                return None
            counts = Counter(coalesce_tool_call_id(call) for call in calls)
            uid = self.staged["call_uids"].get(ref.call_id)
            if counts[ref.call_id] != 1 or not isinstance(uid, str) or not uid:
                return None
            call = next(call for call in calls if coalesce_tool_call_id(call) == ref.call_id)
            if call.get("function", {}).get("name") != ref.name:
                return None
            if uid in self._pending_constructions or any(p.uid == uid for p in self.pending):
                return None
            pending = construct_pending(self, PendingToolCommit, self.staged, uid, ref, args_digest)
            if pending is None:
                return None
            self._bind_native_pending(pending)
            return pending

    def _bind_native_pending(self, pending):
        from agent.native_receipt_origin import NativeDispatchBridge, is_native_tool_name
        if not is_native_tool_name(pending.ref.name):
            return
        bridge = self.native_bridge
        if bridge is None and self.native_bridge_factory is not None:
            try:
                from agent.native_bridge_construction import create_lazy_bridge
                bridge = create_lazy_bridge(self, pending)
            except (ValueError, TypeError, UnicodeError, RecursionError):
                pending.native_origin_error = "native_original_owner_unavailable"
        if type(bridge) is not NativeDispatchBridge or bridge.owner is not self:
            pending.native_origin_error = "native_original_owner_unavailable"
            return
        try:
            pending.native_dispatch_slot = bridge.reserve(pending)
        except (ValueError, TypeError, UnicodeError, RecursionError):
            pending.native_origin_error = "native_reservation_unavailable"


def owner_call(agent, method, *args, default=None):
    owner = getattr(agent, "_canonical_tool_commit_owner", None)
    if not isinstance(owner, ToolCommitOwner):
        return default
    try:
        return getattr(owner, method)(*args)
    except Exception:
        logger.warning("Canonical tool-row witness withheld", exc_info=True)
        return default
