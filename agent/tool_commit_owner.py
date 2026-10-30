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

logger = logging.getLogger(__name__)


class PendingToolCommit:
    def __init__(self, owner, assistant, uid, ref, args_digest):
        self.owner = owner
        keys = ("row_id", "message_uid", "row_version", "calls_digest", "call_uids_digest",
                "turn_holder", "compression_holder")
        self.assistant = {key: deepcopy(assistant[key]) for key in keys}
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

    def prepare(self, message, raw_digest):
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
        if self.receipt is not None or self.message is None:
            return
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
        self.publication_observed = bool(self.owner.publish(self, deepcopy(self.receipt)))


class ToolCommitOwner:
    def __init__(self, agent, scope, validate, publish):
        self.agent = weakref.ref(agent)
        self.db = agent._session_db
        self.scope = deepcopy(scope)
        self.validate = validate
        self.publish = publish
        self.lock = threading.RLock()
        self.watched = {}
        self.pending = []
        self.staged = None

    def usable(self):
        agent = self.agent()
        return (agent is not None and agent._session_db is self.db
                and getattr(agent, "_canonical_tool_commit_owner", None) is self
                and agent.session_id == self.scope["session_id"] and self.validate())

    def binding_current(self, frozen):
        if not self.usable():
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
        with self.lock:
            if len(self.watched) < MAX_WITNESSES and self.usable():
                self.watched[id(message)] = (message, pending)

    def stage(self, message):
        with self.lock:
            self.staged = None
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
                else:
                    pending.consume(witness)
                self.watched.pop(id(message), None)

    def prepare(self, pending, message, raw_digest):
        with self.lock:
            if (pending.owner is self and pending in self.pending
                    and self.binding_current(pending)):
                pending.prepare(message, raw_digest)

    def bind(self, ref):
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
            if any(p.uid == uid for p in self.pending):
                return None
            pending = PendingToolCommit(self, self.staged, uid, ref, args_digest)
            self.pending.append(pending)
            return pending


def owner_call(agent, method, *args, default=None):
    owner = getattr(agent, "_canonical_tool_commit_owner", None)
    if not isinstance(owner, ToolCommitOwner):
        return default
    try:
        return getattr(owner, method)(*args)
    except Exception:
        logger.warning("Canonical tool-row witness withheld", exc_info=True)
        return default
