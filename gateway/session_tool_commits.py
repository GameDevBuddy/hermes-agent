"""Install original-call commit observation on the actual canonical authority's adopted agent."""
from __future__ import annotations

import weakref
import logging

from agent.tool_commit_owner import ToolCommitOwner
from gateway.session_authorities import require_serving
from gateway.session_contract import SessionRef
from hermes_state_runtime import RuntimeStoreError


def install_tool_commit_owner(authority, session_id, generation, agent):
    agent._canonical_tool_commit_owner = None
    try:
        agent._canonical_tool_commit_owner = _make_owner(authority, session_id, generation, agent)
    except Exception:
        logging.getLogger(__name__).warning("Canonical tool commit observer not installed", exc_info=True)


def _make_owner(authority, session_id, generation, agent):
    live = authority.sessions.get(session_id)
    db = getattr(agent, "_session_db", None)
    if live is None or db is not authority.db or agent.session_id != session_id:
        return None
    handle = authority._handle(SessionRef(authority.profile_id, session_id))
    scope = {
        "profile_id": authority.profile_id, "session_id": session_id,
        "instance_id": authority.instance_id, "epoch": authority.epoch,
        "generation": generation, "revision": handle.revision,
        "admission_id": live.event_stream.execution.get("admission_id"),
    }
    agent_ref = weakref.ref(agent)

    def validate():
        current = agent_ref()
        require_serving(authority)
        if (current is None or authority.sessions.get(session_id) is not live
                or authority.adopted.get(session_id) != (generation, current)
                or authority.executing.get(session_id) != generation
                or authority.db is not db or current._session_db is not db
                or current.session_id != session_id
                or authority.profile_id != scope["profile_id"]
                or authority.instance_id != scope["instance_id"] or authority.epoch != scope["epoch"]
                or not scope["admission_id"]
                or live.event_stream.execution.get("admission_id") != scope["admission_id"]
                or live.event_stream.execution.get("authority_epoch") != scope["epoch"]
                or live.event_stream.execution.get("execution_generation") != generation):
            return False
        authority.check_approval_generation(session_id, generation)
        if authority._handle(SessionRef(scope["profile_id"], session_id)).revision != scope["revision"]:
            return False
        with db._read_ctx() as conn:
            row = conn.execute(
                "SELECT epoch, instance_id FROM runtime_epoch WHERE singleton=1"
            ).fetchone()
        return row is not None and row["epoch"] == scope["epoch"] and row["instance_id"] == scope["instance_id"]

    def publish(pending, receipt):
        with live.event_stream.lock:
            try:
                if pending.owner is not owner or not owner.binding_current(pending):
                    return False
            except RuntimeStoreError:
                return False
            return authority.publish_execution(
                session_id, generation, "tool.canonical_committed", receipt)

    owner = ToolCommitOwner(agent, scope, validate, publish)
    return owner
