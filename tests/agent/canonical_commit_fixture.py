"""Synthetic call bodies through the real authority, persistence mixin and owned SQLite DB.

This fixture starts no gateway, listener, provider, native reader, model or tool process.
It does not establish transport authentication, native delivery or process-restart rights.
"""
from types import SimpleNamespace

from agent.session_persistence import SessionPersistenceMixin
from agent.tool_commit_rows import text_digest
from agent.tool_executor import _ToolCallRef, _begin_tool_execution
from gateway.session_authority import LiveSession, SessionAuthority
from hermes_state import SessionDB
from hermes_state_runtime import admit_session_input, begin_runtime_epoch, claim_session_input

SESSION = "canonical-source-fixture"
CALL = "fixture-call"
TOOL = "fixture_echo"


class CommitFixture(SessionPersistenceMixin):
    def __init__(self, tmp_path):
        self._session_db = SessionDB(tmp_path / "source-state.db")
        self._session_db.create_session(SESSION, source="test")
        self.session_id = SESSION
        self._session_db_created = True
        self._active_session_turn_lease_holder = "fixture-original-turn"
        self._active_compression_lock_holder = None
        self._active_session_turn_lease_ttl_seconds = 300
        epoch = begin_runtime_epoch(self._session_db, instance_id="fixture-instance")
        admit_session_input(
            self._session_db, epoch=epoch, principal_id="fixture-human",
            session_id=SESSION, request_id="fixture-request", payload={"text": "synthetic"})
        self.claim = claim_session_input(self._session_db, epoch=epoch, session_id=SESSION)
        assert self._session_db.acquire_session_turn_lease(
            SESSION, self._active_session_turn_lease_holder, wait_seconds=0)
        runner = SimpleNamespace(_draining=False, config=SimpleNamespace(multiplex_profiles=False))
        self.authority = SessionAuthority(
            runner, profile_id="fixture-profile", instance_id="fixture-instance",
            db=self._session_db, epoch=epoch)
        self.live = LiveSession(SimpleNamespace(platform=None), "fixture-route")
        self.authority.sessions[SESSION] = self.live
        self.live.event_stream.execution = {
            "authority_epoch": epoch, "execution_generation": self.claim["generation"],
            "admission_id": self.claim["admission_id"]}
        self.authority.executing[SESSION] = self.claim["generation"]
        self.frames = []
        self.live.event_stream.observers.add(self.frames.append)
        self.authority.adopt_agent(SESSION, self.claim["generation"], self)
        self.owner = self._canonical_tool_commit_owner
        assert self.owner is not None and self.owner.usable()
        self.messages = []
        self.quiet_mode = True
        self.verbose_logging = False
        self.tool_progress_callback = None
        self.tool_start_callback = None
        self.tool_complete_callback = None
        self._checkpoint_mgr = SimpleNamespace(enabled=False)
        self._tool_guardrails = SimpleNamespace(
            before_call=lambda *args: SimpleNamespace(allows_execution=True))
        self._subdirectory_hints = SimpleNamespace(check_tool_call=lambda *args: "")
        self._incremental_persistence_failed = False

    def _touch_activity(self, *args):
        pass

    def _tool_result_content_for_active_model(self, name, content):
        return "canonical projection: " + content

    def _record_file_mutation_result(self, *args, **kwargs):
        pass

    def _append_guardrail_observation(self, name, args, content, **kwargs):
        return content

    def _invoke_tool(self, name, args, *unused, **kwargs):
        return "synthetic dispatch result"

    def flush(self):
        return self._flush_messages_to_session_db(self.messages)

    def stage(self, calls=None):
        message = {
            "role": "assistant", "content": "synthetic request",
            "tool_calls": calls if calls is not None else [
                {"id": CALL, "type": "function",
                 "function": {"name": TOOL, "arguments": '{"original":1}'}}]}
        self.messages.append(message)
        self.owner.stage(message)
        assert self.flush() is True
        return message

    def begin(self, args=None, call_id=CALL, name=TOOL):
        ref = _ToolCallRef(name, args if args is not None else {"effective": 2},
                           SESSION, call_id, [])
        _begin_tool_execution(self, ref, None)
        return ref

    def prepare(self, ref, content="synthetic persisted body", raw="synthetic raw body"):
        message = {"role": "tool", "name": ref.name, "tool_call_id": ref.call_id,
                   "content": content}
        self.owner.prepare(ref.canonical_commit, message, text_digest(raw))
        self.messages.append(message)
        return message

    def canonical_events(self):
        return [frame["params"]["payload"] for frame in self.frames
                if frame["params"]["type"] == "tool.canonical_committed"]

    def rows(self):
        with self._session_db._read_ctx() as conn:
            return [dict(row) for row in conn.execute(
                "SELECT * FROM messages WHERE session_id=? ORDER BY id", (SESSION,))]

    def write_sql(self, *statements):
        def write(conn):
            for sql, params in statements:
                conn.execute(sql, params)
        self._session_db._execute_write(write)

    def close(self):
        self._session_db.close()
