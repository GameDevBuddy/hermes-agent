"""Source integration of original pending calls with the actual canonical writer.

All tool bodies and the extension-policy boundary are synthetic. No native or model call runs.
"""
from types import SimpleNamespace

import pytest

from agent import tool_executor
from agent.tool_commit_owner import owner_call
from agent.tool_commit_rows import json_digest, text_digest
from agent.tool_executor import _ToolCallRef, _commit_tool_result
from hermes_state_runtime import begin_runtime_epoch
from tests.agent.canonical_commit_fixture import CALL, SESSION, TOOL, CommitFixture
from tools.budget_config import DEFAULT_BUDGET


@pytest.fixture
def live(tmp_path):
    fixture = CommitFixture(tmp_path)
    try:
        yield fixture
    finally:
        fixture.close()


def test_actual_result_commit_publishes_bound_row_after_success(live):
    assistant = live.stage()
    ref = live.begin()
    raw = "synthetic original result"
    committed = _commit_tool_result(
        live, live.messages, ref, raw, budget=DEFAULT_BUDGET, tool_duration=0.0,
        is_error=False, blocked=False, effect_disposition=None)
    assert committed is not None
    rows = live.rows()
    receipt, = live.canonical_events()
    assert receipt["assistant_message_uid"] == assistant["message_uid"]
    assert receipt["assistant_row_id"] == rows[0]["id"]
    assert receipt["tool_result_message_uid"] == rows[1]["message_uid"]
    assert receipt["tool_result_row_id"] == rows[1]["id"]
    assert receipt["tool_call_uid"] == rows[1]["tool_call_uid"]
    assert receipt["provider_call_id"] == CALL
    assert receipt["tool_name"] == TOOL
    assert receipt["source_result_utf8"] == text_digest(raw)
    assert receipt["canonical_projection_utf8"] == text_digest(rows[1]["content"])
    assert receipt["source_result_utf8"] != receipt["canonical_projection_utf8"]
    assert receipt["effective_args_digest"] == json_digest({"effective": 2})
    assert receipt["profile_id"] == "fixture-profile"
    assert receipt["session_id"] == SESSION
    assert receipt["instance_id"] == "fixture-instance"
    assert receipt["generation"] == live.claim["generation"]
    assert receipt["epoch"] == live.authority.epoch
    assert receipt["native_ack_allowed"] is False
    assert receipt["replay_allowed"] is False
    assert not live._session_db._conn.in_transaction


def test_repeat_flush_and_reused_original_ref_do_not_publish_twice(live):
    live.stage()
    ref = live.begin()
    live.prepare(ref)
    assert live.flush() is True
    assert live.flush() is True
    assert len(live.rows()) == 2
    assert len(live.canonical_events()) == 1
    assert live.owner.bind(ref) is None


@pytest.mark.parametrize("field", ["profile", "session", "epoch", "generation", "revision",
                                  "admission", "retired", "installed-owner", "turn-holder"])
def test_replaced_live_binding_withholds_observation(live, field):
    live.stage()
    ref = live.begin()
    message = live.prepare(ref)
    authority = live.authority
    mutations = {
        "profile": lambda: setattr(authority, "profile_id", "different-profile"),
        "session": lambda: setattr(live, "session_id", "different-session"),
        "epoch": lambda: begin_runtime_epoch(live._session_db, instance_id="successor-instance"),
        "generation": lambda: authority.executing.__setitem__(
            SESSION, authority.executing[SESSION] + 1),
        "revision": lambda: live.write_sql((
            "UPDATE sessions SET runtime_revision=runtime_revision+1 WHERE id=?", (SESSION,))),
        "admission": lambda: live.live.event_stream.execution.__setitem__(
            "admission_id", "different-admission"),
        "retired": lambda: setattr(authority, "retired", True),
        "installed-owner": lambda: authority.adopt_agent(SESSION, live.claim["generation"], live),
        "turn-holder": lambda: setattr(live, "_active_session_turn_lease_holder", "different-holder"),
    }
    mutations[field]()
    assert owner_call(live, "indices", [message], default=()) == ()
    assert not live.canonical_events()
    assert ref.canonical_commit.receipt is None


def test_replacement_between_consume_and_publisher_withholds_old_event(live):
    live.stage()
    ref = live.begin()
    live.prepare(ref)
    original_publish = live.owner.publish

    def replace_then_publish(pending, receipt):
        live.authority.adopt_agent(SESSION, live.claim["generation"], live)
        return original_publish(pending, receipt)

    live.owner.publish = replace_then_publish
    assert live.flush() is True
    assert len(live.rows()) == 2
    assert ref.canonical_commit.receipt is not None  # the real body did commit
    assert ref.canonical_commit.publication_observed is False
    assert not live.canonical_events()


@pytest.mark.parametrize("kind", ["disabled", "no-db"])
def test_generic_flush_none_does_not_mint_canonical_receipt(live, kind):
    db = live._session_db
    live.stage()
    ref = live.begin()
    assert ref.canonical_commit is not None
    message = live.prepare(ref)
    pending = ref.canonical_commit
    assert pending.receipt is None and pending.message is message
    if kind == "disabled":
        live._persist_disabled = True
    else:
        live._session_db = None
    try:
        assert live.flush() is None
        assert pending.receipt is None and not pending.publication_observed
        assert not live.canonical_events()
        with db._read_ctx() as conn:
            rows = conn.execute("SELECT role FROM messages ORDER BY id").fetchall()
        assert [row["role"] for row in rows] == ["assistant"]
        assert "_row_id" not in message
    finally:
        live._session_db = db


@pytest.mark.parametrize("wrong", ["name", "call-id", "duplicate-call-id"])
def test_only_unique_original_call_can_bind(live, wrong):
    if wrong == "duplicate-call-id":
        call = {"id": CALL, "type": "function",
                "function": {"name": TOOL, "arguments": "{}"}}
        live.stage([call, dict(call)])
    else:
        live.stage()
    ref = live.begin(name="different-tool" if wrong == "name" else TOOL,
                     call_id="different-call" if wrong == "call-id" else CALL)
    assert ref.canonical_commit is None
    assert not live.canonical_events()


@pytest.mark.parametrize("digest", ["calls_digest", "call_uids_digest"])
def test_missing_parent_digest_cannot_match_itself_as_none(live, digest):
    live.stage()
    live.owner.staged[digest] = None
    assert live.begin().canonical_commit is None


def test_new_assistant_occurrence_can_reuse_provider_id_with_a_new_uid(live):
    live.stage()
    first = live.begin()
    live.prepare(first)
    assert live.flush() is True
    live.stage()
    second = live.begin()
    assert second.canonical_commit is not None
    assert first.canonical_commit.uid != second.canonical_commit.uid
    live.prepare(second, content="second synthetic body")
    assert live.flush() is True
    assert len(live.canonical_events()) == 2


def test_original_same_holder_expiry_is_renewed_by_the_actual_writer(live):
    live.stage()
    live.write_sql(("UPDATE session_turn_leases SET expires_at=0", ()))
    ref = live.begin()
    assert ref.canonical_commit is not None
    live.prepare(ref)
    assert live.flush() is True
    assert len(live.canonical_events()) == 1


def test_replacing_durable_parent_definitions_withholds_receipt(live):
    live.stage()
    ref = live.begin()
    live.prepare(ref)
    live.write_sql(("UPDATE messages SET tool_call_uids=NULL WHERE role='assistant'", ()))
    assert live.flush() is True
    assert len(live.rows()) == 2
    assert not live.canonical_events()
    assert ref.canonical_commit.receipt is None


def test_dispatch_inner_ref_survives_concurrent_outcome(live, monkeypatch):
    live.stage()
    monkeypatch.setattr(tool_executor, "_pre_tool_block",
                        lambda agent, ref: (None, {"effective": "rewritten"}))
    outer = _ToolCallRef(TOOL, {"original": 1}, SESSION, CALL, [])
    batch = tool_executor._ConcurrentBatch(live, live.messages, SESSION, [], None)
    gate = SimpleNamespace(advance=lambda callback=None: callback() if callback else None)
    outcome = batch._dispatch_worker(0, outer, None, gate)
    assert outcome is not None and not outcome.blocked and not outcome.is_error
    assert outcome.ref is not outer
    assert outcome.ref.canonical_commit is not None
    assert outcome.ref.args == {"effective": "rewritten"}
    live.prepare(outcome.ref, raw=outcome.result)
    assert live.flush() is True
    receipt, = live.canonical_events()
    assert receipt["effective_args_digest"] == json_digest({"effective": "rewritten"})


def test_dispatch_inner_ref_survives_sequential_consumer(live, monkeypatch):
    live.stage()
    monkeypatch.setattr(tool_executor, "_pre_tool_block",
                        lambda agent, ref: (None, {"effective": "rewritten"}))
    managed = tool_executor._run_agent_tool_execution_middleware(
        live, function_name=TOOL, function_args={"original": 1},
        effective_task_id=SESSION, tool_call_id=CALL,
        execute=lambda args: "synthetic sequential result")
    assert managed.dispatch_ref is not None and managed.dispatch_ref.canonical_commit is not None
    outer = _ToolCallRef(TOOL, {"original": 1}, SESSION, CALL, [])
    assert tool_executor._publish_sequential_result(
        live, live.messages, outer, managed, tool_duration=0.0, index=1,
        budget=DEFAULT_BUDGET, transform_applied=True)
    receipt, = live.canonical_events()
    assert receipt["effective_args_digest"] == json_digest({"effective": "rewritten"})
    assert receipt["source_result_utf8"] == text_digest("synthetic sequential result")


def test_runtime_only_dispatch_keeps_default_outer_ref_path(live, monkeypatch):
    live._canonical_tool_commit_owner = None
    monkeypatch.setattr(tool_executor, "_pre_tool_block", lambda agent, ref: (None, ref.args))
    managed = tool_executor._run_agent_tool_execution_middleware(
        live, function_name=TOOL, function_args={"synthetic": True},
        effective_task_id=SESSION, tool_call_id=CALL, execute=lambda args: "synthetic")
    assert managed.dispatched and managed.dispatch_ref is None
    assert not live.canonical_events()


def test_actual_tool_round_stages_and_commits_original_call_before_dispatch(live, monkeypatch):
    from agent import prepared_compaction, turn_tool_round

    class ReachedSyntheticDispatch(Exception):
        pass

    call = SimpleNamespace(id=CALL, type="function",
                           function=SimpleNamespace(name=TOOL, arguments="{}"))
    assistant = SimpleNamespace(tool_calls=[call], content="synthetic", provider_data=None)
    calls = [{"id": CALL, "type": "function",
              "function": {"name": TOOL, "arguments": "{}"}}]
    monkeypatch.setattr(turn_tool_round, "validate_tool_calls", lambda *args, **kwargs:
                        SimpleNamespace(action="execute", mixed_invalid_batch=False))
    monkeypatch.setattr(prepared_compaction, "maybe_prepare", lambda *args, **kwargs: None)
    live._deduplicate_tool_calls = lambda value: value
    live._cap_delegate_task_calls = lambda value: value
    live._build_assistant_message = lambda *args: {
        "role": "assistant", "content": "synthetic", "tool_calls": calls}
    live._has_content_after_think_block = lambda value: True
    live._should_emit_quiet_tool_messages = lambda: False
    live._interim_assistant_visible_text = lambda message: ""
    live._emit_interim_assistant_message = lambda message: None
    live.stream_delta_callback = None
    observed = []

    def synthetic_dispatch(*args):
        rows = live.rows()
        assert len(rows) == 1 and rows[0]["role"] == "assistant"
        assert live.owner.staged is not None
        assert live.owner.staged["row_id"] == rows[0]["id"]
        ref = live.begin()
        assert ref.canonical_commit is not None
        observed.append(ref)
        raise ReachedSyntheticDispatch()

    live._execute_tool_calls = synthetic_dispatch
    with pytest.raises(ReachedSyntheticDispatch):
        turn_tool_round.run_tool_round(
            live, assistant_message=assistant, finish_reason="tool_calls",
            messages=live.messages, conversation_history=[], api_call_count=1,
            effective_task_id=SESSION, user_message="synthetic", system_message={},
            active_system_prompt="", compression_attempts=0, max_compression_attempts=1,
            final_response="", failed=False, _turn_exit_reason="",
            truncated_tool_call_retries=0, current_turn_user_idx=None)
    assert len(observed) == 1
    assert not live.canonical_events()
