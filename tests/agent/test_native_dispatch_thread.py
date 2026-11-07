"""Native dispatch stays on the assistant/save thread, not a deadline worker.

Exercises actual sequential/concurrent/segmented scheduling and middleware.
The native IO boundary is observed only; this does not grant App permission.
"""
import json
import threading
from types import SimpleNamespace

import pytest

from agent import native_receipt_origin, tool_executor
from tests.agent.test_sequential_tool_timeout import _make_agent

NAME = "gdb_native_01j00000000000000000000002"


@pytest.mark.parametrize("entry", ["sequential", "concurrent", "segmented"])
def test_native_dispatch_and_result_save_share_assistant_thread(tmp_path, monkeypatch, entry):
    agent = _make_agent(tmp_path)
    agent.valid_tool_names.add(NAME)
    original_thread = threading.current_thread()
    observed = []
    saved = []

    def native(agent_arg, ref, gate):
        observed.append((threading.current_thread(), agent_arg, ref.name))
        return json.dumps({"boundary": "observed"})

    def flush(*args, **kwargs):
        saved.append(threading.current_thread())
        return True

    def ordinary(*args, **kwargs):
        pytest.fail("Native name fell through to ordinary registry dispatch")

    monkeypatch.setattr(native_receipt_origin, "execute_native_call", native)
    monkeypatch.setattr("model_tools.handle_function_call", ordinary)
    agent._flush_messages_to_session_db = flush
    call = SimpleNamespace(id="native-thread-call", type="function",
                           function=SimpleNamespace(name=NAME, arguments="{}"))
    messages = []
    getattr(tool_executor, "execute_tool_calls_" + entry)(
        agent, SimpleNamespace(tool_calls=[call]), messages, "thread-test")

    assert observed == [(original_thread, agent, NAME)]
    assert saved and all(thread is original_thread for thread in saved)
    assert len(messages) == 1 and messages[0]["tool_call_id"] == call.id
    assert json.loads(messages[0]["content"]) == {"boundary": "observed"}


def test_ordinary_sequential_tool_still_runs_in_deadline_worker(tmp_path, monkeypatch):
    agent = _make_agent(tmp_path)
    original_thread = threading.current_thread()
    observed = []

    def ordinary(*args, **kwargs):
        observed.append(threading.current_thread())
        return "ordinary-boundary"

    monkeypatch.setattr("model_tools.handle_function_call", ordinary)
    call = SimpleNamespace(id="ordinary-thread-call", type="function",
                           function=SimpleNamespace(name="web_extract", arguments="{}"))
    messages = []
    tool_executor.execute_tool_calls_sequential(
        agent, SimpleNamespace(tool_calls=[call]), messages, "thread-test")
    assert len(observed) == 1 and observed[0] is not original_thread
    assert messages[0]["content"] == "ordinary-boundary"
