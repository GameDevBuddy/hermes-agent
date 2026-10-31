"""Failed original native requests cannot become viewer prompts; lower IO models only."""
import pytest

from gateway import session_server_requests as requests
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_constructor_fixture import constructor_graph, finish, pending_slot


ROUTES = ("waiting", "replay", "response", "withdraw", "answer")


def route(name, events, session, request_id, authorize):
    if name == "waiting":
        return requests.waiting(events, session, "native.invoke")
    if name == "replay":
        return requests.open_requests(events, session)
    if name == "response":
        return requests.resolve({"id": request_id, "result": {"value": "ordinary-reply"}},
                                session_ok=authorize)
    if name == "withdraw":
        return requests.withdraw(events, session, "native.invoke")
    return requests.lock_answer(request_id, "question", {"value": "ordinary-reply"}, session_ok=authorize)


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ROUTES)
async def test_native_request_failed_constructor_cannot_fall_back_to_viewer_route(monkeypatch, tmp_path, name):
    case = await constructor_graph(monkeypatch, tmp_path)
    try:
        monkeypatch.setattr(requests, "_open", {})
        slot = pending_slot(case)
        slot.prepare_source_declaration()
        initialize = requests._Pending.__init__

        def changed_metadata(request, *arguments, **keywords):
            initialize(request, *arguments, **keywords)
            request.native_slot = None
            request.replay = True
            request.qids = ("question",)

        monkeypatch.setattr(requests._Pending, "__init__", changed_metadata)
        with pytest.raises(RuntimeStoreError, match="native_original_source_request_changed") as caught:
            requests.retain_native_candidate(slot)
        request = slot._native_request_object
        construction = request._source_construction
        probes = []

        def authorize(session, events):
            probes.append((session, events))
            return True  # ordinary viewer model; never an App Origin

        result = route(name, case.live.event_stream, case.owner.scope["session_id"],
                       slot._native_request_candidate_id, authorize)
        expected = {"waiting": False, "replay": [], "response": False, "withdraw": 0, "answer": None}
        assert result == expected[name]
        assert probes == []
        assert requests._open[slot._native_request_candidate_id] is request
        assert request.result is None and request.locked == {} and not request.event.is_set()
        assert construction.error is caught.value and construction.phase == "blocked"
        assert request.native_phase == "blocked" and request.native_pair_credit == 2
        assert slot._native_pin is None and request.native_active_use is None
    finally:
        await finish(case)


@pytest.mark.parametrize("name", ROUTES)
def test_ordinary_request_viewer_route_keeps_existing_behavior(monkeypatch, name):
    events, session, request_id = object(), "ordinary-session", "ordinary-request"
    request = requests._Pending(session, qids=("question",), events=events,
                                method="native.invoke", params={"session_id": session})
    monkeypatch.setattr(requests, "_open", {request_id: request})
    result = route(name, events, session, request_id, lambda actual, stream: actual == session and stream is events)
    if name == "waiting":
        assert result is True and requests._open[request_id] is request
    elif name == "replay":
        assert result == [{"id": request_id, "method": "native.invoke", "params": {"session_id": session}}]
        assert not request.event.is_set()
    else:
        assert result == {"response": True, "withdraw": 1, "answer": []}[name]
        assert request_id not in requests._open and request.event.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ("response", "withdraw"))
async def test_native_request_displaced_flag_cannot_enter_viewer_route(monkeypatch, tmp_path, name):
    case = await constructor_graph(monkeypatch, tmp_path)
    try:
        monkeypatch.setattr(requests, "_open", {})
        slot = pending_slot(case)
        slot.prepare_source_declaration()
        initialize = requests._Pending.__init__

        def changed_replay(request, *arguments, **keywords):
            initialize(request, *arguments, **keywords)
            request.replay = True

        monkeypatch.setattr(requests._Pending, "__init__", changed_replay)
        with pytest.raises(RuntimeStoreError, match="native_original_source_request_changed") as caught:
            requests.retain_native_candidate(slot)
        request = slot._native_request_object
        construction = request._source_construction
        assert request.native_slot is slot  # The original constructor restores this after initialization.
        request.native_slot = None  # Explicit metadata displacement; no claim that a real native caller did this.
        result = route(name, case.live.event_stream, case.owner.scope["session_id"],
                       slot._native_request_candidate_id, lambda session, events: True)
        assert result == {"response": False, "withdraw": 0}[name]
        assert requests._open[slot._native_request_candidate_id] is request
        assert not request.event.is_set() and request.result is None
        assert construction.error is caught.value and construction.phase == "blocked"
        assert construction.keywords["_source_native_slot"] is slot
    finally:
        await finish(case)
