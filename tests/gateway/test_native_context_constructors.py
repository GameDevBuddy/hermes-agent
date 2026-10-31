"""Real Stage/Request/Association initializer failures through original consumers; no new authority."""
import sys

import pytest

import hermes_owned_control as control
from gateway import native_app_context as native
from gateway import native_app_context_records as records
from gateway import native_app_context_construction as construction
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_context_constructor_fixture import context_case, original_self, attempt_for, REQUEST


@pytest.mark.parametrize("kind", ["stage", "request", "association"])
def test_original_context_initializer_self_and_first_error_are_retained(monkeypatch, tmp_path, kind):
    case = context_case(monkeypatch, tmp_path)
    error = RuntimeError("original context " + kind + " lower failure")
    initializer = {"stage": records.ContextStage.__init__, "request": records.ContextRequest.__init__,
                   "association": records.OriginalAppContext.__init__}[kind]
    original_read = control.OriginalControlReader.__getattribute__
    original_current = records.ContextRequest.require_current

    def read(self, name):
        if name == "source" and sys._getframe(1).f_code is initializer.__code__:
            raise error
        return original_read(self, name)

    def get_session(session):
        if sys._getframe(1).f_code is records.generation.__code__:
            raise error
        return case.row if session == "s" else None

    def require_current(self):
        if sys._getframe(1).f_code is initializer.__code__:
            raise error
        return original_current(self)

    try:
        if kind != "stage":
            case.stage()
        with monkeypatch.context() as patch:
            if kind == "stage":
                patch.setattr(case.db, "get_session", get_session)
            elif kind == "request":
                patch.setattr(control.OriginalControlReader, "__getattribute__", read)
            else:
                patch.setattr(records.ContextRequest, "require_current", require_current)
            with pytest.raises(BaseException):
                case.stage() if kind == "stage" else case.read()
        value = original_self(error, initializer)
        attempt = attempt_for(case, kind)
        assert attempt.value is value and attempt.error is error and attempt.traceback is not None
        assert value.error is error and case.source._app_context_error is error
        assert attempt.credit >= 0 and attempt.phase == "failed"
        assert all(event["event"] != "native_context_bound" for event in case.events())
    finally:
        case.close()


@pytest.mark.parametrize("kind", ["request", "association"])
def test_metadata_getter_reentry_cannot_spend_another_request_credit(monkeypatch, tmp_path, kind):
    case = context_case(monkeypatch, tmp_path)
    entered, observed = [], []
    reader_get = control.OriginalControlReader.__getattribute__
    request_get = records.ContextRequest.__getattribute__

    def recursive(reader):
        entered.append(kind)
        before = case.source._app_context_credit
        with pytest.raises(RuntimeStoreError):
            native.claim_from_reader(reader, case.frame(requestId="01J00000000000000000000009"))
        observed.append(case.source._app_context_credit == before)

    def read(self, name):
        if (kind == "request" and name == "source" and not entered
                and sys._getframe(1).f_code is construction._record_refs.__code__):
            recursive(self)
        return reader_get(self, name)

    def request_read(self, name):
        if (kind == "association" and name == "stage" and not entered
                and sys._getframe(1).f_code is construction._record_refs.__code__):
            recursive(request_get(self, "reader"))
        return request_get(self, name)

    try:
        case.stage()
        with monkeypatch.context() as patch:
            patch.setattr(control.OriginalControlReader, "__getattribute__", read)
            patch.setattr(records.ContextRequest, "__getattribute__", request_read)
            case.read()
        assert entered == [kind] and observed == [True]
        assert len(case.source._app_context_requests) == 1
        assert case.source._app_context_requests[REQUEST].acknowledged
    finally:
        case.close()
