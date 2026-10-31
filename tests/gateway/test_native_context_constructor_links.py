"""Constructor source custody/default ordering over original context-only consumers; no native rights."""
import sys
from copy import copy

import pytest

import hermes_owned_control as control
from gateway import native_app_context as native
from gateway import native_app_context_records as records
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_context_constructor_fixture import context_case, original_self, attempt_for, NONCE, REQUEST


@pytest.mark.parametrize("kind", ["stage", "request", "association"])
@pytest.mark.parametrize("allocation", ["opaque", "raised"])
def test_context_allocator_returns_are_observed_without_initialization_or_retry(monkeypatch, tmp_path, kind, allocation):
    case = context_case(monkeypatch, tmp_path)
    cls = {"stage": records.ContextStage, "request": records.ContextRequest,
           "association": records.OriginalAppContext}[kind]
    opaque, error, calls = object(), RuntimeError("context no-self allocator"), []

    def allocate(actual, *args):
        calls.append(actual)
        if allocation == "raised":
            raise error
        return opaque

    try:
        if kind != "stage":
            case.stage()
        with monkeypatch.context() as patch:
            patch.setattr(cls, "__new__", staticmethod(allocate))
            if kind == "stage" and allocation == "opaque":
                assert "error" in case.stage()
            else:
                with pytest.raises(BaseException):
                    case.stage() if kind == "stage" else case.read()
        attempt = attempt_for(case, kind)
        assert attempt.value is None and attempt.phase == "failed" and len(calls) == 1
        assert attempt.observed_result is (opaque if allocation == "opaque" else None)
        assert attempt.error is not None and case.source._app_context_error is attempt.error
        credit = case.source._app_context_credit
        if kind == "stage":
            assert "error" in case.stage()
        else:
            with pytest.raises(BaseException):
                case.read()
        assert len(calls) == 1 and case.source._app_context_credit == credit
    finally:
        case.close()


@pytest.mark.parametrize("kind", ["stage", "request", "association"])
def test_original_initializer_assignment_failure_is_held_before_error_callbacks(monkeypatch, tmp_path, kind):
    case = context_case(monkeypatch, tmp_path)
    cls = {"stage": records.ContextStage, "request": records.ContextRequest,
           "association": records.OriginalAppContext}[kind]
    name = {"stage": "contract", "request": "source", "association": "selection"}[kind]
    original, error = cls.__setattr__, RuntimeError("real context assignment model")
    initializer = cls.__init__

    def assign(self, field, value):
        if field == name and sys._getframe(1).f_code is initializer.__code__:
            raise error
        original(self, field, value)

    try:
        if kind != "stage":
            case.stage()
        with monkeypatch.context() as patch:
            patch.setattr(cls, "__setattr__", assign)
            with pytest.raises(BaseException):
                case.stage() if kind == "stage" else case.read()
        attempt = attempt_for(case, kind)
        assert attempt.value is original_self(error, initializer)
        assert attempt.error is error and attempt.value.error is error
        assert attempt.traceback is not None and case.source._app_context_error is error
        with pytest.raises(RuntimeStoreError):
            if kind == "association":
                attempt.value.require_current(case.connection, case.ref)
            else:
                attempt.value.require_current()
    finally:
        case.close()


def test_reader_active_publication_failure_retains_partial_without_consuming_nonce(monkeypatch, tmp_path):
    case = context_case(monkeypatch, tmp_path)
    setter, error = control.OriginalControlReader.__setattr__, RuntimeError("original reader active publication")

    def publish(self, name, value):
        if name == "active" and type(value) is records.ContextRequest:
            raise error
        setter(self, name, value)

    try:
        case.stage()
        with monkeypatch.context() as patch:
            patch.setattr(control.OriginalControlReader, "__setattr__", publish)
            with pytest.raises(BaseException):
                case.read()
        attempt = attempt_for(case, "request")
        stage = case.source._app_context_stages[NONCE]
        assert attempt.error is error and attempt.value.error is error and attempt.value._context_initialized
        assert stage.request is None and attempt.value.stage is None
        assert case.source._app_context_requests[REQUEST] is attempt.value
        assert not case.submissions and not case.connection._gdb_original_app_contexts
    finally:
        case.close()


def test_association_publication_failure_keeps_consumed_original_nonce_and_no_ack(monkeypatch, tmp_path):
    case = context_case(monkeypatch, tmp_path)
    error = RuntimeError("original association book publication")

    class Book(dict):
        def __setitem__(self, key, value):
            raise error

    try:
        case.stage()
        case.connection._gdb_original_app_contexts = Book()
        with pytest.raises(BaseException):
            case.read()
        attempt, request = attempt_for(case, "association"), case.source._app_context_requests[REQUEST]
        assert attempt.error is error and attempt.value is request.association
        assert request.stage.request is request and request.error is error
        assert not request.acknowledged and not case.connection._gdb_original_app_contexts
    finally:
        case.close()


@pytest.mark.parametrize("kind", ["request", "association"])
def test_recursive_original_reader_claim_refuses_before_new_credit(monkeypatch, tmp_path, kind):
    case = context_case(monkeypatch, tmp_path)
    observed = []
    original_get = control.OriginalControlReader.__getattribute__
    original_current = records.ContextRequest.require_current
    initializer = records.ContextRequest.__init__ if kind == "request" else records.OriginalAppContext.__init__

    def recursive(reader):
        before = case.source._app_context_credit
        with pytest.raises(RuntimeStoreError):
            native.claim_from_reader(reader, case.frame(requestId="01J00000000000000000000009"))
        observed.append(case.source._app_context_credit == before)

    def read(self, name):
        if name == "source" and sys._getframe(1).f_code is initializer.__code__:
            recursive(self)
        return original_get(self, name)

    def current(self):
        if sys._getframe(1).f_code is initializer.__code__:
            recursive(self.reader)
        return original_current(self)

    try:
        case.stage()
        with monkeypatch.context() as patch:
            patch.setattr(control.OriginalControlReader, "__getattribute__", read)
            patch.setattr(records.ContextRequest, "require_current", current)
            case.read()
        assert observed == [True] and case.source._app_context_requests[REQUEST].acknowledged
        assert len(case.source._app_context_requests) == 1
    finally:
        case.close()


def test_source_unknown_callback_cannot_replace_original_constructor_first_error(monkeypatch, tmp_path):
    case = context_case(monkeypatch, tmp_path)
    first, secondary = RuntimeError("original generation getter"), RuntimeError("unknown callback")

    def get_session(session):
        if sys._getframe(1).f_code is records.generation.__code__:
            raise first
        return case.row if session == "s" else None

    def unknown(reason):
        assert case.source._app_context_error is first
        case.source._app_context_constructions = {}
        raise secondary

    try:
        with monkeypatch.context() as patch:
            patch.setattr(case.db, "get_session", get_session)
            patch.setattr(case.source, "unknown", unknown)
            with pytest.raises(RuntimeError) as caught:
                case.stage()
        value = original_self(first, records.ContextStage.__init__)
        attempt = value._context_construction
        assert caught.value is secondary and attempt.error is first and value.error is first
        assert attempt.traceback is not None and case.source._app_context_error is first
    finally:
        case.close()


@pytest.mark.parametrize("kind", ["stage", "request", "association"])
def test_changed_allocator_cannot_reinitialize_a_preowned_original_context(monkeypatch, tmp_path, kind):
    original_case = context_case(monkeypatch, tmp_path / "first")
    original_case.stage()
    original_case.read()
    value = (original_case.source._app_context_stages[NONCE] if kind == "stage"
             else original_case.source._app_context_requests[REQUEST])
    if kind == "association":
        value = value.association
    fields, construction, cls = vars(value).copy(), value._context_construction, type(value)
    case = context_case(monkeypatch, tmp_path / "second")

    def allocate(actual, *args):
        return value

    try:
        if kind != "stage":
            case.stage()
        with monkeypatch.context() as patch:
            patch.setattr(cls, "__new__", staticmethod(allocate))
            if kind == "stage":
                assert "error" in case.stage()
            else:
                with pytest.raises(BaseException):
                    case.read()
        attempt = attempt_for(case, kind)
        assert attempt.value is None and attempt.observed_result is value
        assert vars(value) == fields and value._context_construction is construction
        assert construction.phase == "ready" and construction.error is None
    finally:
        case.close()
        original_case.close()


@pytest.mark.parametrize("kind", ["stage", "request", "association"])
def test_original_initializer_cannot_substitute_a_transient_source_or_stage(monkeypatch, tmp_path, kind):
    case = context_case(monkeypatch, tmp_path)
    cls = {"stage": records.ContextStage, "request": control.OriginalControlReader,
           "association": records.ContextRequest}[kind]
    initializer = {"stage": records.ContextStage.__init__, "request": records.ContextRequest.__init__,
                   "association": records.OriginalAppContext.__init__}[kind]
    setter, getter = cls.__setattr__, cls.__getattribute__
    foreign = object()

    def assign(self, name, value):
        setter(self, name, value)
        if name == "_context_initialized" and value is True:
            setter(self, "source", foreign)

    def read(self, name):
        expected = "source" if kind == "request" else "stage"
        if name == expected and sys._getframe(1).f_code is initializer.__code__:
            return foreign
        return getter(self, name)

    try:
        if kind != "stage":
            case.stage()
        with monkeypatch.context() as patch:
            patch.setattr(cls, "__setattr__" if kind == "stage" else "__getattribute__",
                          assign if kind == "stage" else read)
            if kind == "stage":
                assert "error" in case.stage()
            else:
                with pytest.raises(BaseException):
                    case.read()
        attempt = attempt_for(case, kind)
        assert attempt.value is not None and attempt.phase == "failed"
        assert case.source._app_context_error is attempt.error and attempt.value.error is attempt.error
        assert attempt.value._context_initialized is True
    finally:
        case.close()


def test_reader_publication_cannot_consume_an_equal_copied_stage_instead_of_original(monkeypatch, tmp_path):
    case = context_case(monkeypatch, tmp_path)
    setter, replacements = control.OriginalControlReader.__setattr__, []

    def publish(self, name, value):
        if name == "active" and type(value) is records.ContextRequest:
            original = case.source._app_context_stages[NONCE]
            replacement = copy(original)
            assert replacement is not original and vars(replacement) == vars(original)
            case.source._app_context_stages[NONCE] = replacement
            replacements.append((original, replacement))
        setter(self, name, value)

    try:
        case.stage()
        with monkeypatch.context() as patch:
            patch.setattr(control.OriginalControlReader, "__setattr__", publish)
            with pytest.raises(BaseException):
                case.read()
        attempt = attempt_for(case, "request")
        original, replacement = replacements[0]
        assert attempt.stage is original and attempt.value.stage is replacement
        assert attempt.phase == "failed" and attempt.error is not None
        assert not case.submissions and not case.connection._gdb_original_app_contexts
    finally:
        case.close()
