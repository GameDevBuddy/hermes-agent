"""The original private reader keeps its first failure when later work refuses.

Actual retained source/reader entry points over the existing in-memory context
fixture. No socket, native operation, real Job, or product process is created.
"""
import pytest

import hermes_owned_control as control
from hermes_owned_gateway import OwnedGatewayUnknown
from tests.gateway.native_app_context_fixture import Case


def caused_by(error, original):
    while error is not None:
        if error is original:
            return True
        error = error.__cause__
    return False


@pytest.mark.parametrize("phase", ["before_source", "after_source", "original_read"])
def test_original_control_retry_retains_first_actual_failure(monkeypatch, tmp_path, phase):
    case = Case(monkeypatch, tmp_path)
    first = RuntimeError("first original control failure")
    attempted = []
    initializer = control.OriginalControlReader.__init__
    try:
        if phase == "original_read":
            def read_failure():
                attempted.append(phase)
                raise first
            case.input.hook = read_failure
        else:
            def fail_initialization(reader, *arguments, **keywords):
                attempted.append(reader)
                if phase == "after_source":
                    initializer(reader, *arguments, **keywords)
                raise first
            monkeypatch.setattr(control.OriginalControlReader, "__init__", fail_initialization)
        with pytest.raises(OwnedGatewayUnknown) as initial:
            case.source.read_shutdown()
        assert caused_by(initial.value, first)
        original_reader = case.source._control_reader
        with pytest.raises(OwnedGatewayUnknown) as subsequent:
            case.source.read_shutdown()
        assert caused_by(subsequent.value, first)
        assert case.source._control_reader is original_reader
        assert len(attempted) == 1
        assert case.source._shutdown is False and case.source._control_closed is False
    finally:
        case.close()

def test_original_control_retry_keeps_the_first_traceback(monkeypatch, tmp_path):
    case = Case(monkeypatch, tmp_path)
    first = RuntimeError("retained original read traceback")
    try:
        def fail_read():
            raise first
        case.input.hook = fail_read
        with pytest.raises(OwnedGatewayUnknown):
            case.source.read_shutdown()
        trace = first.__traceback__
        assert case.source._control_error is first
        assert case.source._control_traceback is trace
        with pytest.raises(OwnedGatewayUnknown):
            case.source.read_shutdown()
        assert case.source._control_error is first
        assert case.source._control_traceback is trace
        assert first.__traceback__ is trace
    finally:
        case.close()


def test_original_control_retry_healthy_half_close_has_no_failure(monkeypatch, tmp_path):
    case = Case(monkeypatch, tmp_path)
    try:
        case.read(frames=b"")
        assert case.source._shutdown is True and case.source._control_closed is True
        assert case.source._control_error is None and case.source._control_traceback is None
        assert case.source._control_reader is not None
    finally:
        case.close()
