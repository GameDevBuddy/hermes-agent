"""Original context-only handshake case; inherited lower Job/IO/thread identity models, no native rights."""
from tests.gateway.native_app_context_fixture import Case, NONCE, REQUEST

RETAINED = []


def context_case(monkeypatch, directory):
    value = Case(monkeypatch, directory)
    RETAINED.append(value)
    return value


def original_self(error, initializer):
    trace = error.__traceback__
    while trace is not None:
        if trace.tb_frame.f_code is initializer.__code__:
            return trace.tb_frame.f_locals["self"]
        trace = trace.tb_next
    raise AssertionError("Real original context initializer never ran")


def attempt_for(case, kind):
    return case.source._app_context_constructions[(kind, NONCE if kind == "stage" else REQUEST)]
