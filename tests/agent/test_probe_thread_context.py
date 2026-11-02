"""Canonical context propagation survives either default or retained thread construction."""
import contextvars
from types import SimpleNamespace

import pytest

from agent import memory_provider


@pytest.mark.parametrize("retained", [False, True])
def test_scoped_target_uses_creation_context_without_changing_inputs(monkeypatch, retained):
    scope = contextvars.ContextVar("probe-caller-scope", default="unset")
    seen, constructed = [], []
    args, kwargs = ("input",), {"choice": "original"}

    def target(value, *, choice):
        seen.append((scope.get(), value, choice))

    def factory(**values):
        constructed.append(values)
        return SimpleNamespace(**values)

    monkeypatch.setattr(memory_provider.threading, "Thread", factory)
    token = scope.set("creator-profile")
    try:
        thread = memory_provider.spawn_context_thread(target, name="fixture", args=args, kwargs=kwargs,
            thread_factory=factory if retained else None)
        scope.set("later-caller")
        thread.target(*thread.args, **thread.kwargs)
        assert seen == [("creator-profile", "input", "original")]
        assert constructed[0]["args"] is args and constructed[0]["kwargs"] is kwargs
        assert thread.name == "fixture"
    finally:
        scope.reset(token)
