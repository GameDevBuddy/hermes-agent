"""Original local terminal attempts/IO/context remain held on actual mocked boundary failures."""
import asyncio
from types import SimpleNamespace

import pytest

from hermes_owned_gateway import OwnedGatewayUnknown
from tools import terminal_tool_owned as owned
from tests.tools.terminal_owned_fixture import Process, Pipe, construct, finish, terminal_fixture

pytest_plugins = ("tests.tools.terminal_owned_fixture",)


def test_cached_foreign_environment_is_retained_without_adoption(monkeypatch, tmp_path):
    async def scenario():
        source, _runner, terminal, plan, created = terminal_fixture(monkeypatch, tmp_path)
        borrowed = SimpleNamespace(execute=lambda *args: pytest.fail("foreign work"))
        terminal._active_environments["fixture-task"] = borrowed
        with pytest.raises(OwnedGatewayUnknown, match="constructor-unavailable"):
            construct(terminal, plan)
        assert any(record.result is borrowed for record in source.terminal_records)
        assert not created and terminal._active_environments["fixture-task"] is borrowed
    asyncio.run(scenario())


def test_old_parent_cannot_claim_new_current_source_or_poison_its_book(monkeypatch, tmp_path):
    async def scenario():
        from tests.agent.auxiliary_owned_fixture import bind_source
        source, _runner, terminal, plan, _created = terminal_fixture(monkeypatch, tmp_path)
        original = owned.find(construct(terminal, plan))
        newer, _other = bind_source(monkeypatch, tmp_path / "newer")
        with pytest.raises(OwnedGatewayUnknown):
            owned.reserve("process", Process, original)
        assert newer.terminal_records == [] and newer.terminal_credit == 0 and newer.terminal_error is None
        assert original.error is not None and original in source.terminal_records
    asyncio.run(scenario())


def test_direct_foreign_wait_refuses_before_reader_or_process_api(monkeypatch, tmp_path):
    async def scenario():
        from tools.environments.local import LocalEnvironment
        source, _runner, _terminal, _plan, _created = terminal_fixture(monkeypatch, tmp_path)
        foreign = object.__new__(LocalEnvironment)
        proc = Process(["fixture-not-run"])
        with pytest.raises(OwnedGatewayUnknown, match="constructor-unavailable"):
            foreign._wait_for_process(proc)
        assert not proc.stdout.closed and not proc.waits and source.terminal_records[-1].result is foreign
    asyncio.run(scenario())


@pytest.mark.parametrize("backend", ["docker", "ssh", "modal", "fixture-plugin"])
def test_unsupported_backend_refuses_before_real_backend_factory(monkeypatch, tmp_path, backend):
    async def scenario():
        from tools.terminal_tool_backends import _create_environment
        source, _runner, _terminal, _plan, created = terminal_fixture(monkeypatch, tmp_path)
        with pytest.raises(OwnedGatewayUnknown, match="backend-normal-lifecycle-unavailable"):
            _create_environment(backend, "image", str(tmp_path), 30)
        assert not created and source.terminal_records[-1].error is not None
    asyncio.run(scenario())


def test_partial_popen_constructor_keeps_original_self_credit_and_first_exception(monkeypatch, tmp_path):
    error = RuntimeError("fixture-partial-Popen")
    partial = []

    class Failed(Process):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            partial.append(self)
            raise error

    async def scenario():
        from tools.environments import local
        source, _runner, terminal, plan, _created = terminal_fixture(monkeypatch, tmp_path)
        env = construct(terminal, plan)
        monkeypatch.setattr(local.subprocess, "Popen", Failed)
        with pytest.raises(RuntimeError) as caught:
            env.execute("fixture command")
        assert caught.value is error
        failed = next(record for record in source.terminal_records if record.kind == "process" and record.error is error)
        assert caught.value is error and failed.attempted and failed.obj is None
        assert partial and error.__traceback__ is not None and source.terminal_credit >= owned.CREDIT
        assert source.terminal_error is error and not source.terminal_closed
        with pytest.raises(OwnedGatewayUnknown):
            await finish(source)
        assert source.terminal_error is error and not partial[0].signals
    asyncio.run(scenario())


def test_partial_local_initializer_keeps_original_self_before_child_birth(monkeypatch, tmp_path):
    error, partial = RuntimeError("fixture-partial-local"), []

    async def scenario():
        from tools.environments.local import LocalEnvironment
        source, _runner, terminal, plan, created = terminal_fixture(monkeypatch, tmp_path)
        def failed(self):
            partial.append(self)
            raise error
        monkeypatch.setattr(LocalEnvironment, "get_temp_dir", failed)
        with pytest.raises(RuntimeError) as caught:
            construct(terminal, plan)
        record = owned.find(partial[0])
        assert caught.value is record.error is error and not record.done and not created
        assert record.obj is partial[0] and record in source.terminal_records
    asyncio.run(scenario())


def test_factory_cannot_adopt_original_object_from_another_constructor(monkeypatch, tmp_path):
    async def scenario():
        from tools import terminal_tool_backends as backends
        source, _runner, terminal, plan, created = terminal_fixture(monkeypatch, tmp_path)
        original = construct(terminal, plan)
        count = len(created)
        monkeypatch.setitem(backends._ENV_BUILDERS, "local", lambda **kwargs: original)
        with pytest.raises(OwnedGatewayUnknown, match="original-local-constructor"):
            backends._create_environment("local", "", str(tmp_path), 30)
        assert len(created) == count and source.terminal_records[-1].result is original
        assert source.terminal_records[-1].obj is None
    asyncio.run(scenario())


@pytest.mark.parametrize("control", ["timeout", "interrupt", "cancel"])
def test_passive_controls_retain_original_process_without_signal_or_retry(monkeypatch, tmp_path, control):
    async def scenario():
        from tools.environments import local
        from tools import interrupt
        source, _runner, terminal, plan, _created = terminal_fixture(monkeypatch, tmp_path)
        env = construct(terminal, plan)
        commands = []
        cancelled = asyncio.CancelledError("fixture original cancellation")

        def factory(*args, **kwargs):
            proc = Process(*args, **kwargs)
            proc.returncode = None
            commands.append(proc)
            if control == "cancel":
                def cancel():
                    raise cancelled
                proc.on_poll = cancel
            return proc

        monkeypatch.setattr(local.subprocess, "Popen", factory)
        if control == "interrupt":
            monkeypatch.setattr(interrupt, "is_interrupted", lambda: True)
        with pytest.raises((OwnedGatewayUnknown, asyncio.CancelledError)):
            env.execute("fixture command", timeout=0.005)
        original = next(record for record in source.terminal_records if record.kind == "process" and record.obj in commands)
        assert len(commands) == 1 and not commands[0].signals and original.wait_attempted
        assert not original.wait_done and original.error is not None and not source.terminal_closed
        first = source.terminal_error
        with pytest.raises(OwnedGatewayUnknown):
            env.execute("fixture retry must not start")
        assert len(commands) == 1 and source.terminal_error is first
        if control == "cancel":
            assert source.terminal_error is cancelled
    asyncio.run(scenario())


@pytest.mark.parametrize("mutation", ["stdout", "wait-method", "parent", "runtime", "profile", "reader"])
def test_original_process_and_io_links_rechecked_after_read(monkeypatch, tmp_path, mutation):
    async def scenario():
        from tools.environments import local
        from hermes_constants import set_hermes_home_override
        source, runner, terminal, plan, _created = terminal_fixture(monkeypatch, tmp_path)
        env = construct(terminal, plan)
        originals = []

        def factory(*args, **kwargs):
            proc = Process(*args, **kwargs)
            originals.append(proc)

            def change():
                actions = {
                    "stdout": lambda: setattr(proc, "stdout", Pipe()),
                    "wait-method": lambda: setattr(proc, "wait", lambda **kw: 0),
                    "parent": lambda: setattr(owned.find(proc), "parent", object()),
                    "runtime": lambda: setattr(runner, "_gdb_original_source_runtime", object()),
                    "profile": lambda: set_hermes_home_override(str(tmp_path / "changed")),
                    "reader": lambda: setattr(owned.find(proc), "reader", None),
                }
                actions[mutation]()

            proc.stdout.on_read = change
            return proc

        monkeypatch.setattr(local.subprocess, "Popen", factory)
        with pytest.raises(OwnedGatewayUnknown):
            env.execute("fixture command")
        original = owned.find(originals[0])
        assert original.error is not None and original.pipes[0] is not None
        assert not original.wait_done and not source.terminal_closed and not originals[0].signals
    asyncio.run(scenario())
