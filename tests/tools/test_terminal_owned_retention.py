"""Actual legacy/active/final publication boundaries retain original local resources; mocks only."""
import asyncio
import contextvars
from pathlib import Path
import threading

import pytest

from hermes_owned_gateway import OwnedGatewayUnknown
from tools import terminal_tool_owned as owned
from tests.tools.terminal_owned_fixture import Process, construct, finish, terminal_fixture

pytest_plugins = ("tests.tools.terminal_owned_fixture",)


@pytest.mark.parametrize("method", ["_kill_process", "_force_kill_process", "_kill_spawned_tree"])
def test_original_managed_process_cannot_enter_legacy_signal_path(monkeypatch, tmp_path, method):
    async def scenario():
        source, _runner, terminal, plan, created = terminal_fixture(monkeypatch, tmp_path)
        env = construct(terminal, plan)
        with pytest.raises(OwnedGatewayUnknown, match="natural-completion"):
            getattr(env, method)(created[0])
        assert not created[0].signals and owned.find(created[0]).error is not None
        assert not source.terminal_closed
    asyncio.run(scenario())


def test_idle_manual_and_atexit_keep_original_aliases_and_session_files(monkeypatch, tmp_path):
    async def scenario():
        from tools import terminal_tool_lifecycle as lifecycle
        source, _runner, terminal, plan, _created = terminal_fixture(monkeypatch, tmp_path)
        env = construct(terminal, plan)
        Path(env._snapshot_path).write_text("fixture", encoding="utf-8")
        monkeypatch.setattr(terminal, "_get_env_config", lambda: plan.config)
        lifecycle._cleanup_inactive_envs(lifetime_seconds=-1)
        lifecycle.cleanup_vm("fixture-task")
        terminal._atexit_cleanup()
        assert terminal._active_environments["fixture-task"] is env
        assert Path(env._snapshot_path).exists() and not owned.find(env).cleanup_attempted
        await finish(source)
        assert owned.find(env).cleanup_done and not Path(env._snapshot_path).exists()
    asyncio.run(scenario())


@pytest.mark.parametrize("mutation", ["membership", "child-links", "profile"])
def test_original_shutdown_book_checked_after_actual_file_cleanup(monkeypatch, tmp_path, mutation):
    async def scenario():
        from tools.environments import local
        from hermes_constants import set_hermes_home_override
        source, _runner, terminal, plan, _created = terminal_fixture(monkeypatch, tmp_path)
        env = construct(terminal, plan)
        Path(env._snapshot_path).write_text("fixture", encoding="utf-8")
        original_unlink = local.os.unlink

        def changed(path, *args, **kwargs):
            result = original_unlink(path, *args, **kwargs)
            if path == env._snapshot_path:
                actions = {
                    "membership": lambda: source.terminal_records.pop(),
                    "child-links": lambda: setattr(owned.find(env), "children", ()),
                    "profile": lambda: set_hermes_home_override(str(tmp_path / "changed")),
                }
                actions[mutation]()
            return result

        monkeypatch.setattr(local.os, "unlink", changed)
        with pytest.raises(OwnedGatewayUnknown):
            await finish(source)
        record = owned.find(env)
        assert record.cleanup_attempted and not record.cleanup_done and record.error is not None
        assert record.cleanup_snapshot.records and not source.terminal_closed
        first = source.terminal_error
        with pytest.raises(OwnedGatewayUnknown):
            await finish(source)
        assert source.terminal_error is first
    asyncio.run(scenario())


def test_active_original_command_blocks_cleanup_without_cancelling_workers(monkeypatch, tmp_path):
    async def scenario():
        from tools.environments import local
        source, _runner, terminal, plan, _created = terminal_fixture(monkeypatch, tmp_path)
        env = construct(terminal, plan)
        entered, release = threading.Event(), threading.Event()
        errors, children = [], []

        def factory(*args, **kwargs):
            proc = Process(*args, **kwargs)
            children.append(proc)
            def read():
                entered.set()
                release.wait()
            proc.stdout.on_read = read
            return proc

        def command():
            try:
                env.execute("fixture active command")
            except BaseException as error:
                errors.append(error)

        monkeypatch.setattr(local.subprocess, "Popen", factory)
        worker = threading.Thread(target=contextvars.copy_context().run, args=(command,))
        worker.start()
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            with pytest.raises(OwnedGatewayUnknown):
                await finish(source)
            assert not owned.find(env).cleanup_attempted and not source.terminal_closed
            assert not children[0].signals
        finally:
            release.set()
            await asyncio.to_thread(worker.join)
        assert errors and source.terminal_records
    asyncio.run(scenario())


def test_unsupported_background_refuses_before_registry_call(monkeypatch, tmp_path):
    async def scenario():
        from tools.terminal_tool_background import _spawn
        source, _runner, terminal, plan, created = terminal_fixture(monkeypatch, tmp_path)
        env = construct(terminal, plan)
        count = len(created)
        class Registry:
            def spawn_local(self, **kwargs):
                pytest.fail("unsupported original registry birth must not run")
        with pytest.raises(OwnedGatewayUnknown, match="original-normal-lifecycle-unavailable"):
            _spawn(Registry(), env=env, env_type="local", command="fixture", cwd=str(tmp_path),
                   effective_task_id="fixture", task_id="fixture", session_key="fixture", effective_pty=True)
        assert len(created) == count and source.terminal_records[-1].kind == "unsupported-terminal"
    asyncio.run(scenario())
