"""Actual terminal constructor/normal consumer composition; no shells or kernel children."""
import asyncio
from pathlib import Path

from tests.tools.terminal_owned_fixture import construct, finish, terminal_fixture


def test_actual_gateway_settles_original_local_command_and_session_files(monkeypatch, tmp_path):
    async def scenario():
        source, _runner, terminal, plan, created = terminal_fixture(monkeypatch, tmp_path)
        env = construct(terminal, plan)
        Path(env._snapshot_path).write_text("fixture snapshot", encoding="utf-8")
        Path(env._cwd_file).write_text(str(tmp_path), encoding="utf-8")
        result = env.execute("fixture command", stdin_data="fixture input\n", bounded_capture=True)
        assert result["returncode"] == 0 and result["output"] == "fixture-output\n"
        assert created[-1].stdin.closed and created[-1].stdout.closed
        await finish(source)
        assert not Path(env._snapshot_path).exists() and not Path(env._cwd_file).exists()
        assert all(not child.signals and child.waits for child in created)
        assert terminal._active_environments["fixture-task"] is env
    asyncio.run(scenario())


def test_alias_replacement_and_empty_cache_retain_all_original_environments(monkeypatch, tmp_path):
    async def scenario():
        from tools import terminal_tool_lifecycle as lifecycle, terminal_tool_owned as owned
        source, _runner, terminal, plan, _created = terminal_fixture(monkeypatch, tmp_path)
        first = construct(terminal, plan)
        assert construct(terminal, plan) is first
        second = lifecycle._create_configured_env(plan.config, "local", image="", cwd=str(tmp_path),
                                                  timeout=30, task_id="second", host_cwd=None)
        terminal._active_environments["fixture-task"] = second
        terminal._active_environments["alias"] = first
        first.cleanup()
        assert not owned.find(first).cleanup_attempted
        assert lifecycle._unregister_env("alias") is None
        lifecycle._evict_environment_for_task("alias")
        assert terminal._active_environments["alias"] is first
        terminal._active_environments.clear()
        await finish(source)
        assert owned.find(first).cleanup_done and owned.find(second).cleanup_done
        assert first is owned.find(first).obj and second is owned.find(second).obj
        assert source.terminal_closed and source.terminal_records
        await finish(source)
        assert source.terminal_closed
    asyncio.run(scenario())


def test_direct_actual_local_initializer_and_lazy_local_consumer(monkeypatch, tmp_path):
    async def scenario():
        from tools.environments.local import LocalEnvironment
        from tools import terminal_tool_lifecycle as lifecycle, terminal_tool_owned as owned
        source, _runner, terminal, plan, created = terminal_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(terminal, "_get_env_config", lambda: plan.config)
        assert lifecycle.ensure_task_env("fixture-task") is None
        assert not created
        env = LocalEnvironment(cwd=str(tmp_path), persistent=False)
        result = env.execute("fixture file-tool command", bounded_capture=False)
        assert result["output"] == "fixture-output\n" and not env._persist_env
        await finish(source)
        assert owned.find(env).cleanup_done and terminal._active_environments == {}
    asyncio.run(scenario())


def test_original_before_popen_claim_and_original_deadline_workers(monkeypatch, tmp_path):
    async def scenario():
        from tools import terminal_tool_owned as owned
        from tools.environments import local
        from tests.tools.terminal_owned_fixture import Process
        source, _runner, terminal, plan, _created = terminal_fixture(monkeypatch, tmp_path)
        births = []

        def factory(*args, **kwargs):
            record = source.terminal_records[-1]
            assert record.kind == "process" and record.attempted and record.obj is None
            assert record.original_parent in source.terminal_records
            births.append(record)
            return Process(*args, **kwargs)

        monkeypatch.setattr(local.subprocess, "Popen", factory)
        env = construct(terminal, plan)
        env.execute("fixture command")
        assert births and source.terminal_credit == len(source.terminal_records) * owned.CREDIT
        executors = [record for record in source.terminal_records if record.kind == "executor"]
        assert executors and all(record.done and not record.obj.is_alive() for record in executors)
        await finish(source)
        assert all(record.wait_done and record.reader.eof for record in births)
    asyncio.run(scenario())


def test_unmanaged_actual_local_defaults_use_original_cleanup(monkeypatch, tmp_path):
    async def scenario():
        import hermes_owned_gateway as ownership
        from tools.environments.local import LocalEnvironment
        source, _runner, _terminal, _plan, created = terminal_fixture(monkeypatch, tmp_path)
        monkeypatch.setattr(ownership, "_CURRENT", None)
        env = LocalEnvironment(cwd=str(tmp_path), timeout=9, persistent=False)
        Path(env._snapshot_path).write_text("fixture", encoding="utf-8")
        result = env.execute("fixture unmanaged", stdin_data="raw\n")
        env.cleanup()
        assert result["returncode"] == 0 and not Path(env._snapshot_path).exists()
        assert not source.terminal_records and env.timeout == 9 and not env._persist_env
        assert created[-1].stdin.writes == [b"raw\n"]
    asyncio.run(scenario())
