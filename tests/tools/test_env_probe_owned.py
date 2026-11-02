"""Actual private probe consumers over modeled child/threads; no product process or IO."""
import asyncio
import subprocess

import pytest

from hermes_owned_gateway import OwnedGatewayUnknown
from tools import env_probe, env_probe_owned as owned, env_probe_owned_io as pipe_io
from tests.tools.env_probe_owned_fixture import Case, Process


def test_original_probe_waits_child_and_both_readers_then_closes_in_its_profile(monkeypatch, tmp_path):
    async def scenario():
        from hermes_constants import set_hermes_home_override
        case = Case(monkeypatch, tmp_path)
        owner = case.start()
        assert case.source._original_environment_probe is owner
        assert case.processes == [] and owner.thread is env_probe._PROBE_THREAD
        case.run()
        record, = owner.commands
        proc, = case.processes
        assert record["argv"] is case.argv and proc.argv is case.argv
        assert proc.kwargs["env"] is owner.environment is case.environment
        assert case.environment_calls == [{"strip_launch_profile": True}]
        assert record["exit"] == 0 and record["eof"] == [True, True] and record["finished"]
        assert len(case.writers) == 3 and all(not thread.is_alive() for thread in case.writers)
        assert proc.waited == 1 and proc.force_calls == 0 and proc.stdout.closed and proc.stderr.closed
        assert env_probe.get_environment_probe_line() == "probe fixture"
        set_hermes_home_override(str(tmp_path / "different-profile"))
        case.close()
        assert owner.closed and owner.thread.joined == 1
        with pytest.raises(OwnedGatewayUnknown, match="already-attempted"):
            case.close()

    asyncio.run(scenario())


def test_passive_deadline_keeps_original_child_inputs_and_first_error_without_kill(monkeypatch, tmp_path):
    async def scenario():
        case = Case(monkeypatch, tmp_path)
        first = subprocess.TimeoutExpired(case.argv, 3)
        monkeypatch.setattr(Process, "wait_error", first)
        owner = case.start()
        with pytest.raises(OwnedGatewayUnknown):
            case.run()
        proc, = case.processes
        record, = owner.commands
        assert proc.waited == 1 and proc.force_calls == 0 and proc.returncode is None
        assert case.source._environment_probe_error is first
        assert case.source._environment_probe_traceback is not None
        assert record["process"]["original"] is proc and record["argv"] is case.argv
        assert not record["finished"] and not owner.closed and case.source.phase == "unknown"
        with pytest.raises(OwnedGatewayUnknown):
            env_probe.warm_environment_probe_async()
        assert len(case.processes) == owner.next_command == 1

    asyncio.run(scenario())


def test_partial_process_initializer_failure_retains_actual_self_before_callback(monkeypatch, tmp_path):
    async def scenario():
        case = Case(monkeypatch, tmp_path)
        first, partials = ValueError("original diagnostic initializer"), []

        def fail(proc):
            record = case.source.environment_probe.commands[0]
            assert record["process"]["original"] is record["process"]["allocator_result"] is proc
            partials.append(proc)
            raise first

        monkeypatch.setattr(Process, "on_init", fail)
        owner = case.start()
        with pytest.raises(OwnedGatewayUnknown):
            case.run()
        assert len(partials) == 1 and case.processes == partials
        assert case.source._environment_probe_error is first
        assert owner.commands[0]["process"]["error"] is first
        assert owner.next_command == 1 and len(case.writers) == 1 and not owner.closed

    asyncio.run(scenario())


def test_swallowed_process_allocator_reentry_never_reaches_initializer(monkeypatch, tmp_path):
    async def scenario():
        case = Case(monkeypatch, tmp_path)
        initialized, partials = [], []

        def create(cls, argv, **kwargs):
            try:
                env_probe._run(argv)
            except OwnedGatewayUnknown:
                pass
            proc = object.__new__(cls)
            partials.append(proc)
            return proc

        monkeypatch.setattr(Process, "__new__", staticmethod(create))
        monkeypatch.setattr(Process, "__init__", lambda *args, **kwargs: initialized.append(args))
        owner = case.start()
        with pytest.raises(OwnedGatewayUnknown):
            case.run()
        assert len(partials) == 1 and initialized == []
        assert owner.commands[0]["process"]["original"] is partials[0]
        assert case.source._environment_probe_error is not None and not owner.closed

    asyncio.run(scenario())


def test_output_limit_refuses_before_another_buffer_allocation_or_pipe_cleanup(monkeypatch, tmp_path):
    async def scenario():
        case = Case(monkeypatch, tmp_path)
        monkeypatch.setattr(Process, "output", b"x" * (pipe_io.OUTPUT_BYTES + 1))
        owner = case.start()
        with pytest.raises(OwnedGatewayUnknown):
            case.run()
        proc, = case.processes
        record, = owner.commands
        assert len(record["output"][0]) == pipe_io.OUTPUT_BYTES
        assert not proc.stdout.closed and proc.waited == proc.force_calls == 0
        assert not record["finished"] and not record["eof"][0] and not owner.closed
        assert case.source._environment_probe_error is not None

    asyncio.run(scenario())


def test_displaced_probe_publication_refuses_without_a_second_worker(monkeypatch, tmp_path):
    async def scenario():
        case = Case(monkeypatch, tmp_path)
        owner = case.start()
        case.source.environment_probe = object()
        with pytest.raises(OwnedGatewayUnknown):
            env_probe.warm_environment_probe_async()
        assert case.source._original_environment_probe is owner and len(case.writers) == 1
        assert case.processes == [] and not owner.closed

    asyncio.run(scenario())
