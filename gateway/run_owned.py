"""Canonical managed lifecycle, admitted only by the original sealed caller."""
from __future__ import annotations

import asyncio
import logging
import os

from hermes_owned_gateway import OwnedGatewayUnknown

logger = logging.getLogger(__name__)


def _prepare_windows_numeric_runtime():
    """Load installed NumPy before a background reader blocks on piped stdin.

    Windows OpenBLAS/Fortran initialization inspects stdin while holding the
    DLL loader lock. An outstanding pipe read can deadlock both that import
    and later thread creation (numpy/numpy#24290). No audio device is opened.
    """
    if os.name != "nt":
        return
    import importlib
    import importlib.util
    if importlib.util.find_spec("numpy") is not None:
        importlib.import_module("numpy")


def _require_original_source(source):
    from hermes_owned_gateway import OwnedGatewaySource, current
    if type(source) is not OwnedGatewaySource or current(source.contract) is not source:
        raise OwnedGatewayUnknown("actual-original-source-required")
    source.require_attached(source.contract)


def main(source):
    _require_original_source(source)
    from hermes_owned_gateway import require_lifecycle_available
    require_lifecycle_available(source)
    from gateway.owned_startup_construction import construct_loop, remember_failure, require_original
    loop = None
    try:
        loop = construct_loop(source)
        require_original(source, "loop", loop)
        asyncio.set_event_loop(loop)
        from gateway.run import _guard_corrupt_user_config
        from gateway.run_bootstrap import start_gateway
        _guard_corrupt_user_config()
        result = loop.run_until_complete(start_gateway(owned_source=source))
        if result is not True or source.phase != "closed" or asyncio.all_tasks(loop):
            raise OwnedGatewayUnknown("canonical-owned-lifetime-not-settled")
        require_original(source, "loop", loop)
        loop.close()
        return 0
    except BaseException as error:
        # No asyncio.Runner.close cancellation funnel or service exit/restart.
        first = remember_failure(source, error)
        logger.exception("Canonical managed entry failed; retaining its original resources")
        if loop is not None and not loop.is_closed():
            loop.run_until_complete(source.retain_unknown(str(first)))
        source.unknown(str(first))
        source.hold_unknown()


def _claim_profile_ownership(config):
    from gateway.run import _multiplex_profile_homes, get_hermes_home
    from gateway.runtime_ownership import process_ownership
    from gateway.status import _get_pid_path, write_pid_file
    homes = _multiplex_profile_homes(config) if config.multiplex_profiles else []
    process_ownership.reserve([get_hermes_home(), *(home for _name, home in homes)])
    # No get_running_pid, host attach, replace, foreign-process query or adoption.
    if _get_pid_path().exists():
        raise OwnedGatewayUnknown("existing-gateway-record-needs-maintenance")
    write_pid_file()  # O_EXCL; self metadata is not custody or native-call proof
    if homes:
        config._runtime_profile_homes = tuple(homes)


async def start_managed(source, config=None):
    """Policy route called by the same canonical run_bootstrap.start_gateway."""
    _require_original_source(source)
    from hermes_owned_gateway import require_lifecycle_available
    require_lifecycle_available(source)
    from gateway import run as canonical
    from gateway import run_bootstrap as boot
    from gateway.run_runtime import initialize_gateway_runtime, start_gateway_runtime_api, publish_gateway_runtime_ready
    from gateway.owned_startup_construction import construct_runner, construct_settlement
    os.environ["HERMES_EXEC_ASK"] = "1"
    os.environ.setdefault("AI_AGENT", "hermes-agent")
    os.environ.setdefault("HERMES_AGENT", "true")
    boot._start_gateway_resolve_terminal_cwd()
    from hermes_cli.resource_limits import apply_nofile_soft_limit
    apply_nofile_soft_limit()
    from gateway.code_skew import record_boot_fingerprint
    record_boot_fingerprint()
    config = config if config is not None else canonical.load_gateway_config_for_runner()
    _claim_profile_ownership(config)
    boot._start_gateway_configure_logging(0)
    from gateway.run_startup import recover_left_core_at_gateway_start
    await asyncio.to_thread(recover_left_core_at_gateway_start)
    runner = construct_runner(source, config)
    from gateway.native_source_runtime import capture_original_runner
    capture_original_runner(runner, asyncio.current_task(), asyncio.get_running_loop())
    source.bind_runner(runner)
    settlement = construct_settlement(source, runner)
    try:
        _prepare_windows_numeric_runtime()
        await initialize_gateway_runtime(runner)
        boot._start_gateway_early_environment_probe()
        canonical._enable_multiplex_log_routing(runner.config)
        runner._platform_lock_takeover_on_start = False
        asyncio.get_running_loop().set_exception_handler(canonical._gateway_loop_exception_handler)
        control = await boot._start_gateway_start_control_socket(runner)
        if control is None:
            raise OwnedGatewayUnknown("canonical-control-listener-unavailable")
        settlement.bind_control(control)
        canonical._refresh_host_gateway_record(runner)
        canonical._log_standalone_profiles_at_boot(runner)
        await start_gateway_runtime_api(runner)
        settlement.bind_runtime(runner.session_api, runner.session_authorities)
        from gateway.lifecycle_ledger import record_startup
        unclean = record_startup()
        canonical._ensure_windows_gateway_venv_imports()
        await canonical._discover_gateway_mcp_tools(runner.config)
        if not await runner.start() or runner.should_exit_cleanly or not runner._running:
            raise OwnedGatewayUnknown("canonical-start-did-not-become-running")
        if unclean is not None:
            settlement.bind_integrity_writer(boot._start_deferred_state_db_integrity_check(unclean))
        from hermes_cli.nous_auth_keepalive import start_nous_auth_keepalive
        settlement.bind_keepalive(start_nous_auth_keepalive())
        settlement.bind_schedulers(*boot._start_gateway_start_cron_and_housekeeping(runner))
        publish_gateway_runtime_ready(runner)
        # The same original bootstrap task mounts the existing full dashboard
        # and retains its private credential before original-pipe publication.
        await source.prepare_transport(runner)
        source.ready(runner)
        await asyncio.to_thread(source.read_shutdown)
        source.require_shutdown(runner)
        await settlement.stop()
        await settlement.finish()
        source.completed(runner, settlement)
        return True
    except BaseException as error:
        # Includes startup failures: ownership and partial startup resources stay held.
        from gateway.owned_startup_construction import remember_failure
        first = remember_failure(source, error)
        logger.exception("Canonical managed lifecycle failed; retaining partial startup resources")
        await source.retain_unknown(str(first))


class ManagedEntry:
    def __init__(self, source):
        _require_original_source(source)
        from gateway.run_owned_shutdown import ManagedSettlement
        from gateway import run_owned_resources
        self.source = source
        self.main = main
        self.start = start_managed
        self.settlement_type = ManagedSettlement
        self.resources = run_owned_resources

    def run(self):
        _require_original_source(self.source)
        from gateway.owned_entry_construction import require_prepared_entry
        if require_prepared_entry(self.source) is not self:
            raise OwnedGatewayUnknown("canonical-managed-normal-lifecycle-not-composed:foreign-entry")
        from hermes_owned_gateway import require_lifecycle_available
        require_lifecycle_available(self.source)
        return main(self.source)

def prepare_entry(source):
    from gateway.owned_entry_construction import prepare_entry as prepare_original
    return prepare_original(source)
