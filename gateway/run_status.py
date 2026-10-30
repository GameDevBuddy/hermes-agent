"""Runtime status reporting for GatewayRunner; facade imports remain patchable."""
from __future__ import annotations

from typing import Optional


class GatewayRuntimeStatusMixin:
    def _running_agent_count(self) -> int:
        return len(self._running_agents)

    def _status_action_label(self) -> str:
        return "restart" if self._restart_requested else "shutdown"

    def _status_action_gerund(self) -> str:
        """Localized "restarting" / "shutting down" for the busy/drain notices shown in chat."""
        from gateway.run import t
        return t("gateway.busy.action_restarting") if self._restart_requested else t("gateway.busy.action_shutting_down")

    def _update_runtime_status(self, gateway_state: Optional[str] = None, exit_reason: Optional[str] = None) -> None:
        from gateway.run import _write_runtime_status_quiet
        if getattr(self, "_startup_cleanup_only", False):
            return  # cleanup of a start that never ran keeps its startup verdict (startup_failed)
        # ``active_work`` names each unit only while draining — that is when an observer (``hermes
        # update``) needs to know WHAT holds the gateway open; a per-turn write would be wasted I/O.
        active_work = self._describe_active_work() if gateway_state == "draining" else None
        _write_runtime_status_quiet(
            gateway_state=gateway_state, exit_reason=exit_reason,
            restart_requested=self._restart_requested, active_agents=self._active_work_count(),
            active_work=active_work)

    def _persist_active_agents(self) -> None:
        """Persist the live in-flight agent count to ``gateway_state.json`` at every turn boundary.
        Passes ONLY ``active_agents`` so the read-merge-write keeps lifecycle state (gateway_state=None
        would clobber it). Best-effort: a failed write must never disrupt a turn."""
        from gateway.run import _write_runtime_status_quiet
        _write_runtime_status_quiet(active_agents=self._active_work_count())
