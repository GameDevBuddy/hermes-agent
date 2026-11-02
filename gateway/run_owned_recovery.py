"""Refuse legacy work references before managed startup can adopt or replay them.

Empty data only means there is nothing to pass to the legacy recovery callback;
it never proves an old process stopped, writer exclusion, or native authority.
"""
from __future__ import annotations

import json
import logging
import os
import stat

from hermes_owned_gateway import OwnedGatewayUnknown

logger = logging.getLogger("gateway.run")
MAX_CHECKPOINT_BYTES = 256 * 1024


def _require_empty_checkpoint(path):
    try:
        before = path.lstat()
    except FileNotFoundError:
        return
    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
            or getattr(before, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT):
        raise OwnedGatewayUnknown("managed-process-checkpoint-is-not-an-original-regular-file")
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise OwnedGatewayUnknown("managed-process-checkpoint-changed-during-open")
        raw = stream.read(MAX_CHECKPOINT_BYTES + 1)
    if len(raw) > MAX_CHECKPOINT_BYTES:
        raise OwnedGatewayUnknown("managed-process-checkpoint-exceeds-read-credit")
    try:
        entries = json.loads(raw.decode("utf-8-sig"))
    except (ValueError, UnicodeError) as error:
        raise OwnedGatewayUnknown("managed-process-checkpoint-is-unreadable") from error
    if type(entries) is not list or entries:
        raise OwnedGatewayUnknown("previous-process-work-needs-explicit-maintenance")


def refuse_unowned_recovery(source, runner):
    source.require_bound(runner)
    from gateway.run import _profile_runtime_scope
    from gateway.run_runtime import reserved_profile_homes
    from gateway.session_authorities import all_authorities
    from tools.process_registry import _checkpoint_path
    for _name, home in reserved_profile_homes(runner):
        with _profile_runtime_scope(home):
            _require_empty_checkpoint(_checkpoint_path())
    # Use each ORIGINAL SessionDB's canonical read route. No separate store,
    # controller, mutation, migration, PID probe or reaper is introduced.
    for authority in all_authorities(runner):
        db = authority.db
        pending = db._read_one("""SELECT 1 FROM async_delegations
            WHERE state IN ('running','stalling','finalizing')
               OR (delivery_state='pending' AND event_json IS NOT NULL) LIMIT 1""")
        if pending is not None:
            raise OwnedGatewayUnknown("previous-delegation-work-needs-explicit-maintenance")


def restore_legacy_process_work(runner):
    from gateway.run_startup import _log_suppressed
    with _log_suppressed(logging.WARNING, "Process checkpoint recovery: %s"):
        from tools.process_registry import process_registry
        recovered = process_registry.recover_from_checkpoint()
        recovered += runner._recover_secondary_process_checkpoints(process_registry)
        if recovered:
            logger.info("Recovered %s background process(es) from previous run", recovered)
    with _log_suppressed(logging.WARNING, "Could not restore async delegation completions: %s"):
        from tools.async_delegation import restore_undelivered_completions
        restore_undelivered_completions(process_registry.completion_queue)
