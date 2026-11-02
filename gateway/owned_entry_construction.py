"""Original ManagedEntry constructor custody, never startup or native authority.

The sealed launcher calls the existing producer before its still-refusing
lifecycle gate. Credits and retained objects only prevent replacement/replay;
they cannot replace original App authentication, Job or normal lifetime checks.
"""
from __future__ import annotations

import logging

from hermes_owned_gateway import OwnedGatewaySource, OwnedGatewayUnknown, current

logger = logging.getLogger(__name__)


def _source(source):
    if type(source) is not OwnedGatewaySource or current(source.contract) is not source:
        raise OwnedGatewayUnknown("actual-original-source-required")


def _reserve(source):
    _source(source)
    with source._state_lock:
        if source._entry_credit or source._entry_state != "unprepared" or source.entry is not None:
            error = OwnedGatewayUnknown("original-managed-entry-already-attempted")
            if source._entry_state == "initializing" and source._entry_error is None:
                source._entry_error = error
            raise error
        # Nominal retained metadata credit, not allocator/RSS or native proof.
        source._entry_credit = 4096
        source._entry_state = "initializing"
        source._entry_inputs = source


def _first_failure(source, error):
    if source._entry_error is None:
        source._entry_error = error
    if source._entry_traceback is None:
        source._entry_traceback = source._entry_error.__traceback__
    source._entry_state = "failed"
    return source._entry_error


def _callbacks():
    from gateway import run_owned, run_owned_resources
    from gateway.run_owned_shutdown import ManagedSettlement
    return (run_owned.main, run_owned.start_managed, ManagedSettlement, run_owned_resources)


def _initialize(source):
    from gateway.run_owned import ManagedEntry
    source._entry_callbacks = _callbacks()
    source._entry_creator, source._entry_initializer = ManagedEntry.__new__, ManagedEntry.__init__
    if source._entry_error is not None:
        raise source._entry_error
    value = source._entry_creator(ManagedEntry)
    source._entry_allocator_result = value
    if source._entry_original is not None:
        raise OwnedGatewayUnknown("original-managed-entry-construction-displaced")
    source._entry_original = value  # Retain actual partial self BEFORE its initializer.
    if type(value) is not ManagedEntry or vars(value):
        raise OwnedGatewayUnknown("foreign-or-preowned-managed-entry-allocation")
    source._entry_initializer(value, source)
    if source._entry_error is not None:
        raise source._entry_error
    if source.entry is not None or source._entry_original is not value:
        raise OwnedGatewayUnknown("original-managed-entry-publication-displaced")
    source.entry = value
    source._entry_state = "prepared"
    return require_prepared_entry(source)


def prepare_entry(source):
    _reserve(source)
    try:
        source.require_attached(source.contract)
        return _initialize(source)
    except BaseException as error:
        first = _first_failure(source, error)
        logger.error("Original managed entry construction failed",
                     exc_info=(type(first), first, source._entry_traceback))
        raise OwnedGatewayUnknown("original-managed-entry-construction-failed") from first


def require_prepared_entry(source):
    _source(source)
    from gateway.run_owned import ManagedEntry
    value = source._entry_allocator_result
    callbacks = source._entry_callbacks
    if type(callbacks) is not tuple or len(callbacks) != 4:
        raise OwnedGatewayUnknown("canonical-managed-normal-lifecycle-not-composed")
    if (source._entry_state != "prepared" or source._entry_error is not None
            or source._entry_credit != 4096 or source._entry_inputs is not source
            or type(value) is not ManagedEntry or source.entry is not value
            or source._entry_original is not value or value.source is not source
            or source._entry_creator is not ManagedEntry.__new__
            or source._entry_initializer is not ManagedEntry.__init__
            or any(before is not after for before, after in zip(callbacks, _callbacks()))
            or any(before is not after for before, after in zip(
                callbacks, (value.main, value.start, value.settlement_type, value.resources)))):
        raise OwnedGatewayUnknown("canonical-managed-normal-lifecycle-not-composed")
    return value
