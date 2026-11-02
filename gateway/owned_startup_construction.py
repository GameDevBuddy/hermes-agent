"""Retain the three original startup constructors; observations grant no authority.

These fixed records belong to the existing original source, including failed
allocations and partial initializers. No retry, adoption or closer is inferred.
"""
from __future__ import annotations

from hermes_owned_gateway import OwnedGatewaySource, OwnedGatewayUnknown, current

_FIELDS = {"loop": "_loop_construction", "runner": "_runner_construction",
           "settlement": "_settlement_construction"}


def _source(source):
    if type(source) is not OwnedGatewaySource or current(source.contract) is not source:
        raise OwnedGatewayUnknown("actual-original-source-required")


def remember_failure(source, error):
    with source._state_lock:
        if source._startup_error is None:
            source._startup_error = error
        if source._startup_traceback is None:
            source._startup_traceback = source._startup_error.__traceback__
        return source._startup_error


def _reserve(source, kind, inputs):
    _source(source)
    source.require_child_work()
    from gateway.owned_entry_construction import require_prepared_entry
    require_prepared_entry(source)
    field = _FIELDS[kind]
    with source._state_lock:
        prior = getattr(source, "_original" + field)
        if getattr(source, field) is not None or prior is not None or source._startup_error is not None:
            error = OwnedGatewayUnknown("original-startup-construction-already-attempted")
            if prior is not None and prior["state"] == "initializing":
                remember_failure(source, error)
            raise error
        # Nominal reference credit is consumed before imports, policy or factory callbacks.
        if source._startup_credit + 4096 > 3 * 4096:
            raise OwnedGatewayUnknown("original-startup-reference-credit-exhausted")
        source._startup_credit += 4096
        record = {"state": "initializing", "inputs": inputs, "class": None,
                  "creator": None, "initializer": None, "allocator_result": None,
                  "original": None, "error": None, "traceback": None}
        setattr(source, field, record)
        setattr(source, "_original" + field, record)
        return record


def _failed(source, record, error):
    first = remember_failure(source, error)
    record["error"], record["traceback"], record["state"] = first, source._startup_traceback, "failed"


def _initialize(source, kind, record, cls, args):
    record["class"], record["creator"], record["initializer"] = cls, cls.__new__, cls.__init__
    creator, initializer = record["creator"], record["initializer"]
    value = creator(cls) if creator is object.__new__ else creator(cls, *args)
    record["allocator_result"] = value
    if record["original"] is not None:
        raise OwnedGatewayUnknown("original-startup-partial-self-displaced")
    record["original"] = value  # Retain even an opaque result, before any initializer.
    if source._startup_error is not None:
        raise source._startup_error
    if type(value) is not cls or vars(value):
        raise OwnedGatewayUnknown("foreign-or-preowned-startup-allocation")
    initializer(value, *args)
    if (getattr(source, _FIELDS[kind]) is not record
            or getattr(source, "_original" + _FIELDS[kind]) is not record or record["inputs"] is not args
            or record["original"] is not value or record["allocator_result"] is not value
            or cls.__new__ is not creator or cls.__init__ is not initializer):
        raise OwnedGatewayUnknown("original-startup-construction-displaced")
    if source._startup_error is not None:
        raise source._startup_error
    record["state"] = "prepared"
    return value


def _loop_factory():
    import asyncio
    policy = asyncio.get_event_loop_policy()
    # Preserve the canonical policy selection; custom factories remain unsupported.
    if (type(policy) is not asyncio.DefaultEventLoopPolicy
            or type(policy).new_event_loop is not asyncio.events._BaseDefaultEventLoopPolicy.new_event_loop):
        raise OwnedGatewayUnknown("noncanonical-event-loop-policy")
    factory = policy._loop_factory
    if factory not in (getattr(asyncio, "SelectorEventLoop", None),
                       getattr(asyncio, "ProactorEventLoop", None)):
        raise OwnedGatewayUnknown("noncanonical-event-loop-factory")
    return policy, factory


def construct_loop(source):
    args = ()
    record = _reserve(source, "loop", args)
    try:
        record["policy"], cls = _loop_factory()
        value = _initialize(source, "loop", record, cls, args)
        if source.loop_driver is not None:
            raise OwnedGatewayUnknown("original-loop-publication-displaced")
        source.loop_driver = value
        return value
    except BaseException as error:
        _failed(source, record, error)
        raise


def construct_runner(source, config):
    args = (config,)
    record = _reserve(source, "runner", args)
    try:
        from gateway.run import GatewayRunner
        return _initialize(source, "runner", record, GatewayRunner, args)
    except BaseException as error:
        _failed(source, record, error)
        raise


def construct_settlement(source, runner):
    args = (source, runner)
    record = _reserve(source, "settlement", args)
    try:
        from gateway.run_owned_shutdown import ManagedSettlement
        value = _initialize(source, "settlement", record, ManagedSettlement, args)
        if source.settlement is not None:
            raise OwnedGatewayUnknown("original-settlement-publication-displaced")
        source.settlement = value
        return value
    except BaseException as error:
        _failed(source, record, error)
        raise


def require_original(source, kind, value):
    _source(source)
    record = getattr(source, _FIELDS[kind])
    if (record is None or record is not getattr(source, "_original" + _FIELDS[kind])
            or record["state"] != "prepared" or source._startup_error is not None):
        raise OwnedGatewayUnknown("original-startup-construction-unavailable")
    cls = record["class"]
    if (type(value) is not cls or record["original"] is not value
            or record["allocator_result"] is not value or record["error"] is not None
            or cls.__new__ is not record["creator"] or cls.__init__ is not record["initializer"]):
        raise OwnedGatewayUnknown("original-startup-construction-changed")
    return value
