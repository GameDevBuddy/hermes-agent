"""Primary AIAgent associations on the existing managed SDK resource book, not invoke authority."""
from __future__ import annotations

import functools

from agent import auxiliary_owned as owned
from hermes_owned_gateway import OwnedGatewayUnknown

_agents = {}
_ENDPOINTS = ("client", "_anthropic_client", "_codex_session")
_SLOTS = ("_request_client_cache", "_request_anthropic_client_cache")


def links(agent):
    values = tuple(getattr(agent, name, None) for name in _ENDPOINTS)
    slots = tuple(getattr(agent, name, None) for name in _SLOTS)
    return values + tuple(slot.get("client") if isinstance(slot, dict) else None for slot in slots)


def validate_owner(record):
    agent = record.primary_agent
    if (agent is None or _agents.get(id(agent)) is not record
            or getattr(agent, "_gdb_primary_sdk_owner", None) is not record):
        raise OwnedGatewayUnknown("original-primary-agent-association-changed")


def require_record_association(record):
    if record.primary_agent is not None:
        validate_owner(record)
    if record.primary_owner is not None:
        record.primary_owner.require_current()


def owner_for(agent, *, profile=False):
    from hermes_owned_gateway import active
    record = _agents.get(id(agent))
    if record is None or record.primary_agent is not agent:
        if active() is None:
            return None
        record = owned.reserve("unowned-primary-agent", owner_for)
        record.primary_agent, record.result = agent, links(agent)
        error = OwnedGatewayUnknown("primary-agent-original-constructor-unavailable")
        owned.fail(record, error)
        raise error
    record.require_current()
    if profile:
        from hermes_constants import hermes_home_key
        record.source.require_child_work()
        if record.source.auxiliary_closing_attempted or hermes_home_key() != record.home_key:
            error = OwnedGatewayUnknown("original-primary-agent-profile-or-admission-changed")
            owned.fail(record, error)
            raise error
    return record


def _invoke_mutating(agent, factory, args, kwargs):
    try:
        result = factory(agent, *args, **kwargs)
    except BaseException:
        _retained_partial_links = links(agent)
        raise  # The retained original traceback owns these actual pre-return endpoints.
    endpoints = links(agent)
    client = next((value for value in endpoints if value is not None), None)
    return client, (result, endpoints)


def _register_agent(record, agent):
    from run_agent import AIAgent
    with record.source._state_lock:
        record.primary_owner, record.primary_agent = None, agent
        record.result = links(agent)
        if not isinstance(agent, AIAgent):
            raise OwnedGatewayUnknown("actual-primary-agent-constructor-required")
        if id(agent) in _agents or getattr(agent, "_gdb_primary_sdk_owner", None) is not None:
            raise OwnedGatewayUnknown("original-primary-agent-construction-reentry")
        _agents[id(agent)] = record
        agent._gdb_primary_sdk_owner = record


def initializing(factory):
    @functools.wraps(factory)
    def initialize(agent, *args, **kwargs):
        from hermes_owned_gateway import active
        source = active()
        if source is None:
            return factory(agent, *args, **kwargs)
        record = owned.reserve("primary-agent", factory)
        token = None
        try:
            _register_agent(record, agent)
            token = owned._primary_scope.set(record)
            _client, (result, _endpoints) = owned.construct(
                "resolve", _invoke_mutating, agent, factory, args, kwargs, shape="pair")
            record.require_current()
            record.phase = "view" if any(child.client is not None for child in record.children) else "held"
            return result
        except BaseException as error:
            owned.fail(record, error)
            raise
        finally:
            if token is not None:
                owned._primary_scope.reset(token)
    return initialize


def agent_factory(factory=None, *, mutating=False):
    def decorate(actual):
        @functools.wraps(actual)
        def create(agent, *args, **kwargs):
            owner = owner_for(agent, profile=True)
            if owner is None:
                return actual(agent, *args, **kwargs)
            token = owned._primary_scope.set(owner)
            try:
                if mutating:
                    _client, (result, _endpoints) = owned.construct(
                        "resolve", _invoke_mutating, agent, actual, args, kwargs, shape="pair")
                    return result
                return owned.construct("resolve", actual, agent, *args, **kwargs)
            finally:
                owned._primary_scope.reset(token)
        return create
    return decorate(factory) if factory is not None else decorate


def retain(agent, client=None, *, consume=False):
    owner = owner_for(agent, profile=consume)
    if owner is None:
        return False
    if client is not None:
        record = owned.find(client)
        if record is None or record.source is not owner.source or record.primary_owner is not owner:
            # Keep the actual unsupported endpoint; a cache/slot cannot mint its creator.
            attempt = owned.reserve("unowned-primary-client", retain)
            attempt.result, attempt.primary_owner = client, owner
            error = OwnedGatewayUnknown("original-primary-client-constructor-unavailable")
            owned.fail(attempt, error)
            raise error
        record.require_current()
    return True


def defer_release(agent, client=None):
    return retain(agent, client)


@agent_factory
def construct_client(agent, kind, factory, *args, **kwargs):
    return owned.construct(kind, factory, *args, **kwargs)


def legacy_close(agent, client):
    if client is not None and not defer_release(agent, client):
        client.close()


def capture_close_links(record):
    owner = record.primary_owner
    agent = owner.primary_agent if owner is not None else None
    record.primary_close_links = (owner, agent, links(agent) if agent is not None else ())
    return record.primary_close_links


def require_close_links(record, original=None):
    if original is None:
        return
    owner, agent, previous = original
    if record.primary_owner is not owner or (owner is not None and owner.primary_agent is not agent):
        raise OwnedGatewayUnknown("original-primary-agent-owner-changed-during-close")
    if agent is not None and any(before is not after for before, after in zip(previous, links(agent))):
        raise OwnedGatewayUnknown("original-primary-agent-endpoints-changed-during-close")


def retained_agents(runner):
    from hermes_owned_gateway import active
    source = active(runner)
    if source is None:
        raise OwnedGatewayUnknown("original-primary-source-required")
    source.require_shutdown(runner)
    with source._state_lock:
        records = tuple(record for record in source.auxiliary_clients
                        if record is not None and record.primary_agent is not None)
    for record in records:
        record.require_current()
    return tuple(record.primary_agent for record in records)


def retain_cached_agent(agent):
    from run_agent import AIAgent
    from hermes_owned_gateway import active
    if not isinstance(agent, AIAgent) and not any(value is not None for value in links(agent)):
        return False  # A non-AIAgent test/tool object declares no primary SDK endpoint.
    if active() is None and id(agent) not in _agents:
        return False
    retain(agent)
    for client in links(agent):
        if client is not None:
            retain(agent, client)
    return True


async def _close_memory(agent, home):
    from gateway.run import _profile_runtime_scope
    import asyncio
    with _profile_runtime_scope(home):
        manager = getattr(agent, "_memory_manager", None)
        if manager is not None:
            if getattr(manager, "_owned_shutdown_completed", False):
                await asyncio.to_thread(manager.require_owned_settled)
            else:
                await asyncio.to_thread(manager.shutdown_owned)


async def close_agent_memories(runner):
    from hermes_constants import get_default_hermes_root
    with runner._agent_cache_lock:
        cached = tuple(runner._agent_cache.items())
    for key, entry in cached:
        agent = entry[0] if isinstance(entry, tuple) else entry
        if not retain_cached_agent(agent):
            # Preserve the pre-existing memory-only cache contract; it grants no SDK ownership.
            home = runner.session_store._profile_home_for_key(key)
            await _close_memory(agent, home if home is not None else get_default_hermes_root())
    for agent in retained_agents(runner):
        owner = owner_for(agent)
        await _close_memory(agent, owner.home)
    # The SAME SDK resource book is consumed by finish(), after original worker pools join.
