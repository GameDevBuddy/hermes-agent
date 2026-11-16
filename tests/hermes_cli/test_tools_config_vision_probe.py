"""Vision availability is metadata, not an owned SDK/HTTP resource allocation."""
import asyncio
from contextlib import contextmanager

import httpx
import openai
import pytest

from agent import auxiliary_client as auxiliary
from agent import process_bootstrap
from hermes_cli import tools_config
from tests.agent.auxiliary_owned_fixture import bind_source


@pytest.mark.parametrize("available", [True, False], ids=["available", "unavailable"])
def test_vision_availability_uses_probe_without_claims_or_clients(monkeypatch, tmp_path, available):
    async def scenario():
        source, _runner = bind_source(monkeypatch, tmp_path)
        cache = {}
        monkeypatch.setattr(auxiliary, "_client_cache", cache)
        task_config = (
            {"provider": "custom", "model": "fixture-vision",
             "base_url": "https://fixture.invalid/v1", "api_key": "fixture"}
            if available else {"provider": "unconfigured-vision-fixture"}
        )
        monkeypatch.setattr(tools_config, "load_config", dict)
        monkeypatch.setattr(auxiliary, "_get_auxiliary_task_config", lambda task: task_config)

        constructor_calls = []

        def forbidden_constructor(*args, **kwargs):
            constructor_calls.append((args, kwargs))
            pytest.fail("vision availability must not construct SDK/HTTP clients")

        # Fail before a real SDK import/construction or HTTP transport can occur.
        monkeypatch.setattr(auxiliary, "_load_openai_cls", forbidden_constructor)
        for name in ("OpenAI", "AsyncOpenAI"):
            monkeypatch.setattr(openai, name, forbidden_constructor)
        for name in ("Client", "AsyncClient"):
            monkeypatch.setattr(httpx, name, forbidden_constructor)
        monkeypatch.setattr(process_bootstrap, "build_keepalive_http_client", forbidden_constructor)

        actual_probe_mode = auxiliary.aux_probe_mode
        actual_resolver = auxiliary.resolve_vision_provider_client
        probe_entries = []
        resolver_probe_states = []
        resolved_clients = []

        @contextmanager
        def observed_probe_mode():
            with actual_probe_mode():
                probe_entries.append(True)
                yield

        def observed_resolver(*args, **kwargs):
            resolver_probe_states.append(auxiliary._aux_probe_active())
            result = actual_resolver(*args, **kwargs)
            resolved_clients.append(result[1])
            return result

        monkeypatch.setattr(auxiliary, "aux_probe_mode", observed_probe_mode)
        monkeypatch.setattr(auxiliary, "resolve_vision_provider_client", observed_resolver)
        claims_before = list(source.auxiliary_clients)
        credit_before = source.auxiliary_credit
        cache_before = dict(cache)
        assert not auxiliary._aux_probe_active()

        for _ in range(320):
            assert tools_config._toolset_has_keys("vision") is available
            assert not auxiliary._aux_probe_active(), "probe mode leaked into execution"
            assert source.auxiliary_clients == claims_before
            assert source.auxiliary_credit == credit_before
            assert auxiliary._client_cache is cache and cache == cache_before

        assert len(probe_entries) == 320
        assert resolver_probe_states == [True] * 320
        assert len(resolved_clients) == 320
        if available:
            assert all(isinstance(client, auxiliary._AuxProbeClientStub) for client in resolved_clients)
        else:
            assert resolved_clients == [None] * 320
        assert constructor_calls == []

    asyncio.run(scenario())
