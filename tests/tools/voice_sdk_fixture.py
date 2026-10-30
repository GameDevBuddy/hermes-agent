"""Voice SDK lower factories only: no real SDK, HTTP, audio, configuration or provider work."""
import importlib
import sys
from types import ModuleType, SimpleNamespace as Data

import hermes_owned_gateway as ownership
from agent import auxiliary_owned as owned
from tests.agent.auxiliary_owned_fixture import bind_source


class Transport:
    def __init__(self, case):
        self.case, self.closed = case, False

    def close(self):
        self.case.events.append("transport.close")
        self.closed = True


class HTTP:
    def __init__(self, case):
        self.case, self.is_closed = case, False
        self._transport, self._mounts = Transport(case), {}

    def close(self):
        self.case.events.append("http.close")
        self._transport.close()
        self.is_closed = True


class ResponseHTTP:
    def __init__(self, case):
        self.case, self.is_closed = case, False

    def close(self):
        self.case.events.append("response.close")
        if self.case.response_close_error is not None:
            raise self.case.response_close_error
        self.is_closed = True
        if self.case.response_during_close is not None:
            self.case.response_during_close()


class Response:
    def __init__(self, case):
        self.case = case
        self.http_response = ResponseHTTP(case)
        self.headers = {"x-audio-sample-rate": "44100"}

    @property
    def is_closed(self):
        return self.http_response.is_closed

    def close(self):
        self.http_response.close()

    def iter_bytes(self):
        try:
            yield self.case.chunks[0]
            if self.case.iteration_error is not None:
                raise self.case.iteration_error
            yield from self.case.chunks[1:]
        finally:
            self.case.events.append("iterator.finished")


class ResponseContextManager:
    def __init__(self, case):
        self.case, self.__response = case, None

    def __enter__(self):
        self.case.events.append("response.enter")
        if self.case.enter_error is not None:
            raise self.case.enter_error
        self.__response = self.case.response = Response(self.case)
        return self.__response

    def __exit__(self, *exc):
        self.case.events.append("response.exit")
        self.__response.close()


class SDK:
    case = None

    def __init__(self, **kwargs):
        case = self.case
        case.events.append("client.construct")
        case.clients.append(self)
        case.kwargs = kwargs
        case.before_factory.append(tuple(case.source.auxiliary_clients) if case.source is not None else ())
        self._client = HTTP(case)
        speech = Data(_client=self)
        speech.with_streaming_response = Data(_speech=speech, create=self.speech)
        self.audio = Data(_client=self, speech=speech)
        if case.constructor_error is not None:
            raise case.constructor_error

    def speech(self, **kwargs):
        self.case.speech_kwargs = kwargs
        self.case.events.append("response.construct")
        if self.case.response_factory_error is not None:
            raise self.case.response_factory_error
        self.case.context = ResponseContextManager(self.case)
        return self.case.context

    def is_closed(self):
        return self._client.is_closed

    def close(self):
        self.case.events.append("client.close")
        self._client.close()
        if self.case.during_close is not None:
            self.case.during_close()
        if self.case.client_close_error is not None:
            raise self.case.client_close_error


def module(name, **values):
    result = ModuleType(name)
    result.__dict__.update(values)
    return result


class Case:
    def __init__(self, monkeypatch, tmp_path, *, managed):
        self.patch, self.events, self.clients, self.before_factory = monkeypatch, [], [], []
        self.source, self.runner = bind_source(monkeypatch, tmp_path) if managed else (None, None)
        if not managed:
            monkeypatch.setattr(ownership, "_CURRENT", None)
        self.chunks = [b"\x01\x02", b"\x03\x04"]
        self.constructor_error = self.response_factory_error = self.enter_error = None
        self.iteration_error = self.response_close_error = self.client_close_error = self.during_close = None
        self.response_during_close = None
        self.context = self.response = None
        self.kwargs = self.speech_kwargs = None
        monkeypatch.setattr(SDK, "case", self)
        import openai
        import openai._response
        import httpx
        monkeypatch.setattr(openai, "OpenAI", SDK)
        monkeypatch.setattr(openai._response, "ResponseContextManager", ResponseContextManager)
        monkeypatch.setattr(openai._response, "BaseAPIResponse", Response)
        monkeypatch.setattr(openai._response, "APIResponse", Response)
        monkeypatch.setattr(httpx, "Client", HTTP)
        monkeypatch.setattr(httpx, "HTTPTransport", Transport)
        monkeypatch.setattr(httpx, "Response", ResponseHTTP)
        monkeypatch.setitem(sys.modules, "tools.tts_tool", module("tools.tts_tool",
            _get_provider=lambda *a: "openai", _load_tts_config=dict))
        monkeypatch.setitem(sys.modules, "tools.tts_tool_plugins", module("tools.tts_tool_plugins",
            _plugin_pcm_streaming_provider=lambda *a: None, _plugin_voice_kwargs=lambda *a: {}))
        monkeypatch.setitem(sys.modules, "tools.tts_tool_providers", module("tools.tts_tool_providers",
            DEFAULT_XAI_SAMPLE_RATE=24000))
        monkeypatch.setitem(sys.modules, "tools.tts_tool_openai", module("tools.tts_tool_openai",
            _openai_extra_body=lambda section: {"instructions": section["instructions"]} if "instructions" in section else {}))
        self.stt_config = {"openai": {"timeout": 73, "max_retries": 2}}
        monkeypatch.setitem(sys.modules, "tools.transcription_tools", module("tools.transcription_tools",
            _load_stt_config=lambda: self.stt_config))
        import hermes_cli.config
        monkeypatch.setattr(hermes_cli.config, "get_env_value", lambda key: None)
        self.tts = importlib.import_module("tools.tts_streaming")
        self.stt = importlib.import_module("tools.transcription_cloud")
        monkeypatch.setattr(self.tts, "resolve_openai_audio_api_key", lambda: "fixture-key")

    def stream(self, **config):
        self.streamer = self.tts.OpenAIStreamer({}, config)
        return self.streamer.stream("fixture speech")

    def transcribe(self, body=None):
        return self.stt._with_openai_client("fixture-key", "https://fixture.invalid/v1", "fixture.wav", "fixture",
            body or (lambda client: {"success": True, "transcript": "fixture transcript"}))

    def record(self):
        return owned.find(self.clients[-1])
