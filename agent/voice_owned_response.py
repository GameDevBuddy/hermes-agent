"""Original voice response context and iterator, closed before its existing SDK owner."""
from __future__ import annotations

import inspect

from agent.auxiliary_owned_close import _same_callable
from hermes_owned_gateway import OwnedGatewayUnknown


class VoiceResponse:
    def __init__(self, owner, kwargs):
        self.owner, self.kwargs = owner, kwargs
        self.context = self.response = self.http_response = self.iterator = None
        self.consumer_iterator = None
        self.enter_attempted = self.exit_attempted = self.exit_completed = False
        self.iterator_attempted = self.iterator_close_attempted = False
        self.error = self.close_result = None

    def require_current(self):
        self.owner.require_creator()
        self.require_links()

    def require_links(self):
        client = self.owner.client
        if (self.owner.response is not self or client.audio is not self.audio
                or self.audio.speech is not self.speech or self.audio._client is not client
                or self.speech._client is not client or self.speech.with_streaming_response is not self.streaming
                or self.streaming._speech is not self.speech or not _same_callable(self.streaming.create, self.factory)):
            raise OwnedGatewayUnknown("voice-response-original-sdk-resource-changed")
        if self.context is not None:
            if (type(self.context) is not self.spec[0]
                    or getattr(self.context.__enter__, "__func__", None) is not self.spec[1]
                    or getattr(self.context.__exit__, "__func__", None) is not self.spec[2]):
                raise OwnedGatewayUnknown("voice-original-response-context-changed-or-opaque")
        if self.response is not None:
            if (not isinstance(self.response, self.spec[3])
                    or self.context._ResponseContextManager__response is not self.response
                    or self.response.http_response is not self.http_response
                    or not isinstance(self.http_response, self.spec[5])
                    or getattr(self.response.close, "__func__", None) is not self.spec[4]
                    or getattr(self.http_close, "__func__", None) is not self.spec[6]
                    or not _same_callable(self.http_response.close, self.http_close)):
                raise OwnedGatewayUnknown("voice-original-response-or-closer-changed")

    def __enter__(self):
        try:
            client = self.owner.client
            self.audio, self.speech = client.audio, client.audio.speech
            self.streaming = self.speech.with_streaming_response
            self.factory = self.streaming.create
            if self.owner.record is not None:
                from openai._response import ResponseContextManager, APIResponse
                import httpx
                self.spec = (ResponseContextManager, ResponseContextManager.__enter__, ResponseContextManager.__exit__,
                             APIResponse, APIResponse.close, httpx.Response, httpx.Response.close)
                self.require_current()
            self.enter_attempted = True
            self.context = self.factory(**self.kwargs)
            if self.owner.record is not None:
                self.require_current()
            self.response = self.context.__enter__()
            self.http_response = getattr(self.response, "http_response", None)
            self.http_close = getattr(self.http_response, "close", None)
            if self.owner.record is not None:
                self.require_current()
            return self.response
        except BaseException as error:
            self.error = error
            self.owner.fail(error)
            raise

    def stream(self):
        if self.owner.record is not None:
            self.require_current()
            if self.iterator_attempted:
                raise OwnedGatewayUnknown("voice-original-audio-iterator-reentry")
        self.iterator_attempted = True
        self.iterator = self.response.iter_bytes()
        if self.owner.record is not None:
            self.require_current()
            if not inspect.isgenerator(self.iterator):
                raise OwnedGatewayUnknown("voice-audio-iterator-normal-owner-opaque")
            self.consumer_iterator = self.chunks()
            return self.consumer_iterator
        return self.iterator

    def chunks(self):
        try:
            while True:
                self.require_current()
                try:
                    chunk = next(self.iterator)
                except StopIteration:
                    return
                self.require_current()
                yield chunk
        except BaseException as error:
            self.owner.note(error)
            raise

    def close_iterator(self):
        if self.consumer_iterator is not None:
            if inspect.getgeneratorstate(self.consumer_iterator) == inspect.GEN_RUNNING:
                raise OwnedGatewayUnknown("voice-audio-consumer-user-still-active")
            self.consumer_iterator.close()
            self.require_current()
        if self.iterator is None:
            return
        if not inspect.isgenerator(self.iterator):
            raise OwnedGatewayUnknown("voice-audio-iterator-normal-owner-opaque")
        if inspect.getgeneratorstate(self.iterator) == inspect.GEN_CLOSED:
            return
        if self.iterator_close_attempted or inspect.getgeneratorstate(self.iterator) == inspect.GEN_RUNNING:
            raise OwnedGatewayUnknown("voice-audio-iterator-user-still-active")
        self.iterator_close_attempted = True
        self.iterator.close()
        self.require_current()
        if inspect.getgeneratorstate(self.iterator) != inspect.GEN_CLOSED:
            raise OwnedGatewayUnknown("voice-audio-iterator-normal-close-unconfirmed")

    def __exit__(self, exc_type, exc, traceback):
        self.owner.note(exc)
        if self.owner.record is None:
            try:
                if inspect.isgenerator(self.iterator):
                    self.iterator.close()
            finally:
                result = self.context.__exit__(exc_type, exc, traceback)
            return result
        try:
            self.require_current()
            if self.exit_attempted:
                raise OwnedGatewayUnknown("voice-original-response-close-reentry")
            self.exit_attempted = True
            self.owner.record.context.run(self.normal_exit, exc_type, exc, traceback)
            self.exit_completed = True
        except BaseException as error:
            self.error = error
            self.owner.fail(error)
            raise
        return self.close_result

    def normal_exit(self, exc_type, exc, traceback):
        self.require_current()
        self.close_iterator()
        self.require_current()
        self.close_result = self.context.__exit__(exc_type, exc, traceback)
        self.require_current()
        if self.close_result or self.response.is_closed is not True or self.http_response.is_closed is not True:
            raise OwnedGatewayUnknown("voice-original-response-close-unconfirmed")

    def require_settled(self):
        # Observer-only settlement on the source's normal owning loop. It never
        # substitutes a new creator, response exit or client closer.
        self.require_links()
        if (not self.exit_completed or self.error is not None or self.response.is_closed is not True
                or self.response.http_response is not self.http_response
                or self.http_response.is_closed is not True
                or self.context._ResponseContextManager__response is not self.response
                or (self.consumer_iterator is not None
                    and inspect.getgeneratorstate(self.consumer_iterator) != inspect.GEN_CLOSED)
                or (self.iterator is not None and inspect.getgeneratorstate(self.iterator) != inspect.GEN_CLOSED)):
            raise OwnedGatewayUnknown("voice-original-response-user-remains-held")
