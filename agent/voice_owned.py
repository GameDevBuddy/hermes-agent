"""Fresh voice SDK users on the existing auxiliary SDK book; no new runtime or authority."""
from __future__ import annotations

import asyncio
from collections.abc import Iterator
import inspect
import threading

from agent import auxiliary_owned as owned
from agent.auxiliary_owned_close import _closed, _same_callable
from hermes_owned_gateway import OwnedGatewayUnknown


class VoiceClient:
    def __init__(self, factory, **kwargs):
        self.factory, self.kwargs = factory, kwargs
        self.record = self.client = self.partial_client = self.inner = None
        self.response = self.body = self.body_result = self.body_stream = None
        self.body_active = self.body_attempted = self.exit_attempted = False
        self.returned_user = None
        self.first_outcome = self.first_error = self.close_error = None
        self.close_completed = False

    def note(self, error):
        if error is not None and self.first_outcome is None:
            self.first_outcome = error
        if error is not None and not isinstance(error, GeneratorExit) and self.first_error is None:
            self.first_error = error

    def fail(self, error):
        self.note(error)
        if self.record is not None:
            owned.fail(self.record, self.first_error or error)

    def origin(self):
        r = self.record
        return (r.source, r.runner, r.runtime, r.source.task, r.source.loop, r.source.job,
                r.creator_thread, r.creator_loop, r.owner_thread, r.owner_loop, r.context, r.primary_owner)

    def _construct(self):
        self.record = owned._construction.get()
        if self.record is None:
            return self.factory(**self.kwargs)
        self.record.voice_owner = self
        self.originals, self.home_key = self.origin(), self.record.home_key
        self.constructor = self.record.factory
        self.creator_task = asyncio.current_task() if self.record.creator_loop is not None else None
        import openai
        import httpx
        self.spec = (openai.OpenAI, openai.OpenAI.close, httpx.Client, httpx.Client.close,
                     httpx.HTTPTransport, httpx.HTTPTransport.close)
        try:
            if self.factory is not self.spec[0] or not isinstance(self.factory, type):
                raise OwnedGatewayUnknown("voice-original-sdk-factory-required")
            self.client = self.factory(**self.kwargs)
            self.inner = getattr(self.client, "_client", None)
            self.transport = getattr(self.inner, "_transport", None)
            self.mount_book = getattr(self.inner, "_mounts", {})
            self.mounts = tuple(self.mount_book.items())
            self.transport_owners = tuple((transport, transport.close, getattr(transport, "_pool", None),
                getattr(getattr(transport, "_pool", None), "close", None))
                for transport in (self.transport, *self.mount_book.values()) if transport is not None)
            self.sdk_close = self.client.close
            self.inner_close = getattr(self.inner, "close", None)
            return self.client
        except BaseException as error:
            # The genuine constructor traceback, not a reconstructed client,
            # owns an unreturned self and any already-created inner transport.
            code = getattr(getattr(self.factory, "__init__", None), "__code__", None)
            traceback = error.__traceback__
            while traceback is not None:
                candidate = traceback.tb_frame.f_locals.get("self")
                if traceback.tb_frame.f_code is code and type(candidate) is self.factory:
                    self.partial_client = candidate
                    self.inner = getattr(candidate, "_client", None)
                traceback = traceback.tb_next
            self.note(error)
            raise

    def __enter__(self):
        try:
            self.client = owned.construct("sdk", self._construct)
            if self.record is not None:
                self.require_creator()
            self.body_active = True
            return self
        except BaseException as error:
            self.fail(error)
            raise

    def normal_implementation(self):
        return (type(self.client) is self.spec[0] and isinstance(self.inner, self.spec[2])
                and _same_callable(self.client.close, self.sdk_close)
                and getattr(self.sdk_close, "__func__", None) is self.spec[1]
                and getattr(self.inner_close, "__func__", None) is self.spec[3]
                and _same_callable(self.inner.close, self.inner_close))

    def require_association(self):
        r = self.record
        r.require_current()
        if (r.voice_owner is not self or any(a is not b for a, b in zip(self.origin(), self.originals))
                or r.home_key != self.home_key or owned.find(self.client) is not r
                or not _same_callable(r.factory, self.constructor)
                or not _same_callable(r.close_method, self.sdk_close)
                or r.client is not self.client or r.inner is not self.inner
                or self.client._client is not self.inner or self.inner._transport is not self.transport
                or self.inner._mounts is not self.mount_book or len(self.mount_book) != len(self.mounts)
                or r.children or r.shared_refs or not r.supported
                or not self.normal_implementation()):
            raise OwnedGatewayUnknown("voice-original-sdk-association-changed-or-unsupported")
        if any(key is not original or self.mount_book[key] is not value
               for key, (original, value) in zip(self.mount_book, self.mounts)):
            raise OwnedGatewayUnknown("voice-original-private-transport-mount-changed")
        self.require_transports()

    def require_transports(self):
        for transport, close, pool, pool_close in self.transport_owners:
            if (not isinstance(transport, self.spec[4])
                    or getattr(close, "__func__", None) is not self.spec[5]
                    or not _same_callable(transport.close, close) or getattr(transport, "_pool", None) is not pool
                    or (pool is not None and not _same_callable(pool.close, pool_close))):
                raise OwnedGatewayUnknown("voice-original-private-transport-closer-changed-or-opaque")

    def require_creator(self):
        from hermes_constants import hermes_home_key
        self.require_association()
        if (threading.current_thread() is not self.record.creator_thread
                or owned.running_loop() is not self.record.creator_loop or hermes_home_key() != self.home_key
                or (self.creator_task is not None and asyncio.current_task() is not self.creator_task)):
            raise OwnedGatewayUnknown("voice-original-creator-thread-loop-profile-required")

    def invoke(self, body):
        self.body = body
        try:
            if self.record is not None:
                self.require_creator()
                if self.body_attempted or not self.body_active:
                    raise OwnedGatewayUnknown("voice-body-user-reentry-or-closed")
            self.body_attempted = True
            self.body_result = body(self.client)
            if self.record is not None:
                self.require_creator()
                if (inspect.isawaitable(self.body_result) or isinstance(self.body_result, Iterator)
                        or inspect.isasyncgen(self.body_result)):
                    self.returned_user = self.body_result
                    raise OwnedGatewayUnknown("voice-body-returned-active-or-opaque-user")
            return self.body_result
        except BaseException as error:
            self.note(error)
            raise

    def speech(self, **kwargs):
        from agent.voice_owned_response import VoiceResponse
        if self.record is not None:
            self.require_creator()
            if self.response is not None or not self.body_active:
                raise OwnedGatewayUnknown("voice-response-user-reentry-or-closed")
        self.response = VoiceResponse(self, kwargs)
        return self.response

    def retain_stream(self, stream):
        self.body_stream = stream
        return stream

    def __exit__(self, exc_type, exc, traceback):
        self.note(exc)
        self.body_active = False
        if self.record is None:
            close = getattr(self.client, "close", None)
            if callable(close):
                close()
            return
        if self.record.error is not None:
            # An earlier response/association failure already holds the exact
            # client. Preserve its propagating outcome; never retry cleanup.
            if exc is None:
                raise self.record.error
            return
        try:
            self.require_creator()
            if self.exit_attempted or self.record.close_attempted or _closed(self.client):
                raise OwnedGatewayUnknown("voice-original-client-close-reentry")
            self.exit_attempted = True
            self.require_users_settled()
            self.record.close_attempted = True
            self.record.close_parent, self.record.close_task = self, self.creator_task
            self.close_thread = threading.current_thread()
            self.record.context.run(self.normal_close)
            self.close_completed = True
            self.record.phase = "closed"
        except BaseException as error:
            self.close_error = error
            self.fail(error)
            raise
        if self.first_error is not None:
            self.fail(self.first_error)

    def normal_close(self):
        self.require_creator()
        self.require_users_settled()
        self.record.close_result = self.sdk_close()
        if inspect.isawaitable(self.record.close_result):
            raise OwnedGatewayUnknown("voice-sync-close-returned-unowned-awaitable")
        self.require_creator()
        self.require_users_settled()
        if not _closed(self.client) or not _closed(self.inner):
            raise OwnedGatewayUnknown("voice-normal-sdk-close-unconfirmed")

    def require_users_settled(self):
        if self.body_active or self.returned_user is not None:
            raise OwnedGatewayUnknown("voice-body-user-still-active")
        if self.body_stream is not None and (not inspect.isgenerator(self.body_stream)
                or inspect.getgeneratorstate(self.body_stream) != inspect.GEN_CLOSED):
            raise OwnedGatewayUnknown("voice-output-stream-user-still-active")
        if self.response is not None:
            self.response.require_settled()

    def require_settled(self):
        try:
            self.require_association()
            self.require_users_settled()
            if (not self.close_completed or not self.exit_attempted or not self.record.close_attempted
                    or self.record.close_parent is not self or self.record.close_task is not self.creator_task
                    or self.close_thread is not self.record.creator_thread
                    or self.record.phase != "closed" or not _closed(self.client) or not _closed(self.inner)):
                raise OwnedGatewayUnknown("voice-original-normal-close-still-held")
        except BaseException as error:
            self.fail(error)
            raise
