"""Lower executor/thread models for source tests; their joins are never native evidence."""
import asyncio
from concurrent.futures import Future
import threading

_actual_current_thread = threading.current_thread


def deny(*args, **kwargs):
    raise AssertionError("Real product SDK/network/process/thread/store boundary reached in source-only tests")


class Worker:
    def __init__(self):
        self.alive, self.joins = True, []

    def join(self, timeout=None):
        self.joins.append(timeout)
        self.alive = False

    def is_alive(self):
        return self.alive


class Executor:
    """Execute only fixture callbacks in memory while retaining their original modeled worker."""
    current = None

    def __init__(self, max_workers=None, thread_name_prefix="", **kwargs):
        self._threads, self._shutdown = set(), False
        self.shutdown_calls = []

    @staticmethod
    def current_thread():
        return Executor.current or _actual_current_thread()

    def submit(self, callback, *args, **kwargs):
        if self._shutdown:
            raise RuntimeError("cannot schedule new futures after shutdown")
        future, worker = Future(), Worker()
        self._threads.add(worker)
        previous, Executor.current = Executor.current, worker
        try:
            future.set_running_or_notify_cancel()
            future.set_result(callback(*args, **kwargs))
        except BaseException as error:
            future.set_exception(error)
        finally:
            Executor.current = previous
        return future

    def shutdown(self, wait=True, *, cancel_futures=False):
        if cancel_futures:
            deny()
        self.shutdown_calls.append((wait, cancel_futures))
        self._shutdown = True
        if wait:
            for worker in self._threads:
                worker.join()


class DriverThread:
    """No OS thread: only asyncio's ordinary driver shutdown callback may execute here."""
    def __init__(self, target=None, args=(), kwargs=None, name=None, daemon=None):
        self.target, self.args, self.kwargs = target, args, kwargs or {}
        self.ident, self.alive = None, False

    def start(self):
        if getattr(self.target, "__func__", None) is not asyncio.base_events.BaseEventLoop._do_shutdown:
            # Model unrelated import-time background thread factories without
            # executing their product loops. Their lifetimes are not accepted.
            self.alive = True
            return
        self.alive = True
        try:
            self.target(*self.args, **self.kwargs)
        finally:
            self.alive = False

    def join(self, timeout=None):
        if self.alive:
            deny()

    def is_alive(self):
        return self.alive
