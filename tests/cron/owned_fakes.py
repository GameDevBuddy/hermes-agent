"""In-memory executor/thread/worker models. No cron, command, agent or kernel worker runs."""
import concurrent.futures


def deny(*args, **kwargs):
    raise AssertionError("A real cron worker/agent/process/provider/store/network boundary was reached")


class Semaphore:
    def acquire(self, **kwargs):
        return False


class Queue:
    def __init__(self):
        self.items = []
        self.on_put = None

    def put(self, item):
        self.items.append(item)
        if self.on_put is not None:
            self.on_put(item)


class WorkerContext:
    def run(self, task):
        fn, args, kwargs = task
        return fn(*args, **kwargs)


class Pool:
    instances = []
    on_init = on_shutdown = None
    immediate = True

    def __init__(self, max_workers=None, thread_name_prefix="", **kwargs):
        import threading
        self._max_workers, self._thread_name_prefix = max_workers or 3, thread_name_prefix
        self._threads, self._work_queue = set(), Queue()
        self._shutdown_lock, self._idle_semaphore = threading.Lock(), Semaphore()
        self._broken, self._shutdown = False, False
        self.shut_calls, self.vanilla_calls = [], []
        type(self).instances.append(self)
        if type(self).on_init is not None:
            type(self).on_init(self)

    def _create_worker_context(self):
        return WorkerContext()

    def _resolve_work_item_task(self, fn, args, kwargs):
        return fn, args, kwargs

    def _adjust_thread_count(self):
        deny()

    def submit(self, callback, *args, **kwargs):
        # Ordinary default control only: still no product execution, thread or SDK.
        future = concurrent.futures.Future()
        self.vanilla_calls.append((callback, args, kwargs, future))
        return future

    def shutdown(self, wait=True, *, cancel_futures=False):
        self.shut_calls.append((wait, cancel_futures))
        assert cancel_futures is False
        self._shutdown = True
        if type(self).on_shutdown is not None:
            type(self).on_shutdown(self, wait)
        if wait:
            for thread in self._threads:
                thread.join()


class Thread:
    instances = []
    current = None
    on_init = on_start = on_join = None

    def __init__(self, name, target, args):
        self.name, self.target, self.args = name, target, args
        self.alive, self.starts, self.joins = False, 0, []
        type(self).instances.append(self)
        if type(self).on_init is not None:
            type(self).on_init(self)

    def start(self):
        self.starts += 1
        self.alive = True
        if type(self).on_start is not None:
            type(self).on_start(self)
        if Pool.immediate:
            self.run_one()

    def run_one(self):
        pool = self.args[0]()
        item = next((item for item in pool._work_queue.items if item is not None and not item.future.done()), None)
        if item is None:
            return
        previous, type(self).current = type(self).current, self
        try:
            item.run(self.args[1])  # actual stdlib WorkItem, mocked lower worker context
        finally:
            type(self).current = previous

    def join(self, timeout=None):
        self.joins.append(timeout)
        if type(self).on_join is not None:
            type(self).on_join(self)
        self.alive = False

    def is_alive(self):
        return self.alive
