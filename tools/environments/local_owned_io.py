"""Original local pipe EOF, stdin write, worker join and passive normal wait observations."""
import threading
import time
import functools
import logging
from types import BuiltinMethodType

from agent.auxiliary_owned_close import _same_callable
from agent.memory_provider import spawn_context_thread
from tools import terminal_tool_owned as owned
from tools.environments.local_owned import require_process

logger = logging.getLogger(__name__)


def _same_pipe_method(left, right):
    # Built-in IO methods have no __func__ and are rebound on each lookup.
    # Their exact-type equality checks the C method; also pin the pipe by identity.
    return (_same_callable(left, right)
            or (type(left) is BuiltinMethodType and type(right) is BuiltinMethodType
                and left.__self__ is right.__self__ and left == right))


def wait_on_owner(method):
    @functools.wraps(method)
    def invoke(env, proc, timeout=120, **kwargs):
        if owned.active() is not None or owned.find(proc) is not None or owned.find(env) is not None:
            return wait(env, proc, timeout, **kwargs)
        return method(env, proc, timeout, **kwargs)
    return invoke


def _require_io(record):
    record.require()
    parent = record.original_parent
    require_process(parent, record.process_origin)
    index = 0 if record.kind == "pipe-reader" else 2
    if (record.pipe is not parent.pipes[index] or record.thread is not record.obj
            or not _same_pipe_method(record.pipe.close, record.close_method)):
        owned.refuse(record, "terminal-original-pipe-worker-association-changed")
    if record.kind == "pipe-writer" and (
            getattr(record.pipe, "buffer", record.pipe) is not record.target
            or not _same_pipe_method(record.target.write, record.io_method)
            or not _same_pipe_method(record.target.close, record.target_close)):
        owned.refuse(record, "terminal-original-stdin-target-or-method-changed")


def _start(parent, kind, pipe, body, output=None):
    record = owned.reserve(kind, spawn_context_thread, parent)
    record.pipe, record.output = pipe, output
    record.process_origin = parent.origin
    record.eof = record.normal_closed = False
    record.finished = threading.Event()
    record.close_method = pipe.close
    record.io_method = pipe.read if kind == "pipe-reader" else getattr(pipe, "buffer", pipe).write
    record.target = pipe if kind == "pipe-reader" else getattr(pipe, "buffer", pipe)
    record.target_close = record.target.close
    record.attempted = True

    def run():
        try:
            _require_io(record)
            body(record)
            _require_io(record)
            record.done = True
        except BaseException as error:
            owned.fail(record, error)
            logger.exception("Original terminal pipe worker failed")
        finally:
            record.finished.set()

    record.thread = spawn_context_thread(run, name="owned-terminal-" + kind, daemon=True)
    owned.bind(record, record.thread)
    setattr(parent, "reader" if kind == "pipe-reader" else "writer", record)
    record.thread.start()
    return record


def _read(record):
    while True:
        _require_io(record)
        if not _same_pipe_method(record.pipe.read, record.io_method):
            owned.refuse(record, "terminal-original-read-method-changed")
        piece = record.io_method(4096)
        _require_io(record)
        if piece in ("", b""):
            record.eof = True
            break
        record.output.append(piece.decode("utf-8", "replace") if isinstance(piece, bytes) else piece)
    record.close_result = record.close_method()
    _require_io(record)
    if record.pipe.closed is not True:
        owned.refuse(record, "terminal-original-eof-pipe-close-unconfirmed")
    record.normal_closed = True


def drain(proc, output):
    record = owned.find(proc)
    if record is None:
        return None
    require_process(record)
    if record.reader is not None:
        owned.refuse(record, "terminal-original-pipe-reader-reentry")
    return _start(record, "pipe-reader", record.pipes[0], _read, output).thread


def pipe_stdin(proc, data):
    record = owned.find(proc)
    if record is None:
        return False
    require_process(record)
    if record.writer is not None or record.pipes[2] is None:
        owned.refuse(record, "terminal-original-stdin-writer-unavailable-or-reentry")

    def write(worker):
        raw = data.encode("utf-8", "surrogateescape") if isinstance(data, str) else data
        worker.payload = raw
        if (getattr(worker.pipe, "buffer", worker.pipe) is not worker.target
                or not _same_pipe_method(worker.target.write, worker.io_method)
                or not _same_pipe_method(worker.target.close, worker.target_close)):
            owned.refuse(worker, "terminal-original-stdin-target-changed")
        try:
            worker.written = worker.io_method(raw)
            if worker.written != len(raw):
                raise RuntimeError("original-terminal-short-stdin-write")
        except (BrokenPipeError, OSError) as error:
            worker.write_error = error
        _require_io(worker)
        if getattr(worker.pipe, "buffer", worker.pipe) is not worker.target:
            owned.refuse(worker, "terminal-original-stdin-target-changed-after-write")
        worker.close_result = worker.target_close()
        _require_io(worker)
        if worker.target.closed is not True:
            owned.refuse(worker, "terminal-original-stdin-close-unconfirmed")
        worker.normal_closed = True

    writer = _start(record, "pipe-writer", record.pipes[2], write)
    proc._hermes_stdin_thread, proc._hermes_stdin_errors = writer.thread, []
    return True


def _check_deadline(record, deadline, watch_tid):
    from tools.interrupt import is_interrupted, is_thread_interrupted, consume_yield
    record.require()
    if is_interrupted() or is_thread_interrupted(watch_tid):
        return True
    if consume_yield(watch_tid):
        owned.refuse(record, "original-terminal-yielded-work-retained")
    if time.monotonic() >= deadline:
        owned.refuse(record, "original-terminal-passive-deadline-expired")
    return False


def _interrupt_process(process):
    """Stop this command's tree; only a confirmed stop is an ordinary result."""
    import psutil
    from tools.environments import local

    require_process(process)
    proc = process.obj
    try:
        descendants = (local._process_descendants_windows(proc) if local._IS_WINDOWS
                       else psutil.Process(proc.pid).children(recursive=True))
    except psutil.NoSuchProcess:
        descendants = []
    # This is the explicit Stop path, not legacy cleanup of an unknown process.
    # Reuse the local backend's tree teardown, then still require exit and EOF.
    (local._kill_process_windows if local._IS_WINDOWS else local._kill_process_group_posix)(proc)
    process.methods[1](timeout=2)
    _, alive = psutil.wait_procs(descendants, timeout=2)
    if alive:
        owned.refuse(process, "original-terminal-interrupted-descendant-still-running")
    require_process(process)


def _join_io(worker, deadline, wait):
    if worker is None:
        return
    _require_io(worker)
    if not worker.finished.wait(max(0.0, deadline - time.monotonic())):
        owned.refuse(wait, "original-terminal-pipe-worker-not-naturally-complete")
    worker.thread.join()
    _require_io(worker)
    if worker.thread.is_alive() or not worker.done or not worker.normal_closed:
        owned.refuse(wait, "original-terminal-pipe-worker-closure-unconfirmed")


def wait(env, proc, timeout, *, bounded_capture=False, watch_interrupt_tid=None, output=None, yield_handler=None):
    process = owned.find(proc)
    if process is None:
        if owned.require_env(env) is not None:
            owned.refuse(owned.find(env), "terminal-original-process-constructor-unavailable")
        return None
    require_process(process)
    if process.environment.obj is not env or process.wait_attempted:
        owned.refuse(process, "terminal-original-wait-environment-or-attempt-unavailable")
    process.wait_attempted = True
    record = owned.reserve("wait", wait, process)
    record.obj, record.worker = proc, threading.current_thread()
    record.process_origin = process.origin
    process.wait_record = record
    record.active = record.attempted = True
    from tools.environments import base
    record.output = output if output is not None else base._new_output_collector(proc, bounded_capture)
    deadline = time.monotonic() + float(timeout)
    interrupted = False
    try:
        record.reader_thread = base._start_drain_thread(proc, record.output)
        reader, writer = owned.find(record.reader_thread), process.writer
        record.original_io = (reader, writer, record.worker)
        if process.reader is not reader or reader.original_parent is not process:
            owned.refuse(record, "terminal-original-reader-constructor-association-changed")
        while True:
            require_process(process, record.process_origin)
            record.polled_exit = process.methods[0]()
            require_process(process, record.process_origin)
            if record.polled_exit is not None:
                break
            if _check_deadline(record, deadline, watch_interrupt_tid):
                _interrupt_process(process)
                interrupted = True
                deadline = time.monotonic() + 2
                break
            reader.finished.wait(0.005)
        record.wait_result = process.methods[1](timeout=0)
        require_process(process, record.process_origin)
        if type(record.wait_result) is not int or proc.returncode != record.wait_result:
            owned.refuse(record, "terminal-original-parent-normal-wait-unconfirmed")
        _join_io(reader, deadline, record)
        _join_io(writer, deadline, record)
        require_process(process, record.process_origin)
        if (process.reader is not reader or process.writer is not writer
                or record.worker is not record.original_io[2] or not reader.eof):
            owned.refuse(record, "terminal-original-pipe-eof-not-observed")
        record.spill = record.output._spill_fh
        rendered = record.output.render(suffix="\n[Command interrupted by user]" if interrupted else "")
        record.result = env._finalize_wait_result(record.output, rendered, 130 if interrupted else record.wait_result)
        if interrupted:
            record.result["hermes_interrupted"] = True
        if record.spill is not None and record.spill.closed is not True:
            owned.refuse(record, "terminal-original-output-spill-not-closed")
        require_process(process, record.process_origin)
        record.require()
        process.wait_done = process.done = record.done = True
        return record.result
    except BaseException as error:
        owned.fail(record, error)
        owned.fail(process, error)
        raise
    finally:
        record.active = False


def require_settled(process):
    require_process(process)
    wait_record = process.wait_record
    if (not process.wait_attempted or not process.wait_done or not process.done or wait_record is None
            or not wait_record.done or wait_record.active or wait_record.original_parent is not process
            or process.obj.returncode != wait_record.wait_result):
        owned.refuse(process, "terminal-original-process-normal-completion-unavailable")
    if (process.reader is not wait_record.original_io[0] or process.writer is not wait_record.original_io[1]
            or wait_record.worker is not wait_record.original_io[2]):
        owned.refuse(process, "terminal-original-completion-worker-associations-changed")
    for worker in (process.reader, process.writer):
        if worker is not None:
            _require_io(worker)
            if not worker.done or worker.thread.is_alive() or not worker.normal_closed:
                owned.refuse(process, "terminal-original-io-worker-remains")
    if process.reader is None or not process.reader.eof:
        owned.refuse(process, "terminal-original-eof-worker-unavailable")
