"""Original browser command/helper pipes and waits; opaque harness daemons are not adopted."""
from __future__ import annotations

import functools

from tools import browser_owned as owned
from tools import browser_owned_workers as workers
from tools.browser_owned_launcher import popen, require_process

MAX_OUTPUT = 16 * 1024 * 1024


def entry(factory):
    @functools.wraps(factory)
    def invoke(*args, **kwargs):
        record = owned.reserve("command", factory)
        if record is None:
            return factory(*args, **kwargs)
        record.session = None
        token = owned._command.set(record)
        try:
            return owned.call(record, factory, *args, **kwargs)
        finally:
            owned._command.reset(token)
    return invoke


def daemon_guard(daemon, env):
    if owned.active() is not None:
        record = owned.reserve("opaque-harness", daemon_guard)
        record.references, record.arguments = (daemon, env), (daemon, env)
        owned.refuse(record, "browser-harness-daemon-original-normal-owner-unavailable")


def _reader(parent, pipe):
    record = owned.reserve("pipe-reader", _reader, parent)
    owned.bind(record, pipe)
    record.eof, record.chunks, record.count = False, [], 0
    owned.remember_methods(record, ("read", "close"))

    def run():
        while True:
            owned.require_methods(record)
            chunk = record.methods[0][1](16 * 1024)
            owned.require_methods(record)
            require_process(parent)
            if chunk == "":
                record.eof = True
                break
            if not isinstance(chunk, str) or record.count + len(chunk.encode("utf-8")) > MAX_OUTPUT:
                owned.refuse(record, "browser-original-helper-output-bound-or-type")
            record.count += len(chunk.encode("utf-8"))
            record.chunks.append(chunk)
        record.close_attempted = True
        record.methods[1][1]()
        owned.require_methods(record)
        if record.obj.closed is not True:
            owned.refuse(record, "browser-original-helper-reader-did-not-close")
        record.done = record.closed = True

    record.worker = workers.thread(record, run, "browser-owned-reader", _pipe_thread)
    return record


def _writer(parent, pipe, code):
    record = owned.reserve("pipe-writer", _writer, parent)
    owned.bind(record, pipe)
    record.arguments = (code,)
    owned.remember_methods(record, ("write", "flush", "close"))

    def run():
        owned.require_methods(record)
        record.attempted = True
        written = record.methods[0][1](code)
        owned.require_methods(record)
        if written != len(code):
            owned.refuse(record, "browser-original-helper-input-partial-write")
        record.methods[1][1]()
        owned.require_methods(record)
        record.close_attempted = True
        record.methods[2][1]()
        owned.require_methods(record)
        require_process(parent)
        if pipe.closed is not True:
            owned.refuse(record, "browser-original-helper-input-not-closed")
        record.done = record.closed = True

    record.worker = workers.thread(record, run, "browser-owned-writer", _pipe_thread)
    return record


_pipe_thread = workers.threading.Thread


def helper(cmd, code, env, timeout):
    from tools import browser_use_cli as browser
    from tools.browser_owned_session import current, require_session
    parent = owned._command.get()
    session = current()
    if parent is None or session is None:
        owned.unsupported("helper-without-original-session-command")
    require_session(session)
    parent.session = session
    process = popen(parent, browser.subprocess.Popen, (cmd,), dict(
        stdin=browser.subprocess.PIPE, stdout=browser.subprocess.PIPE, stderr=browser.subprocess.PIPE,
        text=True, encoding="utf-8", errors="replace", env=env, **browser._group_popen_kwargs()))
    record = owned.find(process)
    try:
        if any(pipe is None for pipe in record.pipes):
            owned.refuse(record, "browser-original-helper-three-pipes-required")
        record.stdout = _reader(record, process.stdout)
        record.stderr = _reader(record, process.stderr)
        record.stdin = _writer(record, process.stdin, code)
        record.wait_attempted = True
        record.wait_result = record.methods[1][1](timeout=timeout)
        require_process(record)
        record.wait_done = True
        for pipe in (record.stdin, record.stdout, record.stderr):
            workers.join(pipe.worker)
            owned.require_methods(pipe)
            if not pipe.done or not pipe.closed:
                owned.refuse(pipe, "browser-original-helper-pipe-incomplete")
        if not record.stdout.eof or not record.stderr.eof or process.returncode != record.wait_result:
            owned.refuse(record, "browser-original-helper-exit-or-eof-unconfirmed")
        require_session(session)
        if parent.session is not session:
            owned.refuse(parent, "browser-original-helper-command-session-changed")
        owned.members(record.source)
        return browser.subprocess.CompletedProcess(cmd, record.wait_result,
                                                  "".join(record.stdout.chunks), "".join(record.stderr.chunks))
    except BaseException as error:
        owned.fail(record, error)
        raise
