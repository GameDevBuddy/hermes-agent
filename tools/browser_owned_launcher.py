"""Original local launcher/process/log custody; normal exit is observed on the same Popen."""
from __future__ import annotations

import functools
import os

from tools import browser_owned as owned

_process_origins = {}


def launch(factory):
    @functools.wraps(factory)
    def invoke(key, *args, **kwargs):
        from tools import browser_launcher as launcher
        source = owned.active()
        with launcher._lock:
            existing = launcher._browsers.get(key)
        if existing is not None:
            record = owned.protect(existing)
            if record is not None:
                require_browser(record)
                if record.closed:
                    owned.refuse(record, "original-browser-cannot-be-relaunched-after-close")
                return existing, None
        if source is None:
            return factory(key, *args, **kwargs)
        previous = [r for r in owned.members(source) if r.kind == "launcher" and r.key == key]
        if previous:
            owned.refuse(previous[0], "original-browser-launch-cache-entry-lost")
        record = owned.reserve("launcher", factory)
        record.key, record.process, record.browsers = key, None, ()
        if kwargs.get("ephemeral", True) is not True or kwargs.get("prepare") is not None:
            owned.refuse(record, "browser-shared-or-foreign-profile-normal-lifecycle-unavailable")
        result = owned.call(record, factory, key, *args, **kwargs)
        record.browser_result = result
        if result[0] is None:
            owned.refuse(record, "original-browser-launch-did-not-complete")
        require_browser(owned.require_object(result[0], "browser"))
        return result
    return invoke


def browser_created(browser):
    parent = owned._building.get()
    if parent is None or parent.kind != "launcher":
        if owned.active() is not None:
            owned.unsupported("browser-wrapper-outside-original-launch", browser)
        return browser
    record = owned.reserve("browser", browser_created, parent)
    owned.bind(record, browser)
    record.launcher, record.process = parent, parent.process
    record.state = (browser.key, browser.cdp_url, browser.http_url, browser.user_data_dir, browser.owned,
                    browser.ephemeral, browser.home_key, browser.foreign)
    record.references = (browser, parent.process)
    parent.browsers += (record,)
    record.done = True
    return browser


def popen(parent, factory, args, kwargs):
    record = owned.reserve("process", factory, parent)
    parent.process = record
    record.arguments, record.attempted, record.active = (args, kwargs), True, True
    record.wait_attempted = record.wait_done = False
    record.wait_result = record.pipes = record.pipe_origin = None
    try:
        # The original partial Popen is retained even if __init__ fails after acquiring a handle/pipe.
        if not isinstance(factory, type):
            owned.refuse(record, "browser-original-process-constructor-unavailable")
        proc = factory.__new__(factory)
        owned.bind(record, proc)
        factory.__init__(proc, *args, **kwargs)
        record.pipes = (proc.stdin, proc.stdout, proc.stderr)
        record.pipe_origin = record.pipes
        owned.remember_methods(record, ("poll", "wait"))
        _process_origins[record] = (record.original_parent, proc, record.arguments, record.pipes, record.methods, proc.args)
        record.result = proc
        record.require()
        record.done, record.active = True, False
        return proc
    except BaseException as error:
        owned.fail(record, error)
        raise


def spawn(argv, env, user_data_dir):
    from tools import browser_launcher as launcher
    parent = owned._building.get()
    if parent is None or parent.kind != "launcher":
        owned.unsupported("process-without-original-launch")
        return None
    log_path = os.path.join(user_data_dir, launcher._LAUNCH_LOG)
    log = owned.factory("log-writer", open, log_path, "wb", parent=parent)
    log_record = owned.find(log)
    owned.remember_methods(log_record, ("close",))
    parent.log = log_record
    kwargs = dict(stdin=launcher.subprocess.DEVNULL, stdout=log, stderr=log, env=env,
                  **launcher._spawn_kwargs())
    proc = popen(parent, launcher.subprocess.Popen, (argv,), kwargs)
    parent.require()
    return proc


def require_process(record):
    owned.require_methods(record)
    proc = record.obj
    original = _process_origins[record]
    current = (record.original_parent, proc, record.arguments, record.pipes, record.methods, proc.args)
    if (any(a is not b for a, b in zip(original, current)) or record.pipes is not record.pipe_origin
            or any(a is not b for a, b in zip((proc.stdin, proc.stdout, proc.stderr), record.pipe_origin))):
        owned.refuse(record, "original-browser-process-pipes-changed")


def require_browser(record):
    record.require()
    browser, parent = record.obj, record.launcher
    current = (browser.key, browser.cdp_url, browser.http_url, browser.user_data_dir, browser.owned,
               browser.ephemeral, browser.home_key, browser.foreign)
    if (current != record.state or record.process is not parent.process or record not in parent.browsers
            or record.original_parent is not parent or not browser.owned or not browser.ephemeral or browser.foreign):
        owned.refuse(record, "original-local-browser-launch-association-changed")
    if record.process is None:
        owned.refuse(record, "original-browser-process-was-not-retained")
    require_process(record.process)


def lookup(browsers, processes, key):
    browser = browsers.get(key)
    if browser is None:
        return False, None
    record = owned.protect(browser)
    if record is None:
        return False, None
    require_browser(record)
    if processes.get(key) is not record.process.obj:
        owned.refuse(record, "original-browser-process-cache-association-changed")
    return True, browser


def forbid_discard(obj):
    record = owned.protect(obj)
    if record is not None:
        owned.refuse(record, "original-browser-cannot-be-forgotten-or-force-stopped")


def wait_and_close_log(record):
    require_browser(record)
    process, log = record.process, record.launcher.log
    if process.wait_attempted:
        if not process.wait_done:
            owned.refuse(process, "original-browser-wait-attempt-retained")
    else:
        process.wait_attempted = True
        try:
            process.wait_result = process.methods[1][1]()
            require_process(process)
            if process.wait_result != 0 or process.obj.returncode != 0:
                owned.refuse(process, "original-browser-normal-zero-exit-unconfirmed")
            process.wait_done = True
        except BaseException as error:
            owned.fail(process, error)
            raise
    owned.require_methods(log)
    if not log.close_attempted:
        log.close_attempted = True
        try:
            log.close_result = log.methods[0][1]()
            owned.require_methods(log)
            if log.obj.closed is not True:
                owned.refuse(log, "original-browser-log-writer-did-not-close")
            log.closed = True
        except BaseException as error:
            owned.fail(log, error)
            raise
    elif not log.closed:
        owned.refuse(log, "original-browser-log-close-attempt-retained")
    require_browser(record)
