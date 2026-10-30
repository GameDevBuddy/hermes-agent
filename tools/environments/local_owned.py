"""Before original local Popen birth and actual pipe/worker association; no kernel proof."""
from agent.auxiliary_owned_close import _same_callable
from tools import terminal_tool_owned as owned


def popen(environment, factory, *args, **kwargs):
    parent = owned.require_env(environment)
    if parent is None:
        return factory(*args, **kwargs)
    command = owned._command.get()
    if command is not None and command.original_parent is not parent:
        owned.refuse(command, "terminal-original-command-environment-changed")
    record = owned.reserve("process", factory, command or parent)
    record.environment, record.arguments = parent, (args, kwargs)
    record.wait_attempted = record.wait_done = False
    record.wait_record = record.reader = record.writer = None
    record.attempted = True
    try:
        proc = factory(*args, **kwargs)
        owned.bind(record, proc)
        record.result = proc
        record.pipes = (proc.stdout, proc.stderr, proc.stdin)
        record.methods = (proc.poll, proc.wait)
        record.origin = (record.original_parent, record.environment, proc, record.pipes, record.methods)
        if proc.stdout is None or proc.stderr is not None:
            owned.refuse(record, "terminal-original-merged-stdout-pipe-required")
        require_process(record)
        return proc
    except BaseException as error:
        owned.fail(record, error)
        raise


def require_process(record, origin=None):
    record.require()
    original = record.origin if origin is None else origin
    proc = record.obj
    if (record.origin is not original or record.original_parent is not original[0]
            or record.environment is not original[1] or proc is not original[2]
            or record.pipes is not original[3] or record.methods is not original[4]
            or owned.find(proc) is not record or record.environment is not _environment(record)
            or any(actual is not original for actual, original in zip((proc.stdout, proc.stderr, proc.stdin), record.pipes))
            or not _same_callable(proc.poll, record.methods[0]) or not _same_callable(proc.wait, record.methods[1])):
        owned.refuse(record, "terminal-original-process-pipes-methods-or-parent-changed")
    owned.require_env(record.environment.obj)


def _environment(record):
    parent = record.original_parent
    return parent.original_parent if parent.kind == "command" else parent


def force_guard(env, proc):
    owned.prevent_force(env, proc)


def init_failure(env, error):
    record = owned.find(env)
    if record is not None and (record.error is not None or record.source.terminal_error is not None):
        raise error


def cleanup_guard(env):
    if owned.protect_legacy(env):
        return True
    record = owned.find(env)
    if record is not None:
        owned.require_env(env)
        if (record.cleanup_paths != (env._snapshot_path, env._cwd_file) or not record.cleanup_attempted
                or record.cleanup_done):
            owned.refuse(record, "terminal-original-file-cleanup-context-unavailable")
    return False


def foreground_timeout(env):
    record = owned._command.get() or owned.find(env)
    if record is not None:
        owned.refuse(record, "original-terminal-passive-deadline-expired")
