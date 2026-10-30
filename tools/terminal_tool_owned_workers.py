"""Original terminal-owned deadline and janitor thread factories and consumers."""
from agent.auxiliary_owned_close import _same_callable
from agent.memory_provider import spawn_context_thread
from tools import terminal_tool_owned as owned


def _janitor_thread(target, context):
    return spawn_context_thread(context.run, name="owned-terminal-janitor", args=(target,), daemon=True)


def start_deadline(target, *, name, done, box):
    parent = owned._command.get()
    if parent is None:
        return False
    record = owned.reserve("executor", spawn_context_thread, parent)
    record.target, record.finished, record.box = target, done, box
    record.attempted = True
    try:
        thread = spawn_context_thread(target, name=name, daemon=True)
        owned.bind(record, thread)
        record.methods = (thread.join, thread.is_alive)
        thread.start()
    except BaseException as error:
        owned.fail(record, error)
        raise
    return True


def finish_command(record):
    for child in record.children:
        if child.kind == "executor":
            require_thread(child)
            if not child.finished.is_set() or "exc" in child.box:
                owned.refuse(child, "terminal-original-deadline-worker-not-finished")
            child.methods[0]()
            require_thread(child)
            if child.methods[1]():
                owned.refuse(child, "terminal-original-deadline-worker-remains")
            child.done = True


def require_thread(record):
    record.require()
    if (owned.find(record.obj) is not record or not _same_callable(record.obj.join, record.methods[0])
            or not _same_callable(record.obj.is_alive, record.methods[1])):
        owned.refuse(record, "terminal-original-thread-method-or-object-changed")


def start_janitor(module):
    if owned.active() is None:
        return False
    with module._env_lock:
        original = module._cleanup_thread
        if original is not None:
            record = owned.find(original)
            if record is None or record.kind != "janitor" or record.module is not module:
                owned.unsupported("pre-source-terminal-janitor")
            require_thread(record)
            if not original.is_alive():
                owned.refuse(record, "terminal-original-janitor-stopped-before-normal-shutdown")
            return True
        record = owned.reserve("janitor", _janitor_thread)
        record.module, record.target = module, module._cleanup_thread_worker
        record.attempted = True
        try:
            thread = _janitor_thread(record.target, record.context)
            owned.bind(record, thread)
            record.methods = (thread.join, thread.is_alive)
            module._cleanup_running, module._cleanup_thread = True, thread
            thread.start()
        except BaseException as error:
            owned.fail(record, error)
            raise
    return True
