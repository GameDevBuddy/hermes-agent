"""Original local terminal source settlement; no descendant/kernel/native permission proof."""
from dataclasses import dataclass
import os

from tools import terminal_tool_owned as owned
from tools.environments import local_owned_io as io
from tools.terminal_tool_owned_workers import require_thread


@dataclass(frozen=True)
class OriginalBook:
    source: object
    book: object
    records: tuple
    nodes: tuple
    processes: tuple
    janitors: tuple


def _capture(source):
    records = tuple(source.terminal_records)
    if len(records) > owned.MAX_RECORDS or any(record is None for record in records):
        raise source.unknown("terminal-original-construction-hole-or-capacity")
    nodes = tuple((record, record.obj, record.original_parent, record.children) for record in records)
    processes = tuple((record, record.reader, record.writer, record.wait_record) for record in records
                      if record.kind == "process")
    janitors = tuple(record for record in records if record.kind == "janitor")
    return OriginalBook(source, source.terminal_records, records, nodes, processes, janitors)


def require_book(snapshot):
    source, records = snapshot.source, snapshot.records
    if (source.terminal_records is not snapshot.book or len(snapshot.book) != len(records)
            or any(before is not after for before, after in zip(records, snapshot.book))):
        raise source.unknown("terminal-original-source-book-changed")
    with owned._lock:
        indexed = tuple(record for record in owned._index.values() if record.source is source)
    if any(record not in records for record in indexed):
        raise source.unknown("terminal-original-indexed-resource-omitted")
    for record, obj, parent, children in snapshot.nodes:
        record.require()
        if record.obj is not obj or record.original_parent is not parent or record.children is not children:
            owned.refuse(record, "terminal-original-shutdown-associations-changed")
        if any(child not in records or child.original_parent is not record for child in children):
            owned.refuse(record, "terminal-original-child-membership-unavailable")
    for record, reader, writer, wait in snapshot.processes:
        if record.reader is not reader or record.writer is not writer or record.wait_record is not wait:
            owned.refuse(record, "terminal-original-process-completion-links-changed")


def _require_environment(record):
    owned.require_env(record.obj)
    if not record.done or record.active:
        owned.refuse(record, "terminal-original-environment-initializer-incomplete")


def _require_executor(record):
    require_thread(record)
    if not record.done or not record.finished.is_set() or record.obj.is_alive():
        owned.refuse(record, "terminal-original-executor-not-joined")


def _require_done(record):
    if not record.done or record.active:
        owned.refuse(record, "terminal-original-command-or-pipe-incomplete")


_QUIESCENT = {"janitor": require_thread, "environment": _require_environment, "process": io.require_settled,
              "executor": _require_executor, **dict.fromkeys(("command", "wait", "pipe-reader", "pipe-writer"), _require_done)}


def require_quiescent(snapshot):
    require_book(snapshot)
    for record in snapshot.records:
        handler = _QUIESCENT.get(record.kind)
        if handler is None:
            owned.refuse(record, "terminal-original-resource-normal-closer-unavailable")
        handler(record)


def _cleanup_environment(record, snapshot):
    owned.require_env(record.obj)
    if record.cleanup_done:
        return
    if record.cleanup_attempted:
        owned.refuse(record, "terminal-original-cleanup-attempt-retained")
    record.cleanup_attempted = True
    record.cleanup_snapshot = snapshot

    def normal():
        token = owned._normal_cleanup.set(record)
        try:
            record.cleanup_result = record.obj.cleanup()
            require_quiescent(snapshot)
            if record.cleanup_paths != (record.obj._snapshot_path, record.obj._cwd_file):
                owned.refuse(record, "terminal-original-cleanup-paths-changed")
            if any(os.path.lexists(path) for path in record.cleanup_paths):
                owned.refuse(record, "terminal-original-session-files-not-cleaned")
            record.cleanup_done = True
        finally:
            owned._normal_cleanup.reset(token)

    try:
        record.context.run(normal)
    except BaseException as error:
        owned.fail(record, error)
        raise


def _join_janitors(snapshot, module):
    for record in snapshot.janitors:
        require_thread(record)
        if record.module is not module or module._cleanup_thread is not record.obj:
            owned.refuse(record, "terminal-original-janitor-module-association-changed")
        if record.done:
            if record.methods[1]() or module._cleanup_running:
                owned.refuse(record, "terminal-original-joined-janitor-changed")
            continue
        module._cleanup_running = False
        record.methods[0]()
        require_thread(record)
        if record.methods[1]() or module._cleanup_thread is not record.obj or module._cleanup_running:
            owned.refuse(record, "terminal-original-janitor-did-not-join")
        record.done = True


async def close_all(source, module, instances):
    from hermes_owned_gateway import OwnedGatewaySource
    if type(source) is not OwnedGatewaySource:
        return False
    source.require_shutdown(source.runner)
    for env in instances:
        owned.require_env(env)
    if module is not None and module._cleanup_thread is not None and owned.find(module._cleanup_thread) is None:
        source.unresolved_resource_refs.append(module._cleanup_thread)
        raise source.unknown("terminal-original-janitor-constructor-unavailable")
    if not source.terminal_records:
        return False
    with source._state_lock:
        if source.terminal_closing:
            if not source.terminal_closed:
                raise source.unknown("terminal-original-shutdown-attempt-retained")
            snapshot = source.terminal_snapshot
        else:
            source.terminal_closing = True
            snapshot = source.terminal_snapshot = _capture(source)
    try:
        require_quiescent(snapshot)
        for record in snapshot.records:
            if record.kind == "environment":
                _cleanup_environment(record, snapshot)
        _join_janitors(snapshot, module)
        require_quiescent(snapshot)
        if any(record.kind == "environment" and not record.cleanup_done for record in snapshot.records):
            raise source.unknown("terminal-original-file-cleanup-incomplete")
        require_book(snapshot)
        source.terminal_closed = True
        return True
    except BaseException as error:
        source.terminal_error = source.terminal_error or error
        source.unknown("terminal-original-normal-settlement-incomplete")
        raise
