"""Single original SessionDB transaction body; ordinary retry policy stays in SessionDB."""
from __future__ import annotations

from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class WriteAttempt:
    started: bool = False


def execute_once(db, callback, attempt):
    from hermes_state_native_lease import require_transaction_entry, transaction_scope
    require_transaction_entry(db)
    with db._lock, transaction_scope(db) as lease:
        db._raise_if_db_replaced()
        if db._conn is None:
            db._reopen_after_close_locked(context="write")
        db._conn.execute("BEGIN IMMEDIATE")
        try:
            attempt.started = True
            result = callback(db._conn)
            db._conn.commit()
            if lease is not None:
                lease.committed(callback, result)
        except BaseException:
            try:
                db._conn.rollback()
            except Exception:
                logger.exception("SessionDB rollback failed; preserving the original transaction exception")
            raise
    return result
