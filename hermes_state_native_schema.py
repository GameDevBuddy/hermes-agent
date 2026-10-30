"""Cooperating SQLite writer fences, never App permission or global exclusion."""
from __future__ import annotations

from hermes_state_runtime import RuntimeStoreError

MAX_METADATA = 64 * 1024 * 1024
MAX_PINS = 1024


def _trigger(name, event, table, predicate):
    return (f"CREATE TRIGGER native_sql_{name} BEFORE {event} ON {table} WHEN {predicate} "
            "BEGIN SELECT RAISE(ABORT,'native_sql_retained'); END")


def _changed(columns):
    return "(" + " OR ".join(f"NEW.{column} IS NOT OLD.{column}" for column in columns) + ")"


def _scope(condition):
    return ("EXISTS(SELECT 1 FROM native_pin_scopes s JOIN native_original_pins p "
            f"ON p.pin_id=s.pin_id WHERE {condition})")


def _identity_guards():
    result = [_trigger("epoch_" + event.lower(), event, "runtime_epoch",
                       "EXISTS(SELECT 1 FROM native_original_pins)")
              for event in ("INSERT", "UPDATE", "DELETE")]
    session_columns = ("id", "runtime_revision", "runtime_generation", "source", "session_key",
                       "parent_session_id", "ended_at", "end_reason", "model", "model_config",
                       "system_prompt", "system_prompt_hash", "archived", "hidden", "auto_archived",
                       "expiry_finalized", "profile_name", "transport_profile", "handoff_state",
                       "handoff_platform", "handoff_error")
    result.extend((
        _trigger("session_update", "UPDATE", "sessions",
                 _scope("s.session_id IN (OLD.id,NEW.id,OLD.parent_session_id,NEW.parent_session_id)")
                 + " AND " + _changed(session_columns)),
        _trigger("session_delete", "DELETE", "sessions", _scope("s.session_id=OLD.id")),
        _trigger("session_insert", "INSERT", "sessions", _scope("s.session_id IN (NEW.id,NEW.parent_session_id)")),
    ))
    for table, key in (("session_turn_leases", "conversation_id"), ("compression_locks", "session_id")):
        for event in ("INSERT", "UPDATE", "DELETE"):
            aliases = ("NEW",) if event == "INSERT" else (("OLD",) if event == "DELETE" else ("OLD", "NEW"))
            held = " OR ".join(_scope(f"s.session_id={alias}.{key}") for alias in aliases)
            # Same acquisition may extend expiry; replacement, resurrection and expiry reduction refuse.
            if event == "UPDATE":
                held = ("(" + held + ") AND (" + _changed((key, "holder", "acquired_at"))
                        + " OR NEW.expires_at<OLD.expires_at OR OLD.expires_at<=unixepoch('subsec'))")
            result.append(_trigger(table + "_" + event.lower(), event, table, held))
    admission = "EXISTS(SELECT 1 FROM native_original_pins p WHERE p.admission_id=OLD.admission_id)"
    result.extend((
        _trigger("admission_update", "UPDATE", "session_admissions", admission + " AND ("
                 + _changed(("admission_id", "request_id", "principal_id", "target_session_id", "lineage_json",
                             "payload_json", "payload_digest", "intent", "owner_epoch", "generation"))
                 + " OR (NEW.status IS NOT OLD.status AND NEW.status!='unknown'))"),
        _trigger("admission_delete", "DELETE", "session_admissions", admission),
        _trigger("admission_insert", "INSERT", "session_admissions",
                 "EXISTS(SELECT 1 FROM native_original_pins p JOIN session_admissions a "
                 "ON a.admission_id=p.admission_id WHERE a.admission_id=NEW.admission_id OR "
                 "(a.principal_id=NEW.principal_id AND a.target_session_id=NEW.target_session_id "
                 "AND a.request_id=NEW.request_id))"),
    ))
    return result


def _worker_guards():
    old = _scope("s.session_id=OLD.session_id AND p.generation=OLD.generation")
    new = _scope("s.session_id=NEW.session_id AND p.generation=NEW.generation")
    replace = ("EXISTS(SELECT 1 FROM worker_executions w JOIN native_pin_scopes s ON s.session_id=w.session_id "
               "JOIN native_original_pins p ON p.pin_id=s.pin_id WHERE w.execution_id=NEW.execution_id "
               "AND w.generation=p.generation)")
    result = [
        _trigger("worker_delete", "DELETE", "worker_executions", old),
        _trigger("worker_update", "UPDATE", "worker_executions", "(" + old + " OR " + new + " OR " + replace + ") AND ("
                 + _changed(("execution_id", "session_id", "kind", "owner_epoch", "generation", "adoption_digest"))
                 + " OR (NEW.status IS NOT OLD.status AND NOT (NEW.status='unknown' AND OLD.status IN ('registered','running','unknown'))))"),
        _trigger("worker_insert", "INSERT", "worker_executions", _scope("s.session_id=NEW.session_id") + " OR " + replace),
    ]
    for event in ("INSERT", "UPDATE", "DELETE"):
        aliases = ("NEW",) if event == "INSERT" else (("OLD",) if event == "DELETE" else ("OLD", "NEW"))
        condition = " OR ".join(_scope("EXISTS(SELECT 1 FROM sessions q WHERE q.id=s.session_id "
                                     f"AND q.source={alias}.source AND q.session_key={alias}.session_key)") for alias in aliases)
        result.append(_trigger("conversation_" + event.lower(), event, "conversation_generations", condition))
    return result


def _message_guards():
    held = (_scope("s.session_id=OLD.session_id AND OLD.id<=p.message_ceiling")
            + " OR EXISTS(SELECT 1 FROM native_pin_row_links l WHERE l.row_id=OLD.id)")
    replacing = ("EXISTS(SELECT 1 FROM native_original_pins p WHERE "
                 "(NEW.id>0 AND NEW.id<=p.message_ceiling AND "
                 "EXISTS(SELECT 1 FROM native_pin_scopes s WHERE s.pin_id=p.pin_id AND s.session_id=NEW.session_id)) "
                 "OR (p.message_uid=NEW.message_uid AND p.transcript_session_id=NEW.session_id)) "
                 "OR EXISTS(SELECT 1 FROM native_pin_row_links l WHERE l.row_id=NEW.id "
                 "OR (l.message_uid=NEW.message_uid AND l.session_id=NEW.session_id))")
    return [
        _trigger("message_update", "UPDATE", "messages", "(" + held + ") OR " + replacing),
        _trigger("message_delete", "DELETE", "messages", held),
        _trigger("message_insert", "INSERT", "messages", replacing),
    ]


def native_schema():
    statements = [
        "CREATE TABLE native_original_pins (pin_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, "
        "epoch INTEGER NOT NULL, instance_id TEXT NOT NULL, logical_session_id TEXT NOT NULL, "
        "transcript_session_id TEXT NOT NULL, admission_id TEXT NOT NULL, generation INTEGER NOT NULL, "
        "assistant_row_id INTEGER NOT NULL, message_uid TEXT NOT NULL, tool_call_uid TEXT NOT NULL, "
        "message_ceiling INTEGER NOT NULL, capture_json TEXT NOT NULL CHECK(length(CAST(capture_json AS BLOB))<=262144), "
        "capture_sha256 TEXT NOT NULL CHECK(length(capture_sha256)=64), scope_count INTEGER NOT NULL CHECK(scope_count BETWEEN 1 AND 1000), "
        "scope_bytes INTEGER NOT NULL CHECK(scope_bytes BETWEEN 1 AND 262144), credit_bytes INTEGER NOT NULL CHECK(credit_bytes>0), "
        "UNIQUE(epoch,instance_id,transcript_session_id,assistant_row_id,tool_call_uid))",
        "CREATE TABLE native_pin_scopes (pin_id TEXT NOT NULL REFERENCES native_original_pins(pin_id) ON DELETE RESTRICT, "
        "session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 4096), PRIMARY KEY(pin_id,session_id))",
        "CREATE TABLE native_pin_active_uses (pin_id TEXT PRIMARY KEY REFERENCES native_original_pins(pin_id) ON DELETE RESTRICT, "
        "request_id TEXT NOT NULL UNIQUE, capture_sha256 TEXT NOT NULL, metadata_json TEXT NOT NULL "
        "CHECK(length(CAST(metadata_json AS BLOB))<=8192), metadata_sha256 TEXT NOT NULL CHECK(length(metadata_sha256)=64), "
        "credit_bytes INTEGER NOT NULL CHECK(credit_bytes=81920))",
        "CREATE TABLE native_pin_row_links (pin_id TEXT PRIMARY KEY REFERENCES native_pin_active_uses(pin_id) ON DELETE RESTRICT, "
        "session_id TEXT NOT NULL, row_id INTEGER NOT NULL UNIQUE, message_uid TEXT NOT NULL, tool_call_uid TEXT NOT NULL, "
        "row_version TEXT NOT NULL, raw_sha256 TEXT NOT NULL, projection_sha256 TEXT NOT NULL, "
        "witness_json TEXT NOT NULL CHECK(length(CAST(witness_json AS BLOB))<=8192))",
    ]
    for table in ("native_original_pins", "native_pin_scopes", "native_pin_active_uses", "native_pin_row_links"):
        for event in ("UPDATE", "DELETE"):
            statements.append(_trigger(table + "_" + event.lower(), event, table, "1"))
        key = "pin_id=NEW.pin_id AND session_id=NEW.session_id" if table == "native_pin_scopes" else "pin_id=NEW.pin_id"
        duplicate = f"EXISTS(SELECT 1 FROM {table} WHERE {key})"
        if table == "native_original_pins":
            duplicate += (" OR EXISTS(SELECT 1 FROM native_original_pins WHERE epoch=NEW.epoch AND instance_id=NEW.instance_id "
                          "AND transcript_session_id=NEW.transcript_session_id AND assistant_row_id=NEW.assistant_row_id "
                          "AND tool_call_uid=NEW.tool_call_uid)")
        statements.append(_trigger(table + "_replace", "INSERT", table, duplicate))
    statements.extend((
        _trigger("pin_capacity", "INSERT", "native_original_pins",
                 f"(SELECT COUNT(*) FROM native_original_pins)>={MAX_PINS} OR "
                 f"(SELECT COALESCE(SUM(credit_bytes),0) FROM native_original_pins)+NEW.credit_bytes>{MAX_METADATA}"),
        _trigger("scope_bound", "INSERT", "native_pin_scopes",
                 "NOT EXISTS(SELECT 1 FROM native_original_pins p WHERE p.pin_id=NEW.pin_id AND "
                 "(SELECT COUNT(*) FROM native_pin_scopes s WHERE s.pin_id=p.pin_id)<p.scope_count AND "
                 "(SELECT COALESCE(SUM(length(CAST(session_id AS BLOB))),0) FROM native_pin_scopes s WHERE s.pin_id=p.pin_id) "
                 "+length(CAST(NEW.session_id AS BLOB))<=p.scope_bytes)"),
        _trigger("use_bound", "INSERT", "native_pin_active_uses",
                 "NOT EXISTS(SELECT 1 FROM native_original_pins p WHERE p.pin_id=NEW.pin_id AND p.capture_sha256=NEW.capture_sha256)"),
    ))
    return tuple(statements + _identity_guards() + _worker_guards() + _message_guards())


def _normal(sql):
    return " ".join(sql.lower().split()).replace(" if not exists", "")


def require_schema(conn):
    expected = native_schema()
    names = [sql.split()[2] for sql in expected]
    rows = conn.execute("SELECT name,sql FROM sqlite_master WHERE name IN (" + ",".join("?" for _ in names) + ")", names)
    actual = {row[0]: _normal(row[1]) for row in rows}
    if actual != dict(zip(names, map(_normal, expected))):
        raise RuntimeStoreError("native_sql_schema_changed")


def install_schema(conn):
    if conn.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'native_%' LIMIT 1").fetchone():
        require_schema(conn)
        return
    for statement in native_schema():
        conn.execute(statement)
    require_schema(conn)
