"""Inherited pre-import product guards; real SQLite only in this fresh source fixture root."""
from pathlib import Path
import sqlite3
import sys
from urllib.parse import unquote

from tests.agent import shared_pool_test_guard as inherited

_connect = sqlite3.connect


def source_connect(database, *args, **kwargs):
    value = str(database)
    if value == ":memory:":
        return _connect(database, *args, **kwargs)  # Existing SessionDB schema introspection, private scratch only.
    if value.startswith("file:"):
        value = unquote(value[5:].split("?", 1)[0])
    target = Path(value).resolve()
    allowed = Path(sys.prefix).resolve().parent / "sqlite-fixtures"
    if not target.is_relative_to(allowed):
        raise RuntimeError("SQLite source fixture outside fresh declared scope")
    return _connect(database, *args, **kwargs)


def pytest_configure(config):
    inherited.pytest_configure(config)
    config._shared_source_guards.setattr(sqlite3, "connect", source_connect)


def pytest_unconfigure(config):
    inherited.pytest_unconfigure(config)
