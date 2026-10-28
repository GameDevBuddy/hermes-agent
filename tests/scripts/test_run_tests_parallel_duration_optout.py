"""Behavioral cache-boundary checks for the official per-file source runner."""
import json
from pathlib import Path

import pytest

from scripts import run_tests_parallel as runner


@pytest.fixture
def cache_root(tmp_path):
    root = tmp_path / "duration-project"
    root.mkdir()
    return root


def test_disabled_load_performs_no_cache_path_io(cache_root, monkeypatch):
    cache = cache_root / "test_durations.json"
    cache.write_text('{"synthetic": 9.0}', encoding="utf-8")
    real_is_file = Path.is_file
    inspected = []

    def is_file(path):
        if path == cache:
            inspected.append(path)
            raise AssertionError("Disabled cache must not even inspect its input")
        return real_is_file(path)

    monkeypatch.setattr(Path, "is_file", is_file)
    assert runner._load_durations(cache_root, disabled=True) == {}
    assert inspected == []


def test_default_load_keeps_existing_json_behavior(cache_root):
    cache = cache_root / "test_durations.json"
    cache.write_text('{"synthetic": 9.0}', encoding="utf-8")
    assert runner._load_durations(cache_root) == {"synthetic": 9.0}


def test_disabled_save_keeps_owned_fixture_cache_unchanged(cache_root):
    cache = cache_root / "test_durations.json"
    before = b'{"synthetic": 9.0}\n'
    cache.write_bytes(before)
    runner._cache_clean_durations([(cache_root / "new.py", 2.0)], cache_root, disabled=True)
    assert cache.read_bytes() == before
    assert list(cache_root.iterdir()) == [cache]


def test_default_save_still_merges_and_rounds_owned_fixture_history(cache_root):
    cache = cache_root / "test_durations.json"
    cache.write_text('{"synthetic": 9.0}', encoding="utf-8")
    runner._cache_clean_durations([(cache_root / "new.py", 2.3456)], cache_root)
    assert json.loads(cache.read_text(encoding="utf-8")) == {
        "synthetic": 9.0, "new.py": 2.346}


def test_empty_clean_run_does_not_create_duration_cache(cache_root):
    runner._cache_clean_durations([], cache_root)
    assert not (cache_root / "test_durations.json").exists()
