"""The Windows numeric loader must run before managed stdin control starts."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import textwrap
from types import SimpleNamespace

import pytest
from gateway import run_owned


def test_non_windows_does_not_load_numeric_dependencies(monkeypatch):
    monkeypatch.setattr(run_owned, "os", SimpleNamespace(name="posix"))
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: pytest.fail("unexpected dependency lookup"))
    run_owned._prepare_windows_numeric_runtime()


def test_missing_optional_numpy_is_not_installed(monkeypatch):
    monkeypatch.setattr(run_owned, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
    monkeypatch.setattr(importlib, "import_module", lambda name: pytest.fail("unexpected dependency import"))
    run_owned._prepare_windows_numeric_runtime()


def test_installed_numpy_initialization_failure_is_not_hidden(monkeypatch):
    monkeypatch.setattr(run_owned, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    def broken(name):
        assert name == "numpy"
        raise ImportError("native numeric initialization failed")
    monkeypatch.setattr(importlib, "import_module", broken)
    with pytest.raises(ImportError, match="native numeric initialization failed"):
        run_owned._prepare_windows_numeric_runtime()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows native loader regression")
def test_preload_allows_numpy_import_while_control_pipe_is_read(tmp_path):
    pytest.importorskip("numpy")
    root = str(Path(__file__).resolve().parents[2])
    script = tmp_path / "numeric_control_pipe.py"
    script.write_text(textwrap.dedent(f"""
        import os, sys, threading, time
        sys.path.insert(0, {root!r})
        from gateway.run_owned import _prepare_windows_numeric_runtime
        _prepare_windows_numeric_runtime()
        reading = threading.Event()
        def control():
            reading.set()
            os.read(sys.stdin.fileno(), 1)
        threading.Thread(target=control, daemon=True).start()
        assert reading.wait(5)
        time.sleep(0.1)
        import numpy
        print('numeric ready with control read pending', flush=True)
    """), encoding="utf-8")
    child = subprocess.Popen([sys.executable, "-B", "-I", str(script)],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert child.wait(timeout=15) == 0
        stdout, stderr = child.communicate()
        assert b"numeric ready with control read pending" in stdout
        assert not stderr, stderr.decode("utf-8", errors="replace")
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate()
