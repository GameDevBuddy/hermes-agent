"""Pinned-input observation fixtures only; no exclusion/launch/native authority is declared."""
import hashlib
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

import hermes_retained_bootstrap as policy
from pm import environments


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture
def observed(tmp_path, monkeypatch):
    root, home = tmp_path / "payload" / "repo", tmp_path / "observation-home"
    root.mkdir(parents=True)
    scratch = home / "cache" / "scratch"
    scratch.mkdir(parents=True)
    environment = root.parent / "venv"
    environment.mkdir()
    (environment / "pyvenv.cfg").write_text(
        f"version = {sys.version_info.major}.{sys.version_info.minor}.0\n", encoding="utf-8")
    site = environments.site_packages(environment)
    site.mkdir(parents=True)
    executable = root.parent / "tools" / "python.exe"
    executable.parent.mkdir()
    executable.write_bytes(b"INERT SYNTHETIC INPUT; NEVER EXECUTED")
    canonical_path, package_path = root.parent / "manifest.json", root.parent / "gamedevbuddy-runtime.json"
    canonical = {"schema": 1, "ref": "a" * 40, "repo": "repo", "venv": "venv", "store": "tools",
                 "runtime": {"storePython": "tools/python.exe"}}
    write_json(canonical_path, canonical)
    package = {"schema": 1, "fullRuntime": True, "sourceRevision": "a" * 40,
               "files": {"manifest.json": {"kind": "file", "sha256": digest(canonical_path)}}}
    write_json(package_path, package)
    write_json(home / "gamedevbuddy-runtime.json", {"schema": 1, "product": "GameDevBuddy AI", "home": str(home)})
    pins = {}
    for name in policy._DEPENDENCY_INPUTS:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"SYNTHETIC PINNED INPUT")
        pins[name] = digest(path)
    plan = {"schemaVersion": 2, "policy": "retained-bootstrap", "root": str(root), "home": str(home),
            "environment": str(environment), "site": str(site), "selection": "payload", "factsSha256": None,
            "packageManifestSha256": digest(package_path), "sourceRevision": "a" * 40, "dependencyPins": pins,
            "attemptId": "observation-source-case", "inputPolicy": "pinned-input-observations-v1"}
    plan_path = tmp_path / "observed-plan.json"
    fake_sys = SimpleNamespace(flags=SimpleNamespace(no_site=True, isolated=True), dont_write_bytecode=True,
                               modules={}, executable=str(executable), stderr=io.StringIO())
    monkeypatch.setattr(policy, "sys", fake_sys)
    monkeypatch.setattr(policy, "_BOUND", None)
    monkeypatch.setattr(policy, "_PLAN_BYTES", None)
    for name in ("HERMES_RUNTIME_DIR", "HERMES_SCRATCH_DIR"):
        monkeypatch.delenv(name, raising=False)
    for name, value in {"HOME": str(tmp_path / "operator"), "USERPROFILE": str(tmp_path / "operator"),
                        "HERMES_HOME": str(home), "TMPDIR": str(scratch), "TMP": str(scratch), "TEMP": str(scratch)}.items():
        monkeypatch.setenv(name, value)

    def bind(raw=None):
        plan_path.write_bytes(raw if raw is not None else json.dumps(plan).encode("utf-8"))
        return policy.install(str(plan_path), root, ["foreground", "--home", str(home)])

    return SimpleNamespace(root=root, home=home, scratch=scratch, environment=environment, site=site,
                           executable=executable, canonical=canonical, canonical_path=canonical_path,
                           package=package, package_path=package_path, plan=plan, plan_path=plan_path,
                           fake_sys=fake_sys, bind=bind)
