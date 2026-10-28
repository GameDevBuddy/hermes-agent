"""Explicit sealed-host bootstrap declaration; no process or authentication authority.

Only stdlib is imported. This module is loaded directly from the sealed source
root before site hooks and before any Hermes package. Checks are point-in-time
checks under the caller's required writer exclusion, not atomic path proofs.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sys


_BOUND = None
_DEPENDENCY_INPUTS = ("pm/lock.json", "pyproject.toml", "uv.lock")


class BootstrapRefused(SystemExit):
    def __init__(self, reason):
        self.reason = reason
        print(json.dumps({"state": "bootstrap_refused", "reason": reason}),
              file=sys.stderr)
        super().__init__(78)


def _refuse(reason):
    raise BootstrapRefused(reason)


def _present(path):
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        _refuse("cannot-inspect-required-marker")
    return True


def _json(path):
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        _refuse("cannot-read-required-json")
    if not isinstance(value, dict):
        _refuse("required-json-is-not-an-object")
    return value


def _digest(path):
    try:
        with path.open("rb") as stream:
            result = hashlib.sha256()
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                result.update(block)
            return result.hexdigest()
    except OSError:
        _refuse("cannot-hash-required-input")


def _absolute(value):
    if not isinstance(value, str) or not Path(value).is_absolute():
        _refuse("absolute-bound-path-required")
    try:
        return Path(value).resolve()
    except (OSError, RuntimeError, ValueError):
        _refuse("cannot-resolve-required-path")


@dataclass(frozen=True)
class RetainedBootstrap:
    root: Path
    home: Path
    environment: Path
    site: Path
    selection: str
    facts_sha256: str | None
    manifest_sha256: str
    source_revision: str
    dependency_pins: tuple[tuple[str, str], ...]
    attempt_id: str

    @property
    def state(self):
        key = hashlib.sha256(str(self.root).encode("utf-8")).hexdigest()[:16]
        return self.home / "installs" / key

    def refuse(self, reason):
        _refuse(reason)

    def require_binding(self, root):
        if Path(root).resolve() != self.root:
            _refuse("bootstrap-root-changed")
        bound = os.environ.get("HERMES_HOME", "")
        if not bound or _absolute(bound) != self.home:
            _refuse("home-was-not-explicitly-bound")

    def require_inputs(self):
        self.require_binding(self.root)
        if (self.home.is_relative_to(self.root.parent)
                or self.root.parent.is_relative_to(self.home)):
            _refuse("private-home-must-be-outside-sealed-payload")
        if self.state.resolve() != self.state:
            _refuse("install-state-directory-redirected")
        for marker in (
            self.root / ".git",
            self.root / ".hermes-update-zip-swap",
            self.root / "hermes_cli.hermes-update-old",
            self.root / ".update-incomplete",
            self.root / ".lazy-refresh-incomplete",
            self.state / ".repair-incomplete",
            self.state / "publication.json",
            self.state / "source-completion-pending",
        ):
            if _present(marker):
                _refuse("owned-maintenance-required")
        marker = _json(self.home / "gamedevbuddy-runtime.json")
        if (type(marker.get("schema")) is not int or marker.get("schema") != 1
                or marker.get("product") != "GameDevBuddy AI"
                or _absolute(marker.get("home")) != self.home):
            _refuse("runtime-home-marker-mismatch")
        manifest_path = self.root.parent / "gamedevbuddy-runtime.json"
        if _digest(manifest_path) != self.manifest_sha256:
            _refuse("package-manifest-changed")
        package = _json(manifest_path)
        if (type(package.get("schema")) is not int or package.get("schema") != 1
                or package.get("fullRuntime") is not True
                or package.get("sourceRevision") != self.source_revision):
            _refuse("complete-package-declaration-mismatch")
        files = package.get("files", {})
        canonical_record = files.get("manifest.json", {}) if isinstance(files, dict) else {}
        canonical_path = self.root.parent / "manifest.json"
        if (not isinstance(canonical_record, dict) or canonical_record.get("kind") != "file"
                or _digest(canonical_path) != canonical_record.get("sha256")):
            _refuse("canonical-manifest-mismatch")
        canonical = _json(canonical_path)
        if (type(canonical.get("schema")) is not int or canonical.get("schema") != 1
                or canonical.get("ref") != self.source_revision
                or not isinstance(canonical.get("repo"), str)
                or (self.root.parent / canonical["repo"]).resolve() != self.root):
            _refuse("canonical-package-root-mismatch")
        runtime = canonical.get("runtime")
        interpreter = runtime.get("storePython") if isinstance(runtime, dict) else None
        if (not isinstance(interpreter, str)
                or Path(sys.executable).resolve() != (self.root.parent / interpreter).resolve()):
            _refuse("sealed-interpreter-path-mismatch")
        if self.selection == "payload":
            payload_environment = canonical.get("venv")
            if (not isinstance(payload_environment, str)
                    or (self.root.parent / payload_environment).resolve() != self.environment
                    or not self.environment.is_relative_to(self.root.parent)):
                _refuse("payload-environment-mismatch")
        override = os.environ.get("HERMES_RUNTIME_DIR")
        store = canonical.get("store")
        if (override and (not isinstance(store, str)
                or _absolute(override) != (self.root.parent / store).resolve())):
            _refuse("foreign-tool-store-override")
        for name, expected in self.dependency_pins:
            if _digest(self.root / name) != expected:
                _refuse("dependency-input-changed")
        if not (self.environment / "pyvenv.cfg").is_file() or not self.site.is_dir():
            _refuse("prepared-environment-missing")

    def require_bootstrap_tmp(self):
        if "HERMES_SCRATCH_DIR" in os.environ:
            _refuse("inherited-scratch-marker-not-admitted")
        scratch = self.home / "cache" / "scratch"
        if not scratch.is_dir() or scratch.resolve() != scratch:
            _refuse("owned-scratch-maintenance-required")
        for name in ("TMPDIR", "TMP", "TEMP"):
            value = os.environ.get(name, "")
            if not value or _absolute(value) != scratch:
                _refuse("private-temp-was-not-explicitly-bound")

    def require_activation(self, state):
        self.require_inputs()
        if Path(state).resolve() != self.state.resolve():
            _refuse("install-state-escaped-bound-home")
        if self.selection == "generation" and not self.state.is_dir():
            _refuse("prepared-install-state-missing")

    def require_selection(self, state, environment, selected):
        self.require_activation(state)
        if (Path(environment).resolve() != self.environment
                or Path(selected).resolve() != self.site):
            _refuse("prepared-selection-changed")
        facts = self.state / "facts.json"
        if self.facts_sha256 is None:
            if _present(facts):
                _refuse("unprepared-private-facts")
        elif _digest(facts) != self.facts_sha256:
            _refuse("private-facts-changed")
        generation = self.environment.parent
        if self.selection == "generation":
            if (not self.environment.is_relative_to((self.state / "environments").resolve())
                    or not (generation / ".lease-managed").is_file()):
                _refuse("unprepared-generation-lease")
            leases = generation / ".leases"
            if _present(leases) and (not leases.is_dir()
                    or leases.resolve() != generation.resolve() / ".leases"):
                _refuse("generation-lease-directory-redirected")
        elif _present(generation / ".lease-managed"):
            _refuse("sealed-payload-lease-write-not-admitted")


def current():
    return _BOUND


def current_for(root):
    if _BOUND is not None:
        _BOUND.require_binding(root)
    return _BOUND


def install(plan_path, root, public_args):
    global _BOUND
    if _BOUND is not None:
        _refuse("bootstrap-contract-already-selected")
    if (not sys.flags.no_site or not sys.flags.isolated
            or not sys.dont_write_bytecode or "hermes_bootstrap" in sys.modules
            or "hermes_cli" in sys.modules or "pm" in sys.modules):
        _refuse("contract-was-not-selected-at-clean-sealed-entry")
    data = _json(_absolute(plan_path))
    required = {"schemaVersion", "policy", "root", "home", "environment", "site",
                "selection", "factsSha256", "packageManifestSha256",
                "sourceRevision", "dependencyPins", "attemptId", "writerExclusion"}
    if (set(data) != required or type(data["schemaVersion"]) is not int
            or data["schemaVersion"] != 1):
        _refuse("unsupported-bootstrap-contract")
    if data["policy"] != "retained-bootstrap" or data["writerExclusion"] is not True:
        _refuse("required-caller-writer-declaration-missing")
    if data["selection"] not in ("payload", "generation"):
        _refuse("unknown-prepared-selection")
    if (not isinstance(data["attemptId"], str) or not 1 <= len(data["attemptId"]) <= 128
            or not isinstance(data["sourceRevision"], str)
            or re.fullmatch(r"[a-f0-9]{40}", data["sourceRevision"]) is None):
        _refuse("invalid-caller-binding")
    pins = data["dependencyPins"]
    if (not isinstance(pins, dict) or set(pins) != set(_DEPENDENCY_INPUTS)
            or any(not isinstance(value, str)
                   or re.fullmatch(r"[a-f0-9]{64}", value) is None
                   for value in (*pins.values(), data["packageManifestSha256"]))
            or (data["factsSha256"] is not None
                and (not isinstance(data["factsSha256"], str)
                     or re.fullmatch(r"[a-f0-9]{64}", data["factsSha256"]) is None))):
        _refuse("invalid-input-pin")
    parser = argparse.ArgumentParser(prog="retained-bootstrap", allow_abbrev=False)
    parser.add_argument("action", choices=("foreground",))
    parser.add_argument("--home", required=True)
    parser.add_argument("--port", type=int, default=9120)
    args = parser.parse_args(public_args)
    value = RetainedBootstrap(
        _absolute(data["root"]), _absolute(data["home"]),
        _absolute(data["environment"]), _absolute(data["site"]), data["selection"],
        data["factsSha256"], data["packageManifestSha256"], data["sourceRevision"],
        tuple(sorted(pins.items())), data["attemptId"])
    if value.root != Path(root).resolve() or _absolute(args.home) != value.home:
        _refuse("launcher-and-home-contract-mismatch")
    if value.home == (Path.home() / ".hermes").resolve():
        _refuse("separate-product-home-required")
    value.require_inputs()
    value.require_bootstrap_tmp()
    _BOUND = value
    return value
