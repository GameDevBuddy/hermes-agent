"""Selected setup metadata against owned file fixtures and the real PM selectors.

The fixture supplies interpreter observations; these are unit tests, not sealed
package execution or native/current-owner acceptance. The host OS is unchanged.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from hermes_cli import gamedevbuddy as facade
from hermes_cli import gamedevbuddy_environment as subject
from pm import environments as pm


def test_file_read_accepts_stable_api_specific_ctime(tmp_path, monkeypatch):
    path = (tmp_path / "owned-input").resolve()
    body = b"unchanged owned bytes"
    path.write_bytes(body)
    path_ctime = path.lstat().st_ctime_ns
    real_fstat = os.fstat

    def handle_stat(descriptor):
        info = real_fstat(descriptor)
        values = {name: getattr(info, name) for name in dir(info) if name.startswith("st_")}
        # The measured Windows discrepancy was 1,005,100 ns, with stable file identity.
        values["st_ctime_ns"] = path_ctime + 1_005_100
        return SimpleNamespace(**values)

    with monkeypatch.context() as patch:
        patch.setattr(subject.os, "fstat", handle_stat)
        info, signature, read = subject._read(subject._Observation(), path, 1024, keep=True)
    assert info == (str(path), len(body), hashlib.sha256(body).hexdigest())
    assert signature == subject._signature(path.lstat())
    assert read == body


@pytest.mark.parametrize("stage", ["before-open", "after-close"])
def test_file_read_refuses_same_bytes_replacement(tmp_path, monkeypatch, stage):
    path = (tmp_path / "owned-input").resolve()
    replacement = (tmp_path / "replacement").resolve()
    path.write_bytes(b"same bytes")
    replacement.write_bytes(b"same bytes")
    original = path.lstat()
    os.utime(replacement, ns=(original.st_atime_ns, original.st_mtime_ns))
    real_open, real_lstat = Path.open, Path.lstat
    observations = 0

    def replace_before_open(current, *args, **kwargs):
        if current == path:
            replacement.replace(path)
        return real_open(current, *args, **kwargs)

    def replace_after_close(current, *args, **kwargs):
        nonlocal observations
        if current == path:
            observations += 1
            if observations == 2:
                replacement.replace(path)
        return real_lstat(current, *args, **kwargs)

    with monkeypatch.context() as patch:
        if stage == "before-open":
            patch.setattr(Path, "open", replace_before_open)
        else:
            patch.setattr(Path, "lstat", replace_after_close)
        expected = "input-changed-before-read" if stage == "before-open" else "input-changed-after-read"
        with pytest.raises(subject.GatewayDescriptionRefused, match=expected):
            subject._read(subject._Observation(), path, 1024, keep=True)
    assert not replacement.exists()
    assert not os.path.samestat(original, path.lstat())


def test_file_read_refuses_changed_open_handle_metadata(tmp_path, monkeypatch):
    path = (tmp_path / "owned-input").resolve()
    path.write_bytes(b"owned bytes")
    before = path.lstat()
    real_fstat = os.fstat
    observations = 0

    def change_before_second_stat(descriptor):
        nonlocal observations
        observations += 1
        if observations == 2:
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000_000))
        return real_fstat(descriptor)

    with monkeypatch.context() as patch:
        patch.setattr(subject.os, "fstat", change_before_second_stat)
        with pytest.raises(subject.GatewayDescriptionRefused, match="input-changed-during-read"):
            subject._read(subject._Observation(), path, 1024, keep=True)
    assert observations == 2


@pytest.fixture
def package(tmp_path, monkeypatch):
    outer = (tmp_path / "package").resolve()
    root, home = outer / "repo", (tmp_path / "product-home").resolve()
    root.mkdir(parents=True)
    home.mkdir()
    interpreter, environment = outer / "runtime/python.exe", outer / "venv"
    interpreter.parent.mkdir()
    interpreter.write_bytes(b"owned interpreter observation, not executable code")
    site = pm.site_packages(environment)
    site.mkdir(parents=True)
    (environment / "pyvenv.cfg").write_text(
        f"version = {sys.version_info.major}.{sys.version_info.minor}.0\n", encoding="utf-8")
    for relative in (*subject.DEPENDENCIES, *subject.SOURCE_FILES):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"owned metadata input: {relative}".encode())
    revision = "a" * 40
    manifest = {"schema": 1, "repo": "repo", "ref": revision, "venv": "venv",
                "runtime": {"storePython": "runtime/python.exe"}}
    (outer / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    paths = [outer / "manifest.json", interpreter,
             *(root / name for name in (*subject.DEPENDENCIES, *subject.SOURCE_FILES))]
    pins = {path.relative_to(outer).as_posix(): {
        "kind": "file", "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    } for path in paths}
    (outer / "gamedevbuddy-runtime.json").write_text(json.dumps({
        "schema": 1, "fullRuntime": True, "sourceRevision": revision, "files": pins,
    }), encoding="utf-8")
    (home / facade.MARKER).write_text(json.dumps({
        "schema": 1, "product": facade.PRODUCT, "home": str(home),
    }), encoding="utf-8")
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.delenv("HERMES_PROFILE", raising=False)
    monkeypatch.setattr(subject, "__file__", str(root / "hermes_cli/gamedevbuddy_environment.py"))
    monkeypatch.setattr(facade, "__file__", str(root / "hermes_cli/gamedevbuddy.py"))
    monkeypatch.setattr(pm, "__file__", str(root / "pm/environments.py"))
    monkeypatch.setattr(subject, "sys", SimpleNamespace(
        executable=str(interpreter), flags=SimpleNamespace(no_site=True, isolated=True),
        dont_write_bytecode=True, version_info=sys.version_info))
    monkeypatch.syspath_prepend(str(site))
    # Each test represents a fresh setup worker, never a product retry/release.
    monkeypatch.setattr(subject, "_claimed", False)
    monkeypatch.setattr(subject, "_metadata_reserved", 0)
    monkeypatch.setattr(subject, "_observation", None)
    receipt = {"product": facade.PRODUCT, "home": str(home), "created": False,
               "profilesAdded": [], "accountRequired": False, "gatewayStarted": False}
    return SimpleNamespace(root=root, home=home, environment=environment,
                           receipt=receipt, state=pm.install_state_dir(root))


def observe(package):
    return subject.begin_gateway_description(package.root, package.home, 9120)


def generation(package, monkeypatch):
    environment = package.state / "environments/owned-generation/venv"
    site = pm.site_packages(environment)
    site.mkdir(parents=True)
    (environment / "pyvenv.cfg").write_text(
        f"version = {sys.version_info.major}.{sys.version_info.minor}.0\n", encoding="utf-8")
    (environment.parent / ".lease-managed").write_bytes(b"owned fixture marker")
    (package.state / ".install.lock").write_bytes(b"0")
    facts = {"schema": 1, "packages": {"venv": {
        "environment": str(environment), "stamp": "owned-test-stamp", "extras": [],
    }}}
    (package.state / "facts.json").write_text(json.dumps(facts), encoding="utf-8")
    monkeypatch.syspath_prepend(str(site))
    return environment, facts


def test_payload_descriptor_uses_real_selection_without_creating_install_state(package):
    value = observe(package)
    subject.freeze_gateway_description(value, package.receipt)
    output = io.BytesIO()
    subject.emit_gateway_description(value, output)
    wire = output.getvalue()
    data = json.loads(wire)
    assert wire.endswith(b"\n") and wire.count(b"\n") == 1
    assert len(wire) <= subject.MAX_DESCRIPTOR + 1
    assert data["selection"] == "payload"
    assert data["environment"] == str(pm.selected_venv(package.root))
    assert data["preparation"] == package.receipt
    assert data["installStatePresent"] is False and not package.state.exists()
    assert set(data["dependencyInputs"]) == set(subject.DEPENDENCIES)
    assert set(data["sourceFiles"]) == set(subject.SOURCE_FILES)
    assert "writerExclusion" not in data and "attemptId" not in data
    assert data["preparation"]["gatewayStarted"] is False
    with pytest.raises(subject.GatewayDescriptionRefused, match="attempt-unavailable"):
        subject.emit_gateway_description(value, output)
    assert output.getvalue() == wire


def test_generation_descriptor_keeps_committed_environment_and_fact_pin(package, monkeypatch):
    environment, _ = generation(package, monkeypatch)
    value = observe(package)
    subject.freeze_gateway_description(value, package.receipt)
    data = json.loads(value.wire)
    assert data["selection"] == "generation"
    assert data["environment"] == str(environment) == str(pm.committed_venv(package.root))
    assert data["facts"]["sha256"] == hashlib.sha256(
        (package.state / "facts.json").read_bytes()).hexdigest()
    assert data["generationLeaseMarker"]["path"] == str(environment.parent / ".lease-managed")


@pytest.mark.parametrize("record", [None, {}, {"environment": ""},
    {"environment": "not-a-generation", "stamp": "x", "extras": "wrong-type"}])
def test_incomplete_generation_never_falls_back_to_payload(package, monkeypatch, record):
    _, facts = generation(package, monkeypatch)
    facts["packages"]["venv"] = record
    (package.state / "facts.json").write_text(json.dumps(facts), encoding="utf-8")
    value = observe(package)
    with pytest.raises(subject.GatewayDescriptionRefused, match="incomplete-generation"):
        subject.freeze_gateway_description(value, package.receipt)
    assert value.wire is None


def test_orphaned_generation_directory_is_not_payload_permission(package):
    (package.state / "environments").mkdir(parents=True)
    (package.state / ".install.lock").write_bytes(b"0")
    value = observe(package)
    with pytest.raises(subject.GatewayDescriptionRefused, match="missing-generation-selection"):
        subject.freeze_gateway_description(value, package.receipt)
    assert value.wire is None


def test_duplicate_facts_are_refused(package, monkeypatch):
    generation(package, monkeypatch)
    (package.state / "facts.json").write_text(
        '{"schema":1,"schema":1,"packages":{}}', encoding="utf-8")
    value = observe(package)
    with pytest.raises(subject.GatewayDescriptionRefused, match="duplicate-json-key"):
        subject.freeze_gateway_description(value, package.receipt)


def test_conflicting_pyvenv_versions_are_refused(package):
    (package.environment / "pyvenv.cfg").write_text(
        "version = 3.14.0\nversion_info = 3.13.0\n", encoding="utf-8")
    value = observe(package)
    with pytest.raises(subject.GatewayDescriptionRefused, match="conflicting-prepared-python-version"):
        subject.freeze_gateway_description(value, package.receipt)


def test_changed_frozen_dependency_is_not_refreshed_or_retried(package):
    value = observe(package)
    (package.root / "uv.lock").write_bytes(b"changed after original observation")
    with pytest.raises(subject.GatewayDescriptionRefused, match="frozen-description-input-changed") as error:
        subject.freeze_gateway_description(value, package.receipt)
    subject.fail_gateway_description(value, error.value)
    assert value.phase == "failed" and value.wire is None
    with pytest.raises(subject.GatewayDescriptionRefused, match="already-attempted"):
        observe(package)


def test_unsealed_flags_refuse_before_preparation_and_keep_attempt(package):
    subject.sys.flags.no_site = False
    with pytest.raises(subject.GatewayDescriptionRefused, match="sealed-interpreter-flags"):
        observe(package)
    assert subject._claimed and subject._metadata_reserved == subject.METADATA_CREDIT
    with pytest.raises(subject.GatewayDescriptionRefused, match="already-attempted"):
        observe(package)


def test_partial_output_keeps_one_shot_claim(package):
    value = observe(package)
    subject.freeze_gateway_description(value, package.receipt)
    output = Mock()
    output.write.side_effect = lambda frame: len(frame) - 1
    with pytest.raises(subject.GatewayDescriptionRefused, match="partial-no-replay"):
        subject.emit_gateway_description(value, output)
    with pytest.raises(subject.GatewayDescriptionRefused, match="attempt-unavailable"):
        subject.emit_gateway_description(value, output)
    assert output.write.call_count == 1
    output.flush.assert_not_called()


def test_selected_facade_prepares_once_and_emits_only_descriptor(package, monkeypatch, capsys):
    prepare = Mock(return_value=package.receipt)
    monkeypatch.setattr(facade, "prepare_runtime", prepare)
    monkeypatch.setattr(sys, "argv", ["stumpy.py", "prepare-for-gateway", "--home", str(package.home)])
    assert facade.main() == 0
    prepare.assert_called_once_with(package.home, port=9120)
    data = json.loads(capsys.readouterr().out)
    assert data["kind"] == "gdb-prepared-gateway-environment-v1"
    assert data["preparation"] == package.receipt


def test_selected_preparation_failure_is_not_replayed(package, monkeypatch):
    prepare = Mock(side_effect=ValueError("owned preparation failure"))
    monkeypatch.setattr(facade, "prepare_runtime", prepare)
    with pytest.raises(ValueError, match="owned preparation failure"):
        facade._prepare_gateway_environment_source(package.home, port=9120)
    assert subject._observation.phase == "failed"
    with pytest.raises(subject.GatewayDescriptionRefused, match="already-attempted"):
        facade._prepare_gateway_environment_source(package.home, port=9120)
    assert prepare.call_count == 1


def test_ordinary_prepare_keeps_original_receipt_and_does_not_select(package, monkeypatch, capsys):
    monkeypatch.setattr(facade, "prepare_runtime", Mock(return_value=package.receipt))
    selected = Mock(side_effect=AssertionError("ordinary prepare must not select"))
    monkeypatch.setattr(facade, "_prepare_gateway_environment_source", selected)
    monkeypatch.setattr(sys, "argv", ["stumpy.py", "prepare", "--home", str(package.home)])
    assert facade.main() == 0
    assert capsys.readouterr().out == json.dumps(package.receipt) + "\n"
    selected.assert_not_called()
    assert not subject._claimed
