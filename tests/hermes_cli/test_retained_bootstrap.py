"""Synthetic policy/DI checks; no gateway, native reader or real startup hooks.

The small package, process flags and lease below are declared test inputs. They
are not a verified runtime, OS ownership, ABI or actual startup evidence.
"""
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import hermes_retained_bootstrap as policy
from pm import environments


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    root = tmp_path / "payload" / "repo"
    root.mkdir(parents=True)
    home = tmp_path / "agent-home"
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
    executable.write_bytes(b"SYNTHETIC FILE ONLY; NEVER EXECUTED")
    canonical_path = root.parent / "manifest.json"
    package_path = root.parent / "gamedevbuddy-runtime.json"
    canonical = {"schema": 1, "ref": "a" * 40, "repo": "repo", "venv": "venv",
                 "store": "tools", "runtime": {"storePython": "tools/python.exe"}}
    write_json(canonical_path, canonical)
    package = {"schema": 1, "fullRuntime": True, "sourceRevision": "a" * 40,
               "files": {"manifest.json": {"kind": "file", "sha256": digest(canonical_path)}}}
    write_json(package_path, package)
    write_json(home / "gamedevbuddy-runtime.json",
               {"schema": 1, "product": "GameDevBuddy AI", "home": str(home)})
    pins = {}
    for name in policy._DEPENDENCY_INPUTS:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"SYNTHETIC DEPENDENCY INPUT")
        pins[name] = digest(path)
    plan = {"schemaVersion": 1, "policy": "retained-bootstrap", "root": str(root),
            "home": str(home), "environment": str(environment), "site": str(site),
            "selection": "payload", "factsSha256": None,
            "packageManifestSha256": digest(package_path), "sourceRevision": "a" * 40,
            "dependencyPins": pins, "attemptId": "synthetic-source-case", "writerExclusion": True}
    plan_path = tmp_path / "plan.json"
    fake_sys = SimpleNamespace(flags=SimpleNamespace(no_site=True, isolated=True),
                               dont_write_bytecode=True, modules={}, executable=str(executable),
                               stderr=io.StringIO())
    monkeypatch.setattr(policy, "sys", fake_sys)
    monkeypatch.setattr(policy, "_BOUND", None)
    for name in list(os.environ):
        monkeypatch.delenv(name)
    for name, value in {"HOME": str(tmp_path / "operator"), "USERPROFILE": str(tmp_path / "operator"),
                        "HERMES_HOME": str(home), "TMPDIR": str(scratch),
                        "TMP": str(scratch), "TEMP": str(scratch)}.items():
        monkeypatch.setenv(name, value)

    def bind():
        write_json(plan_path, plan)
        return policy.install(str(plan_path), root, ["foreground", "--home", str(home)])

    return SimpleNamespace(root=root, home=home, scratch=scratch, environment=environment,
                           site=site, executable=executable, canonical=canonical,
                           canonical_path=canonical_path, package=package, package_path=package_path,
                           plan=plan, plan_path=plan_path, fake_sys=fake_sys, bind=bind)


def test_valid_binding_is_frozen_and_cannot_be_replaced(prepared):
    contract = prepared.bind()
    assert policy.current_for(prepared.root) is contract
    contract.require_selection(contract.state, prepared.environment, prepared.site)
    with pytest.raises(AttributeError):
        contract.home = prepared.root
    with pytest.raises(policy.BootstrapRefused, match="78"):
        prepared.bind()


@pytest.mark.parametrize("field,value", [("no_site", False), ("isolated", False),
                                         ("dont_write_bytecode", False), ("preloaded", True)])
def test_clean_entry_is_required(prepared, field, value):
    if field == "preloaded":
        prepared.fake_sys.modules["hermes_cli"] = object()
    elif field == "dont_write_bytecode":
        prepared.fake_sys.dont_write_bytecode = value
    else:
        setattr(prepared.fake_sys.flags, field, value)
    with pytest.raises(policy.BootstrapRefused):
        prepared.bind()
    assert policy.current() is None


@pytest.mark.parametrize("marker", [".git", ".hermes-update-zip-swap", "hermes_cli.hermes-update-old",
                                    ".update-incomplete", ".lazy-refresh-incomplete",
                                    "state/.repair-incomplete", "state/publication.json",
                                    "state/source-completion-pending"])
def test_pending_maintenance_refuses_without_changing_marker(prepared, marker):
    key = hashlib.sha256(str(prepared.root.resolve()).encode()).hexdigest()[:16]
    path = (prepared.home / "installs" / key / marker[6:]
            if marker.startswith("state/") else prepared.root / marker)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"PRESERVE SYNTHETIC PENDING INPUT")
    before = path.read_bytes()
    with pytest.raises(policy.BootstrapRefused):
        prepared.bind()
    assert path.read_bytes() == before


@pytest.mark.parametrize("kind", ["extra-plan-key", "boolean-plan-schema", "boolean-home-schema",
                                  "boolean-package-schema", "nonobject-canonical-record", "bad-canonical-pin"])
def test_malformed_metadata_has_a_typed_refusal(prepared, kind):
    if kind == "extra-plan-key":
        prepared.plan["unexpected"] = True
    elif kind == "boolean-plan-schema":
        prepared.plan["schemaVersion"] = True
    elif kind == "boolean-home-schema":
        write_json(prepared.home / "gamedevbuddy-runtime.json",
                   {"schema": True, "product": "GameDevBuddy AI", "home": str(prepared.home)})
    else:
        if kind == "boolean-package-schema":
            prepared.package["schema"] = True
        elif kind == "nonobject-canonical-record":
            prepared.package["files"]["manifest.json"] = []
        else:
            prepared.package["files"]["manifest.json"]["sha256"] = "0" * 64
        write_json(prepared.package_path, prepared.package)
        prepared.plan["packageManifestSha256"] = digest(prepared.package_path)
    with pytest.raises(policy.BootstrapRefused):
        prepared.bind()
    assert policy.current() is None


@pytest.mark.parametrize("key", ["pm/lock.json", "pyproject.toml", "uv.lock"])
def test_dependency_input_change_refuses(prepared, key):
    (prepared.root / key).write_bytes(b"CHANGED SYNTHETIC INPUT")
    with pytest.raises(policy.BootstrapRefused):
        prepared.bind()


@pytest.mark.parametrize("kind", ["home-not-bound", "wrong-temp", "empty-scratch-marker", "missing-scratch"])
def test_home_and_private_temp_are_not_implicitly_prepared(prepared, monkeypatch, kind):
    if kind == "home-not-bound":
        monkeypatch.delenv("HERMES_HOME")
    elif kind == "wrong-temp":
        monkeypatch.setenv("TMP", str(prepared.root))
    elif kind == "empty-scratch-marker":
        monkeypatch.setenv("HERMES_SCRATCH_DIR", "")
    else:
        prepared.scratch.rmdir()
    with pytest.raises(policy.BootstrapRefused):
        prepared.bind()
    assert not (prepared.root / "cache").exists()


def test_unknown_facts_and_changed_selection_refuse(prepared):
    contract = prepared.bind()
    with pytest.raises(policy.BootstrapRefused):
        contract.require_selection(contract.state, prepared.root, prepared.site)
    write_json(contract.state / "facts.json", {"synthetic": True})
    with pytest.raises(policy.BootstrapRefused):
        contract.require_selection(contract.state, prepared.environment, prepared.site)


def test_sealed_payload_cannot_acquire_a_generation_lease(prepared):
    contract = prepared.bind()
    (prepared.environment.parent / ".lease-managed").write_bytes(b"synthetic")
    with pytest.raises(policy.BootstrapRefused):
        contract.require_selection(contract.state, prepared.environment, prepared.site)


@pytest.mark.parametrize("relation", ["inside", "ancestor"])
def test_private_home_and_payload_must_be_disjoint(prepared, monkeypatch, relation):
    contract = prepared.bind()
    other_home = prepared.root / "data" if relation == "inside" else prepared.root.parent.parent
    monkeypatch.setenv("HERMES_HOME", str(other_home))
    with pytest.raises(policy.BootstrapRefused) as caught:
        replace(contract, home=other_home).require_inputs()
    assert caught.value.reason == "private-home-must-be-outside-sealed-payload"


def test_canonical_activation_keeps_selected_hooks_after_policy(prepared, monkeypatch):
    contract = prepared.bind()
    observations = []
    import site
    monkeypatch.setattr(environments, "install_state_dir", lambda root: contract.state)
    monkeypatch.setattr(environments, "payload_venv", lambda root: prepared.environment)
    monkeypatch.setattr(environments, "payload_command_dir", lambda root: None)
    monkeypatch.setattr(environments, "_payload_manifest", lambda root: {"synthetic": True})
    monkeypatch.setattr(site, "addsitedir", lambda path: observations.append((path, policy.current())))
    monkeypatch.setattr(sys, "path", list(sys.path))
    environments.activate_dependencies(prepared.root)
    assert observations == [(str(prepared.site), contract)]
    assert sys.path[:2] == [str(prepared.root.resolve()), str(prepared.site)]


def test_private_generation_is_leased_before_hooks(prepared, monkeypatch):
    key = hashlib.sha256(str(prepared.root.resolve()).encode()).hexdigest()[:16]
    state = prepared.home / "installs" / key
    environment = state / "environments" / "gen-synthetic" / "venv"
    environment.mkdir(parents=True)
    (environment / "pyvenv.cfg").write_text(
        f"version = {sys.version_info.major}.{sys.version_info.minor}.0\n", encoding="utf-8")
    site_path = environments.site_packages(environment)
    site_path.mkdir(parents=True)
    (environment.parent / ".lease-managed").write_bytes(b"synthetic")
    write_json(state / "facts.json", {"packages": {"venv": {"environment": str(environment)}}})
    prepared.plan.update(selection="generation", environment=str(environment), site=str(site_path),
                         factsSha256=digest(state / "facts.json"))
    contract = prepared.bind()
    observations = []
    import site
    from hermes_cli import runtime_state

    @contextmanager
    def locked(root):
        observations.append("lock")
        yield True

    monkeypatch.setattr(runtime_state, "runtime_lock", locked)
    monkeypatch.setattr(runtime_state, "recover_publication", Mock(side_effect=AssertionError("no recovery")))
    monkeypatch.setattr(runtime_state, "lease_generation", lambda env: observations.append("lease"))
    monkeypatch.setattr(environments, "install_state_dir", lambda root: state)
    monkeypatch.setattr(environments, "payload_command_dir", lambda root: None)
    monkeypatch.setattr(environments, "_payload_manifest", lambda root: {"synthetic": True})
    monkeypatch.setattr(site, "addsitedir", lambda path: observations.append("hooks"))
    monkeypatch.setattr(sys, "path", list(sys.path))
    environments.activate_dependencies(prepared.root)
    assert observations == ["lock", "lease", "hooks"]
    assert policy.current() is contract


@pytest.mark.parametrize("retained", [False, True])
def test_site_loss_after_lease_preserves_legacy_and_refuses_retained(prepared, monkeypatch, retained):
    key = hashlib.sha256(str(prepared.root.resolve()).encode()).hexdigest()[:16]
    state = prepared.home / "installs" / key
    state.mkdir(parents=True)
    if retained:
        prepared.bind()
    from hermes_cli import runtime_state

    @contextmanager
    def locked(root):
        yield True

    def lose_site(environment):
        prepared.site.rmdir()
        return lambda: None

    recovery = Mock()
    monkeypatch.setattr(runtime_state, "runtime_lock", locked)
    monkeypatch.setattr(runtime_state, "recover_publication", recovery)
    monkeypatch.setattr(runtime_state, "lease_generation", lose_site)
    monkeypatch.setattr(environments, "install_state_dir", lambda root: state)
    monkeypatch.setattr(environments, "payload_venv", lambda root: prepared.environment)
    if retained:
        with pytest.raises(policy.BootstrapRefused) as caught:
            environments.activate_dependencies(prepared.root)
        assert caught.value.reason == "prepared-dependency-site-missing"
        recovery.assert_not_called()
    else:
        assert environments.activate_dependencies(prepared.root) is None
        recovery.assert_called_once_with(prepared.root)


def test_default_wrapper_does_not_load_policy(monkeypatch):
    path = Path(__file__).resolve().parents[2] / "scripts/build/launcher_wrapper.py"
    spec = importlib.util.spec_from_file_location("default_wrapper_source_case", path)
    wrapper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wrapper)
    imports = []
    target = Mock(return_value=0)
    monkeypatch.setattr(wrapper, "configure", Mock())
    monkeypatch.setattr(wrapper, "importlib", SimpleNamespace(import_module=lambda name:
                        imports.append(name) or SimpleNamespace(main=target)))
    monkeypatch.setattr(wrapper, "HERMES_ENTRY_MODULE", "synthetic_entry")
    monkeypatch.setattr(wrapper, "HERMES_ENTRY_FUNC", "main")
    assert wrapper.main(["synthetic-launcher"]) == 0
    assert imports == ["hermes_bootstrap", "synthetic_entry"]


def test_retained_wrapper_selects_policy_before_bootstrap_and_defers_hooks(prepared, monkeypatch):
    path = Path(__file__).resolve().parents[2] / "scripts/build/launcher_wrapper.py"
    spec = importlib.util.spec_from_file_location("retained_wrapper_source_case", path)
    wrapper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wrapper)
    write_json(prepared.plan_path, prepared.plan)
    argv = [str(prepared.root.parent / "bin" / "stumpy.py"), "--gdb-retained-bootstrap",
            str(prepared.plan_path), "foreground", "--home", str(prepared.home)]
    fake_sys = SimpleNamespace(argv=argv, flags=prepared.fake_sys.flags,
                               dont_write_bytecode=True, modules={}, path=list(sys.path),
                               stdout=sys.stdout, stderr=sys.stderr)
    sites = []
    imports = []
    target = Mock(return_value=0)
    source = object()

    def attach(contract, raw_plan):
        assert contract is policy.current()
        assert raw_plan == prepared.plan_path.read_bytes()
        return source

    owned = SimpleNamespace(attach=Mock(side_effect=attach), current=lambda contract: source,
                            run_foreground=target, require_lifecycle_available=Mock(),
                            OwnedGatewayUnknown=RuntimeError)

    def load(name):
        imports.append(name)
        if name == "hermes_retained_bootstrap":
            assert not sites
            return policy
        if name == "hermes_owned_gateway":
            return owned
        assert policy.current() is not None
        owned.attach.assert_called_once()
        assert not sites
        return SimpleNamespace(main=target)

    monkeypatch.setattr(wrapper, "sys", fake_sys)
    monkeypatch.setattr(wrapper, "site", SimpleNamespace(addsitedir=sites.append))
    monkeypatch.setattr(wrapper, "importlib", SimpleNamespace(import_module=load))
    monkeypatch.setattr(wrapper, "HERMES_ENTRY_MODULE", "hermes_cli.gamedevbuddy")
    monkeypatch.setattr(wrapper, "HERMES_ENTRY_FUNC", "main")
    monkeypatch.setattr(wrapper, "HERMES_REPO_REL", "../repo")
    monkeypatch.setattr(wrapper, "HERMES_SITE_REL", "../" + prepared.site.relative_to(prepared.root.parent).as_posix())
    assert wrapper.main(argv) == 0
    assert imports == ["hermes_retained_bootstrap", "hermes_owned_gateway", "hermes_bootstrap",
                       "hermes_owned_gateway"]
    assert fake_sys.argv == [argv[0], *argv[3:]]
    target.assert_called_once_with(source)
    owned.require_lifecycle_available.assert_called_once_with(source)


def test_unavailable_owned_lifecycle_refuses_before_configure_or_bootstrap(prepared, monkeypatch):
    import hermes_owned_gateway as owned
    path = Path(__file__).resolve().parents[2] / "scripts/build/launcher_wrapper.py"
    spec = importlib.util.spec_from_file_location("unavailable_owned_wrapper_case", path)
    wrapper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wrapper)
    write_json(prepared.plan_path, prepared.plan)
    argv = [str(prepared.root.parent / "bin/stumpy.py"), "--gdb-retained-bootstrap",
            str(prepared.plan_path), "foreground", "--home", str(prepared.home)]
    fake_sys = SimpleNamespace(argv=argv, flags=prepared.fake_sys.flags, dont_write_bytecode=True,
                               modules={}, path=list(sys.path), stdout=sys.stdout, stderr=sys.stderr)

    class RetainedUnknown(Exception):
        pass

    source = SimpleNamespace(contract=object(), require_attached=Mock(),
                             hold_unknown=Mock(side_effect=RetainedUnknown))
    imports = []

    def load(name):
        imports.append(name)
        if name == "hermes_retained_bootstrap":
            return policy
        if name == "hermes_owned_gateway":
            return SimpleNamespace(attach=Mock(return_value=source), current=lambda contract: source,
                                   require_lifecycle_available=owned.require_lifecycle_available,
                                   OwnedGatewayUnknown=owned.OwnedGatewayUnknown)
        raise AssertionError("unavailable lifetime imported bootstrap or gateway")

    configure = Mock(side_effect=AssertionError("unavailable lifetime configured dependencies"))
    monkeypatch.setattr(wrapper, "sys", fake_sys)
    monkeypatch.setattr(wrapper, "importlib", SimpleNamespace(import_module=load))
    monkeypatch.setattr(wrapper, "configure", configure)
    monkeypatch.setattr(wrapper, "HERMES_ENTRY_MODULE", "hermes_cli.gamedevbuddy")
    monkeypatch.setattr(wrapper, "HERMES_REPO_REL", "../repo")
    monkeypatch.setattr(wrapper, "HERMES_SITE_REL", "../" + prepared.site.relative_to(prepared.root.parent).as_posix())
    with pytest.raises(RetainedUnknown):
        wrapper.main(argv)
    assert imports == ["hermes_retained_bootstrap", "hermes_owned_gateway"]
    configure.assert_not_called()
    source.hold_unknown.assert_called_once_with()
