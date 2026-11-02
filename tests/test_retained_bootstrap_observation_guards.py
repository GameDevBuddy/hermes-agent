"""Actual existing input/selection/activation/wrapper guards for observation data; lower runtime DI only."""
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import hermes_retained_bootstrap as policy
from pm import environments
from tests.retained_observations_fixture import digest, write_json

pytest_plugins = ("tests.retained_observations_fixture",)


@pytest.mark.parametrize("field", ["no_site", "isolated", "dont_write_bytecode", "hermes_bootstrap", "hermes_cli", "pm"])
def test_observations_require_the_same_clean_sealed_entry(observed, field):
    changes = {
        "no_site": lambda: setattr(observed.fake_sys.flags, "no_site", False),
        "isolated": lambda: setattr(observed.fake_sys.flags, "isolated", False),
        "dont_write_bytecode": lambda: setattr(observed.fake_sys, "dont_write_bytecode", False),
    }
    change = changes.get(field)
    if change is not None:
        change()
    else:
        observed.fake_sys.modules[field] = object()
    with pytest.raises(policy.BootstrapRefused) as caught:
        observed.bind()
    assert caught.value.reason == "contract-was-not-selected-at-clean-sealed-entry"
    assert policy.current() is None


@pytest.mark.parametrize("marker", [".git", ".update-incomplete", ".lazy-refresh-incomplete", ".hermes-update-zip-swap"])
def test_existing_maintenance_markers_are_never_repaired(observed, marker):
    path = observed.root / marker
    path.write_bytes(b"retain observation input")
    with pytest.raises(policy.BootstrapRefused) as caught:
        observed.bind()
    assert caught.value.reason == "owned-maintenance-required"
    assert path.read_bytes() == b"retain observation input" and policy.current() is None


@pytest.mark.parametrize("name", ["TMPDIR", "TMP", "TEMP", "HERMES_HOME"])
def test_explicit_private_home_and_all_temp_bindings_remain_required(observed, monkeypatch, name):
    monkeypatch.setenv(name, str(observed.root))
    with pytest.raises(policy.BootstrapRefused):
        observed.bind()
    assert policy.current() is None and observed.scratch.is_dir()


@pytest.mark.parametrize("kind", ["scratch-marker", "scratch-missing", "environment-missing", "site-missing"])
def test_prepared_scratch_and_environment_are_not_created_implicitly(observed, monkeypatch, kind):
    changes = {
        "scratch-marker": lambda: monkeypatch.setenv("HERMES_SCRATCH_DIR", ""),
        "scratch-missing": observed.scratch.rmdir,
        "environment-missing": lambda: (observed.environment / "pyvenv.cfg").unlink(),
        "site-missing": observed.site.rmdir,
    }
    changes[kind]()
    with pytest.raises(policy.BootstrapRefused):
        observed.bind()
    assert policy.current() is None


@pytest.mark.parametrize("name", ["pm/lock.json", "pyproject.toml", "uv.lock"])
def test_v2_rechecks_actual_dependency_input_pins(observed, name):
    (observed.root / name).write_bytes(b"changed observation input")
    with pytest.raises(policy.BootstrapRefused) as caught:
        observed.bind()
    assert caught.value.reason == "dependency-input-changed"


@pytest.mark.parametrize("kind", ["package", "canonical", "interpreter", "tool-store", "home-marker"])
def test_existing_manifest_interpreter_and_home_guards_are_not_weakened(observed, monkeypatch, kind):
    changes = {
        "package": lambda: observed.package_path.write_bytes(b"changed"),
        "canonical": lambda: observed.canonical_path.write_bytes(b"changed"),
        "interpreter": lambda: setattr(observed.fake_sys, "executable", str(observed.root / "other")),
        "tool-store": lambda: monkeypatch.setenv("HERMES_RUNTIME_DIR", str(observed.root)),
        "home-marker": lambda: write_json(observed.home / "gamedevbuddy-runtime.json", {"schema": 1}),
    }
    changes[kind]()
    with pytest.raises(policy.BootstrapRefused):
        observed.bind()
    assert policy.current() is None


def test_frozen_legacy_positional_defaults_have_no_observation_or_permission_factory(observed):
    current = observed.bind()
    legacy = policy.RetainedBootstrap(current.root, current.home, current.environment, current.site,
                                      current.selection, current.facts_sha256, current.manifest_sha256,
                                      current.source_revision, current.dependency_pins, current.attempt_id)
    assert legacy.input_policy is None and legacy.dependency_pins == current.dependency_pins
    assert policy.current() is current
    with pytest.raises(policy.BootstrapRefused) as caught:
        policy.original_plan_bytes(legacy)
    assert caught.value.reason == "original-bootstrap-plan-required"


def test_payload_activation_uses_original_contract_and_selection_before_hooks(observed, monkeypatch):
    import site
    contract = observed.bind()
    observations = []
    monkeypatch.setattr(environments, "install_state_dir", lambda root: contract.state)
    monkeypatch.setattr(environments, "payload_venv", lambda root: observed.environment)
    monkeypatch.setattr(environments, "payload_command_dir", lambda root: None)
    monkeypatch.setattr(environments, "_payload_manifest", lambda root: {"synthetic": True})
    monkeypatch.setattr(site, "addsitedir", lambda path: observations.append((path, policy.current())))
    monkeypatch.setattr(sys, "path", list(sys.path))
    environments.activate_dependencies(observed.root)
    assert observations == [(str(observed.site), contract)]
    with pytest.raises(policy.BootstrapRefused):
        contract.require_selection(contract.state, observed.root, observed.site)


def test_generation_keeps_original_facts_and_lease_selection_guards(observed, monkeypatch):
    import site
    from hermes_cli import runtime_state
    key = hashlib.sha256(str(observed.root.resolve()).encode()).hexdigest()[:16]
    state = observed.home / "installs" / key
    environment = state / "environments" / "observation-generation" / "venv"
    environment.mkdir(parents=True)
    (environment / "pyvenv.cfg").write_text(f"version = {sys.version_info.major}.{sys.version_info.minor}.0\n")
    selected = environments.site_packages(environment)
    selected.mkdir(parents=True)
    (environment.parent / ".lease-managed").write_bytes(b"synthetic selection input")
    write_json(state / "facts.json", {"packages": {"venv": {"environment": str(environment)}}})
    observed.plan.update(selection="generation", environment=str(environment), site=str(selected),
                         factsSha256=digest(state / "facts.json"))
    contract, order = observed.bind(), []

    @contextmanager
    def locked(root):
        order.append("lock")
        yield True

    monkeypatch.setattr(runtime_state, "runtime_lock", locked)
    monkeypatch.setattr(runtime_state, "recover_publication", Mock(side_effect=AssertionError("no repair")))
    monkeypatch.setattr(runtime_state, "lease_generation", lambda env: order.append("lease"))
    monkeypatch.setattr(environments, "install_state_dir", lambda root: state)
    monkeypatch.setattr(environments, "payload_command_dir", lambda root: None)
    monkeypatch.setattr(environments, "_payload_manifest", lambda root: {"synthetic": True})
    monkeypatch.setattr(site, "addsitedir", lambda path: order.append("hooks"))
    monkeypatch.setattr(sys, "path", list(sys.path))
    environments.activate_dependencies(observed.root)
    assert order == ["lock", "lease", "hooks"] and policy.current() is contract
    (state / "facts.json").write_bytes(b"changed")
    with pytest.raises(policy.BootstrapRefused) as caught:
        contract.require_selection(state, environment, selected)
    assert caught.value.reason == "private-facts-changed"


def test_actual_wrapper_keeps_v2_availability_refusal_before_any_startup(observed, monkeypatch):
    import hermes_owned_gateway as owned
    path = Path(__file__).resolve().parents[1] / "scripts/build/launcher_wrapper.py"
    spec = importlib.util.spec_from_file_location("observed_wrapper_source_case", path)
    wrapper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wrapper)
    write_json(observed.plan_path, observed.plan)
    raw, imports = observed.plan_path.read_bytes(), []
    argv = [str(observed.root.parent / "bin/stumpy.py"), "--gdb-retained-bootstrap", str(observed.plan_path),
            "foreground", "--home", str(observed.home)]
    fake_sys = SimpleNamespace(argv=argv, flags=observed.fake_sys.flags, dont_write_bytecode=True,
                               modules={}, path=list(sys.path), stdout=sys.stdout, stderr=sys.stderr)

    class Held(Exception):
        pass

    source = SimpleNamespace(require_attached=Mock(), hold_unknown=Mock(side_effect=Held))
    def attach(contract, original):
        assert contract is policy.current() and original == raw
        assert contract.input_policy == "pinned-input-observations-v1"
        source.contract = contract
        return source

    def load(name):
        imports.append(name)
        if name == "hermes_retained_bootstrap":
            return policy
        if name == "hermes_owned_gateway":
            return SimpleNamespace(attach=attach, current=lambda contract: source,
                                   require_lifecycle_available=owned.require_lifecycle_available,
                                   prepare_lifecycle_entry=Mock(),
                                   OwnedGatewayUnknown=owned.OwnedGatewayUnknown)
        raise AssertionError("observation policy imported a runtime before availability")

    configure = Mock(side_effect=AssertionError("observation policy configured startup"))
    monkeypatch.setattr(wrapper, "sys", fake_sys)
    monkeypatch.setattr(wrapper, "configure", configure)
    monkeypatch.setattr(wrapper, "importlib", SimpleNamespace(import_module=load))
    monkeypatch.setattr(wrapper, "HERMES_ENTRY_MODULE", "hermes_cli.gamedevbuddy")
    monkeypatch.setattr(wrapper, "HERMES_REPO_REL", "../repo")
    monkeypatch.setattr(wrapper, "HERMES_SITE_REL", "../" + observed.site.relative_to(observed.root.parent).as_posix())
    with pytest.raises(Held):
        wrapper.main(argv)
    assert imports == ["hermes_retained_bootstrap", "hermes_owned_gateway"]
    configure.assert_not_called()
    source.require_attached.assert_called_once_with(policy.current())
    source.hold_unknown.assert_called_once_with()


def test_default_wrapper_keeps_ordinary_entry_without_observation_selection(monkeypatch):
    path = Path(__file__).resolve().parents[1] / "scripts/build/launcher_wrapper.py"
    spec = importlib.util.spec_from_file_location("ordinary_wrapper_source_case", path)
    wrapper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wrapper)
    imports, target = [], Mock(return_value=0)
    monkeypatch.setattr(wrapper, "configure", Mock())
    monkeypatch.setattr(wrapper, "importlib", SimpleNamespace(import_module=lambda name:
                        imports.append(name) or SimpleNamespace(main=target)))
    monkeypatch.setattr(wrapper, "HERMES_ENTRY_MODULE", "synthetic_ordinary_entry")
    monkeypatch.setattr(wrapper, "HERMES_ENTRY_FUNC", "main")
    assert wrapper.main(["inert-source-launcher"]) == 0
    assert imports == ["hermes_bootstrap", "synthetic_ordinary_entry"]
    target.assert_called_once_with()
