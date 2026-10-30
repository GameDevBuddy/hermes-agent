"""Private setup description of the canonical PM selection; never native authority."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import threading

MIB = 1024 * 1024
MAX_PACKAGE_JSON, MAX_CANONICAL_JSON, MAX_FACTS_JSON = 64*MIB, 8*MIB, 8*MIB
MAX_SMALL, MAX_SOURCE, MAX_DEPENDENCY = 64*1024, 4*MIB, 32*MIB
MAX_INTERPRETER, MAX_DESCRIPTOR, MAX_PATH = 256*MIB, 128*1024, 32*1024
CHUNK, MAX_IO_CREDIT = 64*1024, 1024*MIB
METADATA_CREDIT = 4*(MAX_PACKAGE_JSON+MAX_CANONICAL_JSON+MAX_FACTS_JSON) + 2*MAX_DESCRIPTOR + 16*MAX_SMALL
DEPENDENCIES = ("pm/lock.json", "pyproject.toml", "uv.lock")
SOURCE_FILES = (
    "hermes_cli/gamedevbuddy.py", "hermes_cli/gamedevbuddy_environment.py",
    "hermes_retained_bootstrap.py", "pm/environments.py",
)
TOP_KEYS = {
    "schemaVersion", "kind", "root", "home", "packageRoot", "installState",
    "installStatePresent", "environment", "site", "selection", "facts", "homeMarker",
    "packageManifest", "canonicalManifest", "sourceRevision", "interpreter",
    "dependencyInputs", "sourceFiles", "pyvenvConfig", "generationLeaseMarker", "pythonAbi",
    "preparation",
}
_lock = threading.Lock()
_claimed = False
_metadata_reserved = 0
_observation = None

class GatewayDescriptionRefused(RuntimeError):
    pass

def _refuse(reason):
    raise GatewayDescriptionRefused(reason)

class _Observation:
    __slots__ = ("root", "home", "port", "phase", "error", "files", "directories", "absent",
                 "io_reserved", "wire", "package", "canonical", "package_info", "canonical_info")
    def __init__(self):
        self.root = self.home = self.port = None
        self.phase, self.error = "claimed", None
        self.files, self.directories, self.absent = {}, {}, set()
        self.io_reserved, self.wire = 0, None
        self.package = self.canonical = self.package_info = self.canonical_info = None

def _owned(value, phase):
    if (value is not _observation or type(value) is not _Observation or value.phase != phase
            or _metadata_reserved != METADATA_CREDIT):
        _refuse("description-attempt-unavailable")

def _text(value):
    if (type(value) is not str or not value or len(value.encode("utf-8", "strict")) > MAX_PATH
            or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        _refuse("invalid-path-metadata")
    return value

def _absolute(value):
    path = Path(_text(str(value)))
    if not path.is_absolute():
        _refuse("absolute-path-required")
    return path.resolve()

def _inside(base, relative):
    if (type(relative) is not str or not relative or "\\" in relative or ":" in relative
            or PurePosixPath(relative).is_absolute()
            or any(p in (".", "..") for p in relative.split("/"))):
        _refuse("invalid-package-relative-path")
    result = (base / relative).resolve()
    if not result.is_relative_to(base) or result == base:
        _refuse("package-relative-path-escaped")
    return result

def _present(path):
    try:
        path.lstat()
        return True
    except FileNotFoundError:
        return False

def _directory(path):
    info = path.lstat()
    if path.resolve() != path or not stat.S_ISDIR(info.st_mode):
        _refuse("required-directory-missing-or-redirected")
    return _signature(info)

def _record_directory(value, path):
    value.directories[path] = _directory(path)

def _signature(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)

def _same_file(before, opened):
    # Windows path/handle ctime can differ; each API's ctime is checked separately.
    return (os.path.samestat(before, opened) and stat.S_ISREG(opened.st_mode)
            and before.st_size == opened.st_size and before.st_mtime_ns == opened.st_mtime_ns)

def _read(value, path, limit, *, keep):
    if path.resolve() != path:
        _refuse("input-path-redirected")
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or not 0 <= before.st_size <= limit:
        _refuse("input-file-type-or-byte-bound")
    digest, size, body = hashlib.sha256(), 0, None
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        if not _same_file(before, opened):
            _refuse("input-changed-before-read")
        if keep:
            if value.io_reserved + limit + 1 > MAX_IO_CREDIT:
                _refuse("description-io-credit")
            value.io_reserved += limit + 1
            body = stream.read(limit+1)
            size = len(body)
            digest.update(body)
        else:
            while True:
                if value.io_reserved + CHUNK > MAX_IO_CREDIT:
                    _refuse("description-io-credit")
                value.io_reserved += CHUNK
                block = stream.read(CHUNK)
                if not block:
                    break
                size += len(block)
                if size > limit:
                    _refuse("input-grew-past-bound")
                digest.update(block)
        if size != before.st_size or _signature(os.fstat(stream.fileno())) != _signature(opened):
            _refuse("input-changed-during-read")
    if _signature(path.lstat()) != _signature(before):
        _refuse("input-changed-after-read")
    return (str(path), size, digest.hexdigest()), _signature(before), body

def _record(value, path, limit, *, keep=False):
    key = str(path)
    if key in value.files:
        _refuse("input-observed-twice")
    info, signature, body = _read(value, path, limit, keep=keep)
    value.files[key] = (path, limit, info, signature)
    return info, body

def _info(info):
    path, size, digest = info
    return {"path": path, "bytes": size, "sha256": digest}

def _pairs(items):
    data = {}
    for key, item in items:
        if key in data:
            _refuse("duplicate-json-key")
        data[key] = item
    return data

def _constant(_value):
    _refuse("non-finite-json")

def _json(value, path, limit):
    info, raw = _record(value, path, limit, keep=True)
    data = json.loads(raw.decode("utf-8-sig", "strict"), object_pairs_hook=_pairs, parse_constant=_constant)
    if type(data) is not dict:
        _refuse("json-object-required")
    return info, data

def _file_pin(package, package_root, path, info):
    key = path.relative_to(package_root).as_posix()
    record = package.get("files", {}).get(key)
    if (type(record) is not dict or record.get("kind") != "file"
            or type(record.get("sha256")) is not str
            or record["sha256"] != info[2]):
        _refuse("package-file-pin-mismatch")

def _maintenance(value, state):
    for path in (
        value.root / ".git", value.root / ".hermes-update-zip-swap",
        value.root / "hermes_cli.hermes-update-old", value.root / ".update-incomplete",
        value.root / ".lazy-refresh-incomplete", state / ".repair-incomplete",
        state / "publication.json", state / "source-completion-pending",
    ):
        if _present(path):
            _refuse("owned-maintenance-required")

def _check_package(value, package, canonical):
    revision = package.get("sourceRevision")
    if (type(package.get("schema")) is not int or package["schema"] != 1
            or package.get("fullRuntime") is not True or type(package.get("files")) is not dict
            or type(revision) is not str or re.fullmatch(r"[a-f0-9]{40}", revision) is None
            or type(canonical.get("schema")) is not int or canonical["schema"] != 1
            or canonical.get("ref") != revision
            or _inside(value.root.parent, canonical.get("repo")) != value.root
            or type(canonical.get("runtime")) is not dict):
        _refuse("complete-canonical-package-required")


def begin_gateway_description(root, home, port):
    global _claimed, _metadata_reserved, _observation
    with _lock:
        if _claimed:
            _refuse("description-already-attempted")
        _claimed = True
        _metadata_reserved = METADATA_CREDIT  # BEFORE paths/JSON/hash/record allocation
        value = _observation = _Observation()
    value.root, value.home = _absolute(root), _absolute(home)
    value.port = port
    if (type(port) is not int or not 1024 <= port <= 65535
            or value.root != Path(__file__).resolve().parents[1]
            or not os.environ.get("HERMES_HOME")
            or _absolute(os.environ["HERMES_HOME"]) != value.home
            or os.environ.get("HERMES_PROFILE")):
        _refuse("explicit-product-home-and-source-required")
    package_root = value.root.parent
    if value.home.is_relative_to(package_root) or package_root.is_relative_to(value.home):
        _refuse("home-must-be-outside-package")
    if not sys.flags.no_site or not sys.flags.isolated or not sys.dont_write_bytecode:
        _refuse("sealed-interpreter-flags-required")
    value.package_info, package = _json(value, package_root/"gamedevbuddy-runtime.json", MAX_PACKAGE_JSON)
    value.canonical_info, canonical = _json(value, package_root/"manifest.json", MAX_CANONICAL_JSON)
    _check_package(value, package, canonical)
    _file_pin(package, package_root, package_root/"manifest.json", value.canonical_info)
    interpreter = _inside(package_root, canonical["runtime"].get("storePython"))
    if _absolute(sys.executable) != interpreter:
        _refuse("selected-sealed-interpreter-required")
    interpreter_info, _ = _record(value, interpreter, MAX_INTERPRETER)
    _file_pin(package, package_root, interpreter, interpreter_info)
    inputs = [(name, MAX_DEPENDENCY) for name in DEPENDENCIES]
    inputs += [(name, MAX_SOURCE) for name in SOURCE_FILES]
    for relative, limit in inputs:
        path = value.root/relative
        info, _ = _record(value, path, limit)
        _file_pin(package, package_root, path, info)
    value.package, value.canonical = package, canonical
    value.phase = "preparation"
    return value

def _generation_fact(facts):
    if (type(facts.get("schema")) is not int or facts["schema"] != 1
            or type(facts.get("packages")) is not dict):
        _refuse("invalid-canonical-facts")
    if "venv" not in facts["packages"]:
        return None
    record = facts["packages"]["venv"]
    if (type(record) is not dict or type(record.get("environment")) is not str
            or not record["environment"] or type(record.get("stamp")) is not str
            or not record["stamp"] or type(record.get("extras")) is not list
            or any(type(extra) is not str for extra in record["extras"])):
        _refuse("incomplete-generation-fact")
    return record


def _selected_environment(value, api, state, state_present, record):
    if record is None:
        if _present(state/"environments"):
            _refuse("missing-generation-selection-is-not-payload-permission")
        value.absent.add(state/"environments")
    recorded = api._recorded_venv(value.root)
    selected, committed = api.selected_venv(value.root), api.committed_venv(value.root)
    if record is not None:
        environment = _absolute(record["environment"])
        mode = "generation"
        if (not state_present or recorded != environment
                or not environment.is_relative_to(state/"environments")):
            _refuse("generation-selection-mismatch")
        lease_info, _ = _record(value, environment.parent/".lease-managed", MAX_SMALL)
        leases = environment.parent/".leases"
        if _present(leases):
            _record_directory(value, leases)
    else:
        mode, lease_info = "payload", None
        environment = _inside(value.root.parent, value.canonical.get("venv"))
        if recorded is not None or api.payload_venv(value.root) != environment:
            _refuse("payload-selection-mismatch")
        if _present(environment.parent/".lease-managed"):
            _refuse("payload-must-not-adopt-generation-lease")
    if selected != environment or committed != environment:
        _refuse("canonical-selection-disagrees")
    return environment, mode, lease_info


def _selected_version(value, api, environment):
    cfg_info, cfg = _record(value, environment/"pyvenv.cfg", MAX_SMALL, keep=True)
    versions = []
    for line in cfg.decode("utf-8-sig", "strict").splitlines():
        key, _, text = line.partition("=")
        if key.strip() in ("version", "version_info"):
            major, _, rest = text.strip().partition(".")
            minor, _, _ = rest.partition(".")
            if not major.isdigit() or not minor.isdigit():
                _refuse("invalid-prepared-python-version")
            versions.append((int(major), int(minor)))
    if not versions or any(version != versions[0] for version in versions):
        _refuse("missing-or-conflicting-prepared-python-version")
    version = api.venv_python_version(environment)
    if version != versions[0] or version != sys.version_info[:2]:
        _refuse("prepared-python-abi-mismatch")
    return cfg_info, version


def _selection(value, api, state, state_present):
    _maintenance(value, state)
    facts_path = api.runtime_facts_path(value.root)
    if facts_path != state/"facts.json":
        _refuse("canonical-facts-path-changed")
    facts_info, record = None, None
    if _present(facts_path):
        facts_info, facts = _json(value, facts_path, MAX_FACTS_JSON)
        record = _generation_fact(facts)
    else:
        value.absent.add(facts_path)
    environment, mode, lease_info = _selected_environment(value, api, state, state_present, record)
    _record_directory(value, environment)
    cfg_info, version = _selected_version(value, api, environment)
    site = api.site_packages(environment).resolve()
    if not site.is_relative_to(environment):
        _refuse("canonical-site-escaped-environment")
    _record_directory(value, site)
    if not api.running_from_selected_environment(value.root):
        _refuse("worker-not-running-on-canonical-selection")
    return environment, site, mode, facts_info, cfg_info, lease_info, version

def _recheck_inputs(value):
    # Rehash ONLY the original fixed paths; never refresh DTO, rescan or choose again.
    for path, limit, old_info, old_signature in tuple(value.files.values()):
        info, signature, _ = _read(value, path, limit, keep=False)
        if info != old_info or signature != old_signature:
            _refuse("frozen-description-input-changed")
    for path in value.absent:
        if _present(path):
            _refuse("previously-absent-selection-appeared")
    for path, old_signature in value.directories.items():
        if _directory(path) != old_signature:
            _refuse("prepared-directory-changed")

def _check_receipt(value, receipt):
    if (type(receipt) is not dict or set(receipt) != {
            "product", "home", "created", "profilesAdded", "accountRequired", "gatewayStarted"}
            or receipt["product"] != "GameDevBuddy AI" or receipt["home"] != str(value.home)
            or type(receipt["created"]) is not bool or type(receipt["profilesAdded"]) is not list
            or any(type(name) is not str for name in receipt["profilesAdded"])
            or receipt["accountRequired"] is not False or receipt["gatewayStarted"] is not False):
        _refuse("original-preparation-receipt-changed")


def _freeze_selected(value, receipt, api, state, state_present):
    environment, site, mode, facts, cfg, lease, version = _selection(value, api, state, state_present)
    marker_info, marker = _json(value, value.home/"gamedevbuddy-runtime.json", MAX_SMALL)
    if (type(marker.get("schema")) is not int or marker["schema"] != 1
            or marker.get("product") != "GameDevBuddy AI"
            or _absolute(marker.get("home")) != value.home):
        _refuse("canonical-home-marker-mismatch")
    _check_receipt(value, receipt)
    def observed(path):
        return _info(value.files[str(path)][2])
    data = {
        "schemaVersion": 1, "kind": "gdb-prepared-gateway-environment-v1",
        "root": str(value.root), "home": str(value.home), "packageRoot": str(value.root.parent),
        "installState": str(state), "installStatePresent": state_present,
        "environment": str(environment), "site": str(site), "selection": mode,
        "facts": _info(facts) if facts is not None else None, "homeMarker": _info(marker_info),
        "packageManifest": _info(value.package_info), "canonicalManifest": _info(value.canonical_info),
        "sourceRevision": value.package["sourceRevision"],
        "interpreter": observed(_absolute(sys.executable)),
        "dependencyInputs": {name: observed(value.root/name) for name in DEPENDENCIES},
        "sourceFiles": {name: observed(value.root/name) for name in SOURCE_FILES},
        "pyvenvConfig": _info(cfg), "generationLeaseMarker": _info(lease) if lease is not None else None,
        "pythonAbi": list(version), "preparation": receipt,
    }
    if set(data) != TOP_KEYS:
        _refuse("descriptor-schema-drift")
    _recheck_inputs(value)
    _maintenance(value, state)
    if (api.install_state_dir(value.root) != state or api.runtime_facts_path(value.root) != state/"facts.json"
            or api.selected_venv(value.root) != environment or api.committed_venv(value.root) != environment
            or api.site_packages(environment).resolve() != site):
        _refuse("canonical-selection-changed-after-observation")
    # Catch inputs changed by/concurrently with those exact canonical read calls;
    # retain the SAME previously frozen values, never rebuild from the new state.
    _recheck_inputs(value)
    wire = json.dumps(data, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8", "strict")
    if len(wire) > MAX_DESCRIPTOR:
        _refuse("descriptor-byte-bound")
    value.wire, value.phase = wire, "frozen"

def freeze_gateway_description(value, receipt):
    with _lock:
        _owned(value, "preparation")
        value.phase = "observing"
    from pm import environments as api
    if (Path(api.__file__).resolve() != value.root/"pm/environments.py"
            or set(api.ACTIVATION_INPUTS) != set(DEPENDENCIES)
            or api.dependency_home_root().resolve() != value.home
            or _absolute(os.environ.get("HERMES_HOME", "")) != value.home
            or os.environ.get("HERMES_PROFILE")):
        _refuse("canonical-source-or-home-binding-changed")
    state = api.install_state_dir(value.root)
    if state != value.home/"installs"/api.install_key(value.root) or state.resolve() != state:
        _refuse("canonical-install-state-escaped-home")
    if _present(state):
        _record_directory(value, state)
        lockfile = state/".install.lock"
        if not _present(lockfile) or not stat.S_ISREG(lockfile.lstat().st_mode):
            _refuse("existing-install-lock-required-no-creation")
        from hermes_cli.runtime_state import runtime_lock
        with runtime_lock(value.root) as held:
            if not held:
                _refuse("canonical-install-writer-exclusion-unavailable")
            _freeze_selected(value, receipt, api, state, True)
    else:
        # No runtime_lock call here: it mkdir/creates state even for a read.
        value.absent.add(state)
        _freeze_selected(value, receipt, api, state, False)
        if _present(state):
            _refuse("install-state-appeared-during-payload-description")

def emit_gateway_description(value, output):
    with _lock:
        _owned(value, "frozen")
        value.phase = "emitting"
    frame = value.wire + b"\n"
    if output.write(frame) != len(frame):
        _refuse("descriptor-output-partial-no-replay")
    output.flush()
    value.phase = "emitted"  # output observation only, NOT gateway/native readiness

def fail_gateway_description(value, error):
    with _lock:
        current = _observation
        if current is not None and (value is None or value is current):
            current.phase, current.error = "failed", type(error).__name__
    # Keep claimed/credit/frozen bytes/observations; no rollback, repair, reset or retry.

