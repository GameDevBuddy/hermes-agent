"""Synthetic publisher fixtures, never native/runtime acceptance authority."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

from scripts.bundles import gamedevbuddy
from scripts.bundles.gamedevbuddy import inventory, seal_runtime
from scripts.build.inputs import RESOURCE_ENV

REVISION = "a" * 40


@pytest.fixture
def source_revision(tmp_path, monkeypatch):
    """A real, tagless synthetic Git checkout for the real canonical producer."""
    source = tmp_path / "synthetic-source"
    for relative in (
        "scripts/write_install_stamp.py", "scripts/releases/commit_build.py",
        "hermes_cli/__init__.py", "hermes_cli/update_channel.py", "hermes_cli/release_channels.py",
        "hermes_cli/steward.py", "pm/paths.py", "pm/environments.py", "hermes_constants.py",
    ):
        destination = source / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(Path(__file__).resolve().parents[2] / relative, destination)
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "empty.gitconfig"))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")

    def git(*args):
        return subprocess.run(["git", *args], cwd=source, check=True, capture_output=True,
                              text=True, encoding="utf-8", timeout=30).stdout.strip()

    git("init", "-q", "-b", "synthetic-publisher-test")
    git("config", "user.name", "Synthetic publisher fixture")
    git("config", "user.email", "publisher-fixture@example.invalid")
    git("add", ".")
    git("commit", "-qm", "Synthetic source fixture, not runtime authority")
    monkeypatch.setattr(gamedevbuddy, "ROOT", source)
    return git("rev-parse", "HEAD")


def payload(tmp_path, revision=REVISION):
    root = tmp_path / "runtime"
    files = ["tools/python/python.exe", "venv/Lib/site-packages/installed.py",
             "hermes-agent/LICENSE", "hermes-agent/run_agent.py", "hermes-agent/hermes_cli/gamedevbuddy.py",
             "hermes-agent/pyproject.toml", "hermes-agent/uv.lock", "hermes-agent/pm/lock.json",
             "tools/facts.json", "pm-runtime/pm-runtime.json", "bin/hermes.exe",
             "hermes-agent/hermes_cli/tui_dist/entry.js", "hermes-agent/hermes_cli/tui_dist/package.json",
             "hermes-agent/hermes_cli/web_dist/index.html"]
    for name in files:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("synthetic fixture: " + name, encoding="utf-8")
    for name in RESOURCE_ENV:
        path = root / "hermes-agent" / name
        path.mkdir()
        (path / "preserved.txt").write_text(name, encoding="utf-8")
    (root / "uv-cache/empty-cache-directory").mkdir(parents=True)
    (root / "enabled-features.json").write_text(json.dumps({"schema": 1, "extras": ["voice", "web"]}))
    manifest = {"schema": 1, "target": "win32-x64", "ref": revision,
                "repo": "hermes-agent", "venv": "venv", "store": "tools",
                "runtime": {"repoDir": "hermes-agent", "storePython": "tools/python/python.exe",
                            "sitePackages": "venv/Lib/site-packages", "toolsDir": "tools",
                            "commands": {"hermes": "bin/hermes.exe"}}}
    (root / "manifest.json").write_text(json.dumps(manifest))
    return root, manifest


def test_publication_preserves_every_existing_byte_resource_and_command(tmp_path, source_revision):
    root, canonical = payload(tmp_path, source_revision)
    before = inventory(root)
    result = seal_runtime(root, source_revision)
    assert before.items() <= result["files"].items()
    assert result["commands"] == canonical["runtime"]["commands"]
    assert result["launch"] == {"python": canonical["runtime"]["storePython"],
                                "args": ["-B", "-I", "-S", "bin/stumpy.py"]}
    assert result["extras"] == ["voice", "web"]
    assert result["files"]["uv-cache/empty-cache-directory"]["kind"] == "directory"
    assert inventory(root) == result["files"]
    assert (root / "THIRD_PARTY_NOTICES.txt").read_text().endswith((root / "hermes-agent/LICENSE").read_text())
    saved = (root / "gamedevbuddy-runtime.json").read_bytes()
    with pytest.raises(ValueError, match="already published"):
        seal_runtime(root, source_revision)
    assert (root / "gamedevbuddy-runtime.json").read_bytes() == saved


def test_publication_emits_canonical_app_owned_install_stamp(tmp_path, source_revision, monkeypatch):
    from hermes_cli.venv_sync import _is_sealed
    from hermes_cli.version_info import _stamp_version_info
    from pm.environments import store_root

    root, _ = payload(tmp_path, source_revision)
    result = seal_runtime(root, source_revision)
    repo = root / "hermes-agent"
    stamp_path = repo / "install-stamp.json"
    assert stamp_path.is_file(), "publisher must emit the canonical installation stamp"
    stamp = json.loads(stamp_path.read_text(encoding="utf-8"))
    assert {key: stamp[key] for key in (
        "schemaVersion", "commit", "distribution", "updateMechanism", "payload", "source",
        "branch", "tag", "baseVersion", "displayVersion", "dirty", "runtimeDir",
    )} == {
        "schemaVersion": 2, "commit": result["sourceRevision"], "distribution": "gamedevbuddy-app",
        "updateMechanism": "external", "payload": "runtime", "source": "commit-build",
        "branch": None, "tag": None, "baseVersion": None,
        "displayVersion": f"git.{source_revision[:7]}", "dirty": False, "runtimeDir": "../tools",
    }
    assert isinstance(stamp["commitDate"], int) and stamp["builtAt"]
    assert result["files"]["hermes-agent/install-stamp.json"] == inventory(root)["hermes-agent/install-stamp.json"]
    assert _is_sealed(repo), "the existing sealed-package guard must accept the emitted stamp"
    monkeypatch.setenv("HERMES_INSTALL_ROOT", str(repo))
    info = _stamp_version_info()
    assert info is not None and info.commit == source_revision and info.source == "commit-build"
    assert info.distribution == "gamedevbuddy-app"
    assert info.derived_version == f"git.{source_revision[:7]}"
    # PM's canonical manifest owns its relocatable tools/ layout, independent of cwd.
    monkeypatch.delenv("HERMES_RUNTIME_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    assert store_root(repo) == root / "tools"
    assert (repo / stamp["runtimeDir"]).resolve() == root / "tools"
    relocated = tmp_path / "relocated-runtime"
    root.rename(relocated)
    assert store_root(relocated / "hermes-agent") == relocated / "tools"
    assert (relocated / "hermes-agent" / stamp["runtimeDir"]).resolve() == relocated / "tools"


def test_publication_succeeds_with_non_ascii_output_path(tmp_path, source_revision):
    root, _ = payload(tmp_path / "éditeur", source_revision)
    before = inventory(root)
    result = seal_runtime(root, source_revision)
    stamp = json.loads((root / "hermes-agent/install-stamp.json").read_text(encoding="utf-8"))
    assert (stamp["commit"], stamp["source"], stamp["distribution"], stamp["updateMechanism"], stamp["payload"]) == (
        source_revision, "commit-build", "gamedevbuddy-app", "external", "runtime",
    )
    assert before.items() <= result["files"].items()
    assert inventory(root) == result["files"]
    assert json.loads((root / "gamedevbuddy-runtime.json").read_text(encoding="utf-8")) == result


@pytest.mark.parametrize("variant", ["bootstrap", "bundled", "store", "light", "synthetic-invalid-variant"])
def test_inherited_desktop_ci_environment_cannot_select_stamp_identity(
    tmp_path, source_revision, monkeypatch, variant,
):
    root, _ = payload(tmp_path, source_revision)
    inherited = {
        "HERMES_DESKTOP_VARIANT": variant, "HERMES_BUILD_COMMIT": "b" * 40,
        "HERMES_PAYLOAD_TAG": "synthetic-foreign-tag", "HERMES_PAYLOAD_VERSION": "synthetic-version",
        "GITHUB_SHA": "b" * 40, "GITHUB_REF_NAME": "synthetic-foreign-workflow",
        "GITHUB_HEAD_REF": "synthetic-foreign-branch",
    }
    for key, value in inherited.items():
        monkeypatch.setenv(key, value)
    seal_runtime(root, source_revision)
    stamp = json.loads((root / "hermes-agent/install-stamp.json").read_text(encoding="utf-8"))
    assert (stamp["commit"], stamp["source"], stamp["payload"], stamp["distribution"], stamp["updateMechanism"]) == (
        source_revision, "commit-build", "runtime", "gamedevbuddy-app", "external",
    )
    assert stamp["branch"] is None and stamp["tag"] is None and "channelBuild" not in stamp
    assert stamp["baseVersion"] is None and stamp["displayVersion"] == f"git.{source_revision[:7]}"
    import os
    assert {key: os.environ[key] for key in inherited} == inherited


def test_canonical_producer_refuses_manifest_revision_outside_source_checkout(tmp_path, source_revision):
    # Matching the assembly manifest alone is insufficient: the producer verifies Git HEAD.
    assert source_revision != REVISION
    root, _ = payload(tmp_path)
    before = inventory(root)
    with pytest.raises(subprocess.CalledProcessError) as failure:
        seal_runtime(root, REVISION)
    assert "identity does not match the checkout HEAD" in failure.value.stderr
    assert inventory(root) == before


@pytest.mark.parametrize("existing", [
    b'{"schemaVersion":2,"source":"ci","commit":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'
    b'"distribution":"desktop-app","updateMechanism":"electron-updater","payload":"bundled"}',
    b"synthetic interrupted stamp",
    None,
], ids=["inherited-desktop-ci-stamp", "interrupted-stamp", "stamp-directory"])
def test_existing_install_stamp_is_refused_without_changing_payload(tmp_path, source_revision, existing):
    root, _ = payload(tmp_path, source_revision)
    stamp_path = root / "hermes-agent/install-stamp.json"
    if existing is None:
        stamp_path.mkdir()
    else:
        stamp_path.write_bytes(existing)
    before = inventory(root)
    with pytest.raises(ValueError, match="install stamp"):
        seal_runtime(root, source_revision)
    assert inventory(root) == before
    assert not (root / "gamedevbuddy-runtime.json").exists()
    assert not (root / "bin/stumpy.py").exists()


@pytest.mark.parametrize("missing", ["tools/python/python.exe", "hermes-agent/plugins",
                                     "hermes-agent/hermes_cli/tui_dist/entry.js",
                                     "hermes-agent/hermes_cli/web_dist/index.html"])
def test_incomplete_payload_is_refused_before_publishing(tmp_path, missing):
    root, _ = payload(tmp_path)
    path = root / missing
    if path.is_dir():
        assert path.resolve().is_relative_to(root.resolve())
        shutil.rmtree(path)
    else:
        path.unlink()
    with pytest.raises(ValueError, match="Complete canonical"):
        seal_runtime(root, REVISION)
    assert not (root / "gamedevbuddy-runtime.json").exists()
    assert not (root / "bin/stumpy.py").exists()
    assert not (root / "hermes-agent/install-stamp.json").exists()


def test_source_and_launch_path_mismatch_cannot_publish(tmp_path):
    root, manifest = payload(tmp_path)
    with pytest.raises(ValueError, match="exact source revision"):
        seal_runtime(root, "b" * 40)
    manifest["runtime"]["storePython"] = "../foreign-python"
    (root / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="payload-relative"):
        seal_runtime(root, REVISION)
    assert not (root / "gamedevbuddy-runtime.json").exists()


@pytest.mark.platforms("windows")
def test_foreign_junction_is_refused_without_foreign_changes(tmp_path):
    root, _ = payload(tmp_path)
    outside = tmp_path / "foreign"
    outside.mkdir()
    marker = outside / "kept.txt"
    marker.write_bytes(b"foreign fixture bytes")
    subprocess.run(["cmd", "/c", "mklink", "/J", str(root / "foreign-link"), str(outside)],
                   check=True, capture_output=True)
    with pytest.raises(ValueError, match="not portable and contained"):
        seal_runtime(root, REVISION)
    assert marker.read_bytes() == b"foreign fixture bytes"
    assert not (root / "gamedevbuddy-runtime.json").exists()
