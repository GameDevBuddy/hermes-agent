"""First-party publication retains the complete canonical payload and its ownership boundary."""
import json
from pathlib import Path

import pytest

from scripts.bundles.gamedevbuddy import inventory, seal_runtime
from scripts.build.inputs import RESOURCE_ENV

REVISION = "a" * 40


def payload(tmp_path):
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
    manifest = {"schema": 1, "target": "win32-x64", "ref": REVISION,
                "runtime": {"repoDir": "hermes-agent", "storePython": "tools/python/python.exe",
                            "sitePackages": "venv/Lib/site-packages", "toolsDir": "tools",
                            "commands": {"hermes": "bin/hermes.exe"}}}
    (root / "manifest.json").write_text(json.dumps(manifest))
    return root, manifest


def test_publication_preserves_every_existing_byte_resource_and_command(tmp_path):
    root, canonical = payload(tmp_path)
    before = inventory(root)
    result = seal_runtime(root, REVISION)
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
        seal_runtime(root, REVISION)
    assert (root / "gamedevbuddy-runtime.json").read_bytes() == saved


@pytest.mark.parametrize("missing", ["tools/python/python.exe", "hermes-agent/plugins",
                                     "hermes-agent/hermes_cli/tui_dist/entry.js",
                                     "hermes-agent/hermes_cli/web_dist/index.html"])
def test_incomplete_payload_is_refused_before_publishing(tmp_path, missing):
    import shutil
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
    import subprocess
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
