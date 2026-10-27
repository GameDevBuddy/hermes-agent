"""Publish the complete canonical payload for GameDevBuddy's own installer.

Preparation stays with PM. This consumes prepared dependencies and both built
frontends; it does not acquire dependencies, launch a gateway or contact a model.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build.inputs import RESOURCE_ENV
from scripts.build.launchers import render_wrapper
from pm.filesystem import is_junction

PACKAGE_MANIFEST = "gamedevbuddy-runtime.json"


def _contained(root: Path, relative: str) -> Path:
    path = Path(relative)
    if not relative or path.is_absolute() or ".." in path.parts or "\\" in relative:
        raise ValueError(f"runtime path must be payload-relative: {relative}")
    result = root / path
    if not result.resolve().is_relative_to(root):
        raise ValueError(f"runtime path escapes payload: {relative}")
    return result


def inventory(root: Path) -> dict:
    """Retain every file, empty directory, mode and portable internal link."""
    records = {}
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in sorted(dirs + files):
            path = Path(directory) / name
            relative = path.relative_to(root).as_posix()
            if relative == PACKAGE_MANIFEST:
                continue
            before = path.lstat()
            mode = stat.S_IMODE(before.st_mode)
            if path.is_symlink() or is_junction(path):
                target = os.readlink(path)
                if Path(target).is_absolute() or not path.resolve(strict=True).is_relative_to(root):
                    raise ValueError(f"runtime link is not portable and contained: {relative}")
                records[relative] = {"kind": "link", "target": target, "mode": mode}
                if name in dirs:
                    dirs.remove(name)
            elif path.is_dir():
                records[relative] = {"kind": "directory", "mode": mode}
            elif path.is_file():
                digest = hashlib.sha256()
                with path.open("rb") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(block)
                after = path.stat()
                if (before.st_size, before.st_mtime_ns, before.st_ino) != (
                    after.st_size, after.st_mtime_ns, after.st_ino
                ):
                    raise ValueError(f"runtime file changed while hashing: {relative}")
                records[relative] = {"kind": "file", "bytes": after.st_size,
                                     "sha256": digest.hexdigest(), "mode": mode}
            else:
                raise ValueError(f"unsupported runtime entry: {relative}")
    return dict(sorted(records.items()))


def seal_runtime(payload: Path, revision: str) -> dict:
    """Add our entry and inventory without removing any canonical capability."""
    payload = payload.absolute()
    if payload != payload.resolve() or not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise ValueError("Use a real payload directory and exact source revision")
    additions = [payload / PACKAGE_MANIFEST, payload / "bin/stumpy.py",
                 payload / "THIRD_PARTY_NOTICES.txt"]
    if any(path.exists() or path.is_symlink() for path in additions):
        raise ValueError("Runtime is already published or contains an interrupted publication; use a fresh assembly")
    canonical = json.loads(_contained(payload, "manifest.json").read_text(encoding="utf-8-sig"))
    if canonical.get("schema") != 1 or canonical.get("ref") != revision:
        raise ValueError("Canonical assembly must name the exact source revision")
    runtime = canonical["runtime"]
    repo = _contained(payload, runtime["repoDir"])
    python = _contained(payload, runtime["storePython"])
    site = _contained(payload, runtime["sitePackages"])
    tools = _contained(payload, runtime["toolsDir"])
    required_files = [python, repo / "LICENSE", repo / "run_agent.py",
                      repo / "hermes_cli/gamedevbuddy.py", repo / "pyproject.toml",
                      repo / "uv.lock", repo / "pm/lock.json", tools / "facts.json",
                      payload / "pm-runtime/pm-runtime.json", repo / "hermes_cli/tui_dist/entry.js",
                      repo / "hermes_cli/tui_dist/package.json", repo / "hermes_cli/web_dist/index.html"]
    required_files += [_contained(payload, command) for command in runtime["commands"].values()]
    required_dirs = [site, tools, payload / "uv-cache", *[repo / name for name in RESOURCE_ENV]]
    if (not runtime["commands"] or any(not path.is_file() for path in required_files)
            or any(not path.is_dir() for path in required_dirs)):
        raise ValueError("Complete canonical tools, environment, resources and both frontends are required")
    features = json.loads(_contained(payload, "enabled-features.json").read_text(encoding="utf-8-sig"))
    extras = features.get("extras")
    if (features.get("schema") != 1 or not isinstance(extras, list) or not extras
            or any(not isinstance(extra, str) or not extra for extra in extras)
            or len(set(extras)) != len(extras)):
        raise ValueError("Canonical installed-feature inventory is required")
    # Fail before adding our files if any existing entry escapes the payload.
    inventory(payload)
    wrapper = render_wrapper("hermes_cli.gamedevbuddy:main",
                             "../" + runtime["repoDir"], "../" + runtime["sitePackages"])
    additions[1].parent.mkdir(exist_ok=True)
    additions[1].write_text(wrapper, encoding="utf-8")
    additions[2].write_text("GameDevBuddy AI includes code derived from NousResearch/hermes-agent.\n"
                            "Original MIT licence follows.\n\n" + (repo / "LICENSE").read_text(encoding="utf-8-sig"),
                            encoding="utf-8")
    result = {"schema": 1, "product": "GameDevBuddy AI", "agent": "Stumpy",
              "sourceRevision": revision, "target": canonical["target"], "fullRuntime": True,
              "launch": {"python": runtime["storePython"], "args": ["-B", "-I", "-S", "bin/stumpy.py"]},
              "extras": sorted(extras), "frontends": ["tui", "web"],
              "commands": runtime["commands"], "files": inventory(payload)}
    with additions[0].open("x", encoding="utf-8") as output:
        output.write(json.dumps(result, indent=2) + "\n")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared", type=Path, required=True)
    parser.add_argument("--tui", type=Path, required=True)
    parser.add_argument("--web", type=Path, required=True)
    args = parser.parse_args()
    from scripts.bundles.desktop_prepare import require_source
    from scripts.bundles.native import finish_native
    from scripts.bundles.native_prepared import load_prepared

    prepared = args.prepared.absolute()
    data = json.loads(prepared.read_text(encoding="utf-8-sig"))
    require_source(ROOT, data["revision"])
    inputs = load_prepared(prepared)
    payload = Path(data["out"])
    if (payload / PACKAGE_MANIFEST).exists():
        raise ValueError("Use a fresh unpublished canonical assembly")
    if inputs.ref != data["revision"]:
        raise ValueError("Prepare with the exact source commit")
    finish_native(prepared, {"tui": args.tui.absolute(), "web": args.web.absolute()})
    result = seal_runtime(payload, data["revision"])
    print(json.dumps({"product": result["product"], "sourceRevision": result["sourceRevision"],
                      "entries": len(result["files"]), "manifest": str(payload / PACKAGE_MANIFEST)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
