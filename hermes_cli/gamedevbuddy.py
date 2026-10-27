"""GameDevBuddy's private, full-feature agent runtime composition.

The product host owns sign-in. This module only prepares an explicitly named
GameDevBuddy home and delegates gateway ownership to the canonical supervisor.
It never imports personal logins, changes shell aliases, or edits an engine.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import time

PRODUCT = "GameDevBuddy AI"
MARKER = "gamedevbuddy-runtime.json"
SOUL = """You are Stumpy, the GameDevBuddy AI game-development companion.
Help with design, implementation, debugging, story, lore, art and production.
Keep the user's selected model, tools, skills, memory and session features available.
Explain what was actually observed, and distinguish plans from verified results.
Engine editing uses GameDevBuddy's project-bound, approved native tool bridge.
Never bypass that bridge with shell/editor automation when a native permission,
pause or ownership boundary prevents an operation. A skill is guidance, not permission.
Never read or borrow personal account credentials automatically. Local use needs
no product service account; optional hosted services use GameDevBuddy's account.
"""
PACKS = {
    "gdb-unreal": ("Unreal Engine", "Unreal Engine C++/Blueprints, assets, levels, gameplay and validation."),
    "gdb-unity": ("Unity", "Unity C#, scenes, prefabs, components, assets and gameplay validation."),
    "gdb-godot": ("Godot", "Godot GDScript/C#, scenes, resources, nodes and gameplay validation."),
    "gdb-narrative": ("Narrative", "Story, lore, characters, dialogue, quests and continuity across the game project."),
}


def runtime_environment() -> dict[str, str]:
    """Do not let the engine discover the launching desktop's provider secrets.
    Explicit connections load from this runtime's own vault/.env afterwards.
    No machine environment, registry or user credential file is modified.
    """
    credential_names = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
                        "GOOGLE_APPLICATION_CREDENTIALS", "CLAUDE_CODE_OAUTH_TOKEN",
                        "CODEX_ACCESS_TOKEN", "GDB_BEARER_TOKEN"}
    return {name: value for name, value in os.environ.items()
            if name not in credential_names and not name.endswith(("_API_KEY", "_ACCESS_TOKEN"))}


def prepare_runtime(home: Path, *, port: int = 9120) -> dict:
    """Idempotently compose a new/previously marked home, preserving user edits.

    Existing unmarked configuration is never adopted or rewritten. Profile
    creation uses real canonical APIs, keeps all bundled skills, and writes no
    system aliases. This does not launch the gateway or any model.
    """
    if not home.is_absolute() or not 1024 <= port <= 65535:
        raise ValueError("Use an absolute GameDevBuddy runtime home and an unprivileged port")
    home = home.resolve()
    if home == (Path.home() / ".hermes").resolve():
        raise ValueError("GameDevBuddy requires its own runtime home")
    marker = home / MARKER
    config_path = home / "config.yaml"
    if config_path.exists() and not marker.is_file():
        raise ValueError("Existing unowned agent configuration will not be adopted")
    if marker.is_file():
        receipt = json.loads(marker.read_text(encoding="utf-8"))
        if receipt.get("product") != PRODUCT or receipt.get("home") != str(home):
            raise ValueError("GameDevBuddy runtime ownership does not match this home")
    os.environ["HERMES_HOME"] = str(home)
    os.environ.pop("HERMES_PROFILE", None)
    # Home binding MUST precede these imports; imports may cache profile policy.
    from hermes_cli.config import atomic_config_write, read_user_config_raw
    from hermes_cli.config_defaults import DEFAULT_CONFIG
    from hermes_cli.profiles import _get_profiles_root, create_profile, write_profile_meta
    from hermes_cli.observability.shared_metrics_consent import set_answer
    from utils import atomic_write_text

    expected = home / "profiles"
    if _get_profiles_root().resolve() != expected:
        raise ValueError("Canonical profile root escaped the GameDevBuddy runtime")
    home.mkdir(parents=True, exist_ok=True)
    (home / "workspace").mkdir(exist_ok=True)
    created = not config_path.exists()
    if created:
        config = copy.deepcopy(DEFAULT_CONFIG)
        config["model"] = {"provider": "ollama", "base_url": "http://127.0.0.1:11434/v1", "default": ""}
        config["auth"]["adopt_external_logins"] = False
        config["updates"] = {"check": False}
        set_answer(config, False, False)
        config["webapp"].update({"enabled": True, "host": "127.0.0.1", "port": port})
        config["terminal"]["cwd"] = str(home / "workspace")
        atomic_config_write(config_path, config)
        write_profile_meta(home, display_name="Stumpy")
    else:
        config = read_user_config_raw(config_path)
    if not marker.exists():
        atomic_write_text(marker, json.dumps({"schema": 1, "product": PRODUCT, "home": str(home)}, indent=2) + "\n")
    from hermes_cli.default_soul import DEFAULT_SOUL_MD
    soul_path = home / "SOUL.md"
    if not soul_path.exists() or (created and soul_path.read_text().strip() == DEFAULT_SOUL_MD.strip()):
        atomic_write_text(soul_path, DEFAULT_SOUL_MD + "\n\n## GameDevBuddy project policy\n\n" + SOUL)
    added = []
    for name, (label, focus) in PACKS.items():
        target = expected / name
        if target.exists():
            continue
        profile = create_profile(name, no_alias=True, no_skills=False,
                                 description=f"GameDevBuddy {label} specialist",
                                 soul=DEFAULT_SOUL_MD + "\n\n" + SOUL + "\nSpecialist focus: " + focus + "\n")
        if profile.resolve() != target:
            raise ValueError("Created profile escaped its expected destination")
        profile_config = read_user_config_raw(profile / "config.yaml")
        profile_config["model"] = copy.deepcopy(config.get("model", ""))
        profile_config.setdefault("auth", {})["adopt_external_logins"] = False
        set_answer(profile_config, False, False)
        atomic_config_write(profile / "config.yaml", profile_config)
        write_profile_meta(profile, display_name=f"Stumpy {label}")
        added.append(name)
    return {"product": PRODUCT, "home": str(home), "created": created,
            "profilesAdded": added, "accountRequired": False, "gatewayStarted": False}


def main() -> int:
    parser = argparse.ArgumentParser(prog="gamedevbuddy-ai")
    parser.add_argument("action", choices=["prepare", "start", "status", "stop"])
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--port", type=int, default=9120)
    args = parser.parse_args()
    if args.action in {"prepare", "start"}:
        receipt = prepare_runtime(args.home, port=args.port)
        if args.action == "prepare":
            print(json.dumps(receipt))
            return 0
    else:
        home = args.home.resolve()
        receipt = json.loads((home / MARKER).read_text())
        if receipt.get("product") != PRODUCT or receipt.get("home") != str(home):
            raise ValueError("Not a GameDevBuddy runtime home")
        os.environ["HERMES_HOME"] = str(home)
        os.environ.pop("HERMES_PROFILE", None)
    if args.action == "stop":
        # The generic Windows CLI can escalate a wedged stop to taskkill.
        # The product host's close is cooperative only; a timeout is a blocker.
        from gateway.control_socket import pause_gateway_for_update, identify_gateway
        before = identify_gateway(home)
        if before is None:
            print(json.dumps({"product": PRODUCT, "state": "not_running"}))
            return 0
        result = pause_gateway_for_update(home)
        if not result or not result.get("pausing"):
            print(json.dumps({"product": PRODUCT, "state": "stop_unconfirmed"}))
            return 1
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if identify_gateway(home) is None:
                print(json.dumps({"product": PRODUCT, "state": "control_closed", "pid": before.get("pid")}))
                return 0
            time.sleep(.2)
        print(json.dumps({"product": PRODUCT, "state": "stop_timeout", "pid": before.get("pid")}))
        return 1
    action = ["ensure", "--json"] if args.action == "start" else [args.action]
    return subprocess.call([sys.executable, "-B", "-m", "hermes_cli.main", "gateway", *action], env=runtime_environment())


if __name__ == "__main__":
    raise SystemExit(main())
