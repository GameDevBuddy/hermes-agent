"""GameDevBuddy's private, full-feature agent runtime composition.

The product host owns sign-in and process policy. This facade composes an
explicitly named GameDevBuddy home and dispatches the full canonical gateway.
Direct dispatch does not supply an externally retained ownership policy.
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
    """Remove inherited credential-shaped values before private-home secrets load.

    These suffixes follow canonical credential/config routing, including gateway
    keys, bot tokens and webhook secrets. This finite name filter supplements
    the host's explicit launch environment; it is not general caller isolation.
    Explicit connections load from this runtime's own vault/.env afterwards.
    """
    credential_names = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
                        "GOOGLE_APPLICATION_CREDENTIALS", "CLAUDE_CODE_OAUTH_TOKEN",
                        "CODEX_ACCESS_TOKEN", "GDB_BEARER_TOKEN"}
    credential_suffixes = ("_KEY", "_TOKEN", "_SECRET", "_PASSWORD", "_CREDENTIALS", "_SERVICE_ACCOUNT_JSON")
    return {name: value for name, value in os.environ.items()
            if name.upper() not in credential_names and not name.upper().endswith(credential_suffixes)}


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
    if home.exists() and not marker.is_file() and any(home.iterdir()):
        raise ValueError("Existing unowned agent home will not be adopted")
    if marker.is_file():
        receipt = json.loads(marker.read_text(encoding="utf-8-sig"))
        if receipt.get("product") != PRODUCT or receipt.get("home") != str(home):
            raise ValueError("GameDevBuddy runtime ownership does not match this home")
    os.environ["HERMES_HOME"] = str(home)
    os.environ.pop("HERMES_PROFILE", None)
    # Home binding MUST precede these imports; imports may cache profile policy.
    from hermes_cli.config import atomic_config_write, read_user_config_raw
    from hermes_cli.config_backend import config_exists
    from hermes_cli.config_defaults import DEFAULT_CONFIG
    from hermes_cli.profiles import _get_profiles_root, create_profile, write_profile_meta
    from hermes_cli.observability.shared_metrics_consent import set_answer
    from utils import atomic_write_text

    expected = home / "profiles"
    if _get_profiles_root().resolve() != expected:
        raise ValueError("Canonical profile root escaped the GameDevBuddy runtime")
    home.mkdir(parents=True, exist_ok=True)
    (home / "workspace").mkdir(exist_ok=True)
    created = not config_exists(config_path)
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
    if not soul_path.exists() or (created and soul_path.read_text(encoding="utf-8-sig").strip() == DEFAULT_SOUL_MD.strip()):
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


def _require_gateway_runtime_policy() -> None:
    """A bootstrap-only declaration must not start an unimplemented lifetime."""
    policy_api = sys.modules.get("hermes_retained_bootstrap")
    contract = policy_api.current() if policy_api is not None else None
    if contract is not None:
        contract.refuse("managed-gateway-lifecycle-not-implemented")


def main() -> int:
    parser = argparse.ArgumentParser(prog="gamedevbuddy-ai")
    parser.add_argument("action", choices=["prepare", "start", "status", "stop", "foreground"])
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--port", type=int, default=9120)
    args = parser.parse_args()
    if args.action == "foreground" and (
        not args.home.is_absolute() or args.home.resolve() == (Path.home() / ".hermes").resolve()
    ):
        raise ValueError("Use an absolute, separate GameDevBuddy runtime home")
    if args.action in {"prepare", "start"}:
        receipt = prepare_runtime(args.home, port=args.port)
        if args.action == "prepare":
            print(json.dumps(receipt))
            return 0
    else:
        home = args.home.resolve()
        receipt = json.loads((home / MARKER).read_text(encoding="utf-8-sig"))
        if receipt.get("product") != PRODUCT or receipt.get("home") != str(home):
            raise ValueError("Not a GameDevBuddy runtime home")
        os.environ["HERMES_HOME"] = str(home)
        os.environ.pop("HERMES_PROFILE", None)
    if args.action == "foreground":
        _require_gateway_runtime_policy()
        # Direct dispatch to the SAME full canonical gateway, not `ensure` or
        # a second agent loop. An explicitly marked owned home is required above;
        # this action never prepares or adopts a new home implicitly.
        # Canonical bootstrap/lifecycle behavior is unchanged: this is NOT an
        # externally retained/cooperative-only policy or a readiness receipt.
        # The app must not admit this entry until those separate gates close.
        environment = runtime_environment()
        os.environ.clear()
        os.environ.update(environment)
        previous_argv = sys.argv
        try:
            # The fixed product action/home/port are not gateway CLI options.
            # Gateway configuration still comes from the explicitly bound home.
            sys.argv = [previous_argv[0]]
            from gateway.run import main as gateway_main
            result = gateway_main()
            return 0 if result is None else result
        finally:
            sys.argv = previous_argv
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
    from hermes_cli._launchers import runtime_command
    command = runtime_command(Path(__file__).resolve().parents[1], ["gateway", *action],
                              python=sys.executable, home=args.home.resolve())
    child = subprocess.Popen(command, env=runtime_environment())
    try:
        return child.wait(timeout=120)
    except subprocess.TimeoutExpired:
        # A slow owner handshake is not permission to kill or replay it.
        print(json.dumps({"product": PRODUCT, "state": "command_timeout",
                          "pid": child.pid, "gatewayState": "unknown"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
