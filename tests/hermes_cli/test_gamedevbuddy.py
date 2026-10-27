"""Real temporary-home composition, not source-string/feature-count tests."""
import pytest

from hermes_cli.gamedevbuddy import PACKS, prepare_runtime


def test_own_home_keeps_full_skills_and_preserves_existing_profile_and_config(tmp_path, monkeypatch):
    home = tmp_path / "gdb-ai"
    monkeypatch.setenv("HERMES_HOME", str(home))
    result = prepare_runtime(home)
    from hermes_cli.config import atomic_config_write, read_user_config_raw
    config = read_user_config_raw(home / "config.yaml")
    assert config["auth"]["adopt_external_logins"] is False
    assert config["webapp"]["host"] == "127.0.0.1"
    assert config["toolsets"] == ["hermes-cli"]
    assert config["telemetry"]["shared_metrics"]["enabled"] is False
    assert config["telemetry"]["shared_metrics"]["send"] is False
    assert result["accountRequired"] is False and result["gatewayStarted"] is False
    for name in PACKS:
        profile = home / "profiles" / name
        assert "GameDevBuddy AI" in (profile / "SOUL.md").read_text()
        assert (profile / "skills").is_dir()
        assert not (profile / ".no-skills").exists()
        assert read_user_config_raw(profile / "config.yaml")["telemetry"]["shared_metrics"]["send"] is False
    config["model"] = {"provider":"custom","base_url":"http://127.0.0.1:19099/v1","default":"user-selected"}
    atomic_config_write(home / "config.yaml", config)
    soul = home / "profiles/gdb-narrative/SOUL.md"
    soul.write_text("My own edited narrative bot\n")
    before = (home / "config.yaml").read_bytes()
    assert prepare_runtime(home)["profilesAdded"] == []
    assert (home / "config.yaml").read_bytes() == before
    assert soul.read_text() == "My own edited narrative bot\n"


@pytest.mark.parametrize("existing_name", ["config.yaml", ".env", "auth.json"])
def test_unowned_existing_home_is_refused_without_writes(tmp_path, monkeypatch, existing_name):
    home = tmp_path / "existing"
    home.mkdir()
    config = home / existing_name
    config.write_text("existing fixture-owned state\n")
    monkeypatch.setenv("HERMES_HOME", str(home))
    with pytest.raises(ValueError, match="unowned"):
        prepare_runtime(home)
    assert sorted(p.name for p in home.iterdir()) == [existing_name]
    assert config.read_text() == "existing fixture-owned state\n"
