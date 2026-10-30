"""The companion CLI must retain its installation binding in a sealed child.

The child here is a harmless synthetic module, never a real gateway or model.
"""
import json
import subprocess
import sys

from hermes_cli import gamedevbuddy


def test_status_child_uses_its_own_module_root_without_ambient_pythonpath(tmp_path, monkeypatch, capfd):
    repo = tmp_path / "synthetic-package"
    package = repo / "hermes_cli"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (repo / "hermes_bootstrap.py").write_text('"""Synthetic bootstrap; no dependency or runtime work."""\n', encoding="utf-8")
    (package / "main.py").write_text(
        'import json,os,sys\nprint(json.dumps({"kind":"synthetic", "home":os.environ["HERMES_HOME"], "argv":sys.argv[1:]}))\n',
        encoding="utf-8")
    home = tmp_path / "owned-home"
    home.mkdir()
    (home / gamedevbuddy.MARKER).write_text(json.dumps({"product":gamedevbuddy.PRODUCT,"home":str(home)}), encoding="utf-8")
    monkeypatch.setattr(gamedevbuddy,"__file__",str(package / "gamedevbuddy.py"))
    monkeypatch.setattr(sys,"executable",sys._base_executable)
    monkeypatch.setattr(sys,"argv",["stumpy","status","--home",str(home)])
    monkeypatch.delenv("PYTHONPATH",raising=False)
    monkeypatch.chdir(tmp_path)
    assert gamedevbuddy.main() == 0
    result = json.loads(capfd.readouterr().out)
    assert result == {"kind":"synthetic","home":str(home),"argv":["gateway","status"]}


def test_command_deadline_reports_unknown_without_killing_or_retrying(tmp_path, monkeypatch, capsys):
    home = tmp_path / "owned-home"
    home.mkdir()
    (home / gamedevbuddy.MARKER).write_text(json.dumps({"product":gamedevbuddy.PRODUCT,"home":str(home)}), encoding="utf-8")
    monkeypatch.setattr(sys,"argv",["stumpy","status","--home",str(home)])
    created = []

    class PendingCommand:
        pid = 12345

        def __init__(self, *args, **kwargs):
            created.append(self)

        def wait(self, timeout):
            raise subprocess.TimeoutExpired("synthetic-command",timeout)

        def kill(self):
            raise AssertionError("A command deadline must not force-kill its owner")

    monkeypatch.setattr(subprocess,"Popen",PendingCommand)
    assert gamedevbuddy.main() == 1
    result = json.loads(capsys.readouterr().out)
    assert result["state"] == "command_timeout" and result["gatewayState"] == "unknown"
    assert result["pid"] == PendingCommand.pid
    assert len(created) == 1
