"""Source dispatch checks only; never import or start the canonical gateway."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch


SUBJECT = Path(__file__).resolve().parents[2] / "hermes_cli/gamedevbuddy.py"
spec = importlib.util.spec_from_file_location("gdb_foreground_source_subject", SUBJECT)
subject = importlib.util.module_from_spec(spec)
spec.loader.exec_module(subject)


class ForegroundDispatchTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="gdb-foreground-source-")
        root = Path(self.directory.name).resolve()
        assert root.parent == Path(tempfile.gettempdir()).resolve()
        assert root.name.startswith("gdb-foreground-source-")
        self.addCleanup(self.directory.cleanup)
        self.home = root / "agent-home"
        self.home.mkdir()
        (self.home / subject.MARKER).write_text(json.dumps({
            "product": subject.PRODUCT, "home": str(self.home.resolve()),
        }), encoding="utf-8")

    def invoke(self, callback, home_argument=None, *, inherited=None, on_import=None):
        gateway = types.ModuleType("gateway")
        gateway.__path__ = []
        run = types.ModuleType("gateway.run")
        if on_import is None:
            run.main = callback
        else:
            def resolve(name):
                if name != "main":
                    raise AttributeError(name)
                on_import()
                return callback
            run.__getattr__ = resolve
        gateway.run = run
        argv = ["stumpy.py", "foreground", "--home", str(home_argument or self.home)]
        with patch.dict(sys.modules, {"gateway": gateway, "gateway.run": run}), \
             patch.dict(os.environ, {"HOME": str(Path(self.directory.name)),
                                      "USERPROFILE": str(Path(self.directory.name)),
                                      "HERMES_PROFILE": "unrelated-profile",
                                      "OPENAI_API_KEY": "synthetic-fixture-only",
                                      "GDB_BEARER_TOKEN": "synthetic-host-token",
                                      **(inherited or {})}, clear=True), \
             patch.object(sys, "argv", argv), \
             patch.object(subject, "prepare_runtime") as prepare, \
             patch.object(subject.subprocess, "Popen") as popen:
            try:
                return subject.main()
            finally:
                self.assertIs(sys.argv, argv)
                prepare.assert_not_called()
                popen.assert_not_called()

    def test_direct_dispatch_binds_existing_home_without_a_facade_child(self):
        observed = []
        def canonical():
            observed.append((dict(os.environ), list(sys.argv)))
        self.assertEqual(self.invoke(canonical), 0)
        self.assertEqual(len(observed), 1)
        environment, argv = observed[0]
        self.assertEqual(environment["HERMES_HOME"], str(self.home.resolve()))
        self.assertNotIn("HERMES_PROFILE", environment)
        self.assertNotIn("OPENAI_API_KEY", environment)
        self.assertNotIn("GDB_BEARER_TOKEN", environment)
        self.assertEqual(argv, ["stumpy.py"])

    def test_inherited_listener_and_bot_credentials_are_removed(self):
        credentials = dict.fromkeys([
            "API_SERVER_KEY", "WEBHOOK_SECRET", "TELEGRAM_BOT_TOKEN",
            "ANTHROPIC_AUTH_TOKEN", "VOICE_TOOLS_OPENAI_KEY", "SUDO_PASSWORD",
            "GOOGLE_APPLICATION_CREDENTIALS", "CUSTOM_SERVICE_ACCOUNT_JSON",
        ], "synthetic-fixture-only")
        observed = []
        self.invoke(lambda: observed.append(dict(os.environ)), inherited={
            **credentials, "HERMES_RUNTIME_DIR": "synthetic-sealed-runtime",
        })
        self.assertFalse(credentials.keys() & observed[0].keys())
        self.assertEqual(observed[0]["HERMES_RUNTIME_DIR"], "synthetic-sealed-runtime")
        self.assertEqual(observed[0]["HERMES_HOME"], str(self.home.resolve()))

    def test_gateway_import_sees_only_canonical_argv(self):
        observed = []
        self.invoke(lambda: None, on_import=lambda: observed.append(list(sys.argv)))
        self.assertTrue(observed)
        self.assertTrue(all(argv == ["stumpy.py"] for argv in observed))

    def test_gateway_import_failure_restores_product_argv(self):
        def fail_import():
            raise ImportError("synthetic import failure")
        with self.assertRaisesRegex(ImportError, "synthetic import failure"):
            self.invoke(lambda: None, on_import=fail_import)

    def test_canonical_returned_error_is_not_relabelled_as_success(self):
        self.assertEqual(self.invoke(lambda: 78), 78)

    def test_canonical_exit_is_not_relabelled_as_success(self):
        def canonical():
            raise SystemExit(75)
        with self.assertRaises(SystemExit) as caught:
            self.invoke(canonical)
        self.assertEqual(caught.exception.code, 75)

    def test_unmarked_home_refuses_before_gateway_dispatch(self):
        (self.home / subject.MARKER).unlink()
        callback = Mock()
        with self.assertRaises(FileNotFoundError):
            self.invoke(callback)
        callback.assert_not_called()

    def test_wrong_home_marker_refuses_before_gateway_dispatch(self):
        (self.home / subject.MARKER).write_text(json.dumps({
            "product": subject.PRODUCT, "home": str(self.home.parent),
        }), encoding="utf-8")
        callback = Mock()
        with self.assertRaises(ValueError):
            self.invoke(callback)
        callback.assert_not_called()

    def test_relative_home_refuses_before_gateway_dispatch(self):
        callback = Mock()
        with self.assertRaises(ValueError):
            self.invoke(callback, Path("relative-home"))
        callback.assert_not_called()

    def test_personal_default_home_refuses_before_gateway_dispatch(self):
        callback = Mock()
        with self.assertRaises(ValueError):
            self.invoke(callback, Path(self.directory.name) / ".hermes")
        callback.assert_not_called()

    def test_existing_start_keeps_its_canonical_ensure_command(self):
        package = types.ModuleType("hermes_cli")
        package.__path__ = []
        launchers = types.ModuleType("hermes_cli._launchers")
        launchers.runtime_command = Mock(return_value=["synthetic-installed-python", "sealed-entry"])
        package._launchers = launchers
        child = Mock()
        child.wait.return_value = 0
        with patch.dict(sys.modules, {"hermes_cli": package, "hermes_cli._launchers": launchers}), \
             patch.dict(os.environ, {}, clear=True), \
             patch.object(sys, "argv", ["stumpy.py", "start", "--home", str(self.home)]), \
             patch.object(subject, "prepare_runtime", return_value={}) as prepare, \
             patch.object(subject.subprocess, "Popen", return_value=child) as popen:
            self.assertEqual(subject.main(), 0)
            prepare.assert_called_once_with(self.home, port=9120)
            self.assertEqual(launchers.runtime_command.call_args.args[1], ["gateway", "ensure", "--json"])
            popen.assert_called_once()
            child.wait.assert_called_once_with(timeout=120)


if __name__ == "__main__":
    unittest.main()
