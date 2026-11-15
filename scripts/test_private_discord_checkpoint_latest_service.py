"""Exercise the installed-unit command through native flock and supervision."""
import configparser
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parent.parent


@unittest.skipUnless(sys.platform == "linux" and shutil.which("flock"), "Linux flock required")
class LatestServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.unit = configparser.ConfigParser(interpolation=None)
        self.unit.read(ROOT / "operations/discrawl-latest@.service")
        self.env = {"PATH": os.environ["PATH"], "DISCORD_BOT_TOKEN": "synthetic-private-token",
                    "DISCRAWL_GUILD_ID": "123456789012345678"}
        self.native = self.root / "discrawl"
        self.native.write_text("#!" + sys.executable + "\n"
            "import json,os,pathlib,sys,time\n"
            "root=pathlib.Path(__file__).parent\n"
            "(root/'ready.tmp').write_text(json.dumps({'pid':os.getpid(),'args':sys.argv[1:]}))\n"
            "(root/'ready.tmp').replace(root/'started.json')\n"
            "print(os.environ['DISCORD_BOT_TOKEN'],flush=True)\n"
            "print('private native diagnostic',file=sys.stderr,flush=True)\n"
            "if (root/'wait').exists(): time.sleep(30)\n")
        self.native.chmod(0o700)

    def command(self, key="ExecStart"):
        args = shlex.split(self.unit["Service"][key])
        replace = {"/usr/bin/python3": sys.executable,
                   "/usr/local/bin/discrawl": str(self.native),
                   "/usr/local/libexec/discrawl/private_discord_checkpoint.py":
                       str(ROOT / "scripts/private_discord_checkpoint.py"),
                   "${DISCRAWL_GUILD_ID}": self.env["DISCRAWL_GUILD_ID"]}
        return [replace.get(arg, arg.replace("/var/lib/discrawl/%i", str(self.root))) for arg in args]

    def test_unit_command_keeps_latest_scope_and_redacts_native_output(self):
        result = subprocess.run(self.command(), env=self.env, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        args = json.loads((self.root / "started.json").read_text())["args"]
        self.assertEqual(args, ["--public-errors", "--config", str(self.root / "acquisition.toml"),
            "sync", "--source", "discord", "--guild", self.env["DISCRAWL_GUILD_ID"],
            "--no-update", "--latest-only", "--skip-members", "--with-media=false", "--concurrency", "1"])
        self.assertNotIn(b"private", result.stdout + result.stderr)
        terminal = json.loads(result.stdout.splitlines()[-1].removeprefix(b"native: "))
        self.assertEqual((terminal["phase"], terminal["state"], terminal["reaped"]),
                         ("latest", "complete", True))

    def test_missing_credential_or_guild_fails_before_native_fallback(self):
        command = self.command("ExecStartPre")
        self.assertNotIn(self.env["DISCORD_BOT_TOKEN"], " ".join(command))
        for token, guild, expected in [("", "123", 1), (" \t ", "123", 1),
                                       ("synthetic", "", 1), ("synthetic", "--all", 1),
                                       ("Bot synthetic", "123", 0)]:
            with self.subTest(token=bool(token), guild=guild):
                result = subprocess.run(command, env={**self.env, "DISCORD_BOT_TOKEN": token,
                    "DISCRAWL_GUILD_ID": guild}, capture_output=True, timeout=5)
                self.assertEqual(result.returncode, expected)
                self.assertEqual(result.stdout + result.stderr, b"")
        self.assertFalse((self.root / "started.json").exists())

    def test_busy_acquisition_lock_does_not_start_a_second_writer(self):
        import fcntl
        with (self.root / "acquisition.lock").open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = subprocess.run(self.command(), env=self.env, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 75)
        self.assertFalse((self.root / "started.json").exists())

    def test_main_pid_signal_reaps_writer_and_releases_lock(self):
        import fcntl
        (self.root / "wait").touch()
        child = subprocess.Popen(self.command(), env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 5
            while not (self.root / "started.json").exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue((self.root / "started.json").exists())
            pid = json.loads((self.root / "started.json").read_text())["pid"]
            child.terminate()
            stdout, stderr = child.communicate(timeout=18)
            self.assertNotEqual(child.returncode, 0)
            terminal = json.loads(stdout.splitlines()[-1].removeprefix(b"native: "))
            self.assertEqual((terminal["reason"], terminal["reaped"]), ("cancelled", True))
            self.assertNotIn(b"private", stdout + stderr)
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
            with (self.root / "acquisition.lock").open() as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            if child.poll() is None:
                child.terminate()
                child.communicate(timeout=20)


if __name__ == "__main__":
    unittest.main()
