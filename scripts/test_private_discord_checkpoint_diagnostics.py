import contextlib
import copy
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import private_discord_checkpoint as checkpoint
import verify_discord_checkpoint_migration as admission
from test_private_discord_checkpoint import CACHE, FakeGitHub, native_database


class CommandTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        env = {"PATH": str(self.root) + os.pathsep + os.environ["PATH"], "DISCORD_BACKUP_TOKEN": "fixture"}
        self.env = patch.dict(os.environ, env)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.github = checkpoint.GitHub.__new__(checkpoint.GitHub)
        self.github.deadline = None
        self.github.read_only = False

    def shim(self, code):
        path = self.root / "gh"
        path.write_text("#!" + sys.executable + "\n" + code)
        path.chmod(0o700)

    def test_stderr_flood_is_drained_bounded_and_never_rendered(self):
        self.shim("import os\nos.write(2,b'private sentinel (HTTP 403)\\n')\n"
                  "for _ in range(16): os.write(2,b'secret'*16384)\nraise SystemExit(7)\n")
        with contextlib.redirect_stdout(io.StringIO()) as out, contextlib.redirect_stderr(io.StringIO()) as err:
            with self.assertRaises(checkpoint.CheckpointError) as failure:
                self.github.command(["api", "fixture"], timeout=3, phase="asset_list")
        self.assertEqual(str(failure.exception), "checkpoint_api phase=asset_list exit=7 http=403")
        self.assertEqual(out.getvalue() + err.getvalue(), "")

    def test_success_and_expected_missing_preserve_stdout_contract(self):
        self.shim("import os\nos.write(1,b'HTTP/2.0 200 OK\\n\\n{}')\nos.write(2,b'private')\n")
        self.assertEqual(self.github.command(["api", "fixture"]), b"HTTP/2.0 200 OK\n\n{}")
        self.shim("import os\nos.write(1,b'HTTP/2.0 404 Not Found\\n\\n{}')\nraise SystemExit(1)\n")
        self.assertIsNone(self.github.command(["api", "fixture"], missing=True, phase="latest_read"))

    def test_timeout_kills_owned_descendant_with_inherited_stderr(self):
        marker = self.root / "heartbeat"
        self.shim("import os,time\nfrom pathlib import Path\np=Path(%r)\n"
                  "if os.fork()==0:\n while True:\n  p.write_text(str(time.monotonic_ns()));time.sleep(.01)\n"
                  "else:\n while not p.exists(): time.sleep(.01)\n" % str(marker))
        clock = time.monotonic
        start, ready = clock(), None
        def after_ready():
            nonlocal ready
            # Isolate inherited-pipe cleanup from interpreter startup latency.
            if ready is None:
                self.assertLess(clock() - start, 5, "fixture failed to start")
            if ready is None and marker.exists():
                ready = clock()
            return 0 if ready is None else clock() - ready
        with patch.object(checkpoint.time, "monotonic", after_ready):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "phase=part_download exit=(timeout|reap)"):
                self.github.command(["api", "fixture"], timeout=.2, phase="part_download")
        value = marker.read_bytes()
        time.sleep(.05)
        self.assertEqual(marker.read_bytes(), value)

    def test_closed_stderr_descendant_cannot_keep_writing_successful_output(self):
        marker, group = self.root / "ready", self.root / "group"
        self.shim("import os,time\nfrom pathlib import Path\nPath(%r).write_text(str(os.getpgrp()))\n"
                  "if os.fork()==0:\n os.close(2);Path(%r).touch()\n while True:\n  os.write(1,b'x');time.sleep(.01)\n"
                  "else:\n while not Path(%r).exists(): time.sleep(.01)\n" % (str(group), str(marker), str(marker)))
        target = self.root / "download"
        try:
            try:
                self.github.command(["api", "fixture"], destination=target, phase="part_download")
            except checkpoint.CheckpointError as error:
                self.assertEqual(str(error), "checkpoint_api phase=part_download exit=reap")
            size = target.stat().st_size
            time.sleep(.05)
            self.assertEqual(target.stat().st_size, size)
        finally:
            if group.exists():
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(int(group.read_text()), 9)

    def test_unknown_group_state_never_returns_output(self):
        self.shim("print('unverified output')\n")
        real_kill = os.killpg
        def kill(pid, sig):
            if sig == 0:
                raise PermissionError("fixture")
            return real_kill(pid, sig)
        with patch.object(checkpoint.os, "killpg", kill):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "exit=reap"):
                self.github.command(["api", "fixture"])

    def test_public_admission_uses_bounded_owner_and_distinct_token(self):
        self.shim("import os\nassert os.environ['GH_TOKEN']=='public-fixture'\n"
                  "assert 'DISCORD_BACKUP_TOKEN' not in os.environ\n"
                  "assert 'DISCRAWL_BACKUP_REPOSITORY' not in os.environ\n"
                  "os.write(2,b'private sentinel (HTTP 403)\\n')\n"
                  "for _ in range(16): os.write(2,b'x'*65536)\nraise SystemExit(9)\n")
        with patch.dict(os.environ, {"GH_TOKEN": "public-fixture", "DISCRAWL_BACKUP_REPOSITORY": "example/private"}), \
                patch.object(admission, "DEADLINE", time.monotonic() + 5):
            with self.assertRaises(checkpoint.CheckpointError) as failure:
                admission.api("actions/runs/1")
        self.assertEqual(str(failure.exception), "checkpoint_api phase=source_run_read exit=9 http=403")

    def test_public_admission_shared_deadline_prevents_launch(self):
        with patch.dict(os.environ, {"GH_TOKEN": "public-fixture"}), \
                patch.object(admission, "DEADLINE", time.monotonic() - 1), \
                patch.object(checkpoint.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(checkpoint.CheckpointError, "phase=source_run_read exit=timeout"):
                admission.api("actions/runs/1")
            popen.assert_not_called()

    def test_shared_deadline_stops_before_launch_and_read_only_blocks_mutation(self):
        self.github.deadline = time.monotonic() - 1
        with patch.object(checkpoint.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(checkpoint.CheckpointError, "exit=timeout"):
                self.github.command(["api", "fixture"])
            popen.assert_not_called()
        self.github.deadline = None
        self.github.read_only = True
        for args in (["release", "upload", "tag"], ["api", "fixture", "--method", "DELETE"]):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "diagnostic_read_only"):
                self.github.command(args)

    def test_manifest_download_uses_manifest_not_part_output_limit(self):
        self.github.base = "repos/example/private"
        self.shim("import os\nos.write(1,b'x'*(256*1024));os.write(1,b'x')\n")
        target = self.root / "manifest"
        with self.assertRaisesRegex(checkpoint.CheckpointError, "phase=manifest_download exit="):
            self.github.download({"id": 1, "name": checkpoint.MANIFEST}, target)
        self.assertLessEqual(target.stat().st_size, checkpoint.MAX_MANIFEST)


class DiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        database = self.root / "native.sqlite"
        db = native_database(database)
        self.addCleanup(db.close)
        self.github = FakeGitHub()
        source = {"repository": checkpoint.PRODUCER, "revision": "b" * 40, "run_id": 1,
                  "run_attempt": 1, "workflow": "migrate-discord-checkpoint.yml", "outcome": "migration",
                  "archive_commit": "f" * 40}
        # Multiple parts prove the probe does not accidentally restore all data.
        import test_private_discord_checkpoint as fixtures
        with patch.object(fixtures.checkpoint, "source_identity", return_value=source), patch.object(fixtures.checkpoint, "RAW_PART", 32768):
            fixtures.checkpoint.save(self.github, database, self.root / "unused", "migration", CACHE)
        self.github.latest = None
        self.release = self.github.releases[1]
        self.release.update(draft=True, published_at=None)
        assets = self.release["assets"]
        manifest = next(a for a in assets if a["name"] == checkpoint.MANIFEST)
        self.manifest_sha = checkpoint.digest(self.github.payloads[manifest["id"]])
        inventory = [{k: a[k] for k in ("id", "name", "size", "digest")} for a in sorted(assets, key=lambda a: a["name"])]
        self.inventory_sha = checkpoint.digest(checkpoint.canonical(inventory))
        self.run = {"id": 1, "run_attempt": 1, "status": "completed", "conclusion": "failure",
                    "event": "workflow_dispatch", "head_branch": "main", "workflow_id": 10,
                    "path": ".github/workflows/migrate-discord-checkpoint.yml", "head_sha": "b" * 40,
                    "actor": {"login": "vincentkoc"}, "triggering_actor": {"login": "vincentkoc"}}
        self.env = patch.dict(os.environ, {"GITHUB_REPOSITORY": checkpoint.PRODUCER,
               "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "workflow_dispatch",
               "GITHUB_SHA": "a" * 40, "GITHUB_WORKFLOW_SHA": "a" * 40,
               "GITHUB_WORKFLOW_REF": checkpoint.PRODUCER + "/.github/workflows/migrate-discord-checkpoint.yml@refs/heads/main",
               "GITHUB_RUN_ATTEMPT": "1", "GITHUB_ACTOR": "vincentkoc", "GITHUB_TRIGGERING_ACTOR": "vincentkoc"}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.downloads = []
        download = self.github.download
        def tracked(asset, target):
            self.downloads.append(asset["name"])
            download(asset, target)
            for a in assets:
                if a["id"] == asset["id"]:
                    a["download_count"] = a.get("download_count", 0) + 1
        self.github.download = tracked
        private_api = self.github.api
        def reads_only(suffix, data=None, missing=False):
            self.assertIsNone(data)
            if suffix == "":
                return {"full_name": self.github.repository, "id": 123, "private": True, "archived": False}
            return private_api(suffix, missing=missing)
        self.github.api = reads_only
        self.github.command = lambda *_args, **_kwargs: self.fail("diagnostic attempted a mutation")

    def api(self, path):
        if path == "git/ref/heads/main":
            return {"object": {"sha": "a" * 40}}
        if path.startswith("actions/runs/"):
            return copy.deepcopy(self.run)
        if "?status=" in path:
            return {"total_count": 0}
        return {"id": 10, "state": "disabled_manually"}

    def diagnose(self):
        with patch.object(admission, "api", self.api), patch.object(admission, "GitHub", return_value=self.github), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            admission.diagnose("a" * 40, 1, self.manifest_sha, self.inventory_sha)
            return output.getvalue()

    def test_reads_exact_first_part_without_restore_or_release_changes(self):
        before = copy.deepcopy(self.github.releases)
        output = self.diagnose()
        self.assertEqual(self.downloads, ["checkpoint.json", "state-000.sqlite.gz", "checkpoint.json"])
        self.assertIn("historical failure remains unclassified", output)
        self.assertNotIn(self.github.repository, output)
        self.assertIsNone(self.github.latest)
        self.assertEqual(self.github.deleted, [])
        for a in self.release["assets"]:
            a.pop("download_count", None)
        self.assertEqual(self.github.releases, before)

    def test_wrong_pins_stop_before_part_download(self):
        for field in ("manifest_sha", "inventory_sha"):
            with self.subTest(field=field):
                old = getattr(self, field)
                setattr(self, field, "0" * 64)
                self.downloads.clear()
                with self.assertRaisesRegex(checkpoint.CheckpointError, "diagnostic_evidence_changed"):
                    self.diagnose()
                self.assertEqual(self.downloads, ["checkpoint.json"])
                setattr(self, field, old)

    def test_original_success_rerun_wrong_actor_and_new_source_are_rejected(self):
        for key, value in (("conclusion", "success"), ("run_attempt", 2), ("workflow_id", 11),
                           ("actor", {"login": "other"}), ("head_sha", "c" * 40)):
            with self.subTest(key=key):
                old = self.run[key]
                self.run[key] = value
                with self.assertRaises(checkpoint.CheckpointError):
                    self.diagnose()
                self.run[key] = old
        with patch.dict(os.environ, {"GITHUB_RUN_ATTEMPT": "2"}):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "migration_context"):
                self.diagnose()

    def test_part_corruption_and_post_read_asset_drift_never_pass(self):
        self.github.corrupt_download = True
        with self.assertRaisesRegex(checkpoint.CheckpointError, "diagnostic_part_checksum"):
            self.diagnose()
        self.github.corrupt_download = False
        original = self.github.download
        def drift(asset, target):
            original(asset, target)
            if asset["name"] != checkpoint.MANIFEST:
                self.release["assets"][-1]["id"] += 100
        self.github.download = drift
        with self.assertRaisesRegex(checkpoint.CheckpointError, "diagnostic_evidence_changed"):
            self.diagnose()

    def test_latest_or_private_main_change_stops_without_part_read(self):
        self.github.latest = 1
        with self.assertRaisesRegex(checkpoint.CheckpointError, "diagnostic_publication_changed"):
            self.diagnose()
        self.assertEqual(self.downloads, ["checkpoint.json"])


if __name__ == "__main__":
    unittest.main()
