import base64
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import private_discord_checkpoint as checkpoint
import verify_discord_checkpoint_migration as admission
from test_private_discord_checkpoint import CheckpointTests, FakeGitHub, checkpoint as fixture_checkpoint


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        CheckpointTests.setUp(self)
        self.seed = lambda: CheckpointTests.seed(self)
        self.restore_parent = lambda: CheckpointTests.restore_parent(self)
        self.parent = self.seed()
        fixture_checkpoint.save(self.github, self.path, self.receipt, "success")
        self.release = next(r for r in self.github.releases.values() if r["tag_name"] == "discrawl-state-2-1")
        self.release.update(draft=True, published_at=None)
        self.github.latest = self.parent["release_id"]
        self.github.releases[self.github.latest]["published_at"] = "2026-01-01T00:00:00Z"
        asset = next(a for a in self.release["assets"] if a["name"] == checkpoint.MANIFEST)
        self.manifest_sha = checkpoint.digest(self.github.payloads[asset["id"]])
        self.inventory_sha = checkpoint.digest(checkpoint.canonical([
            {k: a[k] for k in ("id", "name", "size", "digest")}
            for a in sorted(self.release["assets"], key=lambda a: a["name"])]))
        self.run = dict(id=2, run_attempt=1, status="completed", conclusion="failure", event="workflow_dispatch",
                        head_branch="main", workflow_id=10, path=".github/workflows/publish-discord-backup.yml",
                        head_sha="b" * 40, actor={"login": "vincentkoc"}, triggering_actor={"login": "vincentkoc"})
        self.steps = [dict(name=name, status="completed", conclusion=conclusion)
                      for name, conclusion in (("Sync native Discord state", "success"),
                                               ("Verify private ingestion checkpoint", "failure"),
                                               ("Publish Discord archive", "skipped"))]
        self.job = dict(name="publish", run_id=2, head_sha="b" * 40, runner_id=1,
                        status="completed", conclusion="failure", steps=self.steps)
        self.writes, self.rounds, self.directories, self.pushed = [], 0, [], False
        self.preparation_error = None
        self.promotion_error = None
        self.client_calls = 0
        self.post_patch_client_error = False
        self.prepush_error = False
        self.after_round = lambda: None
        self.baseline = b'{"tables":[],"files":{}}'
        self.policy = "e" * 40
        self.github.api = self.private_api
        self.env = patch.dict(os.environ, {"GITHUB_REPOSITORY": checkpoint.PRODUCER,
            "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_SHA": "a" * 40,
            "GITHUB_WORKFLOW_SHA": "a" * 40, "GITHUB_RUN_ID": "4", "GITHUB_RUN_ATTEMPT": "1",
            "GITHUB_WORKFLOW_REF": checkpoint.PRODUCER + "/.github/workflows/migrate-discord-checkpoint.yml@refs/heads/main",
            "GITHUB_ACTOR": "vincentkoc", "GITHUB_TRIGGERING_ACTOR": "vincentkoc"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def identity(self, outcome, archive_commit):
        return CheckpointTests.identity(self, outcome, archive_commit)

    def private_api(self, suffix, data=None, missing=False):
        if suffix == "":
            return dict(full_name=self.github.repository, id=123, private=True, archived=False)
        if suffix.startswith("/contents/"):
            return dict(type="file", encoding="base64", size=len(self.baseline), content=base64.b64encode(self.baseline).decode())
        if suffix == "/git/ref/heads/maintainer-report-policy":
            return {"object": {"sha": self.policy}}
        if data is not None:
            self.writes.append(copy.deepcopy(data))
            if self.promotion_error == "before":
                raise checkpoint.CheckpointError("fixture_response")
        result = FakeGitHub.api(self.github, suffix, data, missing)
        if data:
            self.release["published_at"] = result["published_at"] = "2026-01-01T00:00:05Z"
            if self.promotion_error == "after":
                raise checkpoint.CheckpointError("fixture_response")
        return result

    def public_api(self, path):
        if path == "git/ref/heads/main":
            return {"object": {"sha": "a" * 40}}
        if path.endswith("/jobs?per_page=100"):
            return {"total_count": 1, "jobs": [copy.deepcopy(self.job)]}
        if path.startswith("actions/runs/"):
            return copy.deepcopy(self.run)
        if "?status=" in path:
            return {"total_count": 0}
        return {"id": 10, "state": "disabled_manually"}

    @contextlib.contextmanager
    def preparation(self, github, before, database, directory, policy, deadline):
        self.assertTrue(database.is_file())
        self.assertFalse(list(database.parent.glob("*.gz")))
        self.assertTrue(self.release["draft"], "export eligibility must precede remote promotion")
        if self.preparation_error:
            raise checkpoint.CheckpointError(self.preparation_error)
        def publish(_deadline):
            self.assertFalse(self.release["draft"])
            if self.prepush_error:
                raise checkpoint.CheckpointError("checkpoint_api phase=archive_ref_read exit=1")
            self.pushed = True
        yield publish

    def recover(self):
        download = admission.download_database
        def restore(*args, **kwargs):
            directory = args[2]
            if self.directories:
                self.assertFalse(self.directories[-1].exists())
            self.directories.append(directory)
            self.rounds += 1
            receipt = download(*args, **kwargs)
            self.after_round()
            return receipt
        def client(**_options):
            self.client_calls += 1
            if self.post_patch_client_error and self.client_calls == 3:
                raise checkpoint.CheckpointError("checkpoint_api phase=repository_read exit=1")
            return self.github
        with patch.object(admission, "GitHub", side_effect=client), patch.object(admission, "api", self.public_api), \
                patch.object(admission, "successful_probe", return_value=30), \
                patch.object(admission, "prepare_publication", self.preparation), \
                patch.object(admission, "download_database", restore), contextlib.redirect_stdout(io.StringIO()):
            admission.recover_publisher("a" * 40, 2, self.manifest_sha, self.inventory_sha, 3, "c" * 40)

    def test_two_restores_export_before_single_promotion_then_publication(self):
        payloads = copy.deepcopy(self.github.payloads)
        self.recover()
        self.assertEqual(self.rounds, 2)
        self.assertEqual(self.writes, [{"draft": False, "make_latest": "true"}])
        self.assertTrue(self.pushed)
        self.assertEqual(self.github.payloads, payloads)
        self.assertEqual(self.github.deleted, [])
        self.assertTrue(all(not p.exists() for p in self.directories))

    def test_original_requires_native_success_checkpoint_failure_archive_skipped(self):
        for index, conclusion in ((0, "failure"), (1, "success"), (2, "success")):
            old = self.steps[index]["conclusion"]
            self.steps[index]["conclusion"] = conclusion
            with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_original_step"):
                self.recover()
            self.steps[index]["conclusion"] = old
        self.assertEqual(self.writes, [])
        self.assertEqual(self.rounds, 0)

    def test_publisher_diagnostic_reads_first_part_and_pins_parent_without_restore(self):
        with patch.object(admission, "api", self.public_api), patch.object(admission, "GitHub", return_value=self.github), \
                patch.object(admission, "download_database") as restore, contextlib.redirect_stdout(io.StringIO()):
            admission.diagnose("a" * 40, 2, self.manifest_sha, self.inventory_sha)
        restore.assert_not_called()
        self.assertEqual(self.writes, [])
        self.assertEqual(self.github.latest, self.parent["release_id"])

    def test_parent_asset_identity_drift_after_restore_blocks_all_writes(self):
        def drift():
            if self.rounds == 2:
                self.github.releases[self.parent["release_id"]]["assets"][-1]["id"] += 1000
        self.after_round = drift
        with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_evidence_changed"):
            self.recover()
        self.assertEqual(self.writes, [])

    def test_media_or_embeddings_hold_before_download_and_promotion(self):
        for field, value in (("media", {"files": []}), ("embeddings", [{"file": "fixture"}])):
            self.baseline = checkpoint.canonical({field: value})
            with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_external_payload"):
                self.recover()
        self.assertEqual(self.rounds, 0)
        self.assertEqual(self.writes, [])

    def test_corrupt_full_download_and_export_failure_never_promote(self):
        self.github.corrupt_download = True
        with self.assertRaisesRegex(checkpoint.CheckpointError, "part_checksum"):
            self.recover()
        self.github.corrupt_download = False
        self.directories = []
        self.preparation_error = "recovery_archive_parent"
        with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_archive_parent"):
            self.recover()
        self.assertEqual(self.writes, [])
        self.assertFalse(self.pushed)

    def test_lost_patch_response_before_or_after_commit_holds_without_push_or_retry(self):
        for phase in ("before", "after"):
            self.promotion_error = phase
            self.directories = []
            with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_hold"):
                self.recover()
            self.assertFalse(self.pushed)
        self.assertEqual(len(self.writes), 2)

    def test_expired_final_gate_never_promotes(self):
        with patch.object(admission, "check_deadline", side_effect=checkpoint.CheckpointError("checkpoint_timeout")):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_timeout"):
                self.recover()
        self.assertEqual(self.rounds, 2)
        self.assertEqual(self.writes, [])
        self.assertFalse(self.pushed)

    def test_post_patch_client_admission_failure_is_retained_state_hold(self):
        self.post_patch_client_error = True
        with self.assertRaisesRegex(checkpoint.CheckpointError, "^recovery_hold state=unknown$"):
            self.recover()
        self.assertEqual(len(self.writes), 1)
        self.assertFalse(self.release["draft"])
        self.assertFalse(self.pushed)

    def test_post_promotion_push_preflight_failure_is_retained_state_hold(self):
        self.prepush_error = True
        with self.assertRaisesRegex(checkpoint.CheckpointError, "^recovery_hold state=unknown$"):
            self.recover()
        self.assertEqual(len(self.writes), 1)
        self.assertFalse(self.release["draft"])
        self.assertFalse(self.pushed)


class OfflineCommandTests(unittest.TestCase):
    def test_native_file_writes_are_not_limited_by_captured_output_budget(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "large"
            raw = checkpoint.github_command(["-c", "from pathlib import Path; import sys; Path(sys.argv[1]).write_bytes(b'x'*1048576);print('ok')",
                                              str(target)], os.environ.copy(), "archive_export", limit=32,
                                             timeout=5, executable=sys.executable)
            self.assertEqual(raw, b"ok\n")
            self.assertEqual(target.stat().st_size, 1048576)

    def test_native_stdout_flood_is_bounded_and_not_rendered(self):
        with contextlib.redirect_stdout(io.StringIO()) as out, contextlib.redirect_stderr(io.StringIO()) as err:
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_api_size"):
                checkpoint.github_command(["-c", "import os; os.write(1,b'private'*10000)"], os.environ.copy(),
                                          "archive_export", limit=32, timeout=5, executable=sys.executable)
        self.assertEqual(out.getvalue() + err.getvalue(), "")


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.remote, self.seed = self.root / "remote.git", self.root / "seed"
        self.env = {**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
                    "GIT_AUTHOR_NAME": "Fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
                    "GIT_COMMITTER_NAME": "Fixture", "GIT_COMMITTER_EMAIL": "fixture@example.invalid"}
        self.git("init", "--bare", "--initial-branch=main", str(self.remote))
        self.git("init", "--initial-branch=main", str(self.seed))
        (self.seed / "manifest.json").write_text('{"tables":[],"files":{}}')
        self.git("-C", str(self.seed), "add", "manifest.json")
        self.git("-C", str(self.seed), "commit", "-m", "fixture")
        self.baseline = self.git("-C", str(self.seed), "rev-parse", "HEAD")
        self.git("-C", str(self.seed), "remote", "add", "origin", str(self.remote))
        self.git("-C", str(self.seed), "push", "origin", "HEAD:refs/heads/main")
        self.database = self.root / "native.sqlite"
        with sqlite3.connect(self.database) as db:
            for table in ("guilds", "channels", "members", "messages", "message_attachments", "message_events",
                          "mention_events", "sync_state", "failure_ledger"):
                db.execute(f"CREATE TABLE {table}(id INTEGER PRIMARY KEY, value TEXT)")
                db.execute(f"INSERT INTO {table} VALUES(1,'retained')")
        self.source = dict(repository="openclaw/discrawl", revision="b" * 40, run_id=2, run_attempt=1,
                           archive_commit=self.baseline)
        self.before = ({"source": self.source, "database": {"bytes": self.database.stat().st_size, "sha256": "d" * 64}},
                       {}, {"manifest_sha256": "e" * 64})
        self.github = FakeGitHub()
        self.github.api = self.api
        self.policy = "f" * 40
        self.push_count = 0
        self.response_lost = False
        self.race = False
        self.native = self.root / "discrawl-recovery-native"
        self.native.write_text("#!" + sys.executable + "\n"
            "import hashlib,json,os,subprocess,sys\nfrom pathlib import Path\n"
            "assert '--push' not in sys.argv and 'init' not in sys.argv and 'sync' not in sys.argv\n"
            "assert not any(os.environ.get(k) for k in ['DISCORD_BOT_TOKEN','DISCORD_BACKUP_TOKEN','GH_TOKEN','GITHUB_TOKEN'])\n"
            "assert os.environ['DISCRAWL_NO_AUTO_UPDATE']=='1'\n"
            "repo=Path(sys.argv[sys.argv.index('--repo')+1]);body=b'{\"files\":{\"producer\":\"producer.json\"}}'\n"
            "(repo/'manifest.json').write_bytes(body)\n"
            "producer={k:os.environ['DISCRAWL_PRODUCER_'+k.upper()] for k in ['repository','revision','run_id','run_attempt']}\n"
            "producer.update(run_id=int(producer['run_id']),run_attempt=int(producer['run_attempt']))\n"
            "(repo/'producer.json').write_text(json.dumps(dict(schema=1,manifest_path='manifest.json',manifest_sha256=hashlib.sha256(body).hexdigest(),producer=producer)))\n"
            "subprocess.run(['git','-C',str(repo),'add','manifest.json','producer.json'],check=True)\n"
            "subprocess.run(['git','-C',str(repo),'commit','-m',sys.argv[sys.argv.index('--message')+1]],check=True)\n")
        self.native.chmod(0o700)
        self.environment = patch.dict(os.environ, {**self.env, "RUNNER_TEMP": str(self.root), "GITHUB_REPOSITORY": "openclaw/discrawl",
             "GITHUB_SHA": "a" * 40, "GITHUB_RUN_ID": "4", "DISCORD_BACKUP_TOKEN": "private-fixture",
             "DISCORD_BOT_TOKEN": "must-not-reach-native", "GH_TOKEN": "must-not-reach-native"})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def git(self, *args):
        return subprocess.run(["git", *args], env=self.env, check=True, capture_output=True).stdout.decode().strip()

    def api(self, suffix, data=None, missing=False):
        self.assertIsNone(data)
        if suffix == "/git/ref/heads/main":
            return {"object": {"sha": self.git("-C", str(self.remote), "rev-parse", "main")}}
        if suffix == "/git/ref/heads/maintainer-report-policy":
            return {"object": {"sha": self.policy}}
        name, revision = suffix.removeprefix("/contents/").split("?ref=")
        body = subprocess.run(["git", "-C", str(self.remote), "show", revision + ":" + name],
                              env=self.env, check=True, capture_output=True).stdout
        return dict(type="file", encoding="base64", size=len(body), content=base64.b64encode(body).decode())

    def command(self, arguments, env, phase, **options):
        arguments = list(arguments)
        if arguments[0] == "clone":
            arguments[-2] = str(self.remote)
        if phase == "archive_push":
            self.push_count += 1
            self.assertIn("--force-with-lease=refs/heads/main:" + self.baseline, arguments)
            if self.race:
                self.git("-C", str(self.seed), "commit", "--allow-empty", "-m", "concurrent fixture")
                self.git("-C", str(self.seed), "push", "origin", "HEAD:refs/heads/main")
        output = checkpoint.github_command(arguments, env, phase, **options)
        if phase == "archive_push" and self.response_lost:
            raise checkpoint.CheckpointError("checkpoint_api phase=archive_push exit=timeout")
        return output

    def publish(self):
        directory = self.root / "recovery"
        directory.mkdir()
        with patch.object(admission, "github_command", self.command), contextlib.redirect_stdout(io.StringIO()):
            with admission.prepare_publication(self.github, self.before, self.database, directory, self.policy,
                                               time.monotonic() + 30) as publish:
                self.assertEqual(self.git("-C", str(self.remote), "rev-parse", "main"), self.baseline)
                publish(time.monotonic() + 30)
        self.assertFalse((directory / "git-credentials").exists())

    def test_real_git_single_cas_push_and_exact_provenance_readback(self):
        before = admission.ingestion_facts(self.database, time.monotonic() + 10)
        self.publish()
        child = self.git("-C", str(self.remote), "rev-parse", "main")
        self.assertEqual(self.git("-C", str(self.remote), "rev-list", "--parents", "-n", "1", "main").split(), [child, self.baseline])
        self.assertEqual(self.push_count, 1)
        message = self.git("-C", str(self.remote), "log", "-1", "--format=%B")
        self.assertIn("overall=failure native=success", message)
        self.assertIn("Recovery: openclaw/discrawl@" + "a" * 40, message)
        self.assertEqual(admission.ingestion_facts(self.database, time.monotonic() + 10), before)

    def test_real_git_race_rejects_once_without_rebase_or_retry(self):
        self.race = True
        with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_publication_hold"):
            self.publish()
        self.assertEqual(self.push_count, 1)
        self.assertEqual(self.git("-C", str(self.remote), "log", "-1", "--format=%s"), "concurrent fixture")

    def test_lost_push_response_holds_even_after_remote_commit(self):
        self.response_lost = True
        with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_publication_hold"):
            self.publish()
        self.assertEqual(self.push_count, 1)
        self.assertNotEqual(self.git("-C", str(self.remote), "rev-parse", "main"), self.baseline)


def load_tests(loader, _tests, _pattern):
    return unittest.TestSuite(loader.loadTestsFromTestCase(case) for case in (RecoveryTests, OfflineCommandTests, PublicationTests))
