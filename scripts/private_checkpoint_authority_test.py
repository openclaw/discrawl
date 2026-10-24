"""Invoked by the Go test with a real native database and freshly built CLI."""

import base64
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import private_discord_checkpoint as checkpoint
import verify_discord_checkpoint_migration as admission
from test_private_discord_checkpoint import CACHE, FakeGitHub


DATABASE, NATIVE = map(Path, sys.argv[1:3])
del sys.argv[1:3]
REAL_COMMAND = checkpoint.github_command


class AutomaticAuthorityTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.remote, self.seed = self.root / "remote.git", self.root / "seed"
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith(("GIT_", "GITHUB_", "DISCRAWL_", "GH_", "DISCORD_", "ACTIONS_"))}
        self.env.update(GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1",
                        GIT_AUTHOR_NAME="Fixture", GIT_AUTHOR_EMAIL="fixture@example.invalid",
                        GIT_COMMITTER_NAME="Fixture", GIT_COMMITTER_EMAIL="fixture@example.invalid",
                        GIT_CONFIG_COUNT="2", GIT_CONFIG_KEY_0="commit.gpgsign", GIT_CONFIG_VALUE_0="false",
                        GIT_CONFIG_KEY_1="core.hooksPath", GIT_CONFIG_VALUE_1="/dev/null")
        self.git("init", "--bare", "--initial-branch=main", str(self.remote))
        self.git("init", "--initial-branch=main", str(self.seed))
        (self.seed / "manifest.json").write_text('{"version":1,"tables":[],"files":{}}')
        self.git("-C", str(self.seed), "add", "manifest.json")
        self.git("-C", str(self.seed), "commit", "-m", "fixture baseline")
        self.git("-C", str(self.seed), "remote", "add", "origin", str(self.remote))
        self.git("-C", str(self.seed), "push", "origin", "HEAD:refs/heads/main")
        self.baseline = self.head()
        self.policy = "f" * 40
        self.store = FakeGitHub()
        self.database = self.root / "snapshot.sqlite"
        self.native_hash = checkpoint.file_digest(DATABASE)
        with contextlib.redirect_stdout(io.StringIO()):
            facts = checkpoint.snapshot(DATABASE, self.database)
        database_hash, self.parts = checkpoint.pack(self.database, self.root)
        self.database_manifest = {**facts, **database_hash}
        self.parent, parent_ref = self.seed_release(1, None)
        self.candidate, self.reference = self.seed_release(2, parent_ref)
        self.store.latest = self.parent["id"]
        self.original_assets = copy.deepcopy(self.store.payloads)
        self.original_inventory = {key: copy.deepcopy(value["assets"]) for key, value in self.store.releases.items()}
        self.run = dict(id=2, run_attempt=1, status="completed", conclusion="failure", event="schedule",
                        head_branch="main", workflow_id=10, path=".github/workflows/publish-discord-backup.yml",
                        head_sha="b" * 40, actor={"login": "steipete"}, triggering_actor={"login": "steipete"})
        self.set_steps("split", "verify")
        shutil.copy2(NATIVE, self.root / "discrawl-native")
        self.environment = {**self.env, "RUNNER_TEMP": str(self.root), "GITHUB_REPOSITORY": checkpoint.PRODUCER,
                            "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "schedule", "GITHUB_SHA": "a" * 40,
                            "GITHUB_WORKFLOW_SHA": "a" * 40, "GITHUB_RUN_ID": "4", "GITHUB_RUN_ATTEMPT": "1",
                            "GITHUB_WORKFLOW_REF": checkpoint.PRODUCER + "/.github/workflows/publish-discord-backup.yml@refs/heads/main",
                            "GITHUB_ACTOR": "steipete", "GITHUB_TRIGGERING_ACTOR": "steipete",
                            "DISCRAWL_BACKUP_REPOSITORY": self.store.repository, "POLICY_COMMIT": self.policy,
                            "GH_TOKEN": "public-fixture", "DISCORD_BACKUP_TOKEN": "private-fixture",
                            "DISCORD_BOT_TOKEN": "must-not-reach-native"}
        self.patches, self.pushes, self.exports, self.part_downloads = 0, 0, 0, 0
        self.invalidate = None
        self.corrupt_part = False
        self.log = io.StringIO()

    def git(self, *arguments):
        return subprocess.run(["git", *arguments], env=self.env, check=True, capture_output=True,
                              timeout=15).stdout.decode().strip()

    def head(self):
        return self.git("-C", str(self.remote), "rev-parse", "main")

    def file(self, revision, name):
        return subprocess.run(["git", "-C", str(self.remote), "show", revision + ":" + name],
                              env=self.env, check=True, capture_output=True, timeout=15).stdout

    def seed_release(self, run_id, parent):
        # Fixture bytes are genuine native SQLite/gzip. The exercised path must
        # authenticate them through the real GitHub client and restore owner.
        manifest = {"schema": checkpoint.SCHEMA, "boundary": "native-ingestion-before-publication",
                    "repository": self.store.identity, "parent": parent, "migration": CACHE if parent is None else None,
                    "database": self.database_manifest, "parts": self.parts,
                    "source": {"repository": checkpoint.PRODUCER, "revision": "b" * 40, "run_id": run_id,
                               "run_attempt": 1, "archive_commit": self.baseline,
                               "workflow": "publish-discord-backup.yml" if parent else "migrate-discord-checkpoint.yml",
                               "outcome": "success" if parent else "migration"}}
        checkpoint.validate_manifest(manifest)
        (self.root / checkpoint.MANIFEST).write_bytes(checkpoint.canonical(manifest))
        release = self.store.create(f"discrawl-state-{run_id}-1", self.baseline)
        self.store.command(["release", "upload", release["tag_name"], "--repo", self.store.repository,
                            str(self.root / checkpoint.MANIFEST), *(str(self.root / part["name"]) for part in self.parts)])
        release = self.store.releases[release["id"]]
        release.update(draft=parent is not None, published_at=None if parent else "2026-01-01T00:00:00Z")
        return release, {"release_id": release["id"], "manifest_sha256": checkpoint.digest(checkpoint.canonical(manifest)),
                         "database_sha256": manifest["database"]["sha256"]}

    def set_steps(self, layout, failed):
        phases = [("native", "Sync native Discord state")]
        if layout == "split":
            phases.append(("upload", "Upload private ingestion checkpoint"))
        phases.append(("verify", "Verify private ingestion checkpoint"))
        if layout == "split":
            phases.append(("promote", "Promote verified private checkpoint"))
        phases.append(("archive", "Publish Discord archive"))
        stopped, steps = False, []
        for key, name in phases:
            conclusion = "skipped" if stopped else "failure" if key == failed else "success"
            steps.append(dict(name=name, status="completed", conclusion=conclusion))
            stopped |= key == failed
        self.run["conclusion"] = "success" if failed is None else "failure"
        self.job = dict(name="publish", run_id=2, head_sha="b" * 40, runner_id=1,
                        status="completed", conclusion=self.run["conclusion"], steps=steps)

    def public_response(self, path):
        if path == "git/ref/heads/main":
            return {"object": {"sha": "a" * 40}}
        if path == "actions/runs/2/attempts/1/jobs?per_page=100":
            return {"total_count": 1, "jobs": [self.job]}
        if path in {"actions/runs/2", "actions/runs/2/attempts/1"}:
            return self.run
        if "?status=" in path:
            active = [{"id": 4}] if path == "actions/workflows/10/runs?status=in_progress&per_page=100" else []
            return {"total_count": len(active), "workflow_runs": active}
        workflows = {"publish-discord-backup.yml": (10, "active"), "discord-backup-report.yml": (11, "disabled_manually"),
                     "repair-discord-cache.yml": (12, "deleted")}
        if path.startswith("actions/workflows/") and path.removeprefix("actions/workflows/") in workflows:
            identifier, state = workflows[path.removeprefix("actions/workflows/")]
            return {"id": identifier, "state": state}
        self.fail("unexpected public transport request: " + path)

    def private_response(self, suffix, data):
        if data is not None:
            self.assertEqual(suffix, "/releases/" + str(self.candidate["id"]))
            self.assertEqual(data, {"draft": False, "make_latest": "true"})
            self.patches += 1
            self.assertEqual(self.patches, 1)
            self.candidate["published_at"] = "2026-01-01T00:01:00Z"
            result = self.store.api(suffix, data)
            if self.invalidate == "before_push":
                self.policy = "e" * 40
            return result
        if suffix == "":
            return dict(id=123, full_name=self.store.repository, private=True, archived=False)
        if suffix == "/git/ref/heads/main":
            return {"object": {"sha": self.head()}}
        if suffix == "/git/ref/heads/maintainer-report-policy":
            return {"object": {"sha": self.policy}}
        if suffix.startswith("/git/commits/"):
            revision = suffix.removeprefix("/git/commits/")
            parents = self.git("-C", str(self.remote), "rev-list", "--parents", "-n", "1", revision).split()[1:]
            return dict(sha=revision, parents=[{"sha": parent} for parent in parents],
                        message=self.git("-C", str(self.remote), "log", "-1", "--format=%B", revision))
        if suffix.startswith("/contents/"):
            name, revision = suffix.removeprefix("/contents/").split("?ref=")
            body = self.file(revision, name)
            return dict(type="file", encoding="base64", size=len(body), content=base64.b64encode(body).decode())
        return self.store.api(suffix)

    def transport(self, arguments, env, phase, destination=None, timeout=600, missing=False,
                  limit=checkpoint.MAX_MANIFEST, deadline=None, executable="gh"):
        checkpoint.check_deadline(deadline)
        if executable != "gh":
            self.assertIn(executable, ("git", str(self.root / "discrawl-native")))
            self.assertFalse(any(env.get(key) for key in ("GH_TOKEN", "GITHUB_TOKEN", "DISCORD_BACKUP_TOKEN", "DISCORD_BOT_TOKEN")))
            if phase == "archive_export":
                self.exports += 1
                self.assertEqual(executable, str(self.root / "discrawl-native"))
                self.assertIn("publish", arguments)
                self.assertFalse({"--push", "init", "sync", "import"} & set(arguments))
            if phase == "archive_push":
                self.pushes += 1
                self.assertIn("--force-with-lease=refs/heads/main:" + self.baseline, arguments)
            # Rewrite only the approved synthetic network endpoint. Native CLI
            # Git subprocesses inherit it too; actual export/commit/push run intact.
            env = dict(env)
            index = int(env.get("GIT_CONFIG_COUNT", "0"))
            env.update({"GIT_CONFIG_COUNT": str(index + 1),
                        f"GIT_CONFIG_KEY_{index}": "url." + self.remote.as_uri() + ".insteadOf",
                        f"GIT_CONFIG_VALUE_{index}": "https://github.com/" + self.store.repository + ".git"})
            return REAL_COMMAND(arguments, env, phase, destination, timeout, missing, limit, deadline, executable)
        self.assertEqual(arguments[0], "api")
        method = arguments[arguments.index("--method") + 1] if "--method" in arguments else "GET"
        endpoint = arguments[1]
        if endpoint.startswith("repos/openclaw/discrawl/"):
            self.assertEqual(env["GH_TOKEN"], "public-fixture")
            self.assertNotIn("DISCORD_BACKUP_TOKEN", env)
            self.assertEqual(method, "GET")
            result = self.public_response(endpoint.removeprefix("repos/openclaw/discrawl/"))
        else:
            self.assertEqual(env["GH_TOKEN"], "private-fixture")
            self.assertTrue(endpoint.startswith(self.store.base))
            suffix = endpoint.removeprefix(self.store.base)
            if suffix.startswith("/releases/assets/"):
                self.assertEqual(method, "GET")
                asset_id = int(suffix.rsplit("/", 1)[1])
                body = self.store.payloads[asset_id]
                is_part = any(a["id"] == asset_id and a["name"] != checkpoint.MANIFEST for a in self.candidate["assets"])
                if self.corrupt_part and is_part:
                    body = body[:-1] + bytes([body[-1] ^ 1])
                self.assertLessEqual(len(body), limit)
                with destination.open("xb") as output:
                    output.write(body)
                if is_part:
                    self.part_downloads += 1
                    if self.invalidate == "before_patch" and self.part_downloads == 2 * len(self.parts):
                        self.policy = "e" * 40
                return b""
            self.assertIn(method, {"GET", "PATCH"})
            data = json.loads(Path(arguments[arguments.index("--input") + 1]).read_bytes()) if method == "PATCH" else None
            result = self.private_response(suffix, data)
        body = b"HTTP/2.0 200 OK\r\n\r\n" + checkpoint.canonical(result)
        self.assertLessEqual(len(body), limit)
        return body

    def reconcile(self):
        # No admission, constructor, API, restore or publication function is
        # patched. These two names are the same external command seam.
        with patch.dict(os.environ, self.environment, clear=True), \
                patch.object(checkpoint, "github_command", self.transport), \
                patch.object(admission, "github_command", self.transport), contextlib.redirect_stdout(self.log):
            return admission.automatic_reconciliation()

    def assert_immutable(self):
        self.assertEqual(self.store.payloads, self.original_assets)
        self.assertEqual({key: value["assets"] for key, value in self.store.releases.items()}, self.original_inventory)
        self.assertEqual(checkpoint.file_digest(self.database), self.database_manifest["sha256"])
        self.assertEqual(checkpoint.file_digest(DATABASE), self.native_hash)

    def assert_rounds(self):
        self.assertEqual(self.part_downloads, 2 * len(self.parts))
        log = self.log.getvalue()
        first, second = "checkpoint recovery: round=1 verified", "checkpoint recovery: round=2 verified"
        self.assertLess(log.index(first), log.index(second))
        for phase in ("disk_verify", "sqlite_verify"):
            self.assertEqual(log.count(f"phase={phase} state=complete"), 2)

    def assert_recovered(self, patches=1):
        self.assertTrue(self.reconcile())
        self.assert_rounds()
        child = self.head()
        self.assertNotEqual(child, self.baseline)
        self.assertEqual(self.git("-C", str(self.remote), "rev-list", "--parents", "-n", "1", child).split(), [child, self.baseline])
        producer = json.loads(self.file(child, "producer.json"))
        self.assertEqual(producer["producer"], dict(repository=checkpoint.PRODUCER, revision="b" * 40, run_id=2, run_attempt=1))
        self.assertEqual(producer["manifest_sha256"], checkpoint.digest(self.file(child, "manifest.json")))
        message = self.git("-C", str(self.remote), "log", "-1", "--format=%B")
        self.assertIn("Original: openclaw/discrawl@" + "b" * 40 + " run=2 attempt=1 overall=failure native=success", message)
        self.assertIn("Recovery: openclaw/discrawl@" + "a" * 40 + " run=4 attempt=1", message)
        self.assertIn("Checkpoint-manifest: " + self.reference["manifest_sha256"], message)
        self.assertIn("Checkpoint-database: " + self.database_manifest["sha256"], message)
        self.assertEqual((self.patches, self.pushes, self.exports), (patches, 1, 1))
        self.assertEqual(self.store.latest, self.candidate["id"])
        self.assertFalse(self.candidate["draft"])
        self.assert_immutable()
        # The next automatic run recognizes the actual published child. It
        # neither restores again nor manufactures new collection provenance.
        self.assertFalse(self.reconcile())
        self.assertEqual(self.head(), child)
        self.assertEqual((self.patches, self.pushes, self.exports), (patches, 1, 1))
        self.assertEqual(self.part_downloads, 2 * len(self.parts))

    def test_scheduled_old_monolithic_recovery(self):
        self.set_steps("old", "verify")
        self.assert_recovered()

    def test_scheduled_split_upload_failure(self):
        self.set_steps("split", "upload")
        self.assert_recovered()

    def test_scheduled_split_verification_failure(self):
        self.assert_recovered()

    def test_scheduled_split_promotion_failure(self):
        self.set_steps("split", "promote")
        self.assert_recovered()

    def test_scheduled_split_archive_failure_after_promotion(self):
        self.set_steps("split", "archive")
        self.candidate.update(draft=False, published_at="2026-01-01T00:00:30Z")
        self.store.latest = self.candidate["id"]
        self.assert_recovered(patches=0)

    def test_authorized_manual_dispatch_recovery(self):
        self.environment.update(GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_ACTOR="vincentkoc", GITHUB_TRIGGERING_ACTOR="vincentkoc")
        self.run.update(event="workflow_dispatch", actor={"login": "vincentkoc"}, triggering_actor={"login": "vincentkoc"})
        self.assert_recovered()

    def test_corrupt_download_holds_before_export_or_promotion(self):
        self.corrupt_part = True
        with self.assertRaisesRegex(checkpoint.CheckpointError, "part_checksum"):
            self.reconcile()
        self.assertEqual((self.patches, self.pushes, self.exports), (0, 0, 0))
        self.assertEqual(self.head(), self.baseline)
        self.assertTrue(self.candidate["draft"])
        self.assert_immutable()

    def test_denied_current_actor_mismatch_and_manual_authority(self):
        for change in ({"GITHUB_TRIGGERING_ACTOR": "someone-else"}, {"GITHUB_EVENT_NAME": "workflow_dispatch"}):
            with self.subTest(change=change):
                with patch.dict(self.environment, change):
                    with self.assertRaisesRegex(checkpoint.CheckpointError, "reconciliation_context"):
                        self.reconcile()
                self.assertEqual((self.patches, self.pushes, self.exports, self.part_downloads), (0, 0, 0, 0))
                self.assertEqual(self.head(), self.baseline)
                self.assert_immutable()

    def test_denied_original_actor_mismatch_and_manual_authority(self):
        for event, actor in (("schedule", "someone-else"), ("workflow_dispatch", "steipete")):
            with self.subTest(event=event):
                self.run.update(event=event, triggering_actor={"login": actor})
                with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_original_run"):
                    self.reconcile()
                self.assertEqual((self.patches, self.pushes, self.exports, self.part_downloads), (0, 0, 0, 0))
                self.assertEqual(self.head(), self.baseline)
                self.assert_immutable()

    def test_policy_invalidated_after_restores_prevents_promotion(self):
        self.invalidate = "before_patch"
        with self.assertRaisesRegex(checkpoint.CheckpointError, "reconciliation_policy_changed"):
            self.reconcile()
        self.assert_rounds()
        self.assertEqual((self.patches, self.pushes, self.exports), (0, 0, 1))
        self.assertTrue(self.candidate["draft"])
        self.assertEqual(self.head(), self.baseline)
        self.assert_immutable()

    def test_policy_invalidated_after_promotion_prevents_push_and_holds(self):
        self.invalidate = "before_push"
        with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_hold state=unknown"):
            self.reconcile()
        self.assert_rounds()
        self.assertEqual((self.patches, self.pushes, self.exports), (1, 0, 1))
        self.assertFalse(self.candidate["draft"])
        self.assertEqual(self.store.latest, self.candidate["id"])
        self.assertEqual(self.head(), self.baseline)
        self.assert_immutable()

    def legacy_publication(self):
        # A genuine legacy CLI export has the original producer tuple but a
        # plain commit message. Build that historical state, then test admission.
        self.set_steps("old", None)
        self.candidate.update(draft=False, published_at="2026-01-01T00:00:30Z")
        self.store.latest = self.candidate["id"]
        config = self.root / "legacy.toml"
        working = self.root / "legacy.sqlite"
        shutil.copy2(self.database, working)
        config.write_text("\n".join(key + " = " + json.dumps(str(value)) for key, value in
                                    (("db_path", working), ("cache_dir", self.root / "cache"), ("log_dir", self.root / "logs"))))
        env = {**self.env, "DISCRAWL_NO_AUTO_UPDATE": "1", "DISCRAWL_PRODUCER_REPOSITORY": checkpoint.PRODUCER,
               "DISCRAWL_PRODUCER_REVISION": "b" * 40, "DISCRAWL_PRODUCER_RUN_ID": "2", "DISCRAWL_PRODUCER_RUN_ATTEMPT": "1"}
        subprocess.run([str(NATIVE), "--config", str(config), "publish", "--repo", str(self.seed),
                        "--remote", str(self.remote), "--message", "sync: discord archive"],
                       env=env, check=True, capture_output=True, timeout=30)
        self.git("-C", str(self.seed), "push", "origin", "HEAD:refs/heads/main")

    def test_completed_success_legacy_export_is_unchanged_noop(self):
        self.legacy_publication()
        child = self.head()
        self.assertFalse(self.reconcile())
        self.assertEqual(self.head(), child)
        self.assertEqual((self.patches, self.pushes, self.exports, self.part_downloads), (0, 0, 0, 0))
        producer = json.loads(self.file(child, "producer.json"))
        self.assertEqual(producer["producer"], dict(repository=checkpoint.PRODUCER, revision="b" * 40, run_id=2, run_attempt=1))
        self.assert_immutable()

    def test_legacy_export_corrupt_producer_is_not_an_upgrade_noop(self):
        self.legacy_publication()
        producer_path = self.seed / "producer.json"
        producer = json.loads(producer_path.read_bytes())
        producer["producer"]["run_id"] = 999
        producer_path.write_bytes(checkpoint.canonical(producer))
        self.git("-C", str(self.seed), "add", "producer.json")
        self.git("-C", str(self.seed), "commit", "--amend", "--no-edit")
        self.git("-C", str(self.seed), "push", "--force", "origin", "HEAD:refs/heads/main")
        child = self.head()
        with self.assertRaisesRegex(checkpoint.CheckpointError, "reconciliation_archive_producer"):
            self.reconcile()
        self.assertEqual(self.head(), child)
        self.assertEqual((self.patches, self.pushes, self.exports, self.part_downloads), (0, 0, 0, 0))
        self.assert_immutable()

    def test_legacy_export_without_successful_original_steps_holds(self):
        self.legacy_publication()
        self.job["steps"][1]["conclusion"] = "failure"
        child = self.head()
        with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_original_step"):
            self.reconcile()
        self.assertEqual(self.head(), child)
        self.assertEqual((self.patches, self.pushes, self.exports, self.part_downloads), (0, 0, 0, 0))
        self.assert_immutable()


if __name__ == "__main__":
    unittest.main(verbosity=2)
