from test_private_discord_checkpoint import save
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
from test_private_discord_checkpoint import CheckpointTests, FakeGitHub, save


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        CheckpointTests.setUp(self)
        self.seed = lambda: CheckpointTests.seed(self)
        self.restore_parent = lambda: CheckpointTests.restore_parent(self)
        self.parent = self.seed()
        save(self.github, self.path, self.receipt, "success")
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
        self.allow_published = False
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
    def preparation(self, github, before, database, directory, policy, deadline, outcome="failure"):
        self.assertEqual(outcome, self.run["conclusion"])
        self.assertTrue(database.is_file())
        self.assertFalse(list(database.parent.glob("*.gz")))
        self.assertTrue(self.release["draft"] or self.allow_published, "export eligibility must precede remote promotion")
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

    def test_scheduled_publisher_actor_is_not_manual_dispatch_authority(self):
        # GitHub's actual scheduled publisher uses this cron-associated actor.
        self.run.update(event="schedule", actor={"login": "steipete"}, triggering_actor={"login": "steipete"})
        with patch.object(admission, "api", self.public_api):
            self.assertEqual(admission.failed_publisher(2), self.run["head_sha"])
            original = copy.deepcopy(self.run)
            for changes in ({"event": "workflow_dispatch"}, {"event": "push"}, {"head_branch": "other"},
                            {"run_attempt": 2}, {"workflow_id": 11}, {"actor": {"login": ""}},
                            {"triggering_actor": {"login": "vincentkoc"}}):
                self.run.update(changes)
                with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_original_run"):
                    admission.failed_publisher(2)
                self.run = copy.deepcopy(original)
        def changed_current(path):
            result = self.public_api(path)
            if path == "actions/runs/2":
                result["actor"] = {"login": "vincentkoc"}
            return result
        with patch.object(admission, "api", changed_current):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_original_run"):
                admission.failed_publisher(2)

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


class AutomaticRecoveryTests(unittest.TestCase):
    setUp = RecoveryTests.setUp
    identity = RecoveryTests.identity
    private_api = RecoveryTests.private_api
    public_api = RecoveryTests.public_api
    preparation = RecoveryTests.preparation

    def retained_outcome(self, outcome):
        asset = next(a for a in self.release["assets"] if a["name"] == checkpoint.MANIFEST)
        manifest = json.loads(self.github.payloads[asset["id"]])
        manifest["source"]["outcome"] = outcome
        body = checkpoint.canonical(manifest)
        self.github.payloads[asset["id"]] = body
        asset.update(size=len(body), digest="sha256:" + checkpoint.digest(body))

    def automatic(self):
        download = admission.download_database
        def client(*, deadline, **_options):
            github = copy.copy(self.github)
            github.deadline = deadline
            def api(*args, **kwargs):
                admission.check_deadline(github.deadline)
                return self.github.api(*args, **kwargs)
            github.api = api
            return github
        def restore(*args, **kwargs):
            self.rounds += 1
            result = download(*args, **kwargs)
            self.after_round()
            return result
        with patch.dict(os.environ, {"POLICY_COMMIT": self.policy, "GITHUB_WORKFLOW_REF": checkpoint.PRODUCER +
                                     "/.github/workflows/publish-discord-backup.yml@refs/heads/main"}), \
                patch.object(admission, "GitHub", side_effect=client), \
                patch.object(admission, "verify_automatic_context"), \
                patch.object(admission, "api", self.public_api), \
                patch.object(admission, "prepare_publication", self.preparation), \
                patch.object(admission, "download_database", restore), contextlib.redirect_stdout(io.StringIO()):
            return admission.automatic_reconciliation()

    def test_scheduled_draft_recovery_uses_two_restores_and_one_promotion(self):
        self.assertTrue(self.automatic())
        self.assertEqual(self.rounds, 2)
        self.assertEqual(self.writes, [{"draft": False, "make_latest": "true"}])
        self.assertTrue(self.pushed)

    def test_initial_published_migration_continues_to_acquisition(self):
        del self.github.releases[self.release["id"]]
        self.assertFalse(self.automatic())
        self.assertEqual(self.rounds, 0)
        self.assertEqual(self.writes, [])
        self.assertFalse(self.pushed)

    def test_published_failed_acquisition_continues_without_archive_publication(self):
        self.retained_outcome("failure")
        self.release.update(draft=False, published_at="2026-01-01T00:00:05Z")
        self.github.latest = self.release["id"]
        self.assertFalse(self.automatic())
        self.assertEqual(self.rounds, 0)
        self.assertEqual(self.writes, [])
        self.assertFalse(self.pushed)

    def test_draft_failed_acquisition_is_verified_twice_and_saved_without_export(self):
        self.retained_outcome("failure")
        self.assertTrue(self.automatic())
        self.assertEqual(self.rounds, 2)
        self.assertEqual(self.writes, [{"draft": False, "make_latest": "true"}])
        self.assertFalse(self.pushed)

    def test_draft_cancelled_acquisition_retains_progress_without_export(self):
        self.retained_outcome("cancelled")
        self.run["conclusion"] = self.job["conclusion"] = "cancelled"
        self.steps[0]["conclusion"] = "cancelled"
        self.automatic()
        self.assertEqual(self.rounds, 2)
        self.assertFalse(self.pushed)

    def test_failed_acquisition_lost_promotion_resumes_without_export(self):
        self.retained_outcome("failure")
        self.promotion_error = "after"
        with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_hold"):
            self.automatic()
        self.promotion_error = None
        self.automatic()
        self.assertEqual(len(self.writes), 1)
        self.assertFalse(self.pushed)

    def test_failed_acquisition_cannot_claim_a_published_archive(self):
        self.retained_outcome("failure")
        self.steps[2]["conclusion"] = "success"
        with self.assertRaisesRegex(checkpoint.CheckpointError, "original_step"):
            self.automatic()
        self.assertEqual(self.writes, [])

    def test_published_checkpoint_resumes_archive_without_another_patch(self):
        self.release.update(draft=False, published_at="2026-01-01T00:00:05Z")
        self.github.latest = self.release["id"]
        self.steps[1]["conclusion"] = "success"
        self.steps[2]["conclusion"] = "failure"
        self.allow_published = True
        self.automatic()
        self.assertEqual(self.rounds, 2)
        self.assertEqual(self.writes, [])
        self.assertTrue(self.pushed)

    def test_lost_patch_response_is_reconciled_on_next_schedule(self):
        self.promotion_error = "after"
        with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_hold"):
            self.automatic()
        self.assertFalse(self.pushed)
        self.promotion_error = None
        self.allow_published = True
        self.automatic()
        self.assertEqual(len(self.writes), 1)
        self.assertTrue(self.pushed)

    def test_cancelled_original_preserves_its_terminal_outcome(self):
        self.run["conclusion"] = self.job["conclusion"] = "cancelled"
        self.steps[1]["conclusion"] = "cancelled"
        self.automatic()
        self.assertTrue(self.pushed)
        self.assertEqual(len(self.writes), 1)

    def test_timed_out_original_preserves_its_terminal_outcome(self):
        self.run["conclusion"] = self.job["conclusion"] = "timed_out"
        self.steps[1]["conclusion"] = "timed_out"
        self.automatic()
        self.assertTrue(self.pushed)
        self.assertEqual(len(self.writes), 1)

    def test_discovery_consumes_the_shared_reconciliation_deadline(self):
        readings = iter([100.0])
        with patch.object(admission.time, "monotonic", side_effect=lambda: next(readings, 2500.0)):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_timeout"):
                self.automatic()
        self.assertEqual(self.writes, [])
        self.assertFalse(self.pushed)

    def test_noop_discovery_deadline_is_not_renewed(self):
        del self.github.releases[self.release["id"]]
        readings = iter([100.0])
        with patch.object(admission.time, "monotonic", side_effect=lambda: next(readings, 221.0)):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_timeout"):
                self.automatic()
        self.assertEqual(admission.DEADLINE, 220)
        self.assertEqual(self.rounds, 0)
        self.assertEqual(self.writes, [])

    def test_recovery_keeps_absolute_deadline_after_discovery(self):
        clock = [100.0]
        original = self.github.api
        def discovered(*args, **kwargs):
            clock[0] = max(clock[0], 200.0)
            return original(*args, **kwargs)
        self.github.api = discovered
        self.after_round = lambda: clock.__setitem__(0, 500.0)
        with patch.object(admission.time, "monotonic", side_effect=lambda: clock[0]), \
                patch.object(admission, "reconcile_publisher", wraps=admission.reconcile_publisher) as recover:
            self.assertTrue(self.automatic())
        self.assertEqual(recover.call_args.kwargs["deadline"], 2080)
        self.assertEqual(self.rounds, 2)

    def test_discovery_expiry_cannot_enter_longer_recovery_budget(self):
        readings = iter([100.0])
        with patch.object(admission.time, "monotonic", side_effect=lambda: next(readings, 221.0)), \
                patch.object(admission, "reconcile_publisher") as recover:
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_timeout"):
                self.automatic()
        recover.assert_not_called()
        self.assertEqual(self.writes, [])

    def test_partial_or_multiple_drafts_hold_without_restoring_or_writing(self):
        self.release["assets"].pop()
        with self.assertRaisesRegex(checkpoint.CheckpointError, "asset_inventory"):
            self.automatic()
        self.github.create("foreign", "f" * 40)
        with self.assertRaisesRegex(checkpoint.CheckpointError, "ambiguous_drafts"):
            self.automatic()
        self.assertEqual(self.rounds, 0)
        self.assertEqual(self.writes, [])

    def test_unrelated_archive_or_failed_native_capture_never_reconciles(self):
        original = self.private_api
        def diverged(suffix, *args, **kwargs):
            if suffix == "/git/ref/heads/main":
                return {"object": {"sha": "a" * 40}}
            return original(suffix, *args, **kwargs)
        self.github.api = diverged
        with self.assertRaisesRegex(checkpoint.CheckpointError, "parent_changed"):
            self.automatic()
        self.github.api = original
        self.steps[0]["conclusion"] = "failure"
        with self.assertRaisesRegex(checkpoint.CheckpointError, "original_step"):
            self.automatic()
        self.assertEqual(self.writes, [])

    def exported_archive(self, legacy=False):
        self.release.update(draft=False, published_at="2026-01-01T00:00:05Z")
        self.github.latest = self.release["id"]
        original = self.private_api
        manifest_asset = next(a for a in self.release["assets"] if a["name"] == checkpoint.MANIFEST)
        manifest = json.loads(self.github.payloads[manifest_asset["id"]])
        source, head = manifest["source"], "d" * 40
        manifest_body = checkpoint.canonical({"files": {"producer": "producer.json"}})
        producer = {"schema": 1, "manifest_path": "manifest.json",
            "manifest_sha256": checkpoint.digest(manifest_body), "producer": {
                key: source[key] for key in ("repository", "revision", "run_id", "run_attempt")}}
        message = "sync: discord archive\n" if legacy else (
            "sync: discord archive\n\n" + admission.checkpoint_binding(manifest, {"manifest_sha256": self.manifest_sha}))
        commit = {"sha": head, "parents": [{"sha": source["archive_commit"]}], "message": message}
        def exported(suffix, *args, **kwargs):
            if suffix == "/git/ref/heads/main":
                return {"object": {"sha": head}}
            if suffix == "/git/commits/" + head:
                return copy.deepcopy(commit)
            if suffix.startswith("/contents/"):
                body = checkpoint.canonical(producer) if "/producer.json?" in suffix else manifest_body
                return dict(type="file", encoding="base64", size=len(body), content=base64.b64encode(body).decode())
            return original(suffix, *args, **kwargs)
        self.github.api = exported
        if legacy:
            self.run["conclusion"] = self.job["conclusion"] = "success"
            for step in self.steps:
                step["conclusion"] = "success"
        return commit, producer

    def test_already_exported_checkpoint_is_a_noop_and_requires_exact_binding(self):
        commit, _ = self.exported_archive()
        self.assertFalse(self.automatic())
        self.assertEqual(self.rounds, 0)
        self.assertEqual(self.writes, [])
        self.assertFalse(self.pushed)
        commit["message"] = commit["message"].replace(self.manifest_sha, "0" * 64)
        with self.assertRaisesRegex(checkpoint.CheckpointError, "archive_binding"):
            self.automatic()

    def test_previous_workflow_success_is_accepted_without_republishing(self):
        self.exported_archive(legacy=True)
        self.automatic()
        self.assertEqual(self.rounds, 0)
        self.assertEqual(self.writes, [])
        self.assertFalse(self.pushed)

    def test_legacy_archive_requires_complete_success_and_exact_message(self):
        commit, _ = self.exported_archive(legacy=True)
        for conclusion in ("failure", "cancelled", "timed_out"):
            with self.subTest(conclusion=conclusion):
                self.run["conclusion"] = self.job["conclusion"] = conclusion
                with self.assertRaisesRegex(checkpoint.CheckpointError, "archive_binding"):
                    self.automatic()
        self.run["conclusion"] = self.job["conclusion"] = "success"
        for step in self.steps:
            with self.subTest(step=step["name"]):
                step["conclusion"] = "skipped"
                with self.assertRaisesRegex(checkpoint.CheckpointError, "original_step"):
                    self.automatic()
                step["conclusion"] = "success"
        commit["message"] += "unverified extra text"
        with self.assertRaisesRegex(checkpoint.CheckpointError, "archive_binding"):
            self.automatic()
        self.assertEqual(self.writes, [])

    def test_legacy_archive_still_requires_parent_and_producer_binding(self):
        commit, producer = self.exported_archive(legacy=True)
        parent = commit["parents"]
        commit["parents"] = [{"sha": "0" * 40}]
        with self.assertRaisesRegex(checkpoint.CheckpointError, "archive_binding"):
            self.automatic()
        commit["parents"] = parent
        producer["manifest_sha256"] = "0" * 64
        with self.assertRaisesRegex(checkpoint.CheckpointError, "archive_producer"):
            self.automatic()
        producer["manifest_sha256"] = checkpoint.digest(checkpoint.canonical({"files": {"producer": "producer.json"}}))
        producer["producer"]["run_id"] += 1
        with self.assertRaisesRegex(checkpoint.CheckpointError, "archive_producer"):
            self.automatic()
        self.assertEqual(self.writes, [])

    def test_changed_policy_holds_before_restore_or_mutation(self):
        selected_policy = self.policy
        self.policy = "0" * 40
        with patch.dict(os.environ, {"POLICY_COMMIT": selected_policy}):
            with patch.object(admission, "verify_automatic_context"), patch.object(admission, "GitHub", return_value=self.github):
                with self.assertRaisesRegex(checkpoint.CheckpointError, "policy_changed"):
                    admission.automatic_reconciliation()
        self.assertEqual(self.rounds, 0)
        self.assertEqual(self.writes, [])

    def test_policy_change_during_restore_holds_before_promotion(self):
        def change_policy():
            if self.rounds == 2:
                self.policy = "0" * 40
        self.after_round = change_policy
        with self.assertRaisesRegex(checkpoint.CheckpointError, "reconciliation_policy_changed"):
            self.automatic()
        self.assertEqual(self.rounds, 2)
        self.assertEqual(self.writes, [])
        self.assertFalse(self.pushed)


class SplitRecoveryTests(unittest.TestCase):
    identity = RecoveryTests.identity
    private_api = RecoveryTests.private_api
    public_api = RecoveryTests.public_api
    preparation = RecoveryTests.preparation
    recover = RecoveryTests.recover
    automatic = AutomaticRecoveryTests.automatic
    retained_outcome = AutomaticRecoveryTests.retained_outcome
    exported_archive = AutomaticRecoveryTests.exported_archive

    def setUp(self):
        RecoveryTests.setUp(self)
        self.steps[:] = [dict(name=name, status="completed", conclusion=conclusion) for name, conclusion in (
            ("Sync native Discord state", "success"),
            ("Upload private ingestion checkpoint", "success"),
            ("Verify private ingestion checkpoint", "failure"),
            ("Promote verified private checkpoint", "skipped"),
            ("Publish Discord archive", "skipped"))]

    def failed_phase(self, phase, conclusion="failure"):
        for index, step in enumerate(self.steps):
            step["conclusion"] = "success" if index < phase else conclusion if index == phase else "skipped"
        self.run["conclusion"] = self.job["conclusion"] = conclusion

    def test_each_failed_checkpoint_phase_reconciles_exact_retained_draft(self):
        for phase in (1, 2, 3):
            with self.subTest(phase=phase):
                self.failed_phase(phase)
                self.release.update(draft=True, published_at=None)
                self.github.latest = self.parent["release_id"]
                self.automatic()
        self.assertEqual(self.rounds, 6)
        self.assertEqual(self.writes, [{"draft": False, "make_latest": "true"}] * 3)
        self.assertTrue(self.pushed)
        self.assertEqual(self.github.deleted, [])

    def test_manual_probe_and_recovery_accept_each_failed_phase_without_relaxing_archive_gate(self):
        for phase in (1, 2, 3):
            with self.subTest(phase=phase):
                self.failed_phase(phase)
                self.release.update(draft=True, published_at=None)
                self.github.latest = self.parent["release_id"]
                self.directories = []
                self.recover()
        self.assertEqual(self.rounds, 6)
        self.assertEqual(len(self.writes), 3)

    def test_cancellation_between_phases_and_during_each_phase_is_recoverable(self):
        for phase in (1, 2, 3):
            for conclusion in ("cancelled", "timed_out"):
                self.failed_phase(phase, conclusion)
                with patch.object(admission, "api", self.public_api):
                    self.assertEqual(admission.publisher_run(2, resume=True), self.run)
                if phase > 1:
                    self.steps[phase]["conclusion"] = "skipped"
                    with patch.object(admission, "api", self.public_api):
                        self.assertEqual(admission.publisher_run(2, resume=True), self.run)

    def test_missing_duplicate_reordered_or_impossible_phases_hold_before_any_restore(self):
        original = copy.deepcopy(self.steps)
        changes = [lambda: self.steps.pop(1), lambda: self.steps.pop(3),
                   lambda: self.steps.insert(2, copy.deepcopy(self.steps[2])),
                   lambda: self.steps.reverse(),
                   lambda: self.steps[1].update(conclusion="skipped"),
                   lambda: self.steps[3].update(conclusion="success"),
                   lambda: self.steps[4].update(conclusion="success")]
        for change in changes:
            self.steps[:] = copy.deepcopy(original)
            change()
            with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_original_step"):
                self.automatic()
        self.assertEqual(self.rounds, 0)
        self.assertEqual(self.writes, [])

    def test_failed_native_progress_is_retained_without_archive_export(self):
        for outcome in ("failure", "cancelled"):
            self.retained_outcome(outcome)
            self.failed_phase(2)
            # continue-on-error exposes success despite the manifest's failure.
            self.release.update(draft=True, published_at=None)
            self.github.latest = self.parent["release_id"]
            self.automatic()
        self.assertEqual(self.rounds, 4)
        self.assertEqual(len(self.writes), 2)
        self.assertFalse(self.pushed)

    def test_promotion_response_loss_resumes_then_hash_bound_export_is_noop(self):
        self.failed_phase(3)
        self.promotion_error = "after"
        with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_hold"):
            self.automatic()
        self.assertFalse(self.pushed)
        self.promotion_error = None
        self.allow_published = True
        self.automatic()
        self.assertEqual(self.rounds, 4)
        self.assertEqual(len(self.writes), 1)
        self.assertTrue(self.pushed)
        self.exported_archive()
        self.automatic()
        self.assertEqual(self.rounds, 4)
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.run["conclusion"], "failure")

    def test_archive_failure_resumes_from_published_checkpoint_without_promotion(self):
        self.failed_phase(4)
        self.release.update(draft=False, published_at="2026-01-01T00:00:05Z")
        self.github.latest = self.release["id"]
        self.allow_published = True
        self.automatic()
        self.assertEqual(self.rounds, 2)
        self.assertEqual(self.writes, [])
        self.assertTrue(self.pushed)

    def test_legacy_unbound_three_phase_export_requires_every_success(self):
        self.exported_archive(legacy=True)
        self.automatic()
        for index in range(len(self.steps)):
            self.steps[index]["conclusion"] = "skipped"
            with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_original_step"):
                self.automatic()
            self.steps[index]["conclusion"] = "success"
        self.assertEqual(self.rounds, 0)
        self.assertEqual(self.writes, [])

    def test_successful_scheduled_three_phase_export_is_already_published(self):
        self.exported_archive(legacy=True)
        self.run.update(event="schedule", actor={"login": "steipete"}, triggering_actor={"login": "steipete"})
        self.assertFalse(self.automatic())
        self.assertEqual(self.rounds, 0)
        self.assertEqual(self.writes, [])


class AutomaticAdmissionTests(unittest.TestCase):
    def test_scheduled_context_keeps_main_attempt_and_manual_authority_guards(self):
        env = {"GITHUB_REPOSITORY": checkpoint.PRODUCER, "GITHUB_REF": "refs/heads/main",
               "GITHUB_EVENT_NAME": "schedule", "GITHUB_SHA": "a" * 40, "GITHUB_WORKFLOW_SHA": "a" * 40,
               "GITHUB_RUN_ID": "4", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_ACTOR": "steipete",
               "GITHUB_TRIGGERING_ACTOR": "steipete", "GITHUB_WORKFLOW_REF": checkpoint.PRODUCER +
               "/.github/workflows/publish-discord-backup.yml@refs/heads/main"}
        def api(path):
            if path == "git/ref/heads/main":
                return {"object": {"sha": "a" * 40}}
            if "?status=" in path:
                return {"total_count": 0, "workflow_runs": []}
            return {"id": 10, "state": "active" if "publish-discord-backup" in path else "disabled_manually"}
        with patch.dict(os.environ, env), patch.object(admission, "api", api):
            admission.verify_automatic_context()
            for changes in ({"GITHUB_EVENT_NAME": "workflow_dispatch"}, {"GITHUB_EVENT_NAME": "push"},
                            {"GITHUB_REF": "refs/heads/other"}, {"GITHUB_REPOSITORY": "other/discrawl"},
                            {"GITHUB_RUN_ATTEMPT": "2"}, {"GITHUB_WORKFLOW_SHA": "b" * 40},
                            {"GITHUB_ACTOR": ""}, {"GITHUB_TRIGGERING_ACTOR": "vincentkoc"}):
                with patch.dict(os.environ, changes):
                    with self.assertRaises(checkpoint.CheckpointError):
                        admission.verify_automatic_context()
            with patch.dict(os.environ, {"GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_ACTOR": "vincentkoc",
                                         "GITHUB_TRIGGERING_ACTOR": "vincentkoc"}):
                admission.verify_automatic_context()
            with patch.dict(os.environ, {"GITHUB_SHA": "b" * 40, "GITHUB_WORKFLOW_SHA": "b" * 40}):
                with self.assertRaisesRegex(checkpoint.CheckpointError, "reconciliation_source_changed"):
                    admission.verify_automatic_context()

    def test_control_output_only_exists_after_verified_reconciliation(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            with patch.dict(os.environ, {"CHECKPOINT_MODE": "reconcile", "GITHUB_OUTPUT": str(output)}):
                for recovered in (False, True):
                    with patch.object(admission, "automatic_reconciliation", return_value=recovered):
                        admission.main()
                    self.assertEqual(output.read_text(), "recovered=" + str(recovered).lower() + "\n")
                    output.unlink()
                with patch.object(admission, "automatic_reconciliation", side_effect=checkpoint.CheckpointError("recovery_hold")):
                    with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_hold"):
                        admission.main()
                self.assertFalse(output.exists())

    def test_queued_successor_is_not_a_writer_but_another_running_owner_is(self):
        calls = []
        competing = False
        def api(path):
            calls.append(path)
            if path == "git/ref/heads/main":
                return {"object": {"sha": "a" * 40}}
            if "?status=" in path:
                rows = [{"id": 4}, {"id": 5}] if competing and "/10/" in path and "status=in_progress" in path else []
                return {"total_count": len(rows), "workflow_runs": rows}
            return {"id": 10 if "publish-discord-backup" in path else 11,
                    "state": "active" if "publish-discord-backup" in path else "disabled_manually"}
        env = {"GITHUB_REPOSITORY": checkpoint.PRODUCER, "GITHUB_REF": "refs/heads/main",
               "GITHUB_EVENT_NAME": "schedule", "GITHUB_SHA": "a" * 40, "GITHUB_WORKFLOW_SHA": "a" * 40,
               "GITHUB_RUN_ID": "4", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_ACTOR": "vincentkoc",
               "GITHUB_TRIGGERING_ACTOR": "vincentkoc", "GITHUB_WORKFLOW_REF": checkpoint.PRODUCER +
               "/.github/workflows/publish-discord-backup.yml@refs/heads/main"}
        with patch.dict(os.environ, env), patch.object(admission, "api", api):
            admission.verify_automatic_context()
            self.assertFalse(any("/10/" in call and "status=queued" in call for call in calls))
            self.assertFalse(any("/10/" in call and "status=pending" in call for call in calls))
            self.assertTrue(any("/11/" in call and "status=queued" in call for call in calls))
            competing = True
            with self.assertRaisesRegex(checkpoint.CheckpointError, "writer_active"):
                admission.verify_automatic_context()


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
        self.native = self.root / "discrawl-native"
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
        if suffix.startswith("/git/commits/"):
            revision = suffix.removeprefix("/git/commits/")
            parents = self.git("-C", str(self.remote), "rev-list", "--parents", "-n", "1", revision).split()[1:]
            return {"sha": revision, "parents": [{"sha": parent} for parent in parents],
                    "message": self.git("-C", str(self.remote), "log", "-1", "--format=%B", revision)}
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
        head = self.git("-C", str(self.remote), "rev-parse", "main")
        self.assertNotEqual(head, self.baseline)
        # The next scheduled read can recognize exact completed publication
        # without repeating the uncertain push or regenerating archive bytes.
        admission.exported_checkpoint(self.github, self.before[0], self.before[2], head)
        self.assertEqual(self.push_count, 1)


def load_tests(loader, _tests, _pattern):
    return unittest.TestSuite(loader.loadTestsFromTestCase(case) for case in (
        RecoveryTests, AutomaticRecoveryTests, SplitRecoveryTests, AutomaticAdmissionTests, OfflineCommandTests, PublicationTests))
