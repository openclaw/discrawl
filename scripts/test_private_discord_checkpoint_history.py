import base64
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import unittest
from unittest.mock import patch

import private_discord_checkpoint as checkpoint
import verify_discord_checkpoint_migration as admission
import test_private_discord_checkpoint as fixtures


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CheckpointTests(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db, self.path, self.github = self.fixture.db, self.fixture.path, self.fixture.github
        schema = (Path(__file__).parent.parent / "internal/store/sqlc/schema.sql").read_text()
        for table in ("channels", "sync_state", "failure_ledger"):
            self.db.execute("DROP TABLE " + table)
            self.db.executescript(re.search(r"create table " + table + r" \([^;]*;", schema, re.S)[0])
        for channel, parent, kind in (("100", None, "text"), ("101", "100", "thread_public")):
            self.db.execute("INSERT INTO channels(id,guild_id,parent_id,kind,name,raw_json,updated_at,collection_scope,scope_policy) "
                            "VALUES(?,?,?,?,?,'{}',?,'allowed','policy')",
                            (channel, "900", parent, kind, "private-channel-canary", "2026-01-02T00:00:00Z"))
        self.receipt = dict(guild_id="900", channel_id="101", channel_type=11, since="2026-01-01T00:00:00Z",
                            until="2026-01-02T00:00:00.000000001Z", completed_at="2026-01-02T00:00:01Z",
                            boundary="since", boundary_message_id="123456789012345678")
        self.db.execute("INSERT INTO sync_state(scope,cursor,updated_at) VALUES(?,?,?)",
                        ("channel:101:history_window:v1:" + self.receipt["since"], json.dumps(self.receipt),
                         "2026-01-02T00:00:01Z"))
        self.failure("101", None)
        self.failure("100", "2026-01-02T00:00:03Z")
        self.db.commit()
        with contextlib.redirect_stdout(io.StringIO()):
            self.fixture.seed()
            self.fixture.run_id = 2
            fixtures.save(self.github, self.path, self.fixture.receipt, "success")
        self.release = self.github.releases[self.github.latest]
        self.release["published_at"] = "2026-01-02T00:00:05Z"
        assets = self.release["assets"]
        asset = next(a for a in assets if a["name"] == checkpoint.MANIFEST)
        self.manifest_sha = checkpoint.digest(self.github.payloads[asset["id"]])
        self.inventory_sha = checkpoint.digest(checkpoint.canonical([
            {key: a[key] for key in ("id", "name", "size", "digest")} for a in sorted(assets, key=lambda a: a["name"])]))
        self.policy = dict(schema=1, policy=dict(repository="example/reports", revision="d" * 40,
                           path="policy.json", sha256="e" * 64),
                           catalog=dict(archive_commit="f" * 40, manifest_sha256="e" * 64), guild_id="900",
                           message_root_ids=["100"], forum_root_ids=[], retained_threads=[dict(id="101", parent_id="100")])
        self.policy_bytes = checkpoint.canonical(self.policy)
        hashes = [checkpoint.digest(b"101")]
        self.identity = dict(archive_revision="c" * 40, policy_revision="d" * 40,
                             policy_sha256=checkpoint.digest(self.policy_bytes), since=self.receipt["since"],
                             channel_sha256s=hashes, channel_set_sha256=checkpoint.digest(checkpoint.canonical(hashes)))
        self.run = dict(id=2, run_attempt=1, status="completed", conclusion="failure", event="workflow_dispatch",
                        head_branch="main", workflow_id=10, path=".github/workflows/publish-discord-backup.yml",
                        head_sha="b" * 40, actor={"login": "vincentkoc"}, triggering_actor={"login": "vincentkoc"})
        self.steps = [dict(name=name, status="completed", conclusion="success") for name in
                      ("Sync native Discord state", "Upload private ingestion checkpoint",
                       "Verify private ingestion checkpoint", "Promote verified private checkpoint", "Publish Discord archive")]
        self.job = dict(name="publish", run_id=2, head_sha="b" * 40, runner_id=1,
                        status="completed", conclusion="failure", steps=self.steps)
        self.downloads, self.scratch = [], []
        original_api, original_download = self.github.api, self.github.download
        self.github.api = lambda suffix, data=None, missing=False: self.private_api(original_api, suffix, data, missing)
        def download(asset, destination):
            self.downloads.append(asset["name"])
            self.scratch.append(destination.parent)
            original_download(asset, destination)
        self.github.download = download
        self.github.command = lambda *_args, **_kwargs: self.fail("mutation attempted")

    def failure(self, channel, resolved):
        self.db.execute("INSERT INTO failure_ledger(operation,source,guild_id,channel_id,error_class,error_message,"
                        "first_seen_at,last_seen_at,retry_count,resolved_at,resolution_reason) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        ("private-operation-canary", "private-source-canary", "900", channel, "discordgo.RESTError",
                         'private-error-canary HTTP 403 Forbidden {"code":50001}', "2026-01-02T00:00:00Z",
                         "2026-01-02T00:00:02Z", 4, resolved, "private-resolution-canary"))

    def private_api(self, original, suffix, data, missing):
        self.assertIsNone(data, "read-only diagnostic must never mutate")
        if suffix == "":
            return dict(id=123, full_name=self.github.repository, private=True, archived=False)
        if suffix == "/git/ref/heads/main":
            return {"object": {"sha": "c" * 40}}
        if suffix == "/git/ref/heads/maintainer-report-policy":
            return {"ref": checkpoint.POLICY_REF, "object": {"type": "commit", "sha": "d" * 40}}
        if suffix.startswith("/contents/" + checkpoint.POLICY_PATH):
            body = self.policy_bytes
            return dict(type="file", path=checkpoint.POLICY_PATH, size=len(body), encoding="base64",
                        content=base64.b64encode(body).decode())
        if suffix.startswith("/git/commits/"):
            return dict(sha="c" * 40, parents=[dict(sha="f" * 40)],
                        message="sync: discord archive\n\n" + admission.checkpoint_binding(
                            {"database": {"sha256": self.database_hash()}}, {"manifest_sha256": self.manifest_sha}))
        if suffix.startswith("/contents/"):
            body = b'{"files":{"producer":"producer.json"}}'
            if suffix.startswith("/contents/producer.json"):
                body = checkpoint.canonical(dict(schema=1, manifest_path="manifest.json", manifest_sha256=checkpoint.digest(body),
                    producer=dict(repository=checkpoint.PRODUCER, revision="b" * 40, run_id=2, run_attempt=1)))
            return dict(type="file", size=len(body), encoding="base64", content=base64.b64encode(body).decode())
        return original(suffix, missing=missing)

    def database_hash(self):
        asset = next(a for a in self.release["assets"] if a["name"] == checkpoint.MANIFEST)
        return json.loads(self.github.payloads[asset["id"]])["database"]["sha256"]

    def public_api(self, path):
        if path == "git/ref/heads/main":
            return {"object": {"sha": "a" * 40}}
        if path.endswith("/jobs?per_page=100"):
            return dict(total_count=1, jobs=[copy.deepcopy(self.job)])
        if path.startswith("actions/runs/"):
            return copy.deepcopy(self.run)
        return dict(id=10)

    def diagnose(self, identity=None, manifest=None, inventory=None):
        with patch.object(admission, "api", self.public_api), \
                patch.object(admission, "GitHub", return_value=self.github) as client, \
                patch.dict(os.environ, {"DISCRAWL_GUILD_ID": "900"}), \
                patch.object(admission, "verify_context") as context, contextlib.redirect_stdout(io.StringIO()) as out:
            try:
                admission.diagnose_history("a" * 40, 2, manifest or self.manifest_sha, inventory or self.inventory_sha,
                                           json.dumps(identity or self.identity))
            finally:
                self.assertTrue(all(not directory.exists() for directory in self.scratch))
                self.output = out.getvalue()
            client.assert_called_once_with(deadline=admission.DEADLINE, read_only=True)
            self.assertEqual(context.call_count, 2)
        return json.loads(self.output.splitlines()[-1])

    def test_full_restore_cloud_only_failure_and_private_projection(self):
        payloads = copy.deepcopy(self.github.payloads)
        result = self.diagnose()
        self.assertEqual(result["outcome"], "HISTORY_DIAGNOSTIC_COMPLETE")
        self.assertEqual([f["resolved"] for f in result["failures"]], [False, True])
        self.assertEqual(result["failures"][0]["http_codes"], [403])
        self.assertEqual(result["failures"][0]["discord_codes"], [50001])
        self.assertEqual(result["targets"][0]["receipt"]["until"], self.receipt["until"])
        self.assertFalse(result["absence_proves_non_attempt"])
        self.assertLessEqual(len(checkpoint.canonical(result)), 65536)
        for secret in ("private-channel-canary", "private-error-canary", "private-operation-canary", self.receipt["boundary_message_id"],
                       "private-source-canary", "private-resolution-canary"):
            self.assertNotIn(secret, self.output)
        self.assertEqual(self.github.payloads, payloads)
        self.assertEqual(self.github.deleted, [])
        self.assertTrue(any(name.endswith(".gz") for name in self.downloads))
        with patch.object(admission, "api", self.public_api):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "recovery_original_run"):
                admission.publisher_run(2, resume=True, published=True)

    def test_wrong_pins_and_policy_hash_set_reject_before_parts(self):
        cases = [(dict(self.identity, policy_sha256="0" * 64), None, None),
                 (dict(self.identity, archive_revision="0" * 40), None, None),
                 (dict(self.identity, channel_set_sha256="0" * 64), None, None),
                 (self.identity, "0" * 64, None), (self.identity, None, "0" * 64)]
        unknown = dict(self.identity, channel_sha256s=["0" * 64],
                       channel_set_sha256=checkpoint.digest(checkpoint.canonical(["0" * 64])))
        cases.append((unknown, None, None))
        for identity, manifest, inventory in cases:
            with self.subTest(identity=identity, manifest=manifest, inventory=inventory):
                self.downloads.clear()
                with self.assertRaises(checkpoint.CheckpointError):
                    self.diagnose(identity, manifest, inventory)
                self.assertFalse(any(name.endswith(".gz") for name in self.downloads))

    def test_changed_inventory_rejected_before_first_part(self):
        original = admission.download_database
        def changed(*args, **kwargs):
            self.release["assets"][-1]["id"] += 100
            return original(*args, **kwargs)
        with patch.object(admission, "download_database", changed), \
                self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_custody_changed"):
            self.diagnose()
        self.assertFalse(any(name.endswith(".gz") for name in self.downloads))

    def test_corrupt_checkpoint_never_reaches_projection(self):
        self.github.corrupt_download = True
        with patch.object(admission, "history_projection") as projection, self.assertRaises(checkpoint.CheckpointError):
            self.diagnose()
        projection.assert_not_called()
        self.assertNotIn("HISTORY_DIAGNOSTIC_COMPLETE", self.output)

    def test_failed_native_checkpoint_or_archive_never_downloads(self):
        for step in self.steps:
            with self.subTest(step=step["name"]):
                step["conclusion"] = "failure"
                with self.assertRaises(checkpoint.CheckpointError):
                    self.diagnose()
                self.assertEqual(self.downloads, [])
                step["conclusion"] = "success"

    def test_policy_drift_after_projection_suppresses_result(self):
        projection = admission.history_projection
        def changed(*args, **kwargs):
            result = projection(*args, **kwargs)
            self.policy_bytes += b"\n"
            return result
        with patch.object(admission, "history_projection", changed), self.assertRaises(checkpoint.CheckpointError):
            self.diagnose()
        self.assertNotIn("HISTORY_DIAGNOSTIC_COMPLETE", self.output)

    def project(self, deadline=None):
        return admission.history_projection(self.path, ["101"], "900", self.identity,
                                            deadline if deadline is not None else time.monotonic() + 5)

    def test_receipt_identity_boundary_and_nanosecond_order(self):
        for change in (dict(channel_id="102"), dict(guild_id="901"), dict(channel_type=True),
                       dict(since="2025-12-31T00:00:00Z"), dict(boundary="since", boundary_message_id=""),
                       dict(completed_at="2026-01-02T00:00:00.000000000Z"), dict(until="private-date-canary")):
            receipt = dict(self.receipt, **change)
            self.db.execute("UPDATE sync_state SET cursor=?", (json.dumps(receipt),))
            self.db.commit()
            with self.subTest(change=change), self.assertRaises((checkpoint.CheckpointError, ValueError)):
                self.project()

    def test_native_persisted_message_kinds_and_exact_numeric_spellings(self):
        kinds = {"text": 0, "voice": 2, "announcement": 5, "thread_announcement": 10,
                 "thread_public": 11, "thread_private": 12, "stage": 13}
        for kind, channel_type in [*kinds.items(), *((f"type_{n}", n) for n in kinds.values())]:
            with self.subTest(kind=kind):
                value = dict(self.receipt, channel_type=channel_type)
                self.assertEqual(admission.history_receipt(json.dumps(value), "101", "900", kind, self.receipt["since"])["until"],
                                 self.receipt["until"])
        for kind, channel_type in (("type_013", 13), ("type_+13", 13), ("type_14", 14),
                                   ("type_4", 4), ("type_11", 12), ("thread_public", 10)):
            with self.subTest(kind=kind, channel_type=channel_type), self.assertRaises(checkpoint.CheckpointError):
                admission.history_receipt(json.dumps(dict(self.receipt, channel_type=channel_type)),
                                          "101", "900", kind, self.receipt["since"])

    def test_missing_receipt_and_parent_are_explicit_not_inferred(self):
        self.db.execute("DELETE FROM sync_state")
        self.db.execute("DELETE FROM channels WHERE id='100'")
        self.db.commit()
        result = self.project()
        self.assertIsNone(result["targets"][0]["receipt"])
        self.assertTrue(any(node["missing"] for node in result["channels"]))
        self.assertFalse(result["absence_proves_non_attempt"])

    def test_readonly_transaction_rolls_back_and_closes_on_success_and_failure(self):
        connections = []
        class ObservedConnection(sqlite3.Connection):
            rolled_back = False
            closed = False
            def rollback(self):
                self.rolled_back = True
                return super().rollback()
            def close(self):
                self.closed = True
                return super().close()
        def observed(path):
            db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, factory=ObservedConnection)
            with self.assertRaisesRegex(sqlite3.OperationalError, "readonly"):
                db.execute("DELETE FROM channels")
            db.rollback()
            db.rolled_back = False
            connections.append(db)
            return db
        with patch.object(admission, "readonly", observed):
            self.project()
            with self.assertRaises(checkpoint.CheckpointError):
                self.project(time.monotonic() - 1)
        self.assertEqual(len(connections), 2)
        self.assertTrue(all(db.rolled_back and db.closed for db in connections))
        self.assertEqual(self.db.execute("SELECT count(*) FROM channels").fetchone()[0], 2)

    def test_cycle_row_output_and_deadline_bounds_close_readonly_database(self):
        before = self.path.read_bytes()
        with self.assertRaisesRegex(checkpoint.CheckpointError, "timeout"):
            self.project(time.monotonic() - 1)
        self.assertEqual(self.path.read_bytes(), before)
        self.db.execute("UPDATE channels SET parent_id='101' WHERE id='100'")
        self.db.commit()
        with self.assertRaisesRegex(checkpoint.CheckpointError, "history_ancestry_bound"):
            self.project()
        self.db.execute("UPDATE channels SET parent_id=NULL WHERE id='100'")
        self.db.execute("DELETE FROM failure_ledger")
        for n in range(1001):
            self.db.execute("INSERT INTO failure_ledger(operation,source,channel_id,error_class,error_message,first_seen_at,last_seen_at) "
                            "VALUES(?,'fixture','101','other','private-canary',?,?)",
                            (str(n), "2026-01-02T00:00:00Z", "2026-01-02T00:00:01Z"))
        self.db.commit()
        with self.assertRaisesRegex(checkpoint.CheckpointError, "history_ledger_row_bound"):
            self.project()
        self.db.execute("DELETE FROM failure_ledger WHERE failure_id > (SELECT min(failure_id)+199 FROM failure_ledger)")
        self.db.commit()
        with self.assertRaisesRegex(checkpoint.CheckpointError, "history_output_bound"):
            self.project()

    def test_history_success_cannot_authorize_finalization(self):
        jobs = [dict(id=20, name="diagnose", run_id=3, head_sha="c" * 40, runner_id=1,
                     status="completed", conclusion="success", steps=[
                         dict(name="Inspect retained draft and one existing part without changes", status="completed", conclusion="skipped"),
                         dict(name="Inspect published history and failure ledger without changes", status="completed", conclusion="success")]),
                dict(name="migrate", run_id=3, head_sha="c" * 40, runner_id=0,
                     status="completed", conclusion="skipped", steps=[])]
        with patch.object(admission, "migration_run", return_value=dict(head_sha="c" * 40)), \
                patch.object(admission, "api", return_value=dict(total_count=2, jobs=jobs)), \
                patch.object(admission, "public_read") as logs, \
                self.assertRaisesRegex(checkpoint.CheckpointError, "finalize_probe_step"):
            admission.successful_probe(3, "c" * 40, 2, self.manifest_sha, self.inventory_sha)
        logs.assert_not_called()


if __name__ == "__main__":
    unittest.main()
