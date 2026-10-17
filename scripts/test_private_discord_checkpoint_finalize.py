import contextlib
import copy
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import unittest
from unittest.mock import patch

import private_discord_checkpoint as checkpoint
import verify_discord_checkpoint_migration as admission
from test_private_discord_checkpoint import FakeGitHub
import test_private_discord_checkpoint_diagnostics as diagnostics


class FinalizeTests(unittest.TestCase):
    def setUp(self):
        diagnostics.DiagnosticTests.setUp(self)
        self.probe = {**self.run, "id": 2, "conclusion": "success", "head_sha": "c" * 40}
        self.step = {"name": "Inspect retained draft and one existing part without changes",
                     "status": "completed", "conclusion": "success", "started_at": "2026-01-01T00:00:00Z",
                     "completed_at": "2026-01-01T00:00:04Z"}
        self.jobs = [dict(id=20, name="diagnose", run_id=2, head_sha="c" * 40, runner_id=1,
                         status="completed", conclusion="success", steps=[self.step]),
                     dict(id=21, name="migrate", run_id=2, head_sha="c" * 40, runner_id=0,
                          runner_name=None, status="completed", conclusion="skipped", steps=[])]
        self.writes, self.rounds, self.directories = [], 0, []
        self.after_round = lambda: None
        self.patch_failure = None
        self.post_read_failure = False
        self.public_drift = None
        self.log_override = None
        self.github.api = self.private_api

    def private_api(self, suffix, data=None, missing=False):
        if data is not None:
            self.writes.append((suffix, copy.deepcopy(data)))
            if self.patch_failure == "before":
                raise checkpoint.CheckpointError("checkpoint_api phase=release_promote exit=timeout")
        if suffix == "":
            return {"full_name": self.github.repository, "id": 123, "private": True, "archived": False}
        result = FakeGitHub.api(self.github, suffix, data, missing)
        if data is not None:
            self.release["published_at"] = "2026-01-01T00:00:05Z"
            result["published_at"] = self.release["published_at"]
            if self.patch_failure == "after":
                raise checkpoint.CheckpointError("checkpoint_api phase=release_promote exit=timeout")
            if self.patch_failure == "malformed":
                return {}
        if data is None and self.writes and self.post_read_failure:
            raise checkpoint.CheckpointError("checkpoint_api phase=release_read exit=7")
        return result

    def api(self, path):
        if path == "actions/runs/2/attempts/1/jobs?per_page=100":
            return {"total_count": len(self.jobs), "jobs": copy.deepcopy(self.jobs)}
        if path.startswith("actions/runs/2"):
            return copy.deepcopy(self.probe)
        if self.public_drift and self.rounds == 2:
            if self.public_drift == "source" and path == "git/ref/heads/main":
                return {"object": {"sha": "d" * 40}}
            if self.public_drift == "writer" and path.startswith("actions/workflows/publish"):
                return {"id": 10, "state": "active"}
        return diagnostics.DiagnosticTests.api(self, path)

    def probe_log(self):
        pins = {"CHECKPOINT_MODE": "diagnose", "EXPECTED_SHA": "c" * 40, "ORIGINAL_RUN": "1",
                "MANIFEST_SHA256": self.manifest_sha, "INVENTORY_SHA256": self.inventory_sha,
                "MIGRATION_ACK": "DIAGNOSE_EXISTING_CHECKPOINT"}
        lines = [f"2026-01-01T00:00:00.1000000Z   {key}: {value}" for key, value in pins.items()]
        lines += ["2026-01-01T00:00:01.1000000Z checkpoint diagnostic: metadata verified",
                  "2026-01-01T00:00:02.1000000Z checkpoint diagnostic: first part verified",
                  "2026-01-01T00:00:04.9000000Z checkpoint diagnostic verified: read-only path; historical failure remains unclassified"]
        return ("\n".join(lines) + "\n").encode()

    def finalize(self):
        original = admission.download_database
        def download(github, release, directory, **options):
            self.rounds += 1
            if self.directories:
                self.assertFalse(self.directories[-1].exists(), "previous full restore survived into next round")
            self.directories.append(directory)
            result = original(github, release, directory, **options)
            self.after_round()
            return result
        with patch.object(admission, "api", self.api), patch.object(admission, "GitHub", return_value=self.github), \
                patch.object(admission, "public_read", return_value=self.log_override or self.probe_log()), \
                patch.object(admission, "download_database", download), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            admission.finalize("a" * 40, 1, self.manifest_sha, self.inventory_sha, 2, "c" * 40)
            return output.getvalue()

    def test_two_full_restores_then_one_patch_preserve_provenance_and_all_assets(self):
        before = copy.deepcopy(self.github.payloads)
        output = self.finalize()
        self.assertEqual(self.rounds, 2)
        self.assertEqual(self.writes, [("/releases/1", {"draft": False, "make_latest": "true"})])
        self.assertEqual(self.github.latest, 1)
        self.assertEqual(self.github.payloads, before)
        self.assertEqual(self.github.deleted, [])
        self.assertTrue(all(not path.exists() for path in self.directories))
        for part in (a["name"] for a in self.release["assets"] if a["name"] != checkpoint.MANIFEST):
            self.assertEqual(self.downloads.count(part), 2)
        self.assertIn("round=1 verified", output)
        self.assertIn("round=2 verified", output)
        self.assertNotIn(self.github.repository, output)

    def test_captured_ansi_log_reaches_exact_probe_input_and_marker_validation(self):
        prefix = b"2026-01-01T00:00:00.0000000Z \x1b[36;1mecho fixture\x1b[0m\n"
        executable = self.root / "gh"
        for body, accepted in ((prefix + self.probe_log(), True),
                               (prefix + self.probe_log().replace(b"ORIGINAL_RUN: 1", b"ORIGINAL_RUN: 3"), False)):
            executable.write_text("#!" + sys.executable + "\nimport os,sys\n"
                                  "assert sys.argv[1:]==['api','repos/openclaw/discrawl/actions/jobs/20/logs',"
                                  "'--method','GET','--include','--allow-escape-sequences']\n"
                                  "os.write(1,b'HTTP/2.0 200 OK\\n\\n'+%r)\n" % body)
            executable.chmod(0o700)
            with patch.dict(os.environ, {"PATH": str(self.root), "GH_TOKEN": "public-fixture"}), \
                    patch.object(admission, "api", self.api), patch.object(admission, "DEADLINE", time.monotonic() + 5), \
                    contextlib.redirect_stdout(io.StringIO()) as out, contextlib.redirect_stderr(io.StringIO()) as err:
                if accepted:
                    self.assertEqual(admission.successful_probe(2, "c" * 40, 1, self.manifest_sha, self.inventory_sha), 20)
                else:
                    with self.assertRaisesRegex(checkpoint.CheckpointError, "finalize_probe_inputs"):
                        admission.successful_probe(2, "c" * 40, 1, self.manifest_sha, self.inventory_sha)
            self.assertEqual(out.getvalue() + err.getvalue(), "")

    def test_each_corrupt_round_holds_draft_without_write(self):
        download = self.github.download
        for corrupt_round in (1, 2):
            with self.subTest(round=corrupt_round):
                self.rounds, self.directories = 0, []
                def corrupt(asset, target):
                    download(asset, target)
                    if self.rounds == corrupt_round and asset["name"] != checkpoint.MANIFEST:
                        target.write_bytes(target.read_bytes()[:-1])
                self.github.download = corrupt
                with self.assertRaisesRegex(checkpoint.CheckpointError, "part_checksum"):
                    self.finalize()
                self.assertEqual(self.writes, [])
                self.assertTrue(self.release["draft"])

    def test_fully_hash_bound_wrong_database_facts_fail_parity(self):
        asset = next(a for a in self.release["assets"] if a["name"] == checkpoint.MANIFEST)
        original = self.github.payloads[asset["id"]]
        for field in ("sha256", "schema_sha256", "tables"):
            with self.subTest(field=field):
                self.rounds, self.directories = 0, []
                value = json.loads(original)
                if field == "tables":
                    value["database"][field]["messages"] += 1
                else:
                    value["database"][field] = "0" * 64
                body = checkpoint.canonical(value)
                self.github.payloads[asset["id"]] = body
                asset.update(size=len(body), digest="sha256:" + checkpoint.digest(body))
                self.manifest_sha = checkpoint.digest(body)
                self.inventory_sha = checkpoint.digest(checkpoint.canonical([
                    {key: a[key] for key in ("id", "name", "size", "digest")}
                    for a in sorted(self.release["assets"], key=lambda a: a["name"])]))
                with self.assertRaisesRegex(checkpoint.CheckpointError, "database_checksum|restore_parity"):
                    self.finalize()
                self.assertEqual(self.writes, [])

    def test_probe_run_jobs_inputs_and_executed_markers_are_required(self):
        mutations = [lambda: self.probe.update(run_attempt=2),
                     lambda: self.probe.update(actor={"login": "other"}),
                     lambda: self.probe.update(head_sha="d" * 40),
                     lambda: self.jobs[1].update(runner_id=1),
                     lambda: self.step.update(conclusion="failure")]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                before = copy.deepcopy((self.probe, self.jobs, self.step))
                mutate()
                with self.assertRaises(checkpoint.CheckpointError):
                    self.finalize()
                self.probe, self.jobs, self.step = before
                self.step = self.jobs[0]["steps"][0]
        body = self.probe_log()
        invalid = [body.replace(b"ORIGINAL_RUN: 1", b"ORIGINAL_RUN: 3"),
                   body.replace(b"CHECKPOINT_MODE: diagnose", b"CHECKPOINT_MODE: migrate"),
                   body.replace(b"2026-01-01T00:00:01.1000000Z checkpoint diagnostic", b"echo checkpoint diagnostic"),
                   body.replace(b"00:00:04.9000000Z checkpoint", b"00:00:06.0000000Z checkpoint"),
                   body + body.splitlines()[-1] + b"\n"]
        for log in invalid:
            self.log_override = log
            with self.assertRaises(checkpoint.CheckpointError):
                self.finalize()
        self.assertEqual(self.rounds, 0)
        self.assertEqual(self.writes, [])

    def test_changed_asset_id_after_restore_prevents_promotion(self):
        def drift():
            if self.rounds == 2:
                asset = self.release["assets"][-1]
                self.github.payloads[asset["id"] + 100] = self.github.payloads[asset["id"]]
                asset["id"] += 100
        self.after_round = drift
        with self.assertRaisesRegex(checkpoint.CheckpointError, "diagnostic_evidence_changed"):
            self.finalize()
        self.assertEqual(self.writes, [])

    def test_source_writer_or_latest_drift_after_full_reads_prevents_write(self):
        for drift in ("source", "writer", "latest"):
            self.rounds, self.directories = 0, []
            self.public_drift = drift
            self.after_round = lambda: setattr(self.github, "latest", 1) if self.rounds == 2 and drift == "latest" else None
            with self.assertRaises(checkpoint.CheckpointError):
                self.finalize()
            self.assertEqual(self.writes, [])

    def test_space_deadline_and_unknown_reap_stop_before_write(self):
        with patch.object(checkpoint.shutil, "disk_usage", return_value=type("Space", (), {"free": 1})()):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_capacity"):
                self.finalize()
        self.rounds, self.directories = 0, []
        def expired():
            admission.DEADLINE = time.monotonic() - 1
        self.after_round = expired
        with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_timeout"):
            self.finalize()
        self.rounds, self.directories = 0, []
        self.after_round = lambda: None
        with patch.object(self.github, "download", side_effect=checkpoint.CheckpointError("checkpoint_api phase=part_download exit=reap")):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "exit=reap"):
                self.finalize()
        self.assertEqual(self.writes, [])

    def test_lost_patch_response_always_holds_without_retry(self):
        for failure, state in (("before", "unchanged_draft"), ("after", "published_latest"), ("malformed", "published_latest")):
            with self.subTest(failure=failure):
                self.rounds, self.directories, self.writes = 0, [], []
                self.release.update(draft=True, published_at=None)
                self.github.latest = None
                self.patch_failure = failure
                with self.assertRaisesRegex(checkpoint.CheckpointError, "finalize_hold state=" + state):
                    self.finalize()
                self.assertEqual(len(self.writes), 1)
                self.assertEqual(self.github.deleted, [])

    def test_postpublication_read_failure_holds_without_rollback(self):
        self.post_read_failure = True
        with self.assertRaisesRegex(checkpoint.CheckpointError, "finalize_hold state=unknown"):
            self.finalize()
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.github.latest, 1)
        self.assertFalse(self.release["draft"])
        self.assertEqual(self.github.deleted, [])


class DeadlineTests(unittest.TestCase):
    def test_sqlite_progress_deadline_interrupts_full_check_and_clears_handler(self):
        with contextlib.closing(sqlite3.connect(":memory:")) as db:
            for name in checkpoint.REQUIRED_TABLES:
                db.execute('CREATE TABLE "' + name + '"(value)')
            db.executemany("INSERT INTO messages VALUES(?)", ((i,) for i in range(50000)))
            calls = 0
            def clock():
                nonlocal calls
                calls += 1
                return 0 if calls < 4 else 2
            with patch.object(checkpoint.time, "monotonic", clock):
                with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_timeout"):
                    checkpoint.database_facts(db, deadline=1)
            self.assertEqual(checkpoint.database_facts(db)["tables"]["messages"], 50000)

    def test_hash_and_unpack_refuse_expired_deadline(self):
        with contextlib.ExitStack() as stack:
            import tempfile
            root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            source = root / "source"
            source.write_bytes(b"state")
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_timeout"):
                checkpoint.file_digest(source, time.monotonic() - 1)
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_timeout"):
                checkpoint.unpack({}, root, root / "output", time.monotonic() - 1)
            self.assertFalse((root / "output").exists())


if __name__ == "__main__":
    unittest.main()
