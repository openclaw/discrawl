import copy
import contextlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("private_discord_checkpoint.py")
SPEC = importlib.util.spec_from_file_location("checkpoint", SCRIPT)
checkpoint = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checkpoint)
CACHE = {"id": 1, "key": "discrawl-discord-db-Linux-main-1-1", "ref": "refs/heads/main",
         "version": "a" * 64, "created_at": "2026-01-01T00:00:00.000Z", "size_in_bytes": 1024}


def native_database(path):
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA wal_autocheckpoint=0")
    db.execute("PRAGMA user_version=6")
    for table in checkpoint.REQUIRED_TABLES:
        db.execute(f'CREATE TABLE "{table}"(id TEXT PRIMARY KEY, value TEXT)')
        db.execute(f'INSERT INTO "{table}" VALUES(?,?)', (table, "retained state"))
    db.execute("CREATE VIRTUAL TABLE message_fts USING fts5(content)")
    db.execute("INSERT INTO message_fts VALUES('searchable retained work')")
    db.execute("CREATE TABLE future_native_state(id INTEGER PRIMARY KEY, value BLOB)")
    db.execute("INSERT INTO future_native_state VALUES(1,x'00ff11')")
    db.commit()
    return db


class FakeGitHub:
    repository = "example/private-archive"
    identity = {"name": repository, "id": 123}
    base = "repos/" + repository

    def __init__(self):
        self.releases = {}
        self.payloads = {}
        self.latest = None
        self.next_id = 1
        self.deleted = []
        self.fail_upload = False
        self.fail_delete = False
        self.corrupt_download = False
        self.before_promote = None

    def api(self, suffix, data=None, missing=False):
        if suffix.startswith("/releases?"):
            return copy.deepcopy(list(self.releases.values()))
        if suffix == "/releases/latest":
            if self.latest is None:
                if missing:
                    return None
                raise checkpoint.CheckpointError("checkpoint_api")
            return copy.deepcopy(self.releases[self.latest])
        if suffix == "/git/ref/heads/main":
            return {"object": {"sha": "f" * 40}}
        pieces = suffix.split("/")
        release_id = int(pieces[2])
        if release_id not in self.releases:
            if missing:
                return None
            raise checkpoint.CheckpointError("checkpoint_api")
        release = self.releases[release_id]
        if len(pieces) > 3:
            return copy.deepcopy(release["assets"])
        if data:
            if self.before_promote:
                self.before_promote()
            release.update(data)
            if data.get("make_latest") == "true":
                self.latest = release_id
        return copy.deepcopy(release)

    def create(self, tag, target):
        if any(r["tag_name"] == tag for r in self.releases.values()):
            raise checkpoint.CheckpointError("checkpoint_api")
        release = {"id": self.next_id, "tag_name": tag, "target_commitish": target,
                   "draft": True, "prerelease": False, "assets": []}
        self.next_id += 1
        self.releases[release["id"]] = release
        return copy.deepcopy(release)

    def command(self, args, **_options):
        if args[:2] == ["release", "upload"]:
            if self.fail_upload:
                raise checkpoint.CheckpointError("checkpoint_api")
            release = next(r for r in self.releases.values() if r["tag_name"] == args[2])
            for path in map(Path, args[5:]):
                asset_id = self.next_id
                self.next_id += 1
                raw = path.read_bytes()
                self.payloads[asset_id] = raw
                release["assets"].append({"id": asset_id, "name": path.name, "size": len(raw), "state": "uploaded",
                                          "digest": "sha256:" + checkpoint.digest(raw)})
            return b""
        self.assert_delete(args)
        if self.fail_delete:
            raise checkpoint.CheckpointError("checkpoint_api")
        release_id = int(args[1].split("/")[-1])
        self.deleted.append(release_id)
        del self.releases[release_id]
        return b""

    @staticmethod
    def assert_delete(args):
        assert args[0] == "api" and args[-2:] == ["--method", "DELETE"]

    def download(self, asset, destination):
        body = self.payloads[asset["id"]]
        if self.corrupt_download and asset["name"] != checkpoint.MANIFEST:
            body = body[:-1] + bytes([body[-1] ^ 1])
        destination.write_bytes(body)


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "native.sqlite"
        self.db = native_database(self.path)
        self.addCleanup(self.db.close)
        self.github = FakeGitHub()
        self.run_id = 1
        self.identity_patch = patch.object(checkpoint, "source_identity", self.identity)
        self.identity_patch.start()
        self.addCleanup(self.identity_patch.stop)
        self.receipt = self.root / "parent.json"

    def identity(self, outcome, archive_commit):
        return {"repository": checkpoint.PRODUCER, "revision": "b" * 40, "run_id": self.run_id,
                "run_attempt": 1, "workflow": "migrate-discord-checkpoint.yml" if outcome == "migration" else "publish-discord-backup.yml",
                "outcome": outcome, "archive_commit": archive_commit}

    def seed(self):
        ref = checkpoint.save(self.github, self.path, self.receipt, "migration", CACHE)
        self.restore_parent()
        return ref

    def restore_parent(self, prune=False):
        target = self.root / f"restored-{self.run_id}.sqlite"
        checkpoint.restore(self.github, target, self.receipt, prune)
        target.unlink()
        self.run_id += 1

    def test_consistent_wal_backup_preserves_every_native_table_without_source_mutation(self):
        before = {p.name: p.read_bytes() for p in self.root.glob("native.sqlite*") if not p.name.endswith("-shm")}
        target = self.root / "copy.sqlite"
        facts = checkpoint.snapshot(self.path, target)
        with contextlib.closing(checkpoint.readonly(target)) as copy_db:
            self.assertEqual(checkpoint.database_facts(copy_db), facts)
            self.assertEqual(copy_db.execute("SELECT value FROM future_native_state").fetchone(), (b"\x00\xff\x11",))
            self.assertEqual(copy_db.execute("SELECT count(*) FROM message_fts WHERE message_fts MATCH 'retained'").fetchone(), (1,))
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.root.glob("native.sqlite*") if not p.name.endswith("-shm")})

    def test_backup_metadata_and_pages_use_same_pinned_read_snapshot(self):
        original = checkpoint.database_facts
        changed = False
        def facts(db):
            nonlocal changed
            result = original(db)
            if not changed:
                changed = True
                self.db.execute("INSERT INTO messages VALUES('later','after snapshot')")
                self.db.commit()
            return result
        target = self.root / "copy.sqlite"
        with patch.object(checkpoint, "database_facts", facts):
            result = checkpoint.snapshot(self.path, target)
        self.assertEqual(result["tables"]["messages"], 1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM messages").fetchone(), (2,))

    def test_backup_deadline_aborts_progress(self):
        with self.assertRaisesRegex(checkpoint.CheckpointError, "backup_timeout"):
            checkpoint.snapshot(self.path, self.root / "copy.sqlite", timeout=-1)

    def test_busy_backup_has_a_deadline(self):
        target = self.root / "locked.sqlite"
        with contextlib.closing(sqlite3.connect(target)) as blocker:
            blocker.execute("CREATE TABLE blocker(id)")
            blocker.execute("BEGIN EXCLUSIVE")
            with self.assertRaisesRegex(checkpoint.CheckpointError, "backup_timeout"):
                checkpoint.snapshot(self.path, target, timeout=0.01)
            blocker.rollback()

    def test_capacity_uses_committed_wal_pages_not_main_file_size(self):
        self.db.execute("INSERT INTO future_native_state VALUES(2,zeroblob(4000000))")
        self.db.commit()
        self.assertLess(self.path.stat().st_size, 100000)
        free = 4_000_000 + checkpoint.BLOCK * checkpoint.MAX_PARTS
        with patch.object(checkpoint.shutil, "disk_usage", return_value=SimpleNamespace(free=free)):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_capacity"):
                checkpoint.snapshot(self.path, self.root / "copy.sqlite")
        self.assertFalse((self.root / "copy.sqlite").exists())

    def test_private_round_trip_is_byte_exact_and_retains_only_current_and_parent(self):
        first = self.seed()
        second = checkpoint.save(self.github, self.path, self.receipt, "success")
        self.restore_parent()
        third = checkpoint.save(self.github, self.path, self.receipt, "failure")
        self.assertEqual(self.github.latest, third["release_id"])
        self.assertEqual(set(self.github.releases), {second["release_id"], third["release_id"]})
        self.assertEqual(self.github.deleted, [first["release_id"]])

    def test_failed_upload_preserves_latest_and_next_run_cannot_accumulate_drafts(self):
        first = self.seed()
        self.github.fail_upload = True
        with self.assertRaises(checkpoint.CheckpointError):
            checkpoint.save(self.github, self.path, self.receipt, "failure")
        self.github.fail_upload = False
        self.run_id += 1
        with self.assertRaisesRegex(checkpoint.CheckpointError, "pending_draft"):
            checkpoint.save(self.github, self.path, self.receipt, "success")
        self.assertEqual(len(self.github.releases), 2)
        self.assertEqual(self.github.latest, first["release_id"])

    def test_corrupt_authenticated_readback_never_promotes(self):
        first = self.seed()
        self.github.corrupt_download = True
        with self.assertRaisesRegex(checkpoint.CheckpointError, "part_checksum"):
            checkpoint.save(self.github, self.path, self.receipt, "success")
        self.assertEqual(self.github.latest, first["release_id"])

    def test_restore_failure_never_installs_a_database(self):
        self.seed()
        target = self.root / "never-installed.sqlite"
        self.github.corrupt_download = True
        with self.assertRaises(checkpoint.CheckpointError):
            checkpoint.restore(self.github, target, self.receipt)
        self.assertFalse(target.exists())

    def test_missing_checkpoint_does_not_create_or_import(self):
        target = self.root / "missing.sqlite"
        with self.assertRaises(checkpoint.CheckpointError):
            checkpoint.restore(self.github, target, self.receipt)
        self.assertFalse(target.exists())

    def test_parent_asset_replacement_is_rejected_even_with_identical_bytes(self):
        self.seed()
        asset = self.github.releases[self.github.latest]["assets"][1]
        self.github.payloads[9999] = self.github.payloads[asset["id"]]
        asset["id"] = 9999
        with self.assertRaisesRegex(checkpoint.CheckpointError, "parent_changed"):
            checkpoint.save(self.github, self.path, self.receipt, "success")

    def test_retention_failure_is_retried_before_next_ingestion(self):
        first = self.seed()
        checkpoint.save(self.github, self.path, self.receipt, "success")
        self.restore_parent()
        self.github.fail_delete = True
        with self.assertRaises(checkpoint.CheckpointError):
            checkpoint.save(self.github, self.path, self.receipt, "success")
        self.assertEqual(len(self.github.releases), 3)
        self.github.fail_delete = False
        self.restore_parent(prune=True)
        self.assertNotIn(first["release_id"], self.github.releases)

    def test_manifest_rejects_extra_fields_traversal_duplicate_and_unbounded_parts(self):
        self.seed()
        saved = json.loads(self.receipt.read_bytes())["manifest"]
        for mutate in [lambda x: x.update(extra=True),
                       lambda x: x["parts"][0].update(name="../escape"),
                       lambda x: x["parts"][0].update(bytes=checkpoint.MAX_PART + 1),
                       lambda x: x.update(parts=x["parts"] * 65)]:
            value = copy.deepcopy(saved)
            mutate(value)
            with self.assertRaises(checkpoint.CheckpointError):
                checkpoint.validate_manifest(value)
        with self.assertRaisesRegex(checkpoint.CheckpointError, "duplicate"):
            checkpoint.strict_json(b'{"id":1,"id":2}')

    def test_report_requires_exact_successful_published_producer(self):
        self.seed()
        checkpoint.save(self.github, self.path, self.receipt, "success")
        self.restore_parent()
        source = json.loads(self.receipt.read_bytes())["manifest"]["source"]
        archive = self.root / "archive"
        archive.mkdir()
        manifest = checkpoint.canonical({"files": {"producer": "producer.json"}})
        (archive / "manifest.json").write_bytes(manifest)
        receipt = {"schema": 1, "manifest_path": "manifest.json", "manifest_sha256": checkpoint.digest(manifest),
                   "producer": {k: source[k] for k in ("repository", "revision", "run_id", "run_attempt")}}
        (archive / "producer.json").write_bytes(checkpoint.canonical(receipt))
        self.assertTrue(checkpoint.report_ready(self.receipt, archive))
        receipt["producer"]["run_id"] += 1
        (archive / "producer.json").write_bytes(checkpoint.canonical(receipt))
        self.assertFalse(checkpoint.report_ready(self.receipt, archive))
        receipt["manifest_sha256"] = "0" * 64
        (archive / "producer.json").write_bytes(checkpoint.canonical(receipt))
        with self.assertRaisesRegex(checkpoint.CheckpointError, "publication_receipt"):
            checkpoint.report_ready(self.receipt, archive)

    def test_writer_timeout_reaps_child_before_snapshot_and_preserves_committed_progress(self):
        marker = self.root / "writer.pid"
        code = "import os,sqlite3,time;from pathlib import Path;d=sqlite3.connect(%r);d.execute(\"INSERT INTO messages VALUES('committed','partial')\");d.commit();Path(%r).write_text(str(os.getpid()));time.sleep(30)" % (str(self.path), str(marker))
        completion = self.root / "writer.json"
        self.assertNotEqual(checkpoint.run_writer([sys.executable, "-c", code], 1, completion), 0)
        self.assertEqual(json.loads(completion.read_bytes()), {"reaped": True, "outcome": "cancelled"})
        pid = int(marker.read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        facts = checkpoint.snapshot(self.path, self.root / "after-failure.sqlite")
        self.assertEqual(facts["tables"]["messages"], 2)

    def test_cli_parses_options_and_never_prints_private_child_output(self):
        result = subprocess.run([sys.executable, str(SCRIPT), "run", "--timeout", "2", "--", sys.executable,
                                 "-c", "print('private data');raise SystemExit(9)"], capture_output=True)
        self.assertEqual(result.returncode, 9)
        self.assertNotIn(b"private data", result.stdout + result.stderr)

    def test_unknown_descendant_reap_never_authorizes_snapshot(self):
        completion = self.root / "writer.json"
        real_kill = os.killpg
        def group(pid, sig):
            if sig == 0:
                return None
            return real_kill(pid, sig)
        with patch.object(checkpoint.os, "killpg", group), patch.object(checkpoint.time, "monotonic", side_effect=[0, 0, 10, 20]):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "writer_reap"):
                checkpoint.run_writer([sys.executable, "-c", "pass"], 1, completion)
        self.assertFalse(completion.exists())

    def test_leader_exit_does_not_leave_a_writing_descendant(self):
        marker = self.root / "heartbeat"
        completion = self.root / "writer.json"
        code = """import os,time
from pathlib import Path
p=Path(%r)
if os.fork()==0:
 while True:
  p.write_text(str(time.monotonic_ns()));time.sleep(.01)
else:
 while not p.exists(): time.sleep(.01)
""" % str(marker)
        try:
            checkpoint.run_writer([sys.executable, "-c", code], 2, completion)
        except checkpoint.CheckpointError as error:
            self.assertEqual(str(error), "writer_reap")
            self.assertFalse(completion.exists(), "unknown group state cannot authorize a checkpoint")
        value = marker.read_bytes()
        time.sleep(0.1)
        self.assertEqual(marker.read_bytes(), value)


class AdmissionTests(unittest.TestCase):
    def test_private_repository_identity_requires_no_admin_permission(self):
        expected = {"full_name": "example/private-archive", "private": True, "archived": False, "id": 123}
        with patch.dict(os.environ, {"DISCRAWL_BACKUP_REPOSITORY": expected["full_name"]}):
            for changed in ({"private": False}, {"full_name": "example/reassigned"}, {"archived": True}):
                with patch.object(checkpoint.GitHub, "api", return_value={**expected, **changed}):
                    with self.assertRaisesRegex(checkpoint.CheckpointError, "private_repository"):
                        checkpoint.GitHub()
            with patch.object(checkpoint.GitHub, "api", return_value=expected) as api:
                self.assertEqual(checkpoint.GitHub().identity, {"name": expected["full_name"], "id": 123})
                api.assert_called_once_with("")

    def test_execution_context_rejects_forks_refs_debug_and_source_drift(self):
        env = {"GITHUB_REPOSITORY": checkpoint.PRODUCER, "GITHUB_REF": "refs/heads/main",
               "GITHUB_EVENT_NAME": "schedule", "GITHUB_SHA": "a" * 40, "GITHUB_WORKFLOW_SHA": "a" * 40,
               "GITHUB_WORKFLOW_REF": checkpoint.PRODUCER + "/.github/workflows/publish-discord-backup.yml@refs/heads/main"}
        for changed in ({"GITHUB_REPOSITORY": "fork/discrawl"}, {"GITHUB_REF": "refs/pull/1/merge"},
                        {"RUNNER_DEBUG": "1"}, {"GITHUB_WORKFLOW_SHA": "b" * 40}):
            with patch.dict(os.environ, {**env, **changed}, clear=True):
                with self.assertRaisesRegex(checkpoint.CheckpointError, "execution_context"):
                    checkpoint.execution_context()

    def test_raw_api_failure_is_suppressed(self):
        with tempfile.TemporaryDirectory() as temp:
            shim = Path(temp) / "gh"
            shim.write_text("#!/bin/sh\necho 'private API sentinel' >&2\nexit 1\n")
            shim.chmod(0o700)
            env = {**os.environ, "PATH": temp + os.pathsep + os.environ["PATH"],
                   "DISCRAWL_BACKUP_REPOSITORY": "example/private-archive", "DISCORD_BACKUP_TOKEN": "fixture-token",
                   "GITHUB_REPOSITORY": checkpoint.PRODUCER, "GITHUB_REF": "refs/heads/main",
                   "GITHUB_EVENT_NAME": "schedule", "GITHUB_SHA": "a" * 40, "GITHUB_WORKFLOW_SHA": "a" * 40,
                   "GITHUB_WORKFLOW_REF": checkpoint.PRODUCER + "/.github/workflows/publish-discord-backup.yml@refs/heads/main"}
            result = subprocess.run([sys.executable, str(SCRIPT), "restore", "--db", temp + "/state.sqlite",
                                     "--receipt", temp + "/receipt.json"], capture_output=True, env=env)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(result.stderr, b"checkpoint failed: checkpoint_api phase=repository_read exit=1\n")
            self.assertNotIn(b"private API sentinel", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
