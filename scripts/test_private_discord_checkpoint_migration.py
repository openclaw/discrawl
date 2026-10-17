import copy
import os
import unittest
from unittest.mock import patch

import verify_discord_checkpoint_migration as migration
from private_discord_checkpoint import CheckpointError


class MigrationAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.cache = {"id": 1, "key": "discrawl-discord-db-Linux-main-10-1", "ref": "refs/heads/main",
                      "version": "a" * 64, "created_at": "2026-01-01T00:00:00.000Z", "size_in_bytes": 1024}
        self.run = {"status": "completed", "conclusion": "success", "head_sha": "b" * 40,
                    "head_branch": "main", "path": ".github/workflows/publish-discord-backup.yml", "run_attempt": 1}
        self.state = "disabled_manually"
        self.active = 0
        self.latest = 1
        self.env = patch.dict(os.environ, {"GITHUB_SHA": "c" * 40, "GITHUB_EVENT_NAME": "workflow_dispatch",
                                          "GITHUB_RUN_ATTEMPT": "1", "GITHUB_ACTOR": "vincentkoc",
                                          "GITHUB_TRIGGERING_ACTOR": "vincentkoc"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def api(self, path):
        if path == "git/ref/heads/main":
            return {"object": {"sha": "c" * 40}}
        if path.startswith("actions/caches?"):
            return {"total_count": 1, "actions_caches": [{**self.cache, "id": self.latest}]}
        if path.startswith("actions/runs/"):
            return copy.deepcopy(self.run)
        if "/runs?" in path:
            return {"total_count": self.active, "workflow_runs": []}
        return {"id": 99, "state": self.state}

    def verify(self, cache=None):
        with patch.object(migration, "api", self.api), patch.object(migration, "execution_context", return_value="migrate-discord-checkpoint.yml"):
            return migration.verify(cache or self.cache, "c" * 40, "b" * 40)

    def test_exact_successful_source_and_quiescent_cache_admitted(self):
        self.assertEqual(self.verify(), self.cache)

    def test_failed_source_active_writer_or_changed_cache_blocks(self):
        self.run["conclusion"] = "failure"
        with self.assertRaisesRegex(CheckpointError, "migration_producer"):
            self.verify()
        self.run["conclusion"] = "success"
        self.state = "active"
        with self.assertRaisesRegex(CheckpointError, "migration_writer_enabled"):
            self.verify()
        self.state = "disabled_manually"
        self.active = 1
        with self.assertRaisesRegex(CheckpointError, "migration_writer_active"):
            self.verify()
        self.active = 0
        with self.assertRaisesRegex(CheckpointError, "migration_cache_changed"):
            self.verify({**self.cache, "version": "d" * 64})

    def test_rerun_or_different_actor_cannot_migrate(self):
        for changed in ({"GITHUB_RUN_ATTEMPT": "2"}, {"GITHUB_TRIGGERING_ACTOR": "other"}):
            with patch.dict(os.environ, changed):
                with self.assertRaisesRegex(CheckpointError, "migration_context"):
                    self.verify()
