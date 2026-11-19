"""Selected service consumes a fixed owner roster and proves native completion."""
import contextlib
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import private_discord_checkpoint as checkpoint


class SelectedTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "native.sqlite"
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            db.executescript("CREATE TABLE channels(id TEXT,guild_id TEXT,kind TEXT,collection_scope TEXT,deleted_at TEXT);"
                             "CREATE TABLE sync_state(scope TEXT,cursor TEXT);")
            db.executemany("INSERT INTO channels VALUES(?,'900','text','allowed',NULL)", [("100",), ("101",)])
            db.commit()
        self.config = self.root / "acquisition.toml"
        self.config.write_text('db_path = ' + json.dumps(str(self.db)))
        selection = dict(guild_id="900", selected_ids=["100", "101", "102"], unavailable_ids=["102"], channel_ids=["100", "101"])
        self.manifest = dict(schema="discrawl-selected-history/v1", **selection,
                             selection_sha256=hashlib.sha256(json.dumps(selection, separators=(",", ":")).encode()).hexdigest(),
                             captured_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                             source=dict(snapshot_id="fixture", manifest_sha256="a" * 64, channels_sha256="b" * 64,
                                         checkpoints_sha256="c" * 64, projection_revision=5))
        self.file = self.root / "selected.json"
        self.native = self.root / "native"
        self.native.write_text("#!/usr/bin/env python3\n"
            "import sys,json,sqlite3,datetime,pathlib\n"
            "a=sys.argv[1:]; root=pathlib.Path(__file__).parent\n"
            "(root/'args.json').write_text(json.dumps(a))\n"
            "since=a[a.index('--since')+1]; now=datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00','Z')\n"
            "fault=(root/'fault').read_text() if (root/'fault').exists() else ''\n"
            "with sqlite3.connect(root/'native.sqlite') as db:\n"
            " for channel in a[a.index('--channels')+1].split(','):\n"
            "  if (root/'omit').exists() and channel=='101': continue\n"
            "  receipt=dict(guild_id='900',channel_id=channel,channel_type=0,since=since,until=now,completed_at=now,boundary='end')\n"
            "  if fault=='missing_type': receipt.pop('channel_type')\n"
            "  if fault=='wrong_type': receipt['channel_type']=11\n"
            "  if fault=='since_boundary': receipt['boundary']='since'\n"
            "  if fault=='future_completion': receipt['completed_at']='2099-01-01T00:00:00Z'\n"
            "  if fault=='changed_kind': db.execute(\"UPDATE channels SET kind='thread_public' WHERE id=?\",(channel,))\n"
            "  if fault=='unavailable': db.execute('INSERT INTO sync_state VALUES(?,?)',('channel:'+channel+':unavailable','{}'))\n"
            "  db.execute('INSERT INTO sync_state VALUES(?,?)',('channel:'+channel+':history_window:v1:'+since,json.dumps(receipt)))\n")
        self.native.chmod(0o700)

    def invoke(self):
        self.file.write_text(json.dumps(self.manifest))
        self.file.chmod(0o600)
        with contextlib.redirect_stdout(io.StringIO()):
            return checkpoint.selected_history(self.native, self.config, self.file, "900", 10)

    def test_real_supervised_process_exact_roster_and_utc_window(self):
        self.assertEqual(self.invoke(), 0)
        args = json.loads((self.root / "args.json").read_text())
        self.assertEqual(args[args.index("--channels") + 1], "100,101")
        self.assertEqual(args[args.index("--since") + 1], checkpoint.acquisition_since())
        self.assertNotIn("--include-stored-threads", args)
        self.assertNotIn("--latest-only", args)
        self.assertIn("--skip-members", args)

    def test_stale_manifest_does_not_launch(self):
        self.manifest["captured_at"] = "2020-01-01T00:00:00Z"
        with self.assertRaisesRegex(checkpoint.CheckpointError, "selected_stale"):
            self.invoke()
        self.assertFalse((self.root / "args.json").exists())

    def test_missing_native_target_does_not_shrink_roster(self):
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            db.execute("DELETE FROM channels WHERE id='101'")
            db.commit()
        with self.assertRaisesRegex(checkpoint.CheckpointError, "selected_catalog_changed"):
            self.invoke()
        self.assertFalse((self.root / "args.json").exists())

    def test_successful_process_without_every_receipt_is_not_qualified(self):
        (self.root / "omit").touch()
        with self.assertRaisesRegex(checkpoint.CheckpointError, "selected_history_incomplete"):
            self.invoke()

    def test_invalid_native_receipts_and_changed_targets_do_not_qualify(self):
        for fault in ("missing_type", "wrong_type", "since_boundary", "future_completion", "changed_kind", "unavailable"):
            with self.subTest(fault=fault):
                with contextlib.closing(sqlite3.connect(self.db)) as db:
                    db.execute("DELETE FROM sync_state")
                    db.execute("UPDATE channels SET kind='text'")
                    db.commit()
                (self.root / "fault").write_text(fault)
                with self.assertRaises(checkpoint.CheckpointError):
                    self.invoke()

    def test_altered_denominator_does_not_launch(self):
        self.manifest["channel_ids"] = ["100"]
        with self.assertRaises(checkpoint.CheckpointError):
            self.invoke()
        self.assertFalse((self.root / "args.json").exists())

    def test_zero_timeout_does_not_launch(self):
        self.file.write_text(json.dumps(self.manifest))
        self.file.chmod(0o600)
        with self.assertRaises(checkpoint.CheckpointError):
            checkpoint.selected_history(self.native, self.config, self.file, "900", 0)
        self.assertFalse((self.root / "args.json").exists())

    def test_acquire_still_requires_workflow_context(self):
        with patch.object(checkpoint, "execution_context", side_effect=checkpoint.CheckpointError("fixture_workflow_guard")), \
                patch("sys.argv", ["helper", "acquire"]):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "fixture_workflow_guard"):
                checkpoint.main()


if __name__ == "__main__":
    unittest.main()
