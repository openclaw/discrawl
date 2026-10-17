import contextlib
import gzip
import io
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

from test_private_discord_checkpoint import CheckpointTests, checkpoint


class SaveDeadlineTests(CheckpointTests):
    # Reuse the native fixture, not the inherited suite's test collection.
    def test_save_checks_source_and_authenticated_restore_only(self):
        self.seed()
        with patch.object(checkpoint, "database_facts", wraps=checkpoint.database_facts) as facts:
            checkpoint.save(self.github, self.path, self.receipt, "success")
        self.assertEqual(facts.call_count, 2)
        self.assertTrue(all(call.args[1] == self.github.deadline for call in facts.call_args_list[1:]))

    def test_corrupted_snapshot_is_rejected_after_authentication_without_promotion(self):
        parent = self.seed()
        original = checkpoint.snapshot
        def corrupt(*args, **kwargs):
            facts = original(*args, **kwargs)
            with contextlib.closing(sqlite3.connect(args[1])) as db, db:
                db.execute("UPDATE messages SET value='different retained bytes'")
                db.execute("INSERT INTO messages VALUES('extra','unexpected row')")
            return facts
        with patch.object(checkpoint, "snapshot", corrupt):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_save_hold.*restore_parity"):
                checkpoint.save(self.github, self.path, self.receipt, "success")
        self.assertEqual(self.github.latest, parent["release_id"])
        self.assertEqual(len(self.github.releases), 2)

    def test_pack_expiry_never_creates_a_draft(self):
        self.seed()
        with patch.object(checkpoint.time, "monotonic", return_value=101):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_timeout"):
                checkpoint.pack(self.path, self.root, deadline=100)
        self.assertEqual(len(self.github.releases), 1)
        self.assertEqual(list(self.root.glob("state-*.sqlite.gz")), [])

    def test_gzip_checks_shared_deadline_between_blocks(self):
        self.db.execute("INSERT INTO future_native_state VALUES(2,zeroblob(2000000))")
        self.db.commit()
        copy = self.root / "copy.sqlite"
        checkpoint.snapshot(self.path, copy)
        clock = [0]
        original = checkpoint.gzip.GzipFile.write
        def write(file, data):
            result = original(file, data)
            clock[0] = 101
            return result
        with patch.object(checkpoint.time, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(checkpoint.gzip.GzipFile, "write", write):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_timeout"):
                checkpoint.pack(copy, self.root, deadline=100)

    def test_deadline_leaves_one_minute_before_promotion(self):
        parent = self.seed()
        clock = [0]
        original = checkpoint.download_database
        def restored(*args, **kwargs):
            result = original(*args, **kwargs)
            clock[0] = 41
            return result
        with patch.object(checkpoint.time, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(checkpoint, "download_database", restored):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_save_hold.*publication_budget"):
                checkpoint.save(self.github, self.path, self.receipt, "success", deadline=100)
        self.assertEqual(self.github.latest, parent["release_id"])

    def test_expiry_after_promotion_holds_without_deleting_or_retrying(self):
        self.seed()
        checkpoint.save(self.github, self.path, self.receipt, "success")
        self.restore_parent()
        clock = [0]
        self.github.before_promote = lambda: clock.__setitem__(0, 101)
        with patch.object(checkpoint.time, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(self.github, "api", wraps=self.github.api) as api:
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_save_hold.*checkpoint_timeout"):
                checkpoint.save(self.github, self.path, self.receipt, "success", deadline=100)
        self.assertEqual(len([call for call in api.call_args_list if len(call.args) > 1]), 1)
        self.assertEqual(len(self.github.releases), 3)
        self.assertEqual(self.github.deleted, [])

    def test_expiry_after_retirement_reports_hold_without_second_delete(self):
        oldest = self.seed()
        checkpoint.save(self.github, self.path, self.receipt, "success")
        self.restore_parent()
        clock = [0]
        original = self.github.command
        def command(args, **options):
            result = original(args, **options)
            if args[-1] == "DELETE":
                clock[0] = 101
            return result
        with patch.object(checkpoint.time, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(self.github, "command", command):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_save_hold.*checkpoint_timeout"):
                checkpoint.save(self.github, self.path, self.receipt, "success", deadline=100)
        self.assertEqual(self.github.deleted, [oldest["release_id"]])
        self.assertEqual(len(self.github.releases), 2)

    def test_phase_output_contains_only_fixed_labels_and_elapsed_time(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.seed()
        for line in output.getvalue().splitlines():
            self.assertRegex(line, r"^checkpoint: phase=(source_facts|backup|pack|upload|restore_verify|promote|download|reconstruct|disk_verify|sqlite_verify) state=(begin|complete)( elapsed_seconds=[0-9.]+)?$")
        for phase in ("download", "reconstruct", "disk_verify", "sqlite_verify"):
            self.assertIn(f"phase={phase} state=complete", output.getvalue())
        with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_phase"):
            with checkpoint.checkpoint_phase("private-secret"):
                pass

    def test_pack_carries_exact_disk_hashes_and_unchanged_gzip_bytes_without_rereads(self):
        for size in (0, 1, 17, 18, 34, 39):
            with self.subTest(size=size):
                directory = self.root / str(size)
                directory.mkdir()
                source = directory / "input"
                data = bytes(range(size))
                source.write_bytes(data)
                with patch.object(checkpoint, "RAW_PART", 17), patch.object(checkpoint, "BLOCK", 5), \
                     patch.object(checkpoint, "file_digest", side_effect=AssertionError("redundant hash read")):
                    database, parts = checkpoint.pack(source, directory)
                self.assertEqual(database, {"bytes": size, "sha256": checkpoint.digest(data)})
                self.assertEqual(len(parts), (size + 16) // 17)
                for index, part in enumerate(parts):
                    expected = io.BytesIO()
                    chunk = data[index * 17:(index + 1) * 17]
                    with gzip.GzipFile(filename="", mode="wb", fileobj=expected, mtime=0) as compressed:
                        for start in range(0, len(chunk), 5):
                            compressed.write(chunk[start:start + 5])
                    actual = (directory / part["name"]).read_bytes()
                    self.assertEqual(actual, expected.getvalue())
                    self.assertEqual(gzip.decompress(actual), chunk)
                    self.assertEqual(part, {"name": f"state-{index:03d}.sqlite.gz", "bytes": len(actual),
                                            "raw_bytes": len(chunk), "sha256": checkpoint.digest(actual)})

    def test_short_writes_include_header_flush_and_trailer_in_carried_hash(self):
        class ShortWriter(io.BytesIO):
            def write(self, data):
                return super().write(data[:3])
        raw = ShortWriter()
        sink = checkpoint.HashingWriter(raw, None)
        with gzip.GzipFile(filename="", mode="wb", fileobj=sink, mtime=0) as compressed:
            compressed.write(b"first native bytes")
            compressed.flush()
            compressed.write(b"second native bytes")
        actual = raw.getvalue()
        self.assertEqual(gzip.decompress(actual), b"first native bytessecond native bytes")
        self.assertEqual(sink.size, len(actual))
        self.assertEqual(sink.sha256.hexdigest(), checkpoint.digest(actual))

    def test_invalid_write_counts_never_hash_unaccepted_bytes(self):
        for count in (0, None, -1, 5, True, 1.5):
            with self.subTest(count=count):
                class InvalidWriter:
                    def write(self, _data):
                        return count
                sink = checkpoint.HashingWriter(InvalidWriter(), None)
                with self.assertRaisesRegex(checkpoint.CheckpointError, "part_write"):
                    sink.write(b"data")
                self.assertEqual(sink.size, 0)
                self.assertEqual(sink.sha256.hexdigest(), checkpoint.digest(b""))

    def test_partial_write_failure_hashes_only_the_accepted_prefix(self):
        class FailedWriter(io.BytesIO):
            def write(self, data):
                if self.tell():
                    raise OSError("write failed")
                return super().write(data[:2])
        raw = FailedWriter()
        sink = checkpoint.HashingWriter(raw, None)
        self.assertEqual(sink.write(b""), 0)
        self.assertEqual(raw.getvalue(), b"")
        with self.assertRaisesRegex(OSError, "write failed"):
            sink.write(b"data")
        self.assertEqual(raw.getvalue(), b"da")
        self.assertEqual(sink.size, 2)
        self.assertEqual(sink.sha256.hexdigest(), checkpoint.digest(b"da"))

    def test_sink_deadline_and_capacity_refuse_before_write(self):
        raw = io.BytesIO()
        sink = checkpoint.HashingWriter(raw, 100)
        with patch.object(checkpoint.time, "monotonic", return_value=101):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "checkpoint_timeout"):
                sink.write(b"data")
        with patch.object(checkpoint, "MAX_PART", 3):
            with self.assertRaisesRegex(checkpoint.CheckpointError, "part_size"):
                sink.write(b"data")
        self.assertEqual(raw.getvalue(), b"")

    def test_pack_hash_failure_never_creates_a_draft(self):
        parent = self.seed()
        class FailedHash:
            def update(self, _data):
                raise OSError("hash failed")
        original_pack, original_hash = checkpoint.pack, checkpoint.hashlib.sha256
        for failed in (0, 1):
            def broken_pack(*args, **kwargs):
                hashes = [original_hash(), original_hash()]
                hashes[failed] = FailedHash()
                with patch.object(checkpoint.hashlib, "sha256", side_effect=hashes):
                    return original_pack(*args, **kwargs)
            with self.subTest(hash=failed), patch.object(checkpoint, "pack", broken_pack):
                with self.assertRaisesRegex(OSError, "hash failed"):
                    checkpoint.save(self.github, self.path, self.receipt, "success")
            self.assertEqual(self.github.latest, parent["release_id"])
            self.assertEqual(len(self.github.releases), 1)

    def test_gzip_or_file_close_failure_never_creates_a_draft(self):
        parent = self.seed()
        gzip_close, path_open = checkpoint.gzip.GzipFile.close, Path.open
        def failed_gzip_close(compressed):
            opened = compressed.fileobj is not None
            gzip_close(compressed)
            if opened:
                raise OSError("gzip close failed")
        class FailedClose:
            def __init__(self, output):
                self.output = output
            def __enter__(self):
                return self.output
            def __exit__(self, *args):
                self.output.__exit__(*args)
                raise OSError("file close failed")
        def failed_file_close(path, *args, **kwargs):
            output = path_open(path, *args, **kwargs)
            return FailedClose(output) if path.name.startswith("state-") and args == ("xb",) else output
        for owner, name, replacement in ((checkpoint.gzip.GzipFile, "close", failed_gzip_close),
                                          (Path, "open", failed_file_close)):
            with self.subTest(close=name), patch.object(owner, name, replacement):
                with self.assertRaisesRegex(OSError, "close failed"):
                    checkpoint.save(self.github, self.path, self.receipt, "success")
            self.assertEqual(self.github.latest, parent["release_id"])
            self.assertEqual(len(self.github.releases), 1)

    def test_cli_deadline_starts_before_private_admission(self):
        self.seed()
        completion = self.root / "writer.json"
        completion.write_bytes(checkpoint.canonical({"reaped": True, "outcome": "success"}))
        observed = []
        def client(*, deadline):
            observed.append(deadline)
            return self.github
        with patch.object(sys, "argv", ["checkpoint", "save", "--timeout", "840", "--db", str(self.path),
                                        "--receipt", str(self.receipt), "--writer-receipt", str(completion)]), \
             patch.object(checkpoint, "execution_context"), patch.object(checkpoint, "GitHub", client), \
             patch.object(checkpoint.time, "monotonic", return_value=10), \
             patch.object(checkpoint, "save") as save:
            checkpoint.main()
        self.assertEqual(observed, [850])
        self.assertEqual(save.call_args.args[-1], 850)


# The fixture class supplies setup/teardown helpers only in this module.
def load_tests(loader, _tests, _pattern):
    suite = unittest.TestSuite()
    for name in SaveDeadlineTests.__dict__:
        if name.startswith("test_"):
            suite.addTest(SaveDeadlineTests(name))
    return suite


if __name__ == "__main__":
    unittest.main()
