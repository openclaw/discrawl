import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import private_discord_checkpoint as checkpoint


class NativeDiagnosticsTests(unittest.TestCase):
    def record(self, **changes):
        return {"schema": 1, "class": "http", "exit": 1, "http_status": 403, "discord_code": 50001, **changes}

    def terminal(self, value):
        return b"discrawl_error=" + checkpoint.canonical(value) + b"\n"

    def test_terminal_record_is_strict_and_bounded(self):
        valid = self.record()
        self.assertEqual(checkpoint.native_failure(b"private warning\n" + self.terminal(valid), 1), valid)
        malformed = [self.terminal(valid)[:-1], self.terminal(valid) + b"private error\n",
                     b'discrawl_error={"schema":1,"schema":1,"class":"unknown","exit":1}\n',
                     b'discrawl_error={"schema":1,"class":' + b'[' * 980 + b']' * 980 + b',"exit":1}\n',
                     b"discrawl_error=" + b" " * 2048 + checkpoint.canonical(valid) + b"\n"]
        for changes in ({"schema": True}, {"class": "private-value"}, {"class": []}, {"exit": True},
                        {"exit": 2}, {"http_status": 600}, {"discord_code": 2 ** 31},
                        {"sqlite_code": False}, {"message": "private"}):
            malformed.append(self.terminal(self.record(**changes)))
        for raw in malformed:
            with self.subTest(raw=raw[:80]):
                self.assertEqual(checkpoint.native_failure(raw, 1), {"class": "unknown"})

    def test_flood_is_drained_and_only_terminal_fields_reach_public_output(self):
        raw = self.terminal(self.record())
        code = ("import os; os.write(1,b'private stdout'*100000); "
                "os.write(2,b'private stderr'*100000); os.write(2,b'\\n'+%r); raise SystemExit(1)") % raw
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(output):
            receipt = Path(directory) / "writer.json"
            original = checkpoint.native_failure
            def bounded(tail, exit_code):
                self.assertLessEqual(len(tail), checkpoint.MAX_WRITER_STDERR)
                return original(tail, exit_code)
            with patch.object(checkpoint, "native_failure", bounded):
                result = checkpoint.run_writer([sys.executable, "-c", code], 5, receipt, "latest")
            self.assertEqual(result, 1)
            self.assertEqual(json.loads(receipt.read_bytes()), {"reaped": True, "outcome": "failure"})
        self.assertNotIn("private", output.getvalue())
        terminal = json.loads(output.getvalue().splitlines()[-1].removeprefix("native: "))
        self.assertEqual(terminal["error"], self.record())
        self.assertEqual((terminal["phase"], terminal["reason"], terminal["reaped"]), ("latest", "exit", True))

    def test_termination_grace_drains_child_and_preserves_cancelled_receipt(self):
        code = """import os,signal,time
def stop(sig,frame):
 os.write(2,b'private shutdown'*100000)
 os.write(2,b'\\ndiscrawl_error={"schema":1,"class":"cancelled","exit":1}\\n')
 raise SystemExit(1)
signal.signal(signal.SIGTERM,stop)
time.sleep(30)
"""
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(output):
            receipt = Path(directory) / "writer.json"
            self.assertEqual(checkpoint.run_writer([sys.executable, "-c", code], .2, receipt, "selected"), 1)
            self.assertEqual(json.loads(receipt.read_bytes()), {"reaped": True, "outcome": "cancelled"})
        self.assertNotIn("private", output.getvalue())
        terminal = json.loads(output.getvalue().splitlines()[-1].removeprefix("native: "))
        self.assertEqual(terminal["reason"], "timeout")
        self.assertEqual(terminal["error"]["class"], "cancelled")
        self.assertEqual(terminal["exit"], 1, "graceful exit must not block behind a full stderr pipe")

    def test_capture_setup_failure_reaps_child_without_receipt(self):
        children = []
        original = subprocess.Popen
        def spawn(*args, **kwargs):
            child = original(*args, **kwargs)
            children.append(child)
            return child
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            receipt = Path(directory) / "writer.json"
            with patch.object(checkpoint.subprocess, "Popen", spawn), \
                    patch.object(checkpoint.os, "set_blocking", side_effect=OSError("private path")):
                with self.assertRaises(OSError):
                    checkpoint.run_writer([sys.executable, "-c", "import time;time.sleep(30)"], 1, receipt)
            self.assertFalse(receipt.exists())
            self.assertIsNotNone(children[0].poll())
            with self.assertRaises(ProcessLookupError):
                os.killpg(children[0].pid, 0)

    def test_phase_is_fixed_before_spawn(self):
        with patch.object(checkpoint.subprocess, "Popen") as spawn:
            with self.assertRaisesRegex(checkpoint.CheckpointError, "writer_phase"):
                checkpoint.run_writer(["native"], 1, phase="private-channel")
            spawn.assert_not_called()


if __name__ == "__main__":
    unittest.main()
