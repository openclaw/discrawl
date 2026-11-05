"""Offline behavior/privacy tests; the real parser and request builder run."""

import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import diagnose_discord_content as diagnostic

TOKEN = "synthetic-diagnostic-secret"
PRIVATE = "synthetic-private-content-😀"
APP = "111111111111111111"
BOT = "222222222222222222"
AUTHOR = "333333333333333333"


class Response:
    def __init__(self, value, status=200, headers=None):
        self.body = json.dumps(value).encode() if not isinstance(value, bytes) else value
        self.status = status
        self.headers = headers or {}
        self.reads = []

    def getheader(self, name, default=None):
        return self.headers.get(name, default)

    def read(self, limit):
        self.reads.append(limit)
        return self.body[:limit]


class ContentDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.event = self.root / "event.json"
        self.inputs = dict(expected_sha="a" * 40, expected_application_id=APP,
                           expected_bot_id=BOT, expected_author_id=AUTHOR, ordinary_text_confirmed="true")
        self.environment = dict(GITHUB_REPOSITORY="openclaw/discrawl", GITHUB_WORKFLOW_REF=diagnostic.WORKFLOW,
                                GITHUB_REF="refs/heads/main", GITHUB_EVENT_NAME="workflow_dispatch",
                                GITHUB_RUN_ATTEMPT="1", GITHUB_ACTOR="vincentkoc",
                                GITHUB_TRIGGERING_ACTOR="vincentkoc", GITHUB_SHA="a" * 40,
                                CHECKED_OUT_SHA="a" * 40, GITHUB_EVENT_PATH=str(self.event), DISCORD_BOT_TOKEN=TOKEN)
        self.application = dict(id=APP, bot=dict(id=BOT), flags=(1 << 18),
                                flags_new=str((1 << 52) | (1 << 18)), owner=dict(private=PRIVATE), team=PRIVATE)
        self.message = dict(id=diagnostic.MESSAGE, channel_id=diagnostic.CHANNEL, author=dict(id=AUTHOR),
                            type=0, timestamp="2026-10-27T02:10:03.198000+00:00", mentions=[], content=PRIVATE)
        self.responses = []
        self.requests = []
        self.connections = []
        self.failure = None
        test = self

        class Connection:
            def __init__(self, host, timeout):
                test.assertEqual((host, timeout), ("discord.com", 10))
                self.closed = False
                test.connections.append(self)

            def request(self, method, path, body=None, headers=None):
                test.assertEqual(method, "GET")
                test.assertIsNone(body)
                test.assertEqual(headers, {"Authorization": "Bot " + TOKEN, "Accept": "application/json",
                                          "User-Agent": "discrawl-read-only-diagnostic"})
                test.assertIn(path, (diagnostic.APPLICATION_PATH, diagnostic.MESSAGE_PATH))
                test.requests.append(path)
                if test.failure:
                    raise test.failure
                self.response = test.responses[len(test.requests) - 1]

            def getresponse(self):
                return self.response

            def close(self):
                self.closed = True

        patcher = patch.object(diagnostic.http.client, "HTTPSConnection", Connection)
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_diagnostic(self):
        self.event.write_text(json.dumps(dict(inputs=self.inputs)))
        if not self.responses:
            self.responses = [Response(self.application), Response(self.message)]
        out, err = io.StringIO(), io.StringIO()
        before = set(self.root.iterdir())
        with patch.dict(os.environ, self.environment, clear=True), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = diagnostic.main()
        self.assertEqual(set(self.root.iterdir()), before, "diagnostic persisted data")
        self.assertEqual(err.getvalue(), "")
        for sensitive in (TOKEN, PRIVATE, json.dumps(PRIVATE)[1:-1], AUTHOR, "owner", "team", "Authorization"):
            self.assertNotIn(sensitive, out.getvalue())
        self.assertTrue(all(connection.closed for connection in self.connections))
        return code, json.loads(out.getvalue())

    def test_two_fixed_gets_report_only_verified_flags_identity_status_and_length(self):
        code, report = self.run_diagnostic()
        self.assertEqual(code, 0)
        self.assertEqual(self.requests, [diagnostic.APPLICATION_PATH, diagnostic.MESSAGE_PATH])
        self.assertEqual(report, dict(application_http_status=200, message_http_status=200,
                                     application_id=APP, bot_id=BOT, message_content=True,
                                     message_content_limited=False, message_identity_validated=True,
                                     content_length=len(PRIVATE), content_length_unit="unicode_code_points", result="measured"))
        self.assertEqual([response.reads for response in self.responses], [[diagnostic.MAX_BODY + 1]] * 2)

    def test_empty_original_content_is_measured_without_inventing_a_cause(self):
        self.message["content"] = ""
        code, report = self.run_diagnostic()
        self.assertEqual((code, report["content_length"], report["result"]), (0, 0, "measured"))

    def test_operator_source_and_confirmation_fail_before_network(self):
        changes = [{key: value} for key, value in (
            ("GITHUB_REPOSITORY", "fork/discrawl"), ("GITHUB_WORKFLOW_REF", diagnostic.WORKFLOW.replace("main", "other")),
            ("GITHUB_REF", "refs/heads/other"), ("GITHUB_EVENT_NAME", "schedule"), ("GITHUB_RUN_ATTEMPT", "2"),
            ("GITHUB_ACTOR", "other"), ("GITHUB_TRIGGERING_ACTOR", "other"), ("GITHUB_SHA", "b" * 40),
            ("CHECKED_OUT_SHA", "b" * 40), ("DISCORD_BOT_TOKEN", ""), ("DISCORD_BOT_TOKEN", "bad\r\nheader"))]
        original = self.environment.copy()
        for change in changes:
            with self.subTest(change=change):
                self.environment = original | change
                self.assertEqual(self.run_diagnostic()[0], 1)
                self.assertEqual(self.requests, [])
        self.environment = original
        for key, value in (("expected_sha", ""), ("expected_sha", "b" * 40), ("ordinary_text_confirmed", "false"),
                           ("expected_application_id", ""), ("expected_author_id", BOT), ("expected_bot_id", "../escape")):
            with self.subTest(input=key, value=value):
                original_inputs = self.inputs.copy()
                self.inputs[key] = value
                self.assertEqual(self.run_diagnostic()[0], 1)
                self.assertEqual(self.requests, [])
                self.inputs = original_inputs

    def test_flags_encoding_and_absence_are_not_silently_false(self):
        cases = [(dict(flags=0), False, False), (dict(flags=1 << 19), False, True),
                 (dict(flags_new=str((1 << 70) | (1 << 18) | (1 << 19))), True, True),
                 (dict(flags=1 << 18, flags_new=str((1 << 60) | (1 << 18))), True, False)]
        for fields, full, limited in cases:
            with self.subTest(fields=fields):
                self.assertEqual(diagnostic.flags(fields), dict(message_content=full, message_content_limited=limited))
        for fields in ({}, dict(flags=None), dict(flags=True), dict(flags=-1), dict(flags=2**31),
                       dict(flags="262144"), dict(flags_new=262144), dict(flags_new="-1"),
                       dict(flags_new="1.0"), dict(flags_new="01"), dict(flags_new=None),
                       dict(flags=0, flags_new=str(1 << 18))):
            with self.subTest(invalid=fields), self.assertRaises(diagnostic.DiagnosticError):
                diagnostic.flags(fields)

    def test_unverified_application_or_flags_stops_before_message_get(self):
        for change in (dict(id=AUTHOR), dict(bot={}), dict(flags_new=None), dict(flags=0)):
            with self.subTest(change=change):
                self.requests.clear()
                self.responses = [Response(self.application | change)]
                code, report = self.run_diagnostic()
                self.assertEqual(code, 1)
                self.assertNotIn("content_length", report)
                self.assertEqual(self.requests, [diagnostic.APPLICATION_PATH])

    def test_message_identity_ordinary_text_window_and_exceptions_fail_closed(self):
        changes = [dict(id=APP), dict(channel_id=APP), dict(guild_id=APP), dict(author=dict(id=APP)),
                   dict(author=dict(id=AUTHOR, bot=True)), dict(author=dict(id=AUTHOR, system=True)),
                   dict(type=20), dict(type=True), dict(webhook_id=APP), dict(application_id=APP),
                   dict(application={}), dict(interaction={}), dict(interaction_metadata={}),
                   dict(message_reference={}), dict(message_snapshots=[]),
                   dict(timestamp="2026-10-25T02:10:03.198Z"), dict(timestamp="2026-10-28T00:00:00Z"),
                   dict(timestamp="2026-10-27T02:10:04.198Z"), dict(timestamp="2026-10-27T02:10:03.198"),
                   dict(timestamp=PRIVATE), dict(mentions=None), dict(mentions=[{}]), dict(content=None)]
        for identity in (APP, BOT):
            changes += [dict(mentions=[dict(id=identity)]), dict(content=f"<@{identity}>"),
                        dict(content=f"<@!{identity}>")]
        for change in changes:
            with self.subTest(change=change):
                self.requests.clear()
                self.responses = [Response(self.application), Response(self.message | change)]
                code, report = self.run_diagnostic()
                self.assertEqual(code, 1)
                self.assertNotIn("content_length", report)
                self.assertNotIn("message_identity_validated", report)
                self.assertEqual(self.requests, [diagnostic.APPLICATION_PATH, diagnostic.MESSAGE_PATH])

    def test_http_errors_redirects_and_rate_limits_never_read_body_or_retry(self):
        for phase in (0, 1):
            for status, error in ((401, "permission"), (403, "permission"), (429, "rate_limited"),
                                  (302, "redirect"), (307, "redirect"), (404, "http_status"), (500, "http_status")):
                with self.subTest(phase=phase, status=status):
                    self.requests.clear()
                    response = Response(PRIVATE.encode(), status, {"Location": "https://evil.invalid/" + TOKEN})
                    self.responses = [Response(self.application), response] if phase else [response]
                    code, report = self.run_diagnostic()
                    self.assertEqual((code, report["error"]), (1, error))
                    self.assertEqual(response.reads, [])
                    self.assertEqual(len(self.requests), phase + 1)
                    self.assertNotIn("content_length", report)

    def test_bounded_invalid_payloads_are_private_and_stop_without_retry(self):
        for response, error in ((Response(b"x" * (diagnostic.MAX_BODY + 1)), "response_too_large"),
                                (Response(PRIVATE.encode()), "invalid_response"),
                                (Response(b'{"flags":0,"flags":262144}'), "invalid_response"),
                                (Response(b'{"flags":NaN}'), "invalid_response"),
                                (Response([]), "invalid_response"),
                                (Response({}, headers={"Content-Encoding": "gzip"}), "invalid_response")):
            with self.subTest(error=error):
                self.requests.clear()
                self.responses = [response]
                code, report = self.run_diagnostic()
                self.assertEqual((code, report["error"]), (1, error))
                self.assertEqual(self.requests, [diagnostic.APPLICATION_PATH])
                self.assertLessEqual(len(response.reads), 1)

    def test_transport_exceptions_and_deadline_do_not_render_private_details(self):
        for failure, error in ((OSError(PRIVATE + TOKEN), "transport_or_internal"),
                               (TimeoutError(PRIVATE + TOKEN), "timeout")):
            with self.subTest(error=error):
                self.requests.clear()
                self.failure = failure
                code, report = self.run_diagnostic()
                self.assertEqual((code, report), (1, {"error": error}))
                self.assertEqual(self.requests, [diagnostic.APPLICATION_PATH])
        with self.assertRaises(TimeoutError):
            diagnostic.timeout(None, None)

    def test_nonallowlisted_endpoint_cannot_send_a_credential(self):
        with self.assertRaises(diagnostic.DiagnosticError):
            diagnostic.get_json("https://evil.invalid/", TOKEN, {})
        self.assertEqual(self.requests, [])
        self.assertEqual(self.connections, [])


if __name__ == "__main__":
    unittest.main()
