import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('macos', Path(__file__).with_name('discrawl-macos.py'))
macos = importlib.util.module_from_spec(spec)
spec.loader.exec_module(macos)


class ServiceTests(unittest.TestCase):
    def test_capture_starts_with_native_continuous_worker(self):
        service = macos.Service()
        service.command, service.state = Mock(), Mock()
        service.logs = [Mock(), Mock()]
        with patch.object(macos, 'blocked', return_value=None), \
             patch.object(service, 'embedding_env', return_value={'OPENAI_API_KEY': 'fixture'}), \
             patch.object(macos.subprocess, 'Popen') as popen:
            service.resume()
        self.assertIn('--embed-live', popen.call_args.args[0])
        self.assertEqual(popen.call_args.kwargs['env'], {'OPENAI_API_KEY': 'fixture'})
        self.assertEqual(service.command.call_args.args[0], 'repairing')

    def test_missing_embedding_credentials_still_starts_capture(self):
        service = macos.Service()
        service.command, service.state = Mock(), Mock()
        service.logs = [Mock(), Mock()]
        with patch.object(macos, 'blocked', return_value=None), \
             patch.object(macos.subprocess, 'run', side_effect=TimeoutError('fixture')), \
             patch.object(macos.subprocess, 'Popen') as popen:
            service.resume()
        self.assertIn('--embed-live', popen.call_args.args[0])
        self.assertIs(popen.call_args.kwargs['env'], macos.ENV)

    def test_embedding_credentials_cached_without_changing_global_environment(self):
        service = macos.Service()
        with patch.object(macos.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, 'fixture-secret\n')) as lookup:
            first = service.embedding_env()
            second = service.embedding_env()
        lookup.assert_called_once()
        self.assertEqual(first['OPENAI_API_KEY'], 'fixture-secret')
        self.assertEqual(second, first)
        self.assertIsNot(first, macos.ENV)

    def test_empty_credential_keeps_capture_environment(self):
        with patch.object(macos.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, '')):
            self.assertIs(macos.Service().embedding_env(), macos.ENV)

    def test_shutdown_does_not_restart_capture(self):
        service = macos.Service()
        service.stopping = True
        service.command = Mock()
        service.resume()
        service.command.assert_not_called()

    def test_running_service_never_stops_capture_for_hourly_indexing(self):
        service = macos.Service()
        service.child = Mock()
        service.child.poll.return_value = None
        service.stop, service.resume = Mock(), Mock()
        sleeps = []
        def sleep(seconds):
            sleeps.append(seconds)
            if len(sleeps) == 740:
                service.stopping = True
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(macos, 'RUNTIME', Path(directory)), \
                 patch.object(macos, 'blocked', return_value=None), \
                 patch.object(macos, 'save'), \
                 patch.object(macos.signal, 'signal'), \
                 patch.object(macos.subprocess, 'Popen') as popen, \
                 patch.object(macos.time, 'sleep', side_effect=sleep):
                service.run()
        self.assertGreater(sum(sleeps), 3600)
        self.assertEqual(popen.call_count, 2)  # log rotators only
        service.stop.assert_called_once_with()  # shutdown only
        service.resume.assert_not_called()

    def test_stop_reaps_writer(self):
        service = macos.Service()
        process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
        service.child = process
        try:
            service.stop()
            self.assertIsNotNone(process.poll())
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

    def test_health_requires_fresh_native_worker_status(self):
        for state, expected in [('running', False), ('paused', True), ('stale', True), ('stopped', True)]:
            with self.subTest(state=state):
                report = {'last_tail_event_at': macos.now(), 'background_work': {'state': state}}
                with patch.object(macos, 'cli_json', return_value=report), \
                     patch.object(macos, 'read_json', return_value={'state': 'running', 'child_pid': os.getpid()}), \
                     patch.object(macos, 'save'):
                    self.assertEqual(bool(macos.health()), expected)


if __name__ == '__main__':
    unittest.main()
