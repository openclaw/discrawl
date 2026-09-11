#!/opt/homebrew/bin/python3
"""Mac scheduling only; Discrawl owns capture, repair, storage, and embeddings."""
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone

ROOT = Path(os.environ.get('DISCRAWL_JOB_ROOT', '/Volumes/Data/AppData/discrawl'))
RUNTIME = ROOT / 'runtime'
LOGS = Path(os.environ.get('DISCRAWL_JOB_LOGS', '/Users/hrudolph/Library/Logs/discrawl'))
BIN = os.environ.get('DISCRAWL_JOB_BIN', '/Volumes/Data/Projects/openclaw-tools/bin/discrawl')
COMMAND = [BIN, '--config', str(ROOT / 'config.toml')]
ENV = dict(os.environ, DISCRAWL_NO_AUTO_UPDATE='1', DISCRAWL_NO_UPDATE_CHECK='1')
STATE = RUNTIME / 'tail-state.json'


def now():
    return datetime.now(timezone.utc).isoformat()


def read_json(path):
    return json.loads(path.read_text()) if path.exists() else {}


def save(path, value):
    temporary = path.with_name(path.name + f'.{os.getpid()}.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def cli_json(*args, timeout=45):
    result = subprocess.run(COMMAND + ['--json', *args], env=ENV,
                            capture_output=True, text=True, timeout=timeout, check=True)
    return json.loads(result.stdout)


def blocked():
    if (RUNTIME / 'maintenance.inhibit').exists():
        return 'manual maintenance'
    wal = ROOT / 'discrawl.db-wal'
    if shutil.disk_usage(ROOT).free < 10 * 1024**3:
        return 'free space below 10 GiB'
    if wal.exists() and wal.stat().st_size > 4 * 1024**3:
        return 'WAL exceeds 4 GiB'
    return None


class Service:
    def __init__(self):
        self.child = None
        self.stopping = False
        self.api_key = None
        self.logs = []

    def state(self, phase, **extra):
        save(STATE, dict(state=phase, child_pid=self.child.pid if self.child else 0,
                         updated_at=now(), **extra))

    def signal(self, signum, _frame):
        self.stopping = True
        if self.child and self.child.poll() is None:
            self.child.send_signal(signum)

    def stop(self):
        if self.child:
            if self.child.poll() is None:
                self.child.terminate()
                try:
                    self.child.wait(timeout=180)
                except subprocess.TimeoutExpired:
                    self.child.kill()
            self.child.wait()
            self.child = None

    def command(self, phase, args, capture=False, env=ENV):
        self.child = subprocess.Popen(COMMAND + (['--json'] if capture else []) + args,
                                      env=env, stdout=subprocess.PIPE if capture else self.logs[0].stdin,
                                      stderr=self.logs[1].stdin, text=capture)
        self.state(phase)
        try:
            output, _ = self.child.communicate(timeout=300)
            if self.child.returncode and not self.stopping:
                raise subprocess.CalledProcessError(self.child.returncode, args)
            return json.loads(output) if capture and not self.stopping else None
        finally:
            self.stop()

    def resume(self):
        if self.stopping or blocked():
            return
        self.command('repairing', ['sync', '--source', 'discord', '--latest-only',
                                 '--guilds', '1456350064065904867', '--skip-members',
                                 '--with-embeddings', '--no-update'])
        if self.stopping:
            return
        self.child = subprocess.Popen(COMMAND + ['tail', '--guilds', '1456350064065904867',
                                                 '--repair-every', '6h', '--embed-live'],
                                      env=self.embedding_env(), stdout=self.logs[0].stdin, stderr=self.logs[1].stdin)
        self.state('running')

    def embedding_env(self):
        if not self.api_key:
            try:
                result = subprocess.run(['/usr/bin/security', 'find-generic-password', '-s',
                                         'discrawl/openai-embeddings', '-a', 'embedding-worker', '-w'],
                                        capture_output=True, text=True, timeout=10, check=True)
                self.api_key = result.stdout.strip()
                if not self.api_key:
                    raise RuntimeError('empty embedding credential')
            except Exception as error:
                print(f'{now()} Embedding credential unavailable; capture continues: {error}',
                      file=sys.stderr, flush=True)
                return ENV
        return dict(ENV, OPENAI_API_KEY=self.api_key)

    def run(self):
        with (RUNTIME / 'service.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                for filename in ['tail.log', 'tail.err.log']:
                    self.logs.append(subprocess.Popen(['/usr/sbin/rotatelogs', '-f', '-n', '7',
                                                       str(LOGS / filename), '100M'], stdin=subprocess.PIPE))
                for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
                    signal.signal(signum, self.signal)
                while not self.stopping:
                    reason = blocked()
                    if reason:
                        self.stop()
                        self.state('paused', reason=reason)
                    else:
                        if self.child and self.child.poll() is not None:
                            raise RuntimeError(f'tail exited ({self.child.returncode}); launchd will restart it')
                        if self.child is None:
                            self.resume()
                    time.sleep(5)
            finally:
                self.stop()
                self.state('stopped')
                for logger in self.logs:
                    logger.stdin.close()
                    try:
                        logger.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        logger.kill()
                        logger.wait()


def health():
    report = cli_json('status')
    stamp = report.get('last_tail_event_at')
    state = read_json(STATE)
    worker = report.get('background_work') or {}
    problems = []
    if state.get('state') in ('running', 'embedding', 'repairing'):
        pid = state.get('child_pid', 0)
        if not isinstance(pid, int) or pid <= 0:
            problems.append('capture process is not running')
        else:
            try:
                os.kill(pid, 0)
            except (ProcessLookupError, PermissionError):
                problems.append('capture process is not running')
    else:
        problems.append('capture service is not running')
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(stamp.replace('Z', '+00:00'))).total_seconds() if stamp else float('inf')
    if state.get('state') == 'running' and age > 1800:
        problems.append('no tail event within 30 minutes')
    if worker.get('state') not in ('running',):
        problems.append('embedding worker is ' + worker.get('state', 'missing'))
    report.update(service=state, problems=problems)
    save(RUNTIME / 'health-latest.json', report)
    print(json.dumps(report), flush=True)
    return bool(problems)


def main():
    os.umask(0o077)
    RUNTIME.mkdir(mode=0o700, parents=True, exist_ok=True)
    LOGS.mkdir(mode=0o700, parents=True, exist_ok=True)
    aliases = {'discrawl-health': 'health', 'discrawl-auto-index': 'embed'}
    mode = sys.argv[1] if len(sys.argv) > 1 else aliases.get(Path(sys.argv[0]).name, '')
    if mode in ('run', 'tail'):
        Service().run()
    elif mode == 'health':
        return health()
    elif mode in ('embed', 'request-embed'):
        print('Embedding is managed by native tail --embed-live; use health to inspect it.')
    else:
        raise SystemExit('usage: discrawl-service run|health|embed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
