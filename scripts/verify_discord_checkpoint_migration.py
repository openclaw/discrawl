#!/usr/bin/env python3
"""Read-only admission for the one-time full-state transport migration."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys

from private_discord_checkpoint import CheckpointError, canonical, execution_context, keys, require, strict_json


def api(path):
    result = subprocess.run(["gh", "api", "repos/openclaw/discrawl/" + path],
                            capture_output=True, timeout=60, check=False)
    require(result.returncode == 0 and len(result.stdout) <= 1024 * 1024, "migration_api")
    return json.loads(result.stdout)


def verify(cache, expected, producer_revision):
    require(execution_context() == "migrate-discord-checkpoint.yml"
            and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and os.environ.get("GITHUB_RUN_ATTEMPT") == "1"
            and os.environ.get("GITHUB_ACTOR") == "vincentkoc"
            and os.environ.get("GITHUB_TRIGGERING_ACTOR") == "vincentkoc"
            and expected == os.environ.get("GITHUB_SHA")
            and re.fullmatch(r"[0-9a-f]{40}", producer_revision or ""), "migration_context")
    require(api("git/ref/heads/main")["object"]["sha"] == expected, "migration_source_changed")
    keys(cache, "id key ref version created_at size_in_bytes")
    require(cache["ref"] == "refs/heads/main" and re.fullmatch(r"discrawl-discord-db-Linux-main-[1-9][0-9]*-[1-9][0-9]*", cache["key"]), "migration_cache")
    # List-by-key is the cache API's read surface; there is no get-cache-by-ID.
    query = "actions/caches?ref=refs/heads/main&key=" + cache["key"] + "&per_page=100"
    exact = api(query)
    require(exact["total_count"] == 1 and len(exact["actions_caches"]) == 1
            and {k: exact["actions_caches"][0][k] for k in cache} == cache, "migration_cache_changed")
    newest = api("actions/caches?ref=refs/heads/main&key=discrawl-discord-db-Linux-main-&sort=created_at&direction=desc&per_page=1")
    require(newest["actions_caches"] and newest["actions_caches"][0]["id"] == cache["id"], "migration_cache_not_latest")
    run_id, attempt = map(int, cache["key"].rsplit("-", 2)[1:])
    run = api(f"actions/runs/{run_id}/attempts/{attempt}")
    require(run["status"] == "completed" and run["conclusion"] == "success"
            and run["head_sha"] == producer_revision and run["head_branch"] == "main"
            and run["path"] == ".github/workflows/publish-discord-backup.yml"
            and run["run_attempt"] == attempt, "migration_producer")
    for name in ("publish-discord-backup.yml", "discord-backup-report.yml", "repair-discord-cache.yml"):
        workflow = api("actions/workflows/" + name)
        require(workflow["state"] in {"disabled_manually", "disabled_inactivity", "deleted"}, "migration_writer_enabled")
        for status in ("queued", "in_progress", "waiting", "pending", "requested"):
            runs = api(f'actions/workflows/{workflow["id"]}/runs?status={status}&per_page=1')
            require(runs["total_count"] == 0, "migration_writer_active")
    return cache


def main():
    os.umask(0o077)
    require(os.environ.get("MIGRATION_ACK") == "MIGRATE_FULL_NATIVE_STATE", "migration_ack")
    cache = verify(strict_json(os.environ.get("CACHE_IDENTITY", "").encode()),
                   os.environ.get("EXPECTED_SHA", ""), os.environ.get("PRODUCER_REVISION", ""))
    Path(os.environ["CACHE_IDENTITY_FILE"]).write_bytes(canonical(cache))
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write("cache_key=" + cache["key"] + "\n")
    print("Migration source and quiescence verified.")


if __name__ == "__main__":
    try:
        main()
    except (CheckpointError, OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as error:
        print("migration blocked: " + (str(error) if isinstance(error, CheckpointError) else "internal"), file=sys.stderr)
        sys.exit(1)
