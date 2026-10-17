#!/usr/bin/env python3
"""Admission and recovery for the one-time full-state transport migration."""

import json
from datetime import datetime
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time

from private_discord_checkpoint import (CheckpointError, GitHub, canonical, check_deadline, digest,
                                        download_database, execution_context,
                                        file_digest, github_command, hash_value, integer, keys,
                                        release_manifest, require, strict_json)


DEADLINE = None


def public_read(path, limit=1024 * 1024):
    phase = ("source_log_read" if path.endswith("/logs") else
             "source_ref_read" if path.startswith("git/") else
             "source_cache_read" if path.startswith("actions/caches") else
             "source_run_read" if path.startswith("actions/runs/") else
             "source_active_runs" if "?status=" in path else "source_workflow_read")
    # Public admission uses the job token, never the private checkpoint token.
    env = {key: value for key, value in os.environ.items()
           if key not in {"DISCORD_BACKUP_TOKEN", "DISCRAWL_BACKUP_REPOSITORY"}}
    env.update(GH_PROMPT_DISABLED="1", GH_DEBUG="", GH_HOST="github.com")
    require(env.get("GH_TOKEN"), "migration_credential")
    arguments = ["api", "repos/openclaw/discrawl/" + path, "--method", "GET", "--include"]
    if phase == "source_log_read":
        # Actions script echoes contain ANSI bytes. Capture them unchanged for
        # validation; this bounded response is never rendered to a terminal.
        arguments.append("--allow-escape-sequences")
    raw = github_command(arguments, env, phase, timeout=60, limit=limit, deadline=DEADLINE)
    parts = re.split(rb"\r?\n\r?\n", raw, maxsplit=1)
    require(len(parts) == 2 and re.match(rb"HTTP/\S+ 2[0-9][0-9]\b", parts[0]), "migration_api_status phase=" + phase)
    return parts[1]


def api(path):
    return json.loads(public_read(path))


def verify_context(expected):
    require(execution_context() == "migrate-discord-checkpoint.yml"
            and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and os.environ.get("GITHUB_RUN_ATTEMPT") == "1"
            and os.environ.get("GITHUB_ACTOR") == "vincentkoc"
            and os.environ.get("GITHUB_TRIGGERING_ACTOR") == "vincentkoc"
            and expected == os.environ.get("GITHUB_SHA"), "migration_context")
    require(api("git/ref/heads/main")["object"]["sha"] == expected, "migration_source_changed")


def verify_quiescence():
    for name in ("publish-discord-backup.yml", "discord-backup-report.yml", "repair-discord-cache.yml"):
        workflow = api("actions/workflows/" + name)
        require(workflow["state"] in {"disabled_manually", "disabled_inactivity", "deleted"}, "migration_writer_enabled")
        for status in ("queued", "in_progress", "waiting", "pending", "requested"):
            runs = api(f'actions/workflows/{workflow["id"]}/runs?status={status}&per_page=1')
            require(runs["total_count"] == 0, "migration_writer_active")


def verify(cache, expected, producer_revision):
    verify_context(expected)
    require(re.fullmatch(r"[0-9a-f]{40}", producer_revision or ""), "migration_context")
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
    verify_quiescence()
    return cache


def migration_run(run_id, conclusion):
    run = api(f"actions/runs/{run_id}/attempts/1")
    current = api(f"actions/runs/{run_id}")
    workflow = api("actions/workflows/migrate-discord-checkpoint.yml")
    require(run["id"] == run_id and run["run_attempt"] == 1 and run["status"] == "completed"
            and run["conclusion"] == conclusion and run["event"] == "workflow_dispatch"
            and run["head_branch"] == "main" and run["workflow_id"] == workflow["id"]
            and run["path"] == ".github/workflows/migrate-discord-checkpoint.yml"
            and run["actor"]["login"] == "vincentkoc" and run["triggering_actor"]["login"] == "vincentkoc"
            and re.fullmatch(r"[0-9a-f]{40}", run["head_sha"])
            and all(current.get(key) == run.get(key) for key in
                    ("id", "run_attempt", "status", "conclusion", "head_sha", "workflow_id", "event", "head_branch",
                     "path", "actor", "triggering_actor")), "diagnostic_original_run")
    return run


def failed_migration(run_id):
    return migration_run(run_id, "failure")["head_sha"]


def inspect_checkpoint(github, run_id, revision, manifest_sha, inventory_sha, directory, draft=True):
    releases = github.api("/releases?per_page=100")
    require(isinstance(releases, list) and len(releases) == 1
            and releases[0].get("tag_name") == f"discrawl-state-{run_id}-1", "diagnostic_release")
    release = github.api('/releases/' + str(releases[0]["id"]))
    manifest, assets, ref = release_manifest(github, release, directory, draft=draft)
    inventory = [{key: asset[key] for key in ("id", "name", "size", "digest")}
                 for _, asset in sorted(assets.items())]
    source = manifest["source"]
    require(ref["manifest_sha256"] == manifest_sha and digest(canonical(inventory)) == inventory_sha,
            "diagnostic_evidence_changed")
    require(manifest["parent"] is None and source["run_id"] == run_id and source["run_attempt"] == 1
            and source["revision"] == revision and source["workflow"] == "migrate-discord-checkpoint.yml"
            and source["outcome"] == "migration"
            and (release.get("published_at") is None if draft else isinstance(release.get("published_at"), str))
            and release.get("target_commitish") == source["archive_commit"], "diagnostic_source")
    latest = github.api("/releases/latest", missing=True)
    require(github.api("/git/ref/heads/main")["object"]["sha"] == source["archive_commit"]
            and (latest is None if draft else latest is not None and latest.get("id") == release["id"]
                 and latest.get("draft") is False), "diagnostic_publication_changed")
    # Download counts change during this probe; asset identity and bytes cannot.
    bound_assets = {name: {key: asset[key] for key in ("id", "name", "size", "digest", "state")}
                    for name, asset in assets.items()}
    return manifest, bound_assets, ref


def diagnose(expected, run_id, manifest_sha, inventory_sha):
    global DEADLINE
    DEADLINE = time.monotonic() + 900
    require(integer(run_id) and hash_value(manifest_sha) and hash_value(inventory_sha), "diagnostic_arguments")
    verify_context(expected)
    revision = failed_migration(run_id)
    verify_quiescence()
    github = GitHub(deadline=DEADLINE, read_only=True)
    with tempfile.TemporaryDirectory() as temp:
        directory = Path(temp)
        before = inspect_checkpoint(github, run_id, revision, manifest_sha, inventory_sha, directory)
        print("checkpoint diagnostic: metadata verified", flush=True)
        # Probe only the first authenticated part. No decompression, database
        # restore, cache read, release mutation, or success inference for other parts.
        part = before[0]["parts"][0]
        destination = directory / part["name"]
        github.download(before[1][part["name"]], destination)
        require(destination.stat().st_size == part["bytes"] and file_digest(destination) == part["sha256"],
                "diagnostic_part_checksum")
        destination.unlink()
        print("checkpoint diagnostic: first part verified", flush=True)
        after = directory / "after"
        after.mkdir()
        require(inspect_checkpoint(github, run_id, revision, manifest_sha, inventory_sha, after) == before,
                "diagnostic_evidence_changed")
        repo = github.api("")
        require(repo.get("id") == github.identity["id"] and repo.get("full_name") == github.repository
                and repo.get("private") is True and repo.get("archived") is False, "private_repository")
        verify_context(expected)
        require(failed_migration(run_id) == revision, "diagnostic_original_run")
        verify_quiescence()
        require(time.monotonic() < DEADLINE, "diagnostic_timeout")
    print("checkpoint diagnostic verified: read-only path; historical failure remains unclassified")


def successful_probe(probe_run, probe_revision, original_run, manifest_sha, inventory_sha):
    run = migration_run(probe_run, "success")
    require(run["head_sha"] == probe_revision, "finalize_probe_source")
    result = api(f"actions/runs/{probe_run}/attempts/1/jobs?per_page=100")
    jobs = result["jobs"]
    require(result["total_count"] == len(jobs) and len(jobs) in {2, 3}
            and {job["name"] for job in jobs} in ({"diagnose", "migrate"}, {"diagnose", "migrate", "finalize"}),
            "finalize_probe_jobs")
    for job in jobs:
        require(job["status"] == "completed" and job["run_id"] == probe_run
                and job["head_sha"] == probe_revision, "finalize_probe_jobs")
        if job["name"] != "diagnose":
            require(job["conclusion"] == "skipped" and not job.get("runner_id")
                    and not job.get("runner_name") and job["steps"] == [], "finalize_probe_jobs")
    job = next(job for job in jobs if job["name"] == "diagnose")
    require(job["conclusion"] == "success" and integer(job["id"]) and integer(job.get("runner_id")),
            "finalize_probe_jobs")
    steps = [step for step in job["steps"]
             if step["name"] == "Inspect retained draft and one existing part without changes"]
    require(len(steps) == 1 and steps[0]["status"] == "completed" and steps[0]["conclusion"] == "success",
            "finalize_probe_step")
    step = steps[0]
    start = datetime.fromisoformat(step["started_at"]).timestamp()
    # Actions step metadata has second precision; log lines retain fractions.
    end = datetime.fromisoformat(step["completed_at"]).timestamp() + 1
    pins = {"CHECKPOINT_MODE": "diagnose", "EXPECTED_SHA": probe_revision, "ORIGINAL_RUN": str(original_run),
            "MANIFEST_SHA256": manifest_sha, "INVENTORY_SHA256": inventory_sha,
            "MIGRATION_ACK": "DIAGNOSE_EXISTING_CHECKPOINT"}
    markers = ["checkpoint diagnostic: metadata verified", "checkpoint diagnostic: first part verified",
               "checkpoint diagnostic verified: read-only path; historical failure remains unclassified"]
    seen, executed = set(), []
    body = public_read(f'actions/jobs/{job["id"]}/logs', limit=256 * 1024)
    for line in body.decode("utf-8-sig").splitlines():
        match = re.fullmatch(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z) (.*)", line)
        if not match:
            continue
        at, text = datetime.fromisoformat(match[1]).timestamp(), match[2]
        key, separator, value = text.strip().partition(": ")
        if key in pins:
            require(separator and value == pins[key], "finalize_probe_inputs")
            if start <= at < end:
                require(not executed, "finalize_probe_order")
                seen.add(key)
        if text in markers:
            require(start <= at < end and len(seen) == len(pins)
                    and len(executed) < len(markers) and text == markers[len(executed)]
                    and (not executed or at >= executed[-1]), "finalize_probe_markers")
            executed.append(at)
    require(len(seen) == len(pins) and len(executed) == len(markers), "finalize_probe_markers")
    return job["id"]


def finalize(expected, run_id, manifest_sha, inventory_sha, probe_run, probe_revision):
    global DEADLINE
    final_deadline = time.monotonic() + 2400
    # Reserve three minutes for the single write and bounded readback. The
    # last minute is read-only classification if that write loses its response.
    DEADLINE = final_deadline - 180
    require(integer(run_id) and integer(probe_run) and run_id != probe_run
            and hash_value(manifest_sha) and hash_value(inventory_sha)
            and re.fullmatch(r"[0-9a-f]{40}", probe_revision or ""), "finalize_arguments")

    def admission():
        verify_context(expected)
        revision = failed_migration(run_id)
        successful_probe(probe_run, probe_revision, run_id, manifest_sha, inventory_sha)
        verify_quiescence()
        return revision

    revision = admission()
    github = GitHub(deadline=DEADLINE, read_only=True)
    with tempfile.TemporaryDirectory() as temp:
        directory = Path(temp)
        initial = directory / "initial"
        initial.mkdir()
        before = inspect_checkpoint(github, run_id, revision, manifest_sha, inventory_sha, initial)
        expected_receipt = {"manifest": before[0], "reference": before[2],
                            "assets": [{"name": name, "id": asset["id"], "bytes": asset["size"], "digest": asset["digest"]}
                                       for name, asset in sorted(before[1].items())]}
        required = before[0]["database"]["bytes"] + sum(part["bytes"] for part in before[0]["parts"])
        for number in (1, 2):
            started = time.monotonic()
            # Each independent restore owns one scratch image; it is gone
            # before the next begins, so capacity never assumes two full DBs.
            with tempfile.TemporaryDirectory(dir=directory) as scratch:
                print(f"checkpoint finalize: round={number} free_bytes={shutil.disk_usage(scratch).free} required_bytes={required}", flush=True)
                release = github.api('/releases/' + str(before[2]["release_id"]))
                receipt = download_database(github, release, Path(scratch), draft=True, deadline=DEADLINE)
                require(receipt == expected_receipt, "finalize_restore_changed")
            print(f"checkpoint finalize: round={number} verified elapsed_seconds={time.monotonic() - started:.3f}", flush=True)
        require(admission() == revision, "finalize_source_changed")
        writer = GitHub(deadline=final_deadline - 60)
        require(writer.identity == github.identity, "private_repository")
        recheck = directory / "recheck"
        recheck.mkdir()
        require(inspect_checkpoint(writer, run_id, revision, manifest_sha, inventory_sha, recheck) == before,
                "finalize_evidence_changed")
        check_deadline(DEADLINE)
        error = None
        try:
            published = writer.api('/releases/' + str(before[2]["release_id"]), {"draft": False, "make_latest": "true"})
            require(published.get("id") == before[2]["release_id"] and published.get("draft") is False,
                    "promotion_unverified")
        except (CheckpointError, OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as failure:
            error = str(failure) if isinstance(failure, CheckpointError) else "internal"
        state = "unknown"
        try:
            reader = GitHub(deadline=final_deadline, read_only=True)
            require(reader.identity == github.identity, "private_repository")
            observed = reader.api('/releases/' + str(before[2]["release_id"]))
            require(type(observed.get("draft")) is bool, "release_identity")
            after = directory / "after"
            after.mkdir()
            require(inspect_checkpoint(reader, run_id, revision, manifest_sha, inventory_sha, after,
                                       draft=observed["draft"]) == before, "finalize_evidence_changed")
            state = "unchanged_draft" if observed["draft"] else "published_latest"
        except (CheckpointError, OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as failure:
            if error is None:
                error = str(failure) if isinstance(failure, CheckpointError) else "internal"
        # A lost response remains a hold even if one read observes publication.
        # Never repeat or undo the PATCH, or infer that an uncertain child ended.
        require(error is None and state == "published_latest", "finalize_hold state=" + state
                + (" reason=" + error if error else ""))
    print("checkpoint finalized: full state verified twice; original provenance preserved")


def main():
    os.umask(0o077)
    mode = os.environ.get("CHECKPOINT_MODE", "migrate")
    if mode == "finalize":
        require(os.environ.get("MIGRATION_ACK") == "FINALIZE_EXISTING_CHECKPOINT", "migration_ack")
        finalize(os.environ.get("EXPECTED_SHA", ""), int(os.environ.get("ORIGINAL_RUN", "")),
                 os.environ.get("MANIFEST_SHA256", ""), os.environ.get("INVENTORY_SHA256", ""),
                 int(os.environ.get("PROBE_RUN", "")), os.environ.get("PROBE_REVISION", ""))
        return
    if mode == "diagnose":
        require(os.environ.get("MIGRATION_ACK") == "DIAGNOSE_EXISTING_CHECKPOINT", "migration_ack")
        diagnose(os.environ.get("EXPECTED_SHA", ""), int(os.environ.get("ORIGINAL_RUN", "")),
                 os.environ.get("MANIFEST_SHA256", ""), os.environ.get("INVENTORY_SHA256", ""))
        return
    require(mode == "migrate", "migration_mode")
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
    except (CheckpointError, OSError, ValueError, TypeError, KeyError, sqlite3.Error, subprocess.SubprocessError) as error:
        print("migration blocked: " + (str(error) if isinstance(error, CheckpointError) else "internal"), file=sys.stderr)
        sys.exit(1)
