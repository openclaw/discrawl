#!/usr/bin/env python3
"""Private, complete native SQLite checkpoints. Never use the filtered share export."""

import argparse
import base64
import contextlib
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import selectors
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time


PRODUCER = "openclaw/discrawl"
SCHEMA = "discrawl-private-checkpoint/v1"
MANIFEST = "checkpoint.json"
MAX_MANIFEST = 256 * 1024
MAX_PART = 1024 ** 3
RAW_PART = MAX_PART // 2
MAX_PARTS = 64
BLOCK = 1024 * 1024
REQUIRED_TABLES = {"guilds", "channels", "members", "messages", "sync_state", "failure_ledger"}
WORKFLOWS = {"publish-discord-backup.yml", "migrate-discord-checkpoint.yml"}
POLICY_REF = "refs/heads/maintainer-report-policy"
POLICY_PATH = "policy/report-selectors.json"
MAX_POLICY = 64 * 1024
MAX_TARGETS = 4096
API_PHASES = {"api_read", "api_write", "repository_read", "archive_ref_read", "release_list", "latest_read", "release_read",
              "asset_list", "manifest_download", "part_download", "release_create", "asset_upload",
              "release_promote", "release_delete", "source_ref_read", "source_run_read",
              "source_workflow_read", "source_active_runs", "source_cache_read", "source_log_read"}


class CheckpointError(Exception):
    """Only fixed error codes may reach the public job log."""


def require(condition, code):
    if not condition:
        raise CheckpointError(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def check_deadline(deadline):
    require(deadline is None or time.monotonic() < deadline, "checkpoint_timeout")


def file_digest(path, deadline=None):
    with open(path, "rb") as file:
        if deadline is None:
            return hashlib.file_digest(file, "sha256").hexdigest()
        result = hashlib.sha256()
        while chunk := file.read(BLOCK):
            check_deadline(deadline)
            result.update(chunk)
        check_deadline(deadline)
        return result.hexdigest()


def strict_json(data):
    require(len(data) <= MAX_MANIFEST, "manifest_size")

    def object_pairs(pairs):
        result = dict(pairs)
        require(len(result) == len(pairs), "manifest_duplicate_key")
        return result

    return json.loads(data, object_pairs_hook=object_pairs)


def keys(value, expected):
    require(isinstance(value, dict) and set(value) == set(expected.split()), "manifest_fields")


def integer(value, maximum=2 ** 53 - 1, minimum=1):
    return type(value) is int and minimum <= value <= maximum


def hash_value(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)


def reference(value):
    keys(value, "release_id manifest_sha256 database_sha256")
    require(integer(value["release_id"]) and hash_value(value["manifest_sha256"])
            and hash_value(value["database_sha256"]), "parent_identity")


def database_facts(db, deadline=None):
    check_deadline(deadline)
    if deadline is not None:
        db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 10000)
    try:
        return _database_facts(db)
    except sqlite3.OperationalError:
        check_deadline(deadline)
        raise
    finally:
        if deadline is not None:
            db.set_progress_handler(None, 0)
        check_deadline(deadline)


def _database_facts(db):
    require(db.execute("PRAGMA integrity_check").fetchall() == [("ok",)], "sqlite_integrity")
    require(not db.execute("PRAGMA foreign_key_check").fetchall(), "sqlite_foreign_keys")
    schema = db.execute("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name").fetchall()
    tables = [row[1] for row in schema if row[0] == "table"]
    require(REQUIRED_TABLES <= set(tables) and len(tables) <= 512, "native_tables")
    counts = {name: db.execute('SELECT count(*) FROM "' + name.replace('"', '""') + '"').fetchone()[0]
              for name in tables}
    return {"user_version": db.execute("PRAGMA user_version").fetchone()[0],
            "schema_sha256": digest(canonical(schema)), "tables": counts}


def readonly(path):
    require(path.is_file() and not path.is_symlink(), "database_file")
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)


def snapshot(source, destination, timeout=600):
    deadline = time.monotonic() + timeout

    def progress(_status, _remaining, _total):
        require(time.monotonic() < deadline, "backup_timeout")

    # Pin the source image before reading metadata. In WAL mode a later writer
    # must not change the image copied by backup or the manifest derived from it.
    with contextlib.closing(readonly(source)) as src:
        src.execute("BEGIN")
        src.execute("SELECT count(*) FROM sqlite_schema").fetchone()
        logical_bytes = src.execute("PRAGMA page_count").fetchone()[0] * src.execute("PRAGMA page_size").fetchone()[0]
        require(0 < logical_bytes <= RAW_PART * MAX_PARTS, "checkpoint_capacity")
        # The main file can be tiny while committed pages are still in WAL.
        # Reserve the standalone image and worst-case compressed parts together.
        require(shutil.disk_usage(destination.parent).free > logical_bytes * 2 + BLOCK * MAX_PARTS, "checkpoint_capacity")
        facts = database_facts(src)
        with contextlib.closing(sqlite3.connect(destination)) as out:
            src.backup(out, pages=256, progress=progress, sleep=0.05)
            out.execute("PRAGMA journal_mode=DELETE")
        src.rollback()
    with contextlib.closing(readonly(destination)) as restored:
        require(database_facts(restored) == facts, "backup_parity")
    return facts


def pack(database, directory):
    parts = []
    with open(database, "rb") as source:
        while source.tell() < database.stat().st_size:
            require(len(parts) < MAX_PARTS, "checkpoint_capacity")
            name = f"state-{len(parts):03d}.sqlite.gz"
            target = directory / name
            raw_bytes = 0
            with target.open("xb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
                while raw_bytes < RAW_PART:
                    chunk = source.read(min(BLOCK, RAW_PART - raw_bytes))
                    if not chunk:
                        break
                    compressed.write(chunk)
                    raw_bytes += len(chunk)
            size = target.stat().st_size
            require(0 < size <= MAX_PART, "part_size")
            parts.append({"name": name, "bytes": size, "raw_bytes": raw_bytes, "sha256": file_digest(target)})
    return parts


def validate_manifest(value):
    keys(value, "schema boundary repository source parent migration database parts")
    keys(value["repository"], "name id")
    require(value["schema"] == SCHEMA and value["boundary"] == "native-ingestion-before-publication"
            and isinstance(value["repository"]["name"], str)
            and re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", value["repository"]["name"])
            and integer(value["repository"]["id"]), "manifest_identity")
    source = value["source"]
    keys(source, "repository revision run_id run_attempt workflow outcome archive_commit")
    require(source["repository"] == PRODUCER and source["workflow"] in WORKFLOWS
            and source["outcome"] in {"success", "failure", "cancelled", "migration"}
            and integer(source["run_id"]) and integer(source["run_attempt"])
            and re.fullmatch(r"[0-9a-f]{40}", source["revision"] or "")
            and re.fullmatch(r"[0-9a-f]{40}", source["archive_commit"] or ""), "source_identity")
    if value["parent"] is not None:
        reference(value["parent"])
        require(value["migration"] is None and source["outcome"] != "migration", "parent_identity")
    else:
        cache = value["migration"]
        keys(cache, "id key ref version created_at size_in_bytes")
        require(source["outcome"] == "migration" and source["workflow"] == "migrate-discord-checkpoint.yml"
                and integer(cache["id"]) and integer(cache["size_in_bytes"])
                and cache["ref"] == "refs/heads/main" and hash_value(cache["version"])
                and re.fullmatch(r"discrawl-discord-db-Linux-main-[1-9][0-9]*-[1-9][0-9]*", cache["key"] or "")
                and re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,9}Z", cache["created_at"] or ""), "migration_identity")
    database = value["database"]
    keys(database, "bytes sha256 user_version schema_sha256 tables")
    require(integer(database["bytes"], RAW_PART * MAX_PARTS)
            and hash_value(database["sha256"]) and hash_value(database["schema_sha256"])
            and integer(database["user_version"]) and isinstance(database["tables"], dict)
            and REQUIRED_TABLES <= database["tables"].keys() and len(database["tables"]) <= 512
            and all(isinstance(k, str) and integer(v, minimum=0) for k, v in database["tables"].items()), "database_manifest")
    parts = value["parts"]
    require(isinstance(parts, list) and 0 < len(parts) <= MAX_PARTS, "part_inventory")
    for index, part in enumerate(parts):
        keys(part, "name bytes raw_bytes sha256")
        require(part["name"] == f"state-{index:03d}.sqlite.gz" and integer(part["bytes"], MAX_PART)
                and integer(part["raw_bytes"], RAW_PART) and hash_value(part["sha256"]), "part_inventory")
    require(sum(part["raw_bytes"] for part in parts) == database["bytes"], "part_inventory")
    return value


def unpack(manifest, directory, destination, deadline=None):
    check_deadline(deadline)
    with destination.open("xb") as output:
        for part in manifest["parts"]:
            file = directory / part["name"]
            require(file.stat().st_size == part["bytes"] and file_digest(file, deadline) == part["sha256"], "part_checksum")
            written = 0
            with gzip.open(file, "rb") as compressed:
                while chunk := compressed.read(min(BLOCK, part["raw_bytes"] - written + 1)):
                    check_deadline(deadline)
                    written += len(chunk)
                    require(written <= part["raw_bytes"], "expanded_size")
                    output.write(chunk)
            require(written == part["raw_bytes"], "expanded_size")
        output.flush()
        os.fsync(output.fileno())
    expected = manifest["database"]
    require(destination.stat().st_size == expected["bytes"] and file_digest(destination, deadline) == expected["sha256"], "database_checksum")
    with contextlib.closing(readonly(destination)) as db:
        require(database_facts(db, deadline) == {k: expected[k] for k in ("user_version", "schema_sha256", "tables")}, "restore_parity")


def reap_command(child, phase):
    # A reaped leader and closed stderr do not prove stdout is stable: a
    # descendant can still own it. Unknown group state must not yield bytes.
    deadline = time.monotonic() + 5
    code = "checkpoint_api phase=" + phase + " exit=reap"
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError as error:
        raise CheckpointError(code) from error
    try:
        child.wait(timeout=max(0, deadline - time.monotonic()))
    except subprocess.TimeoutExpired as error:
        raise CheckpointError(code) from error
    while True:
        try:
            os.killpg(child.pid, 0)
        except ProcessLookupError:
            return
        except PermissionError as error:
            raise CheckpointError(code) from error
        require(time.monotonic() < deadline, code)
        time.sleep(0.05)


def github_command(arguments, env, phase, destination=None, timeout=600, missing=False,
                   limit=MAX_MANIFEST, deadline=None):
    require(phase in API_PHASES, "api_phase")
    command_deadline = time.monotonic() + timeout
    if deadline is not None:
        command_deadline = min(command_deadline, deadline)
    require(time.monotonic() < command_deadline, "checkpoint_api phase=" + phase + " exit=timeout")

    def bound_output():
        resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))

    with (destination.open("xb") if destination is not None else tempfile.TemporaryFile()) as output:
        child = subprocess.Popen(["gh", *arguments], stdout=output, stderr=subprocess.PIPE,
                                 env=env, preexec_fn=bound_output, start_new_session=True)
        captured = bytearray()
        failure = None
        try:
            # Drain even after the retained prefix fills: waiting on a full
            # stderr pipe deadlocks, and inherited pipes need the same deadline.
            with selectors.DefaultSelector() as selector:
                os.set_blocking(child.stderr.fileno(), False)
                selector.register(child.stderr, selectors.EVENT_READ)
                while selector.get_map() or child.poll() is None:
                    require(time.monotonic() < command_deadline, "checkpoint_api phase=" + phase + " exit=timeout")
                    for key, _ in selector.select(min(0.1, max(0, command_deadline - time.monotonic()))):
                        try:
                            chunk = os.read(key.fd, 16384)
                        except BlockingIOError:
                            continue
                        if not chunk:
                            selector.unregister(key.fileobj)
                        captured.extend(chunk[:max(0, 65536 - len(captured))])
        except BaseException as error:
            failure = error
        finally:
            try:
                reap_command(child, phase)
            finally:
                child.stderr.close()
        if failure is not None:
            raise failure
        size = output.tell()
        output.seek(0)
        raw = output.read(min(size, limit)) if destination is None else b""
        if missing and child.returncode != 0:
            if re.match(rb"HTTP/\S+ 404\b", raw):
                return None
        if child.returncode != 0:
            status = re.search(rb"(?:^HTTP/\S+ |\(HTTP )([1-5][0-9]{2})(?:\b|\))", raw[:4096] + bytes(captured))
            raise CheckpointError(f"checkpoint_api phase={phase} exit={child.returncode}"
                                  + (" http=" + status[1].decode("ascii") if status else ""))
        if destination is not None:
            require(size <= limit, "asset_size")
            return None
        require(size <= limit, "checkpoint_api_size")
        return raw


class GitHub:
    def __init__(self, deadline=None, read_only=False):
        self.deadline = deadline
        self.read_only = read_only
        self.repository = os.environ.get("DISCRAWL_BACKUP_REPOSITORY", "")
        require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", self.repository), "checkpoint_repository")
        self.base = "repos/" + self.repository
        repo = self.api("")
        require(repo.get("full_name") == self.repository and repo.get("private") is True
                and repo.get("archived") is False and integer(repo.get("id")), "private_repository")
        self.identity = {"name": self.repository, "id": repo["id"]}

    def command(self, arguments, destination=None, timeout=600, missing=False, phase="repository_read", limit=None):
        # Private custody uses only its existing token. Public admission passes
        # its own workflow token to the same bounded process owner.
        env = {**os.environ, "GH_TOKEN": os.environ.get("DISCORD_BACKUP_TOKEN", ""),
               "GH_PROMPT_DISABLED": "1", "GH_DEBUG": "", "GH_HOST": "github.com"}
        require(env["GH_TOKEN"], "checkpoint_credential")
        require(not self.read_only or (arguments[0] == "api" and "--method" not in arguments
                                      and "--input" not in arguments), "diagnostic_read_only")
        if self.read_only:
            arguments = [*arguments, "--method", "GET"]
        limit = limit if limit is not None else MAX_PART if destination is not None else MAX_MANIFEST
        return github_command(arguments, env, phase, destination, timeout, missing, limit, self.deadline)

    def api(self, suffix, data=None, missing=False):
        if suffix == "":
            phase = "repository_read"
        elif suffix == "/git/ref/heads/main":
            phase = "archive_ref_read"
        elif suffix.startswith("/releases?"):
            phase = "release_list"
        elif suffix == "/releases/latest":
            phase = "latest_read"
        elif re.fullmatch(r"/releases/[1-9][0-9]*/assets\?per_page=100", suffix):
            phase = "asset_list"
        elif re.fullmatch(r"/releases/[1-9][0-9]*", suffix):
            phase = "release_promote" if data is not None else "release_read"
        else:
            phase = "api_write" if data is not None else "api_read"
        args = ["api", self.base + suffix, "--include", "-H", "X-GitHub-Api-Version: 2022-11-28"]
        def parse(raw):
            if raw is None:
                return None
            parts = re.split(rb"\r?\n\r?\n", raw, maxsplit=1)
            require(len(parts) == 2 and re.match(rb"HTTP/\S+ 2[0-9][0-9]\b", parts[0]), "checkpoint_api_status phase=" + phase)
            return strict_json(parts[1])
        if data is None:
            return parse(self.command(args, missing=missing, phase=phase, timeout=60 if self.read_only else 600))
        with tempfile.NamedTemporaryFile(mode="wb") as body:
            body.write(canonical(data))
            body.flush()
            return parse(self.command([*args, "--method", "PATCH", "--input", body.name], phase=phase))

    def create(self, tag, target):
        with tempfile.NamedTemporaryFile(mode="wb") as body:
            body.write(canonical({"tag_name": tag, "target_commitish": target, "name": tag,
                                  "draft": True, "prerelease": False, "make_latest": "false"}))
            body.flush()
            return strict_json(self.command(["api", self.base + "/releases", "--method", "POST", "--input", body.name],
                                            phase="release_create"))

    def download(self, asset, destination):
        manifest = asset["name"] == MANIFEST
        self.command(["api", self.base + "/releases/assets/" + str(asset["id"]),
                      "-H", "Accept: application/octet-stream"], destination,
                     timeout=60 if self.read_only and manifest else 600,
                     phase="manifest_download" if manifest else "part_download",
                     limit=MAX_MANIFEST if manifest else MAX_PART)


def release_manifest(github, release, directory, draft=False):
    require(integer(release.get("id")) and release.get("draft") is draft
            and release.get("prerelease") is False, "release_identity")
    assets = github.api(f'/releases/{release["id"]}/assets?per_page=100')
    require(isinstance(assets, list) and 1 < len(assets) <= MAX_PARTS + 1
            and len({a.get("name") for a in assets}) == len(assets)
            and len({a.get("id") for a in assets}) == len(assets)
            and all(integer(a.get("id")) and a.get("state") == "uploaded" for a in assets), "asset_inventory")
    by_name = {asset["name"]: asset for asset in assets}
    require(MANIFEST in by_name and integer(by_name[MANIFEST].get("size"), MAX_MANIFEST), "manifest_asset")
    github.download(by_name[MANIFEST], directory / MANIFEST)
    raw = (directory / MANIFEST).read_bytes()
    require(len(raw) == by_name[MANIFEST]["size"]
            and by_name[MANIFEST].get("digest") == "sha256:" + digest(raw), "manifest_checksum")
    manifest = validate_manifest(strict_json(raw))
    require(manifest["repository"] == github.identity, "repository_identity")
    source = manifest["source"]
    require(release.get("tag_name") == f'discrawl-state-{source["run_id"]}-{source["run_attempt"]}', "release_tag")
    require(set(by_name) == {MANIFEST, *(part["name"] for part in manifest["parts"])}, "asset_inventory")
    for part in manifest["parts"]:
        require(by_name[part["name"]].get("size") == part["bytes"]
                and by_name[part["name"]].get("digest") == "sha256:" + part["sha256"], "asset_checksum")
    return manifest, by_name, {"release_id": release["id"], "manifest_sha256": digest(raw),
                               "database_sha256": manifest["database"]["sha256"]}


def download_database(github, release, directory, draft=False, deadline=None):
    check_deadline(deadline)
    manifest, assets, ref = release_manifest(github, release, directory, draft)
    require(shutil.disk_usage(directory).free > manifest["database"]["bytes"]
            + sum(p["bytes"] for p in manifest["parts"]), "checkpoint_capacity")
    for part in manifest["parts"]:
        check_deadline(deadline)
        github.download(assets[part["name"]], directory / part["name"])
    unpack(manifest, directory, directory / "restored.sqlite", deadline)
    inventory = [{"name": name, "id": asset["id"], "bytes": asset["size"], "digest": asset["digest"]}
                 for name, asset in sorted(assets.items())]
    return {"reference": ref, "manifest": manifest, "assets": inventory}


def execution_context():
    workflow = os.environ.get("GITHUB_WORKFLOW_REF", "")
    prefix = PRODUCER + "/.github/workflows/"
    require(os.environ.get("GITHUB_REPOSITORY") == PRODUCER
            and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_EVENT_NAME") in {"schedule", "workflow_dispatch"}
            and os.environ.get("GITHUB_WORKFLOW_SHA") == os.environ.get("GITHUB_SHA")
            and workflow.startswith(prefix) and workflow.endswith("@refs/heads/main")
            and os.environ.get("RUNNER_DEBUG") != "1" and os.environ.get("ACTIONS_STEP_DEBUG") != "true"
            and os.environ.get("ACTIONS_RUNNER_DEBUG") != "true", "execution_context")
    name = workflow[len(prefix):].split("@")[0]
    require(name in WORKFLOWS | {"discord-backup-report.yml"}
            and re.fullmatch(r"[0-9a-f]{40}", os.environ.get("GITHUB_SHA", "")), "execution_context")
    return name


def source_identity(outcome, archive_commit):
    workflow = execution_context()
    return {"repository": PRODUCER, "revision": os.environ["GITHUB_SHA"],
            "run_id": int(os.environ["GITHUB_RUN_ID"]), "run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
            "workflow": workflow, "outcome": outcome, "archive_commit": archive_commit}


def restore(github, database, receipt, prune=False):
    require(not database.exists() and not database.is_symlink()
            and not any(Path(str(database) + suffix).exists() for suffix in ("-wal", "-shm", "-journal")), "restore_destination")
    if prune:
        require_no_drafts(github)
    release = github.api("/releases/latest")  # Pin once; never mix assets from a changing latest pointer.
    database.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.TemporaryDirectory(dir=database.parent) as temp:
        directory = Path(temp)
        saved = download_database(github, release, directory)
        if prune:
            retention = directory / "retention"
            retention.mkdir()
            retire_grandparent(github, saved, retention)
        os.replace(directory / "restored.sqlite", database)
    receipt.write_bytes(canonical(saved))
    return saved["reference"]


def read_parent(github, receipt, directory):
    saved = strict_json(receipt.read_bytes())
    keys(saved, "reference manifest assets")
    reference(saved["reference"])
    validate_manifest(saved["manifest"])
    require(github.api("/releases/latest")["id"] == saved["reference"]["release_id"], "parent_changed")
    release = github.api('/releases/' + str(saved["reference"]["release_id"]))
    manifest, assets, ref = release_manifest(github, release, directory)
    inventory = [{"name": name, "id": asset["id"], "bytes": asset["size"], "digest": asset["digest"]}
                 for name, asset in sorted(assets.items())]
    require(ref == saved["reference"] and manifest == saved["manifest"]
            and inventory == saved["assets"], "parent_changed")
    return saved


def retire_grandparent(github, current, directory):
    parent_ref = current["manifest"]["parent"]
    if parent_ref is None:
        return
    release = github.api('/releases/' + str(parent_ref["release_id"]))
    manifest, _, ref = release_manifest(github, release, directory)
    require(ref == parent_ref and ref["release_id"] != current["reference"]["release_id"], "retention_lineage")
    # Only the exact grandparent named by the verified parent's manifest is
    # eligible. A failed deletion blocks the next writer instead of accumulating.
    grandparent = manifest["parent"]
    if grandparent is not None:
        require(grandparent["release_id"] not in {ref["release_id"], current["reference"]["release_id"]}, "retention_lineage")
        candidate = github.api('/releases/' + str(grandparent["release_id"]), missing=True)
        if candidate is None:
            return
        with tempfile.TemporaryDirectory(dir=directory) as temp:
            _, _, actual = release_manifest(github, candidate, Path(temp))
        require(actual == grandparent, "retention_lineage")
        require(github.api("/releases/latest")["id"] == current["reference"]["release_id"], "parent_changed")
        github.command(["api", github.base + '/releases/' + str(grandparent["release_id"]), "--method", "DELETE"], phase="release_delete")
        require(github.api('/releases/' + str(grandparent["release_id"]), missing=True) is None, "retirement_unverified")


def require_no_drafts(github):
    releases = github.api("/releases?per_page=100")
    require(isinstance(releases, list) and len(releases) < 100, "release_inventory")
    # An interrupted upload remains a draft. Hold subsequent writers until
    # that exact private attempt is reconciled; never accumulate or delete it.
    require(not any(release.get("draft") is True for release in releases), "pending_draft_requires_reconciliation")
    return releases


def save(github, database, receipt, outcome, migration=None):
    with tempfile.TemporaryDirectory(dir=database.parent) as temp:
        directory = Path(temp)
        releases = require_no_drafts(github)
        if migration is None:
            parent_dir = directory / "parent"
            parent_dir.mkdir()
            parent = read_parent(github, receipt, parent_dir)
        else:
            require(releases == [], "migration_not_empty")
            parent = None
        archive_commit = github.api("/git/ref/heads/main")["object"]["sha"]
        source = source_identity(outcome, archive_commit)
        standalone = directory / "checkpoint.sqlite"
        facts = snapshot(database, standalone)
        manifest = {"schema": SCHEMA, "boundary": "native-ingestion-before-publication",
                    "repository": github.identity, "source": source,
                    "parent": parent["reference"] if parent else None, "migration": migration,
                    "database": {**facts, "bytes": standalone.stat().st_size, "sha256": file_digest(standalone)},
                    "parts": pack(standalone, directory)}
        validate_manifest(manifest)
        (directory / MANIFEST).write_bytes(canonical(manifest))
        tag = f'discrawl-state-{source["run_id"]}-{source["run_attempt"]}'
        draft = github.create(tag, archive_commit)
        require(draft.get("draft") is True and draft.get("tag_name") == tag and integer(draft.get("id")), "draft_identity")
        github.command(["release", "upload", tag, "--repo", github.repository,
                        str(directory / MANIFEST), *(str(directory / part["name"]) for part in manifest["parts"])], phase="asset_upload")
        standalone.unlink()
        for part in manifest["parts"]:
            (directory / part["name"]).unlink()
        verification = directory / "verification"
        verification.mkdir()
        current = download_database(github, github.api('/releases/' + str(draft["id"])), verification, draft=True)
        require(current["manifest"] == manifest, "uploaded_manifest")
        if parent:
            recheck = directory / "recheck"
            recheck.mkdir()
            require(read_parent(github, receipt, recheck) == parent, "parent_changed")
        else:
            require(github.api("/releases/latest", missing=True) is None, "parent_changed")
        published = github.api('/releases/' + str(draft["id"]), {"draft": False, "make_latest": "true"})
        require(published.get("id") == draft["id"] and published.get("draft") is False
                and github.api("/releases/latest")["id"] == draft["id"], "promotion_unverified")
        retention = directory / "retention"
        retention.mkdir()
        retire_grandparent(github, current, retention)
        return current["reference"]


def report_ready(receipt, archive):
    saved = strict_json(receipt.read_bytes())
    manifest = validate_manifest(saved["manifest"])
    body_path, producer_path = archive / "manifest.json", archive / "producer.json"
    require(body_path.is_file() and not body_path.is_symlink()
            and body_path.stat().st_size <= MAX_MANIFEST
            and producer_path.is_file() and not producer_path.is_symlink()
            and producer_path.stat().st_size <= 4096, "publication_receipt")
    body = body_path.read_bytes()
    published = strict_json(body)
    producer = strict_json(producer_path.read_bytes())
    keys(producer, "schema manifest_path manifest_sha256 producer")
    keys(producer["producer"], "repository revision run_id run_attempt")
    require(producer["schema"] == 1 and producer["manifest_path"] == "manifest.json"
            and published.get("files", {}).get("producer") == "producer.json"
            and producer["manifest_sha256"] == digest(body), "publication_receipt")
    expected = {key: manifest["source"][key] for key in producer["producer"]}
    return manifest["source"]["outcome"] == "success" and producer["producer"] == expected


def validate_policy(raw, expected_sha256, guild):
    require(len(raw) <= MAX_POLICY and hash_value(expected_sha256)
            and digest(raw) == expected_sha256, "policy_checksum")
    policy = strict_json(raw)
    keys(policy, "schema policy catalog guild_id message_root_ids forum_root_ids retained_threads")
    require(type(policy["schema"]) is int and policy["schema"] == 1, "policy_schema")
    source, catalog = policy["policy"], policy["catalog"]
    keys(source, "repository revision path sha256")
    keys(catalog, "archive_commit manifest_sha256")
    commit = lambda value: isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value)
    require(isinstance(source["repository"], str)
            and re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", source["repository"])
            and commit(source["revision"]) and hash_value(source["sha256"])
            and isinstance(source["path"], str) and len(source["path"]) <= 512
            and re.fullmatch(r"[A-Za-z0-9_./-]+", source["path"])
            and all(part not in {"", ".", ".."} for part in source["path"].split("/"))
            and commit(catalog["archive_commit"]) and hash_value(catalog["manifest_sha256"]), "policy_provenance")
    native_id = lambda value: (isinstance(value, str) and re.fullmatch(r"[1-9][0-9]{0,19}", value)
                               and int(value) < 2 ** 64)
    require(native_id(guild) and policy["guild_id"] == guild, "policy_guild")
    targets = []
    for name in ("message_root_ids", "forum_root_ids"):
        values = policy[name]
        require(isinstance(values, list) and len(values) <= MAX_TARGETS
                and all(native_id(value) for value in values), "policy_targets")
        targets.extend(values)
    parents = set(targets)
    threads = policy["retained_threads"]
    require(isinstance(threads, list) and len(threads) <= MAX_TARGETS, "policy_targets")
    for thread in threads:
        keys(thread, "id parent_id")
        require(native_id(thread["id"]) and thread["parent_id"] in parents, "policy_thread_parent")
        targets.append(thread["id"])
    require(0 < len(targets) <= MAX_TARGETS and len(set(targets)) == len(targets), "policy_targets")
    return policy, sorted(targets, key=int)


def load_policy(github, revision, expected_sha256, guild, destination):
    require(isinstance(revision, str) and re.fullmatch(r"[0-9a-f]{40}", revision)
            and hash_value(expected_sha256) and not destination.exists(), "policy_pin")
    ref = github.api("/git/ref/heads/maintainer-report-policy")
    require(isinstance(ref, dict) and ref.get("ref") == POLICY_REF
            and isinstance(ref.get("object"), dict) and ref["object"].get("type") == "commit"
            and ref["object"].get("sha") == revision, "policy_ref")
    blob = github.api("/contents/" + POLICY_PATH + "?ref=" + revision)
    require(isinstance(blob, dict) and blob.get("type") == "file" and blob.get("path") == POLICY_PATH
            and integer(blob.get("size"), MAX_POLICY) and blob.get("encoding") == "base64"
            and isinstance(blob.get("content"), str), "policy_file")
    raw = base64.b64decode(blob["content"].replace("\n", ""), validate=True)
    require(len(raw) == blob["size"], "policy_size")
    validate_policy(raw, expected_sha256, guild)
    # This private input stays outside the archive, SQLite, and upload inventory.
    with destination.open("xb") as file:
        file.write(raw)
    destination.chmod(0o600)


def acquisition_since(value=None):
    now = datetime.now(timezone.utc)
    if not value:
        value = (now.replace(hour=0, minute=0, second=0, microsecond=0)
                 - timedelta(days=1)).isoformat().replace("+00:00", "Z")
    require(isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}T00:00:00Z", value), "acquisition_window")
    since = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(timedelta() < now - since < timedelta(hours=48), "acquisition_window")
    return value


def acquire(native, config, policy_path, expected_sha256, guild, completion, timeout=1200, since=None):
    require(policy_path.is_file() and not policy_path.is_symlink()
            and policy_path.stat().st_size <= MAX_POLICY, "policy_file")
    _, targets = validate_policy(policy_path.read_bytes(), expected_sha256, guild)
    require(0 < timeout <= 1200, "writer_arguments")
    since = acquisition_since(since)
    base = [str(native), "--config", str(config), "sync", "--source", "discord", "--guild", guild,
            "--skip-members", "--no-update"]
    # General archive refresh remains first; report roots only scope the extra
    # bounded scan. Native forum expansion and report privacy keep their owners.
    commands = [[*base, "--latest-only"],
                [*base, "--full", "--since", since, "--channels", ",".join(targets), "--with-media=false"]]
    return run_writers(commands, timeout, completion)


def run_writer(command, timeout, completion=None):
    return run_writers([command], timeout, completion)


def run_writers(commands, timeout, completion=None):
    require(commands and all(commands) and timeout > 0, "writer_arguments")
    require(completion is None or not completion.exists(), "writer_receipt_exists")
    child = None
    cancelled = False

    def stop(_signal, _frame):
        nonlocal cancelled
        cancelled = True
        if child and child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)

    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        with open(os.devnull, "wb") as output:
            env = {key: value for key, value in os.environ.items() if key not in {"GH_TOKEN", "DISCORD_BACKUP_TOKEN"}}
            deadline = time.monotonic() + timeout
            result = 1
            for command in commands:
                if cancelled or time.monotonic() >= deadline:
                    cancelled = True
                    break
                try:
                    child = subprocess.Popen(command, stdout=output, stderr=output, env=env, start_new_session=True)
                except OSError:
                    # Prior phases are reaped; preserve their committed progress.
                    result = 1
                    break
                while child.poll() is None and not cancelled and time.monotonic() < deadline:
                    try:
                        child.wait(timeout=0.2)
                    except subprocess.TimeoutExpired:
                        pass
                if child.poll() is None:
                    cancelled = True
                    os.killpg(child.pid, signal.SIGTERM)
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.wait(timeout=5)
                # No next phase or checkpoint may run while descendants write.
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(child.pid, signal.SIGKILL)
                reap_deadline = time.monotonic() + 5
                while True:
                    try:
                        os.killpg(child.pid, 0)
                    except ProcessLookupError:
                        break
                    except PermissionError as error:
                        raise CheckpointError("writer_reap") from error
                    require(time.monotonic() < reap_deadline, "writer_reap")
                    time.sleep(0.05)
                result = 1 if cancelled else child.returncode
                if result != 0:
                    break
            result = 1 if cancelled else result
            if completion is not None:
                completion.write_bytes(canonical({"reaped": True, "outcome": "cancelled" if cancelled else
                                                   "success" if result == 0 else "failure"}))
            return result
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("restore", "save", "migrate", "run", "report-check", "policy", "acquire"))
    parser.add_argument("--db", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--writer-receipt", type=Path)
    parser.add_argument("--cache-identity", type=Path)
    parser.add_argument("--timeout", type=int, default=2700)
    parser.add_argument("--prune", action="store_true")
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--policy", type=Path)
    parser.add_argument("--policy-commit")
    parser.add_argument("--policy-sha256")
    parser.add_argument("--native", type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--since")
    args, command = parser.parse_known_args()
    if args.operation == "run":
        return run_writer(command[1:] if command[:1] == ["--"] else command, args.timeout, args.writer_receipt)
    require(not command, "arguments")
    if args.operation in {"policy", "acquire"}:
        execution_context()
        require(args.policy is not None, "arguments")
        guild = os.environ.get("DISCRAWL_GUILD_ID", "")
        if args.operation == "policy":
            since = acquisition_since(args.since)
            load_policy(GitHub(), args.policy_commit, args.policy_sha256, guild, args.policy)
            print("since=" + since)
            return 0
        require(args.native is not None and args.config is not None and args.writer_receipt is not None, "arguments")
        return acquire(args.native, args.config, args.policy, args.policy_sha256, guild, args.writer_receipt, args.timeout, args.since)
    if args.operation == "report-check":
        require(args.receipt is not None and args.archive is not None, "arguments")
        return 0 if report_ready(args.receipt, args.archive) else 3
    require(args.db is not None and args.receipt is not None, "arguments")
    execution_context()
    github = GitHub()
    if args.operation == "restore":
        ref = restore(github, args.db, args.receipt, args.prune)
    else:
        migration = strict_json(args.cache_identity.read_bytes()) if args.operation == "migrate" else None
        outcome = "migration"
        if migration is None:
            require(args.writer_receipt is not None, "writer_receipt")
            completed = strict_json(args.writer_receipt.read_bytes())
            keys(completed, "reaped outcome")
            require(completed["reaped"] is True and completed["outcome"] in {"success", "failure", "cancelled"}, "writer_receipt")
            outcome = completed["outcome"]
        ref = save(github, args.db, args.receipt, outcome, migration)
    print("private checkpoint verified")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (CheckpointError, OSError, ValueError, TypeError, KeyError, sqlite3.Error, subprocess.SubprocessError) as error:
        print("checkpoint failed: " + (str(error) if isinstance(error, CheckpointError) else "internal"), file=sys.stderr)
        sys.exit(1)
