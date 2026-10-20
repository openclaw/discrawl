# Private workflow checkpoints

The archive publisher restores complete native SQLite state from private GitHub
Releases in the repository configured by `DISCRAWL_BACKUP_REPOSITORY`. It uses
the existing `DISCORD_BACKUP_TOKEN`; no additional credential is required.
The destination must remain private and keep the same repository ID. Assets are
uploaded only to drafts. Published assets are never changed; retention deletes
the exact whole release and preserves tags. This also supports
[immutable releases](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases)
without requesting repository administration permissions.

## Boundary and recovery

A checkpoint contains every native table, including crawl cursors, retry history,
FTS and future tables. It is separate from the filtered Git snapshot. A missing
or invalid checkpoint stops acquisition; the workflows never import a published
snapshot to replace native state.

The publisher supervises a built native executable. After the writer exits, the
supervisor terminates any remaining task-owned descendants and requires the whole
process group to disappear before issuing its completion receipt. Only then can
the checkpoint helper open a read transaction and copy SQLite through its backup
API. The pinned image supplies schema, table counts and integrity results.

The checkpoint is the **native ingestion boundary before Git publication**.
It deliberately precedes the publisher's later `share:*` bookkeeping. Replaying
that boundary preserves messages, native cursors and failure history. Every
workflow sets `DISCRAWL_NO_AUTO_UPDATE=1`; sync also uses `--no-update`. The daily
reporter restores state without subscribing to or importing the Git archive.

Each save creates a unique draft release, uploads bounded compressed parts and
a strict manifest, then downloads the exact asset IDs and verifies their bytes,
full SQLite integrity, schema and all table counts. It promotes latest only
after that readback and a fresh check of its parent release and asset identities.
Workflow concurrency provides serialization; GitHub's latest update is not a
server-side compare-and-swap operation.

Checkpoint saves use three sequential steps with independent deadlines:
540 seconds for source integrity, backup, compression and upload; 900 seconds
for authenticated download, reconstruction, disk hashing and full SQLite
verification; and 180 seconds for promotion and retention. Their outer step
caps are 10, 16 and 4 minutes respectively. The same split applies to initial
migration. Slow SQLite verification can exhaust only its own budget; it cannot
consume the promotion reserve.

The upload step records the exact manifest, asset identities and verified parent
in a private runner-local receipt. Only a complete authenticated restore creates
the verification receipt consumed by promotion. These files stay on the same
runner and are never uploaded as workflow artifacts. Promotion rechecks the
original producer tuple, manifest, every asset identity and parent against those
receipts before its one mutation, and still requires at least one minute left.
A failed phase leaves its exact draft for reconciliation before another capture.

The intermediate backup is not scanned again: its hash and pinned source facts
are checked against the downloaded image. Packing carries the database hash
from its raw input traversal and compressed part hashes from successful output
writes, including the gzip trailer. Gzip level 1 favors compression speed over
payload size; existing higher-compression checkpoints remain readable. The
authenticated restored image receives an independent disk hash, full SQLite
integrity and foreign-key checks, schema/version validation and all table counts.
Fixed phase names and elapsed times expose timing without exposing data.
Deadline checks are cooperative for filesystem and library calls; the outer
minute is cleanup margin, not a hard completion guarantee. Any error after an
attempted mutation holds the retained draft or published state for inspection,
without a retry or rollback.

Normal ingestion failure or the supervisor's time limit can still save committed
partial progress, but the job fails and skips Git publication. A forced
runner cancellation or loss cannot guarantee a new upload. The previously
verified checkpoint remains the recovery point until a new one is promoted.
The reporter visibly skips a checkpoint unless its successful producer tuple
matches the private Git archive's hash-bound publication receipt.

## Capacity and retention

Before backup, admission uses the pinned logical SQLite page count and page size,
including committed WAL pages. Free space must cover the standalone image plus
worst-case compressed parts. Uploaded local copies are removed before readback;
the latter requires the actual database size plus compressed asset sizes. These
checks account for files already occupying the runner. They do not delete tools
or unrelated payloads to obtain space.

Keep the current checkpoint and its immediate verified parent. After promotion,
only the exact grandparent named by that authenticated lineage can be deleted.
Tags and unrelated releases are untouched. A failed retention operation blocks
the next ingestion until that same deletion succeeds. Automatic reconciliation
first handles a complete retained publisher draft or unpublished latest
checkpoint. An incomplete or ambiguous draft holds for operator inspection;
it is never removed or replaced automatically. Do not bulk-delete releases by
age or prefix.

The publisher has a 125-minute job ceiling. Phase caps total at most 93 minutes:
checkout/setup/build/configuration 18, policy validation 2, restore 10,
ingestion/reap 22, checkpoint upload/verification/promotion 30, outcome gate 1 and Git publication 10.
Acquisition shares one 20-minute deadline across its phases. This ceiling does not
promise a fifteen-minute completion time or durable FIFO scheduling.

Before restore, the publisher spends at most 120 seconds checking retained state.
Already published checkpoints, retained failed-acquisition progress, and the
initial migration checkpoint can continue to fresh acquisition. Their metadata
check raises the normal phase ceiling to 95 minutes. When a complete retained
publisher checkpoint needs recovery, that run instead performs the existing
33-minute recovery inside a 34-minute step and skips fresh acquisition. Including
setup, this path has a 54-minute phase ceiling. The two paths never combine their
budgets. Recovery time includes discovery; it does not restart the deadline.

Historical provenance accepts either the former monolithic checkpoint step or
the ordered upload, verification and promotion steps. A failed or interrupted
phase requires skipped descendants. Successful ingestion may resume private Git
publication after two complete authenticated restores. Failed ingestion can
retain its verified progress, but cannot publish an archive. Original capture
source and outcome stay unchanged. A verified recovery emits a fixed control
output and reports that fresh acquisition waits for the next run; an uncertain
result emits no continuation output and stops the job.

Scheduled publisher provenance follows GitHub's
[scheduled actor contract](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#actor-for-scheduled-workflows):
the actor can change with cron or default-branch edits. Admission requires the
canonical main workflow, first attempt and matching actor/triggering actor,
without a scheduled-user allowlist. Manual dispatch remains restricted to
`vincentkoc`; historical run metadata must still match its current run record.

Archive commits bind the checkpoint manifest and database hashes. An already
completed recovery is recognized by those hashes, the exact producer receipt,
and the direct parent commit without repeating its push. Older archive commits
without checkpoint hashes are accepted only when the original run and every
native, checkpoint and archive step completed successfully. Lost mutation
responses remain holds in that run; a later run independently validates the
retained state before deciding whether any work remains.

## Private bounded acquisition

The publisher runs at 43 minutes past each even UTC hour. The daily 06:17 UTC
member refresh replaces the ordinary 06:43 run. The shortest scheduled gap is
94 minutes; metadata admission plus ordinary phase caps can reach 95 minutes.
The shared group serializes any overlap. GitHub can delay scheduled
jobs; concurrency does not provide durable FIFO or a completion guarantee.
Enable this cadence only after a manual run qualifies full-state custody and
measured restore, acquisition, checkpoint and publication behavior.

The workflow pins `POLICY_COMMIT` and `POLICY_SHA256` once for acquisition.
Changing acquisition scope requires a reviewed source change. The existing
backup credential reads `policy/report-selectors.json` from the private backup
repository. Its `refs/heads/maintainer-report-policy` must point to exactly the
pinned commit; the file is then fetched by that commit and its bytes must
match the pinned SHA-256. A missing, changed or invalid policy stops before
native state is restored. No policy branch, repository variable or credential
is created by the workflow.

The policy is a JSON object with these exact fields:

- `schema`: integer `1`.
- `policy`: `repository`, full `revision`, repository-relative `path` and
  `sha256` identifying the reviewed canonical report selector source.
- `catalog`: `archive_commit` and `manifest_sha256` identifying the catalog used
  to select the roots and retained children.
- `guild_id`: the exact configured Discord guild ID.
- `message_root_ids` and `forum_root_ids`: arrays of native decimal IDs.
- `retained_threads`: objects with `id` and `parent_id`; each parent must be one
  of the declared message or forum roots. These pin existing leaves. Ordinary
  `--channels` selection expands forum children; acquisition also includes stored
  direct text and announcement-root children discovered by its first pass.

The approved policy digest binds that provenance and scope together. The file
is limited to 64 KiB and 4,096 distinct targets in total. It cannot contain
commands, arbitrary flags, wildcards, names or additional fields. Creation and
review of this private policy are separate operator actions. Do not store it on
archive `main`, in SQLite, or in public workflow inputs, logs or artifacts. The
workflow keeps its temporary copy private and outside all export paths.

In `mode: current` (the default), the supervisor first runs the existing guild-wide latest-only refresh. It then
runs native `sync --full --since --include-stored-threads` on the policy targets,
including stored direct threads of selected text and announcement roots. This
keeps threads discovered by the first pass in the bounded coverage scan without
rewriting the policy. Selected thread leaves do not expand to siblings or descendants.
The scan starts at the previous
UTC midnight by default. An optional `since` input may pin a UTC midnight less
than 48 hours ago. It is fixed before restore and revalidated before acquisition;
a stale or future value stops rather than silently selecting a different day.
The daily 06:17 UTC route requires a member refresh in the first guild-wide
phase; ordinary scheduled and manual runs skip members. A required member-refresh failure stops before
the targeted phase. The second phase always uses `--skip-members`,
`--with-media=false` and `--no-update`.
Both phases share the same 1,200-second deadline, and the second
starts only after the first process group has exited. Any failure prevents Git
publication; committed partial work can still enter a verified private
checkpoint after all owned processes are reaped.

For a separately reviewed historical backfill, manually dispatch the same
workflow with `mode: historical` and an explicit `since` UTC midnight less than
31 days ago. Scheduled runs always use `current`; historical mode cannot request
members. It runs only the selected native full-history scan, using the same pinned
policy, stored-thread selection and 1,200-second supervisor. The 5-minute native
channel deadline remains in effect. Restore, checkpoint verification, failure
handling and publication use the same serialized owner as current acquisition.

The native scan deliberately traverses from the live head through `since`, so it
also collects newer messages. There is no `until` input. A completed native
history-window receipt ends at that channel's scan-start time; export or workflow
completion cannot extend it. Different `since` values retain distinct receipts.
Consumers must keep their original reporting cutoff and verify every required
channel's receipt; this mode does not declare a report complete. If the budget
expires, retain the partial checkpoint and inspect the missing coverage before
approving another run. Source merge alone does not authorize a live backfill.

This policy limits **acquisition cost and scope**, not archive privacy. Native
forum selection includes all bot-visible children of the selected forums,
including newly discovered children. Full forum discovery traverses archived
thread metadata even though message acquisition is date-bounded. The normal
Discord permissions and native unavailable markers still apply. A successful
process does not mean every channel is available or complete; report projection
must inspect each channel's bounded receipts and preserve its privacy rules.

This publisher does not export or upload to cloud storage. Existing cloud data
is unchanged and is not made current by this private-only run. Restoring cloud
publication requires separate verification of its configured destination and
access policy.

## One-time migration

Keep the publisher, daily reporter and retired cache-repair workflow disabled.
Merge and qualify the transport source before invoking the manual-only
`migrate-discord-checkpoint` workflow. The reviewed invocation pins current main,
the successful cache producer's source revision, and the cache's exact ID, key,
ref, version, creation time and byte size. It accepts only the latest matching
main cache and verifies writer quiescence both before and after exact restore.

The migration performs no Discord acquisition, Git publication, cloud upload or
cache deletion. It creates the initial private checkpoint and independently
restores it through the normal authenticated path. Inspect its private manifest
for full parity and use workflow step durations for transfer/verification timing.
Before dispatch, compare actual runner free space with the logical database,
WAL and backup/compressed/readback peak. Insufficient capacity is a stop.

After independent restore proof, an operator can retire the explicitly reviewed
old caches. Keep schedules disabled until source restore/publication, ongoing
acquisition coverage and measured end-to-end cadence are qualified. Source merge
or a small fixture alone does not establish production credential access,
full-state migration, or report completeness.

## Diagnose a retained migration draft

If migration fails, preserve its draft and recovery cache. Do not rerun the
migration to diagnose an unknown API error. The same manual workflow offers
`mode: diagnose`, with acknowledgement `DIAGNOSE_EXISTING_CHECKPOINT`, the
reviewed current `expected_sha`, `original_run`, `manifest_sha256`, and
`inventory_sha256`. The inventory hash covers canonical JSON of assets sorted
by name, each containing `id`, `name`, `size`, and `digest`. Review these pins
through authenticated metadata before dispatch; private repository and asset
IDs are not workflow inputs.

This mode requires the original manual migration's first attempt to have
completed with failure, the current diagnostic to be a reviewed first attempt
on main, and all archive writers to remain disabled and idle. It checks the
existing draft, manifest, asset identities, private archive main, and absence
of latest, downloads only the first manifest part, verifies its bytes, then
rechecks those bindings. It never decompresses or restores a database, reads a
cache, uploads, promotes, or deletes a release. Temporary downloads are removed.

The diagnostic has one 15-minute deadline inside a 20-minute job. Metadata calls
allow at most 60 seconds; the part download allows at most 600 seconds, always
limited by the remaining deadline. API errors expose only a fixed operation,
numeric exit or timeout, and an HTTP status when available. Failed `gh api`
requests that already include response headers can also report `rate_remaining`,
`rate_reset` (UTC epoch seconds), and `retry_after` (seconds). Only single numeric
values of at most ten digits from the first complete response header block are
reported; duplicate, malformed, or missing fields are omitted. No response body
or private stderr text is included. A 403 or 429 with `rate_remaining=0` indicates
the primary rate limit; wait until the reported reset before an operator-approved
retry. Honor `retry_after` when present. Missing fields or a 403 with remaining
quota do not establish its cause. Inspect the existing credential's permissions
and GitHub's rate-limit guidance without substituting credentials or blindly
retrying. The helper does not retry automatically. See
[GitHub's rate-limit documentation](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api#exceeding-the-rate-limit).
Private command
output is drained with a bounded retained prefix and never logged. Public
admission and private reads share this process boundary while retaining their
separate tokens. The helper terminates its command group and allows up to five
seconds to verify it has disappeared before inspecting output; unknown cleanup
state stops the diagnostic. A successful
probe proves the current credential and first-part read path; it does not locate
an earlier unrecorded failure or verify the other parts. Full recovery still
requires a separately reviewed complete restore and parity check.

## Finalize a retained initial checkpoint

After a successful diagnostic, the same manual workflow offers `mode: finalize`
with acknowledgement `FINALIZE_EXISTING_CHECKPOINT`. Keep the original-run,
manifest and inventory pins; supply the reviewed current `expected_sha` plus
`probe_run` and `probe_revision` from that successful diagnostic. Runtime checks
the exact first attempt, actors, source, jobs, successful diagnostic step, its
six public input values and all three executed log markers. The operator's
original dispatch receipt binds the submitted payload bytes separately; runtime
does not reconstruct an unexposed API payload from run status.

Finalization accepts only the initial migration's sole retained draft with no
latest checkpoint and all writers disabled and idle. It downloads every part
twice through the canonical restore path. Each round verifies part and full
database hashes, SQLite integrity and foreign keys, schema, version and every
table count against the original manifest. The first scratch image and parts
are removed before the second round. Safe phase output reports free and required
bytes and each round's duration; it contains no table counts or private IDs.

The 45-minute job gives verification and publication a shared 40-minute ceiling,
including hashing, decompression and SQLite checks. Three minutes are reserved
for the final publication phase, including one minute for read-only outcome
classification. Insufficient space, time, changed evidence or unknown process
cleanup stops before publication. No source cache, Discord collection, new
snapshot, upload, asset deletion or retention operation runs.

After both rounds, all source, probe, quiescence and private custody checks run
again. One PATCH publishes the exact existing draft as latest; authenticated
metadata and manifest readback must then agree. The original manifest, producer
revision and run identity remain unchanged. A lost or invalid PATCH response
always holds, even if readback observes publication. It never retries, rolls
back or deletes the release. Inspect the recorded `unchanged_draft`,
`published_latest` or `unknown` outcome before any further action. A successful
finalization establishes complete private custody; cache retirement and enabling
writers remain separate reviewed operations.

## Recover an unpublished publisher checkpoint

Use `diagnose` for the exact failed publisher run before requesting
`recover-publisher` with acknowledgement `RECOVER_RETAINED_PUBLISHER`. The same
manifest, inventory, original-run and successful-probe pins apply. Admission
requires successful native ingestion, a failed checkpoint phase, and a
skipped archive publication step on the original first attempt. The overall
original run remains a failure; it is never relabeled as a successful workflow.

The candidate must name the current published parent. Recheck that parent's
authenticated manifest and asset identities against its previously qualified
custody receipt; do not download its database again. The recovery restores the
candidate twice, removing the first image before the second and retaining only
the second verified database for offline publication. The private archive main
and policy ref stay pinned. Baseline and exported manifests must have no media
or embedding payloads; recovery holds rather than dropping unavailable files.

The native publisher exports and commits once, without `--push`, Discord
credentials, source import, or collection. Native source-table counts and exact
progress/failure rows must remain unchanged. Derived SQLite maintenance may
change database bytes, so the immutable checkpoint remains the custody artifact.
The new archive commit must have exactly the pinned main as its sole parent.
Its producer receipt retains the original native source/run tuple; its commit
message separately records recovery provenance and checkpoint hashes. Export
time does not extend any native history-window interval.

All admission and custody gates run again before promoting the existing draft
once. After authenticated readback, one explicit Git lease for the pinned old
main publishes the verified direct child. This update is a fast-forward and
rejects both advances and rewinds of main. There is no native push retry, rebase
retry, tag push, or import bookkeeping. Exact remote commit, manifest and
producer bytes must match afterward. Unknown promotion or push outcomes hold
without retry, rollback, or deletion; a promoted checkpoint with no verified
archive requires operator reconciliation.

Recovery has a shared 33-minute ceiling inside its 34-minute step and 45-minute
job. The two restores have at most 20 minutes; offline export must finish with
three minutes still available for final gates, promotion and push/readback.
Actual space is checked for restore and Git export; no cadence or completion
time is inferred from source tests. No cache, new checkpoint upload, retention
deletion, cloud publication, or scheduler activation is part of this route.
