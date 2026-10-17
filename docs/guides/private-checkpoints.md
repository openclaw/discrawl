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
the next ingestion until that same deletion succeeds. An interrupted draft also
blocks new ingestion; an operator must reconcile that exact attempt. Do not
bulk-delete releases by age or prefix.

The publisher has a 125-minute job ceiling. Phase caps total at most 79 minutes:
checkout/setup/build/configuration 18, policy validation 2, restore 10,
ingestion/reap 23, checkpoint 15, outcome gate 1 and Git publication 10.
The two acquisition phases share one 20-minute deadline. This ceiling does not
promise a fifteen-minute completion time or durable FIFO scheduling.

## Private bounded acquisition

The publisher currently has **only a manual trigger**. Keep it disabled until
the full-state migration is verified. Enabling it later permits a measured
manual run without also starting a cron schedule. Recurring acquisition and the
daily member refresh need a later reviewed cadence change, based on measured
restore, acquisition, checkpoint and publication durations.

Each manual run requires `policy_commit` and `policy_sha256`. The existing
backup credential reads `policy/report-selectors.json` from the private backup
repository. Its `refs/heads/maintainer-report-policy` must point to exactly the
requested commit; the file is then fetched by that commit and its bytes must
match the requested SHA-256. A missing, changed or invalid policy stops before
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
  of the declared message or forum roots. Retained children of text roots must
  be explicit targets: native targeted discovery expands forum children only.

The approved policy digest binds that provenance and scope together. The file
is limited to 64 KiB and 4,096 distinct targets in total. It cannot contain
commands, arbitrary flags, wildcards, names or additional fields. Creation and
review of this private policy are separate operator actions. Do not store it on
archive `main`, in SQLite, or in public workflow inputs, logs or artifacts. The
workflow keeps its temporary copy private and outside all export paths.

The supervisor first runs the existing guild-wide latest-only refresh. It then
runs native `sync --full --since` on the policy targets, starting at the previous
UTC midnight by default. An optional `since` input may pin a UTC midnight less
than 48 hours ago. It is fixed before restore and revalidated before acquisition;
a stale or future value stops rather than silently selecting a different day.
The second phase uses `--skip-members`, `--with-media=false` and `--no-update`.
Both phases share the same 1,200-second deadline, and the second
starts only after the first process group has exited. Any failure prevents Git
publication; committed partial work can still enter a verified private
checkpoint after all owned processes are reaped.

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
numeric exit or timeout, and an HTTP status when available. Private command
output is drained with a bounded retained prefix and never logged. Public
admission and private reads share this process boundary while retaining their
separate tokens. The helper terminates its command group and allows up to five
seconds to verify it has disappeared before inspecting output; unknown cleanup
state stops the diagnostic. A successful
probe proves the current credential and first-part read path; it does not locate
an earlier unrecorded failure or verify the other parts. Full recovery still
requires a separately reviewed complete restore and parity check.
