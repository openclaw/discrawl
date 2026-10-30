# Data layout

Everything lives in one local SQLite file. By default, Discrawl stores it at
`${XDG_DATA_HOME:-~/.local/share}/discrawl/discrawl.db` on Linux and
`~/Library/Application Support/discrawl/discrawl.db` on macOS. Setting `XDG_DATA_HOME` overrides
the new default base directory on any OS.

Existing `~/.discrawl/discrawl.db` installs continue to use that database until
the new default database file exists. Discrawl does not copy or migrate the
SQLite file during upgrade. Copy it yourself before creating the new database
path if you want to move an existing archive to the platform-native location.

## What is stored

- guild metadata
- channels and threads in one table (Discord models threads as channels)
- current member snapshot
- canonical message rows
- append-only message event records
- FTS5 index rows
- optional local embedding queue metadata and vectors
- local-only sync/import/media/embedding failure history

Messages imported from Discord Desktop use the same message, attachment, mention, and FTS paths as bot-synced messages.

## Content rows, checks and metrics

The unreleased record-efficiency candidate treats `guilds`, `channels`, `members`, `messages` and
`message_attachments` as content rows: a write happens only when a content
column or the canonical provider metadata changes, and `updated_at` then means
"content last changed". The 6-hourly repair, the daily member refresh and
Gateway replays of unchanged rows write no content row, no search-index row
and no content change-feed entry. Name-only rereads still refresh metadata in
every enabled lexical index without retokenizing or advancing content clocks.
Evidence and checks are appended separately. A message's `updated_at` still advances when one of
its attachments or mentions changes.

`raw_json` holds one canonical form per entity, whichever transport delivered
it:

- **Merge rule.** A value that the incoming transport omits, nulls or zeroes
  never replaces a known stored value. Messages keep the Gateway `guild_id`,
  `member` and `mentions[].member`, and keep REST `author.flags`/
  `author.public_flags` (also inside `referenced_message`) when Gateway sends
  zeros. Referenced-message transport metadata is merged only for the same
  referenced message identity. Empty-reread admission uses the same transport
  comparison, so metadata shape alone cannot clear archived reply text. Members keep `guild_id`, member `flags` and `user.flags`/
  `user.public_flags`. Channels keep `guild_id`, a non-voice `bitrate` and a
  public thread's `thread_metadata.invitable`. Guilds keep the Gateway
  `joined_at`, `large` and a `region` that REST reports as `deprecated`. A
  provider that genuinely resets one of these fields to zero is always retained in exact evidence, even when the canonical display
  keeps the known value.
- **Unordered sets** are sorted: guild `features` and channel
  `permission_overwrites` (by id, then type).
- **Timestamps** in member `joined_at`, `premium_since`,
  `communication_disabled_until`, channel `archive_timestamp` (column and
  `thread_metadata`) and `thread_metadata.create_timestamp` are truncated to
  milliseconds, the precision both transports share.
- **Guild collections** retain their complete nested fields, including unknown
  metadata. A missing or empty transport catalog keeps the last known catalog.
  Exact incoming missing/null/array shape is preserved in evidence.
- When the canonical form equals the incoming payload, the incoming bytes are
  stored unchanged; otherwise the canonical encoding (sorted keys) is stored.
  Rows written before 0.20.0 are not rewritten: they compare by canonical form
  and change on their next real content change.

`entity_metrics(entity_type, entity_id, metric, observed_at, value)` is a
value-change series for live counters, appended only when a value differs
from the latest recorded one: guild `member_count`, `approximate_member_count`,
`approximate_presence_count` and `premium_subscription_count`, and channel
`message_count`, `member_count` and `total_message_sent`.
Every observed numeric value, including zero, is recorded. No inference that
a zero means absent is applied to metric history. `last_message_id` remains
content because it is an identity relationship, not a numeric count. These counters are no longer stored in content
`raw_json`. `discrawl` does not seed old values automatically; the store
function `SeedEntityMetrics` backfills them from legacy `raw_json` without
touching content rows.

`entity_checks(table_name, row_key, check_kind, checked_at, succeeded_at,
outcome)` holds the latest check per row: `observed` for every accepted
observation (`outcome` `changed` or `unchanged`) and `attachment_text` for
every attachment text extraction attempt (`outcome` is the text status).
`observation_started` retains a message mutation request start separately, so a
new repair start does not duplicate the unchanged message evidence body.
`row_key` uses the change feed's JSON primary-key encoding. Freshness guards
("a write after this observation started is newer") use the later of
`updated_at` and the observed check. `message_attachments.text_attempted_at`
and `text_succeeded_at` are check fields: they are refreshed only with a
content write (for example a new text status, error or text), and text retries
are ordered by the later of `text_attempted_at` and the `attachment_text`
check.

All efficiency tables are additive and do not change `user_version`, so an
older binary can still open the archive. None is exported to Git snapshots.

### Exact evidence and check history

`entity_checks` is a latest-state convenience overlay, not the only check log.
`entity_check_history(check_id, table_name, row_key, check_kind, checked_at,
succeeded_at, outcome)` appends every accepted check, with a monotonic integer
`check_id`. It has no deletion or retention policy.

Before canonicalization, `entity_observations(observation_id, table_name,
row_key, observed_at, evidence_hash)` binds each observation to an immutable
`entity_evidence(evidence_hash, payload_json)` object. The hash is SHA-256 of the
serialized native record. Decode `payload_json` into that native record or
message mutation (local `WriteOptions` are not source data): its `RawJSON` string is the exact original input, preserving
whitespace, missing versus null, array order, numeric spelling, full timestamp
precision and signed URLs. Message mutations also retain the original event
payload, attachments and mentions. Identical receipts share one evidence body;
A-B-A retains three observations, not two. These are exact store inputs, not a
claim to recover network bytes already discarded by an upstream SDK.

Unknown fields remain meaningful content, including unknown null/empty values.
Only the documented known transport equivalences normalize comparison. Historical
rows are not rewritten or compacted, and legacy raw data is retained before its
first canonical replacement. A new consumer must join evidence through its
owner key and apply the owner's privacy restrictions, never expose an unscoped
blob hash. Evidence and check rows are not content versions.

`mention_event_history(event_id, payload_json)` retains the original row before
an ID is removed by duplicate reconciliation, disappearance, or metadata update.
Resolve historical IDs here when absent from `mention_events`. Existing IDs are
not renumbered; current identical natural identities keep their original ID.
Before exact snapshot replacement, current mention identities are retained in
history. Imported source event IDs are mapped through
`mention_event_id_map(source_event_id, identity_key, local_event_id)`, keyed by
`(source_event_id,identity_key)`, where identity_key is the JSON array
`[message_id,target_type,target_id]`. A source ID colliding with local history
(including retained DMs) gets a fresh local ID above all allocated IDs; repeated
imports reuse the mapping. Existing local IDs are never reassigned. Original
source rows and source IDs remain in entity evidence bound to the local ID.
Resolve an external source ID with its natural identity through this map; bare
local IDs continue to resolve through current rows or mention_event_history.
Permission metadata is not transport noise: missing/null `permission_overwrites`
is distinct from a known empty array, preserving publication preflight semantics.
Accepted role events record observed freshness even when their content is equal,
so delayed repair cannot restore a stale permission grant. Routine Git snapshot
merges use the later of content revision and local observed check for guilds,
channels, members and messages; exact replacement remains explicitly destructive.
Exact replacement resets only latest check overlays for superseded owners,
preserving current local-only DM overlays and all immutable check histories, so
old local observations cannot fence the explicitly selected replacement forever.


## DMs

Proven DMs use the synthetic guild id `@me`. Unclassifiable desktop-cache payloads are skipped instead of being stored as unknown synthetic data.

## Attachments

Attachment binaries are not stored in SQLite. SQLite stores attachment metadata, filenames, optional extracted text, and media cache bookkeeping.

`discrawl attachments fetch` and `discrawl sync --with-media` download media into `cache_dir/media` and record the relative media path, SHA-256, byte size, fetch time, and fetch status on the attachment row.

Discord attachment URLs are not guaranteed to stay fetchable forever. Expired or removed CDN objects are recorded as failed fetches with their HTTP status, commonly `404`; already cached files remain usable and can still be exported.

Set `sync.attachment_text = false` if you want to keep attachment metadata and filenames but disable attachment body fetches for text indexing.

## Multi-guild ready

The schema is multi-guild ready even when the common UX stays single-guild simple. Threads are stored as channels because that matches the Discord model. Archived threads are part of the sync surface.

## Schema migrations

SQLite schema migrations are versioned with `PRAGMA user_version`. Startup fails fast when a local DB schema is newer than the supported binary - that means you have a binary older than the database.

Index maintenance also runs on writable opens of an already-versioned database.
An unchanged `user_version` therefore does not mean that a writer open will do
no schema work. Read-only opens do not run this maintenance.

### Channel update-cursor index

After v0.14.1, Discrawl adds `idx_messages_channel_updated_id` on
`messages(channel_id, updated_at, id)` for per-channel changed-message queries.
It preserves the existing creation-time indexes and does not change
`user_version`. The first writable open that finds the index missing builds it
synchronously before returning; later opens retain the existing index.

That first build can delay the initiating command and other writers. Build
duration, peak memory, temporary disk requirements, persistent index size and
ongoing message-write overhead remain **unmeasured for the target archive**.
Small synthetic read timings and successful CI are not production capacity
estimates or acceptance of those costs. The historical operational decision in
[PR214](https://github.com/openclaw/discrawl/pull/214) is not resolved by this
documentation.

### Before broader index rollout

Require an explicitly approved, backup-first canary and a decision on its
measurements before expanding adoption. These are operator prerequisites, not
an automatic software gate or authorization to run a canary:

1. Have the owner approve the candidate revision, isolated target, maintenance coordination and rollback plan. Define elapsed-time, peak-memory, disk headroom and write-impact limits, with stop criteria, before starting.
2. Coordinate archive writers and take a verified SQLite-consistent backup under that plan. Retain the matching pre-upgrade binary and configuration. Do not copy only the main database file while uncheckpointed WAL data may exist; use an approved consistent-backup method and verify restoration.
3. Use a separately approved disposable copy, with explicit isolated config, database, cache, log and share paths. Prevent provider, Git-share and backend requests. Do not run collection or publication, and leave the source archive and other deployments untouched.
4. On that copy only, measure the first writable open, peak memory, temporary and persistent disk growth, remaining headroom, and a bounded local write workload before and after the index. Keep archive contents private; record aggregate measurements and the exact candidate revision.
5. Stop expansion if a limit is exceeded or evidence is incomplete. The owner must explicitly approve the measured tradeoff for the intended rollout; successful publication elsewhere does not establish acceptable local costs.
6. For rollback, restore the matching pre-upgrade binary, configuration and database from the verified backup, accounting for any writes since backup. A binary downgrade alone is not a verified rollback plan.

## Change feed

`change_log`, `change_feed_tables` and `change_feed_meta` record which rows of
`message_attachments`, `mention_events`, `message_embedding_history` and
`attachment_text_history` changed, so readers can copy only those rows. They
are local-only and excluded from Git snapshots. See the
[source change feed](change-feed.html) contract.

## Failure history

`failure_ledger` records bounded error text and available Discord row
identifiers. Unresolved failures are retained; resolved failures age out after
90 days. The table is intentionally absent from Git snapshot exports and
imports, so one machine's operational diagnostics never become published
archive content. Query it through [`failures`](../commands/failures.html).

## Querying directly

Anything you want, with read-only SQL:

```bash
discrawl sql 'select count(*) as messages from messages'
echo 'select guild_id, count(*) from messages group by guild_id' | discrawl sql -
```

See [`sql`](../commands/sql.html).

## See also

- [`status`](../commands/status.html) - high-level archive status
- [`coverage`](../commands/coverage.html) - per-guild/channel archive readiness and wiretap progress counters
- [`diagnostics`](../commands/diagnostics.html) - SQLite integrity, WAL, freshness, and writer-lock state
- [`failures`](../commands/failures.html) - local ingestion and write failure history
- [Source change feed](change-feed.html) - incremental reader contract for relationship and history tables
- [`channels`](../commands/channels.html) - channel directory
- [`members`](../commands/members.html) - member directory
- [Security](../security.html)

## Preservation boundary and coordinated activation

The current candidate preserves exact native store inputs, not original REST
response bodies or Gateway frame bytes. The Discord client currently decodes
responses through typed discordgo APIs before `syncer/records.go` reserializes
them. Unknown fields discarded by that SDK cannot be reconstructed from the
new evidence tables. The preservation contract covers all already-admitted native
records and RawJSON values, not fields the legacy SDK never retained. No new
unrestricted HTTP or Gateway capture is introduced. Activation remains a separate
coordinated operation requiring reader acceptance and the consistent clone.

### Coordinated migration and rollback procedure

1. Keep the current writer and readers unchanged until the coordinator has a
   consistent stopped-writer storage clone covering SQLite and WAL, the derived
   store, configuration and matching binaries. Do not copy a running database
   or run an archive-scale rehearsal for this candidate.
2. After native review and reader acceptance are complete, use the
   protected-main signed workflow described in [Releasing](../RELEASING.md),
   then verify the exact archive with `make verify-release VERSION=vX.Y.Z
   ARTIFACT_DIR=/path/to/downloaded-assets`. The operator-selected version and
   artifact receipt must be pinned in the coordinated cutover plan.
3. The first authorized writable `Store.Open` creates the additive evidence,
   check and metric tables in one durable transaction, then installs contract-2
   triggers. Partial additive-schema creation rolls back on error. It leaves `user_version=7`,
   contract-1 generations and all existing content IDs intact. The optional
   `SeedEntityMetrics` store operation is idempotent; it seeds only missing
   metric keys from legacy raw rows and does not rewrite them. It is not an
   automatic startup migration or a public CLI command. Legacy evidence is
   copied lazily before a canonical replacement, not by a bulk archive rewrite.
   Attachment legacy evidence preserves the complete native row as the RawJSON
   string before replacement/removal; refetch evidence retains every error code.
4. With writers still paused, verify a representative unchanged check, content
   A-B-A, zero metric transition, old mention ID, evidence reconstruction,
   allowed/denied reader and feed-v1/v2 cursor behavior. Fixture tests cover
   additive DDL idempotence and transaction rollback without live data.
5. If acceptance fails, restore the coordinated clone and matching old binaries
   and configuration before resuming writes. Do not drop additive tables or
   vacuum as a rollback substitute. An old binary can open the unchanged schema
   version, but it cannot produce the new evidence contract. After new writes
   resume, preserve/replay them before any older-clone restoration.

### Validation runtime budget

The complete race suite uses a 25-minute per-package watchdog, with a 35-minute
hosted test-job bound. The share package creates many fully durable temporary
archives and can exceed Go's implicit ten-minute package limit on hosted runners.
Individual behavior deadlines, race detection, all test cases and the 85% coverage
floor are unchanged. Additive evidence-schema setup uses one transaction instead
of ten separate durable DDL commits; production FULL synchronization is not relaxed.
