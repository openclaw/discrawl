# Source change feed

Discrawl records a durable change feed so that a reader can copy only changed
rows instead of rereading whole tables. The archive serves two contracts side
by side. Contract 1 is unchanged; contract 2 adds the content tables and the
metric series, with the same entry semantics:

| Table | Primary key (`row_key` order) | Contract |
| --- | --- | --- |
| `message_attachments` | `attachment_id` | 1 |
| `mention_events` | `event_id` | 1 |
| `message_embedding_history` | `message_id, provider, model, input_version, embedded_at` | 1 |
| `attachment_text_history` | `attachment_id, text_sha256` | 1 |
| `guilds` | `id` | 2 |
| `channels` | `id` | 2 |
| `members` | `guild_id, user_id` | 2 |
| `messages` | `id` | 2 |
| `entity_metrics` | `entity_type, entity_id, metric, observed_at` | 2 |
| `entity_evidence` | `evidence_hash` | 2 |
| `entity_observations` | `observation_id` | 2 |
| `entity_check_history` | `check_id` | 2 |
| `mention_event_history` | `event_id` | 2 |
| `mention_event_id_map` | `source_event_id, identity_key` | 2 |

A contract-2 reader reads every table of both contracts.

The feed is local archive state. It is not exported to Git snapshots and does
not change the columns of any tracked table or any existing reader contract.

## Tables

`change_log` holds one entry per changed native row:

| Column | Meaning |
| --- | --- |
| `seq` | `INTEGER PRIMARY KEY AUTOINCREMENT`; global across tracked tables, never reused |
| `table_name` | Tracked table |
| `op` | `i` insert, `u` update, `d` delete |
| `row_key` | JSON array of the primary-key values in key order. A blob key value is encoded as `{"blob_hex": "..."}` |
| `recorded_at_ms` | Local UTC Unix milliseconds. Used only for retention and cursor validation, never for ordering |

`change_feed_tables` has one row per tracked table: `generation` (counter),
`generation_id` (random 128-bit hex), `generation_seq` (feed head when the
generation started), `reason`, `schema_sha256` (hash of the table's
`sqlite_master.sql`), `contract_version` (the contract that introduced the
table: 1 or 2) and `updated_at`.

`change_feed_contracts` lists the contracts the archive serves:
`contract_version` and `tables_json` (the JSON array of that contract's tables,
in the order above) and `updated_at`. Discrawl 0.20.0 and later write rows 1
and 2.

`change_feed_meta` is a singleton: `contract_version` (the baseline contract,
which stays 1 so contract-1 readers keep working),
`compacted_through` (highest compacted `seq`), `compacted_entry` (the digest of
that compacted entry, or null), `compacted_at` and `retention_seconds` (604800).

An entry's digest is `json_array(table_name, op, row_key, recorded_at_ms)`.

## How entries are written

`AFTER INSERT`, `AFTER UPDATE` and `AFTER DELETE` triggers on each tracked
table append entries in the writer's own transaction. Every writer is covered,
including snapshot imports, guild deletion, repairs, older Discrawl binaries
opening the same file, and manual SQL.

- An insert writes `i` with the new key; a delete writes `d` with the old key.
- An update writes `u` only when at least one column changed (`old.c IS NOT
  new.c` for each column; values of different storage classes never compare
  equal). If the primary key changed, `d` for the old key precedes `u` for the
  new key.
- `INSERT OR IGNORE` that inserts nothing writes nothing.
- `INSERT OR REPLACE` (snapshot imports) removes the conflicting row without a
  delete trigger. This is still complete because each tracked table's only
  uniqueness constraint is its primary key, so the replacement's `i` entry
  names the same key. A test enforces this invariant.

Because SQLite admits one writer at a time, a reader transaction sees a
sequence prefix: every later commit allocates larger `seq` values. Entries do
not copy row contents. A reader rereads the native row by key.

Since 0.20.0 writers change content rows only when their content changes (see
[Data storage](data-storage.md#content-rows-checks-and-metrics)). An
unchanged repair, member refresh or Gateway replay writes no content row, so
it writes no content entry. Evidence/check entries still record the check. Storing a message reconciles its relationship rows in place:
an attachment row is updated in one statement only when one of its content
columns changes, and a mention keeps its `event_id` for the same
`(message_id, target_type, target_id)`. Only mentions and attachments that
appear or disappear are inserted or deleted. `mention_events` is therefore
still not insert-only (an in-place update writes `u`). Older binaries opening
the same file delete and reinsert these rows; the feed records that
faithfully too.

The two history tables are only written by `insert or ignore` in current code
(`archiveMessageEmbeddings`, `retainAttachmentText`) and have no update,
delete or retention path. They are tracked anyway, because their composite keys
have no stable monotonic order a reader could use.

## Generations

A table starts a new generation, with a new random `generation_id`, when the
log cannot express what happened:

| Reason | Cause |
| --- | --- |
| `enabled` | First installation of the feed for this table |
| `schema_changed` | The table definition changed, for example a migration added a column (which changes every row without row writes) |
| `triggers_restored` | A trigger was missing or different at open, so writes may be unrecorded |
| `contract_upgraded` | The feed contract version changed |
| `feed_restored` | `change_log`, `change_feed_tables`, `change_feed_meta` or its row was missing at open, so entries or the compaction floor may be lost (applies to every table). A recreated log continues numbering above the highest retained floor or generation start, and the head becomes the new floor |
| `snapshot_import` | A full Git snapshot replacement (`subscribe --exact`/`Import`) restored the tracked snapshot tables (`guilds`, `channels`, `members`, `messages`, `message_attachments`, `mention_events`). Their triggers are suspended during the replacement, so the import writes no per-row content entries; preservation of replaced mention identities and source-ID mappings still logs evidence/history inserts |

Writable opens check the feed after every migration. When the installed
triggers already match, the check issues no DDL, so routine opens do not change
`PRAGMA schema_version`. Merge imports do not start a generation; their row
writes are recorded like any other writer.

## Retention and compaction

`discrawl tail` compacts hourly. Compaction removes only a `seq` prefix of
entries whose `recorded_at_ms` is older than 7 days, stops at the first entry
inside the window (so a wall-clock step never removes newer entries), and
records the new `compacted_through` in the same transaction. Entries above
`compacted_through` are never deleted. A reader whose cursor is below
`compacted_through` must reconcile once; that is the only way compaction
affects a reader. `AUTOINCREMENT` keeps sequences unique even after the whole
log has been compacted. Other commands do not compact; growth then pauses at
whatever the archive accumulates until the next tail run.

`discrawl status --json` reports `change_feed` with the contract version, head,
compaction floor and each table's generation.

## Contract 2

Contract 2 keeps every contract-1 rule: the entry format, generations,
compaction, digest and reader algorithm are identical, and contract-1 tables
keep their generations when an archive gains contract 2. A reader selects a
contract as follows:

- **Contract 1 reader** (unchanged): require `change_feed_meta.contract_version
  = 1` and read only the four contract-1 tables.
- **Contract 2 reader**: require `change_feed_meta.contract_version = 1` and a
  `change_feed_contracts` row with `contract_version = 2` whose `tables_json`
  lists the table. Otherwise treat the table as untracked (full read), exactly
  as for an absent feed.

What contract 2 adds for readers:

- `messages`, `members`, `channels` and `guilds` entries mean a content change
  (or a tombstone, scope or relationship-owner update). A reader can replace
  `updated_at >= cursor` scans with changed keys. `messages.updated_at` still
  advances when a message's attachments or mentions change, so the existing
  owner rebinding remains valid; the feed now also covers it directly.
- `entity_metrics` is an append-only value-change series. Each entry is an
  insert of a new value; rows are never updated by Discrawl.
- `entity_checks` (latest check per entity) is deliberately **not** tracked:
  it changes on every observation. Read it directly by `checked_at` if check
  clocks are needed.

An older (contract-1) binary that opens an archive with contract 2 leaves the
contract-2 triggers in place, so its writes are still recorded (including its
unconditional rewrites). If it recreates a lost log, the compaction floor
moves to the head, so contract-2 cursors below it reconcile.

## Reader algorithm

A reader stores, per table, a cursor `(generation_id, seq, digest)`, where
`digest` is the digest of the entry at `seq`, or `compacted_entry` (empty when
null) when no entry exists there.

```text
capture(T, cursor):
  BEGIN read transaction                         # one consistent snapshot
  if change_feed_tables has no row for T: full snapshot (feed absent)
  G     = select generation_id from change_feed_tables where table_name = T
  floor, compacted = select compacted_through, coalesce(compacted_entry, '') from change_feed_meta
  head  = max(sqlite_sequence.seq for 'change_log', max(change_log.seq), floor)
  valid = cursor != null
      and cursor.generation_id == G
      and floor <= cursor.seq <= head
      and (digest of entry at cursor.seq == cursor.digest
           or (no entry at cursor.seq and cursor.seq == floor
               and cursor.digest == compacted))
  if not valid:
      rows = full read of T (apply the reader's own scope filter)
  else:
      for each row_key with entries for T where cursor.seq < seq <= head,
          keeping the op of its highest seq:
        row = select * from T where primary key = row_key
        if row: upsert(row)                       # last op must not be 'd'
        else:   delete(row_key)                   # last op must be 'd'
        a contradiction means the contract was broken: reconcile fully
  cursor = (G, head, digest of entry at head, or compacted if head == floor and absent)
  COMMIT
```

The digest check detects a store restored in place from an earlier copy and
then written again: the generation is unchanged, but the restored file
reassigns sequences after its own head to different entries. Because
compaction keeps the digest of the last entry it removes, the check still
works when the reassigned entry at the reader's cursor has been compacted. A file replaced
by a new inode should also be detected by the reader's file identity check.
`internal/store/change_feed_test.go` contains a reference implementation that
reproduces each table exactly across random writes, repairs, deletions,
compaction, generation changes and an in-place restore.

### What the feed does not cover

- **Scope.** Entries are not filtered. Collection scope lives in `channels`;
  a reader that filters by allowed channels must recompute its allowed set on
  every capture and backfill or remove whole channels whose admission changed.
- **Owning messages (contract 1).** `mention_events` and `message_attachments`
  join to `messages`. Message writes that change relationship rows produce
  entries, but owner-only changes (for example a deletion tombstone or a text
  repair) do not. A contract-1 reader that embeds owner state must rebind it
  from its own message change source, such as the existing
  `messages.updated_at` cursor. Contract 2 tracks `messages` itself. An owner
  stream was rejected before 0.20.0 because its triggers ran on every
  unconditional message write; with conditional writes, unchanged rereads no
  longer execute a message statement at all.
- **Check clocks.** `entity_checks` is not tracked (see Contract 2).

## Writer cost

Each changed row in a tracked table writes one `change_log` row (plus the
`sqlite_sequence` update `AUTOINCREMENT` requires). The opt-in measurement
`DISCRAWL_CHANGE_FEED_OVERHEAD=1 go test -run TestChangeFeedWriterOverhead
./internal/store` uses an archive-shaped fixture: 20,000 messages with 1.5 KB
payloads, 13% with a mention, 1.5% with an attachment, and 10% with an
embedding, written in batches of 50 as an ingest, an unchanged resync and an
edit pass. The best of three runs on a Mac Studio (M-series, local SSD):

| Pass | Without feed | With feed | Added per message |
| --- | ---: | ---: | ---: |
| Ingest | 6.22 s | 7.77 s (+24.8%) | 77 µs |
| Unchanged resync | 6.37 s | 7.07 s (+11.0%) | 35 µs |
| Edit | 8.05 s | 8.33 s (+3.5%) | 14 µs |

The run wrote 17,700 entries using 888,832 bytes (1.50% of the 59 MB fixture,
about 50 bytes per entry). Most of the time cost is not the log rows: every
statement that writes a tracked table also compiles its trigger programs, and
the message writer runs relationship delete statements even for messages
without attachments or mentions. Baselines varied by about 15% between runs,
so treat the percentages as estimates. At the Studio archive's observed live
rate of a few changed rows per minute, the absolute cost is negligible; bulk
repairs cost proportionally more.
