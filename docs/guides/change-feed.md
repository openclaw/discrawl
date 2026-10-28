# Source change feed

Discrawl records a durable change feed for four tables so that a reader can
copy only changed rows instead of rereading whole tables:

| Table | Primary key (`row_key` order) |
| --- | --- |
| `message_attachments` | `attachment_id` |
| `mention_events` | `event_id` |
| `message_embedding_history` | `message_id, provider, model, input_version, embedded_at` |
| `attachment_text_history` | `attachment_id, text_sha256` |

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
`sqlite_master.sql`), `contract_version` and `updated_at`.

`change_feed_meta` is a singleton: `contract_version` (currently 1),
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

Ordinary writers rewrite relationship rows: storing a message deletes and
reinserts its attachments, then updates their text status, and deletes and
reinserts its mention events with new `event_id` values. The feed records
these native changes faithfully, so `mention_events` is not insert-only.

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
| `snapshot_import` | A full Git snapshot replacement (`subscribe --exact`/`Import`) restored `message_attachments` and `mention_events` |

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
- **Owning messages.** `mention_events` and `message_attachments` join to
  `messages`. Message writes that change relationship rows produce entries,
  but owner-only changes (for example a deletion tombstone or a text repair)
  do not. A reader that embeds owner state must rebind it from its own message
  change source, such as the existing `messages.updated_at` cursor. An owner
  stream was measured and rejected because its triggers ran on every message
  write and cost about 62% of message write time.

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
