# Member change index benchmark

This local synthetic benchmark measures the first writable open that installs
`idx_members_updated_identity`. It also measures a later open with the index
present. It is not a production timing, a live archive test, or a canary.

## Reproduce

Run from the repository root with the toolchain and dependencies in `go.mod`.
Use a temporary filesystem with sufficient free space for the generated
database, WAL, and SQLite temporary files. Set `TMPDIR` and `GOTMPDIR` to
that filesystem before testing. The benchmark never reads an existing archive.

```bash
GOMAXPROCS=2 GOWORK=off go test ./internal/store \
  -run '^TestMemberChangeIndex' -count=1
GOMAXPROCS=2 GOWORK=off go test ./internal/store \
  -run '^$' -bench '^BenchmarkMemberChangeIndexMigration$' \
  -benchtime=1x -count=1 -timeout=15m -v
```

The benchmark runs one iteration for each of 200,000 and 1,000,000 fake members.
For three independent iterations, use `-count=3` with `-benchtime=1x`.
Do not use adaptive calibration for routine qualification of these large fixtures.
Each iteration removes its temporary database after closing it.

## Fixture and checks

The fixture uses the real store schema, SQLite driver, and writable-open path.
It removes only the member update index before inserting deterministic rows.
The rows span 100 guilds, with repeated users across guilds, 1,000 distinct
timestamps, and a removal tombstone on every fifth row.
The fixture builds the normal member search index before measurement.

Fixture creation, inserts, search indexing, hashing, validation, and closing
connections are outside the measured intervals. `migration-open-ns/op` includes
all writable-open work, including the missing index build. It does not isolate
the `CREATE INDEX` statement. `indexed-reopen-ns/op` measures a later writable
open with the index present. The standard `ns/op` combines these two intervals.

The benchmark checks and reports:

- Go and SQLite runtime versions, row counts, identities, and timestamp ties.
- Database page counts and page size before and after migration.
- File bytes after closing each connection, including checkpointed WAL contents.
- Net logical file growth, separate from unmeasured peak temporary disk use.
- Equal SHA-256 hashes before migration, after migration, and after reopening.
- An unchanged schema version and a successful SQLite `quick_check`.
- A query plan using the member update index without a temporary sort.

The hash covers `rowid` and every member column, ordered by composite identity.
JSON encoding preserves field boundaries and NULL values.
Checks assert correctness, not arbitrary performance thresholds.

## Local synthetic result

Measured on September 25, 2026 with Go 1.27.1, `GOMAXPROCS=2`, Linux amd64,
an AMD EPYC-Genoa CPU, and Btrfs storage. Each scale used one iteration.
The integrated branch pins Crawlkit v0.16.4, modernc SQLite v1.59.0, and
libc v1.75.7, matching main at `a7ec0d0e3079c7225f09fcfc135cce8362d6abd3`.
SQLite reported runtime version 3.53.4. These measurements do not qualify a
different dependency set or a live publisher.

| Members | Migration open | Indexed reopen | File bytes before | File bytes after | Net growth |
| --- | ---: | ---: | ---: | ---: | ---: |
| 200,000 | 395.764 ms | 2.208 ms | 79,925,248 | 83,943,424 | 4,018,176 |
| 1,000,000 | 2,018.261 ms | 1.462 ms | 393,498,624 | 414,232,576 | 20,733,952 |

At a 4,096-byte page size, page counts changed from 19,513 to 20,494 and
from 96,069 to 101,131. Both scales preserved the row hash across all three
reads and retained schema version 6. Both returned `ok` from `quick_check`.
The query plan changed from a table scan with a temporary sort to
`SEARCH members USING INDEX idx_members_updated_identity (updated_at>?)`.

## Interpretation

Retain the exact source revision, command, dependency versions, storage type,
and aggregate output when qualifying a candidate.
Host load, cache state, member sizes, other archive tables, and filesystem
behavior can change timings. This fixture has no concurrent writers.
The index can reuse free database pages, so file growth is not its standalone
size. Logical file growth is not a disk-space budget for a real archive.

No result authorizes a main merge or live upgrade. The scheduled publisher
consumes main, so merging activates the migration on its next configured run.
Complete the separately reviewed, backup-first publisher canary and activation
qualification before merging. See the
[publisher activation boundary](../guides/data-storage.html#member-update-cursor-index)
and [backup-first procedure](../guides/data-storage.html#before-broader-index-rollout).
