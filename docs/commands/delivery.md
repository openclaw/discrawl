# `delivery`

Deliver qualified native report changes through a private archive service.
These commands use explicit database paths and do not load the collector's
configuration, check for releases, contact Discord, or start a schedule.

## Usage

```sh
discrawl delivery prepare-seed --source-db /private/native.sqlite \
  --output /private/new-seed --guilds 123456789
discrawl delivery init --outbox /private/outbox.sqlite \
  --registration /private/registration.json --ownership /private/ownership.jsonl
discrawl delivery capture --outbox /private/outbox.sqlite \
  --source-db /private/native.sqlite --max-events 10000 --max-bytes 8388608
discrawl delivery drain --outbox /private/outbox.sqlite \
  --token-env DISCRAWL_PUBLISHER_TOKEN --max-parts 64
```

`prepare-seed` creates an absent private output directory. Its parent must exist.
It exports the five native report tables for the explicit whole-guild scope and
records their native baseline in the same read transaction. It includes private
channels in that scope; this is a private service seed, not a public Git export.
Guild and channel coverage checkpoints follow the same scope rules as incremental
delivery. Host/worker receipts and absent-channel checkpoints are excluded.

The output contains `snapshot/`, a sorted `ownership.jsonl`, and
`preparation.json`. The last file is written only after successful export and
ownership checks; its SHA-256 is the reported `qualification_sha256`. It proves
local snapshot/baseline agreement, not remote seed completion or permission to
publish. Keep it private: the baseline includes physical file identity. Temporary
ownership sorting uses SQLite on disk, with a bounded page cache. The default
timeout is 30 minutes. Failed output is retained and never overwritten.

The source must already have the current schema and native coverage feed; the
command opens it read-only. The existing v1 snapshot contract rejects BLOBs,
invalid text, oversized rows and integers outside its exact JSON range rather
than silently changing them. A source with those values needs a separately
qualified seed encoding. Ordinary acquisition may continue while the read
transaction is open; provision disk space for its retained WAL and the export.

`init` creates an absent outbox with private permissions. Existing files are
never overwritten. A failed initialization leaves its file for diagnosis;
do not treat it as a usable outbox or delete it without reconciling the failure.
Its default timeout is five minutes. `capture` and `drain` default to 30 seconds.
All commands accept `--timeout` with a positive Go duration.

`capture` opens the source read-only and requires its existing native change
feed. It does not migrate an older database. The qualified physical file,
generation identities and acknowledged cursor must match. A pending capture
blocks new capture before any source read. The event and byte limits bound one
complete native interval; overflow fails without advancing the baseline.
Native capture currently supports macOS and Linux file identities.

`drain` reads the named environment variable and sends only the pending capture
present at entry. It uses the origin, archive and binding stored at initialization;
there is no destination override. It neither captures new data nor retries a
failed request automatically. Invoke it again to replay the exact persisted
part after a failed request or local receipt write. Only final completion advances
the acknowledged native baseline. The maximum part count is 64.

## Qualified input files

Initialization requires existing qualification evidence. The command verifies
the input format and ownership digest; it does not prove that a remote seed
matches the native source or grant permission to publish private data.

The registration is a JSON object with exactly two fields:

- `format`: `discrawl-delivery-registration/v1`.
- `registration`: the JSON encoding of `remotedelivery.Registration`.

The nested registration uses these exact exported field names:

| Field | Required value |
| --- | --- |
| `Origin` | Service origin, without credentials, path, query or fragment |
| `Archive`, `BindingID` | Registered private archive and immutable binding |
| `SeedManifestSHA256`, `QualificationSHA256` | Lowercase SHA-256 digests of the completed seed manifest and qualification evidence |
| `OwnedKeysSHA256`, `OwnedKeysCount` | Digest and count of the qualified seed ownership stream |
| `Scope` | `{"GuildIDs":["<qualified-guild-id>"]}`; whole-guild private scope |
| `Baseline` | `store.NativeBaseline`: `SourceID` and `Cursor` with `Sequence`, `Entry`, and all five `Generations` (`Table`, `GenerationID`, `SchemaSHA256`) |
| `ServerCursor` | Completed seed's receiver cursor |

Registration input is limited to 1 MiB. Unknown fields and trailing JSON are
rejected. Keep these files private: the baseline contains local file identity.
Copying a database changes that identity; a copy's baseline does not qualify the
live source. Observing a current cursor alone does not qualify seed contents.

The ownership file contains one JSON-encoded `remotedelivery.SeedKey` per line:

```json
{"Table":"channels","Key":[{"Kind":"text","Text":"123"}]}
{"Table":"guilds","Key":[{"Kind":"text","Text":"456"}]}
```

Every key must belong to the qualified seed projection, including its scoped
coverage metadata. Sort strictly by `remotedelivery.OwnershipKey`, without
duplicates. `OwnedKeysSHA256` hashes each canonical key's UTF-8 bytes, prefixed
by its unsigned 64-bit big-endian byte length. The outbox verifies this digest
and the count while streaming. Each input line is limited to 1 MiB.

## Results and recovery

Output is JSON regardless of global display flags. Status is `prepared`, `initialized`,
`idle`, `staged`, `pending`, `part_limit`, or `complete`. Capture results include
the decimal-string `native_sequence`; persisted captures include `capture_id`.
Delivery includes `receipts_recorded` when nonzero. Results omit message content,
source paths, tokens and registration details.

A failed drain can print a partial result and still exit nonzero. This means
that remote or local receipts may already exist, not that the operation rolled
back. Preserve the outbox and resume its saved bytes. Keep the original source,
qualification records and outbox together until a separately qualified cutover.
