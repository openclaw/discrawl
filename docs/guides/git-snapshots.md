# Git-backed snapshots

Discrawl can publish the SQLite archive as sharded, compressed NDJSON snapshots in a private Git repo, then auto-import that repo before local read commands. This gives readers org memory without Discord credentials.

Snapshot packing/import and git mirror mechanics are shared through
`crawlkit`. Discrawl still owns Discord-specific privacy policy: `@me` direct
messages, wiretap sync state, and local-only desktop rows are excluded from
published snapshots and are preserved locally on import.

## Publisher

```bash
discrawl publish --remote https://github.com/example/discord-archive.git --push
discrawl publish --readme path/to/discord-backup/README.md --push
discrawl publish --tag backup-2026-06-19 --push
```

The publisher uses your existing bot-synced archive. It exports non-DM tables only.
Use publish filters to share a narrower snapshot without narrowing the local
archive:

```bash
discrawl publish --public-only --push
discrawl publish --public-only --include-channels 1458141495701012561 --push
```

Filter rules:

- `--public-only` keeps only channels where the guild `@everyone` role has
  `VIEW_CHANNEL` after category and channel permission overwrites
- private threads are excluded
- `--include-channels` and `--exclude-channels` accept comma-separated channel
  ids; exclusions win
- including a forum parent also includes its allowed public threads
- combined filters intersect, so `--public-only --include-channels A,B` exports
  only included channels that are also public

The publisher can keep syncing a richer local archive. Filters only narrow the
Git snapshot seen by subscribers.

Filtered publishes currently cannot use `--readme`, because report totals are
computed from the full local archive. Filtered publishes also remove previously
generated Discrawl `README.md` reports from the share repo before committing, so
stale full-archive totals are not carried forward. Custom README files without
Discrawl report markers are left alone.

Published table shards retain their stable `tables/<table>/<ordinal>.jsonl.gz`
paths, including when the shared exporter uses private generation directories.
Existing released subscribers can consume unchanged, edited and appended
publications with ordinary merge updates. This does not convert checkpoints
created by unshipped generation-path publishers; genuinely removed shards still
require an explicit replacement decision.

Installation prepares replacements and backups of the actual working-tree
files before replacing any owned shard. A handled failure or cancellation before
manifest installation restores those preimages. If rollback itself fails, the
error reports possible partial state and the retained backup directories; restore
their `previous` files to the named targets and remove newly introduced targets
before publishing again. Do not delete those backups until recovery is complete.
After the new manifest is installed, obsolete-file cleanup failures return that
installed manifest with an error and stop before Git commit or push.

This requires temporary disk space for both staged replacements and backups.
It is not a crash, power-loss, concurrent-reader/writer or restart-recovery
guarantee. Unrelated tracked, staged and untracked files remain outside
publication ownership.

### Producer receipts

Automated publishers can opt in with all four environment variables:

```text
DISCRAWL_PRODUCER_REPOSITORY=openclaw/discrawl
DISCRAWL_PRODUCER_REVISION=<full lowercase Git commit SHA>
DISCRAWL_PRODUCER_RUN_ID=<positive GitHub run ID>
DISCRAWL_PRODUCER_RUN_ATTEMPT=<positive GitHub run attempt>
```

All absent retains legacy behavior; partial or invalid inputs fail before
publication. The source repository is fixed to the public Discrawl repository.
The publisher must independently check that the revision is its actual checkout.

`manifest.json` declares `files.producer = "producer.json"`. The schema-1 receipt
binds the exact manifest bytes, including the final newline, to the supplied
source/run assertions. It contains no destination URL, local path, token or
archive contents. A valid binding is not authenticated provenance, a binary
digest, payload verification or evidence that a push succeeded. Missing,
unsupported or mismatched receipts provide no usable producer attribution.

An unrelated `producer.json` is never silently adopted. Omitting producer inputs
on a later publication retires only a previously declared receipt. Commit and
push validate declared receipts; a retry that changes the committed manifest
or receipt is refused rather than restamped. README-only report updates leave
the receipt unchanged.

Crawlkit v0.15.0 also rejects authenticated cross-origin HTTP redirects. Configure
the final HTTPS endpoint instead of depending on an authenticated redirect.

## Subscriber

```bash
discrawl subscribe https://github.com/example/discord-archive.git
discrawl search "launch checklist"
discrawl messages --channel general --hours 24
```

`subscribe` is the Git-only setup path. It writes a config with `discord.token_source = "none"`, imports the snapshot, and does not require a Discord bot token. `sync` and `tail` remain disabled in this mode because they need live Discord access.

## Auto-update

Once `share.remote` is configured, read commands auto-fetch and import when the last share check is older than `share.stale_after` (default `15m`):

```bash
discrawl subscribe --stale-after 15m https://github.com/example/discord-archive.git
discrawl subscribe --no-auto-update https://github.com/example/discord-archive.git
discrawl subscribe --exact https://github.com/example/public-archive.git
```

`discrawl update` runs the configured pull/import step manually. The default `share.update_mode = "merge"` is delta-planned from crawlkit shard fingerprints. Older manifests without those fields fall back to Git blob identity, so the common publish shape only imports changed canonical shards. Routine merges preserve destination-only rows and do not replay generated event history or remote sync cursors. `subscribe --exact` persists exact replacement for a dedicated snapshot-only cache, so rows omitted from newer privacy-filtered snapshots are removed during both manual and automatic updates.

Discrawl does not silently fall back from merge mode to a full import. Removed shards and incompatible table changes leave the current database intact and require `discrawl update --force`. Forced and configured exact updates replace public snapshot tables and rebuild search indexes; local DM rows remain untouched. Exact mode intentionally removes other destination-only rows, so do not use it on a richer archive that also ingests live Discord or desktop-cache data. `--force` is a one-shot override; `--exact` is the persistent subscription contract.

`discrawl sync` does **not** auto-import the share unless `--update=auto` or `--update=force` is provided. Auto mode uses the configured update mode (merge by default); force mode performs exact replacement before live deltas.

## Hybrid mode

Keep normal Discord credentials configured **and** set `share.remote`:

```bash
discrawl sync --update=auto       # import snapshot delta first, then live deltas
discrawl messages --sync          # blocking pre-query sync for matched scope
discrawl sync --all-channels      # broader live repair
discrawl sync --full              # historical backfill
```

## What is published

- non-DM archive tables (DM `@me` rows are always excluded)
- cached non-DM attachment media as gzip-compressed files by default; use
  `publish --no-media` to omit files that are already in `cache_dir/media`
- with publish filters: only matching channel-scoped rows, matching embedding
  rows, and member rows referenced by matching messages
- with publish filters: no share manifest state and no guild-level member
  freshness markers, because those describe the full archive
- without publish filters and with `--readme`: README activity block - latest
  update time, latest archived message, archive totals, day/week/month activity
- `embedding_jobs` is never exported

## Backing up media

Media backup is publisher-driven and local-cache based:

```bash
discrawl sync --with-media
discrawl publish --push
```

`sync --with-media` and `attachments fetch` download Discord attachment bytes
into `cache_dir/media`. `publish --push` then exports cached non-DM media into
the Git snapshot repo as gzip-compressed `media/...gz` files. Imports restore
those files back into the raw local cache layout. Older snapshots that contain
raw `media/...` files still import; the next media publish clears the legacy
media tree and rewrites it in gzip form. `publish` does not fetch missing
Discord files itself, so scheduled Git backups that should include media must
fetch media before publishing. Set `sync.attachment_media = true` for scheduled
sync jobs and leave `share.media = true` to include cached media in
publish/update flows.

Discord CDN URLs can expire or be removed. Those fetches are stored as failed
with their HTTP status, commonly `404`; this does not block publishing files
that were fetched successfully.

## Backing up vectors

```bash
discrawl publish --with-embeddings --push
discrawl subscribe --with-embeddings https://github.com/example/discord-archive.git
discrawl update --with-embeddings
```

Stored under `embeddings/<provider>/<model>/<input_version>/...`. Import only restores matching identities; Ollama/nomic subscribers do not accidentally pick up OpenAI/text-embedding vectors. Publishing without `--with-embeddings` omits embedding manifests instead of carrying forward an older bundle.

## CI

The Docker smoke test installs `discrawl` in a clean Go container, subscribes to a Git snapshot repo, then checks `search`, `messages`, `sql`, and `report`:

```bash
DISCRAWL_DOCKER_TEST=1 go test ./internal/cli -run TestDockerGitSourceSmoke -count=1
```

The backup workflows restore and save `.discrawl-ci/discrawl.db` with `actions/cache`. On a warm runner cache, scheduled publishers skip the pre-sync snapshot import and go straight to the live latest-message delta before publishing. Cache misses still import the latest published snapshot first so `--latest-only` has channel cursors to resume from.

## See also

- [`publish`](../commands/publish.html)
- [`subscribe`](../commands/subscribe.html)
- [`update`](../commands/update.html)
- [`report`](../commands/report.html)
