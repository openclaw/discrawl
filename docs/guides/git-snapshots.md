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

## Subscriber

```bash
discrawl subscribe https://github.com/example/discord-archive.git
discrawl search "launch checklist"
discrawl messages --channel general --hours 24
```

`subscribe` is the Git-only setup path. It writes a config with `discord.token_source = "none"`, imports the snapshot, and does not require a Discord bot token. `sync` and `tail` remain disabled in this mode because they need live Discord access.

## Auto-update

Once `share.remote` is configured, read commands auto-fetch and import when the local share import is older than `share.stale_after` (default `15m`):

```bash
discrawl subscribe --stale-after 15m https://github.com/example/discord-archive.git
discrawl subscribe --no-auto-update https://github.com/example/discord-archive.git
```

`discrawl update` forces the same pull/import step manually. `discrawl update --ref <tag-or-commit>` reads the historical Git objects directly and leaves the share checkout unchanged. Snapshot imports are delta-planned from crawlkit shard fingerprints. Older manifests without those fields fall back to Git blob identity, so the common publish shape only imports the changed message tail shard plus small cursor tables. Unsafe table-shape changes still fall back to a full import.

`discrawl sync` does **not** auto-import the share unless `--update=auto` or `--update=force` is provided, so routine live refreshes stay fast.

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
