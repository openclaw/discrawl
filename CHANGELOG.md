# Changelog

## 0.11.1 - Unreleased

### Fixes

- Keep routine Git snapshot refreshes fast when only the newest message shards changed, avoiding unnecessary full archive rebuilds for subscribed readers.

### Maintenance

- Refresh the shared archive, SQLite, system, and text-processing dependencies.
- Harden the repo-local autoreview agent scope guard and harness.

## 0.11.0 - 2026-06-11

### Changes

- Add optional faster semantic-search scoring with `[search.embeddings].vector_backend = "turbovec"`, while keeping exact cosine scoring as the default. Thanks @vincentkoc.
- Added the Homebrew install command to the `discrawl.sh` landing hero and agent docs index, with a one-row desktop layout and copy button.
- Let `discrawl cloud publish` upload a sanitized SQLite mirror to Cloudflare R2, so remote subscribers can restore or inspect an archive snapshot as well as query live Cloudflare D1 rows.
- Keep the remote SQLite mirror privacy-bounded: it excludes `@me` rows, raw Discord JSON, attachment URLs, embeddings, and local-only tables.
- Compress the R2 SQLite mirror into bounded gzip chunks with an explicit row-count and privacy manifest before upload.

### Fixes

- Kept resumed `sync --full` backfills from moving latest-message checkpoints backward, avoiding duplicate recrawls after large interrupted channel syncs. Thanks @hannesrudolph.
- Made `messages --sync` fail fast with an omit-`--sync` hint when a live `tail` process owns the sync lock, while plain `messages` reads continue without waiting. Thanks @jeanmonet.
- Add `discrawl cloud publish --sqlite-only` so publishers can refresh the R2 SQLite bundle without re-ingesting D1 rows.
- Split Cloudflare SQLite mirror uploads into 64 MiB chunks that stay within Worker upload limits.

### Maintenance

- Refresh the shared archive runtime through `crawlkit` v0.12.0.
- Add a CI deadcode check.
- Refresh the secret-scanning action.

## 0.10.0 - 2026-05-27

### Changes

- Add read-only Cloudflare remote archive mode, so subscribers can search and read hosted archive data without creating a local SQLite database or using Discord credentials.
- Add `[remote]` config, `subscribe-cloud`, GitHub-backed `remote login`, `remote status`, `remote archives`, and cloud-mode `status --json` for remote archive setup and inspection.
- Route cloud-mode `search` and filtered `messages` reads to the hosted archive, so subscribers can inspect remote D1 data without local SQLite.
- Add `discrawl cloud publish` so publishers can send non-DM local SQLite rows to a hosted remote archive without changing Git snapshot publishing.

### Fixes

- Update the release workflow's Homebrew tap target.

### Maintenance

- Refresh the shared archive runtime through `crawlkit` v0.8.0.
- Add the repo-local autoreview agent skill and harness.
- Add constrained Crabbox remote-validation setup.
- Refresh CI and release workflow dependencies.

## 0.9.1 - 2026-05-18

### Changes

- Add `discrawl check-update` and cached interactive terminal notices so users can see when a newer Discrawl release is available without checking GitHub by hand.

### Fixes

- Derive release checks from the configured release repository, so forks and relocated installs do not report updates from the wrong GitHub project.

## 0.9.0 - 2026-05-17

### Changes

- Media-enabled `discrawl publish` now stores shared attachment media as gzip files, reducing Git snapshot size while still importing older raw-media snapshots.
- Semantic search now ranks lightweight embedding rows first and hydrates full message details only for winning results, so large vector sets do less SQLite work per query.

### Fixes

- Bounded gzip media restore size and hash verification so malformed shared snapshots cannot decompress unbounded data.
- Cancelled concurrent message-sync workers when a peer hits a fatal channel error, avoiding wasted work after the run is already going to fail.
- Rejected inaccessible explicit guild targets during `init --guild` and `sync --guild/--guilds` instead of silently treating them as successful empty syncs.
- Hardened embedding snapshot imports against unsafe manifest paths, symlink escapes, and unbounded gzip input.
- Limited FTS search fallback to missing or unsupported FTS infrastructure errors, so unrelated query failures are reported instead of hidden by fallback search.
- Re-sniffed previously skipped wiretap cache files instead of treating unchanged skipped fingerprints as permanently unchanged.

### Maintenance

- Refresh the secret-scanning action.

## 0.8.0 - 2026-05-15

### Changes

- Added attachment media caching with `discrawl attachments`, `attachments fetch`, `sync --with-media`, and Git snapshot backup/restore for cached non-DM media files.
- Documented the media backup workflow, including Discord CDN fetch failures, local cache behavior, and when `discrawl publish` can include cached media.
- Added a Docker image that keeps config, SQLite data, cache, and Git snapshot state under `/data`, with smoke coverage for the packaged image.

### Fixes

- Kept large Git snapshot imports and FTS rebuilds from exhausting memory on small hosts by using file-backed SQLite temporary storage and a bounded import cache. The reported case was a 7.3 GB archive on a 7.4 GB RAM host. (#65) Thanks @hxy91819.

### Maintenance

- Moved stable store SQL for sync state, messages, attachments, embedding jobs, members, and status reads/writes to generated typed wrappers while leaving dynamic FTS, semantic search, report, share, and user SQL handwritten.

## 0.7.2 - 2026-05-11

### Fixes

- Kept Git snapshot imports incremental when metadata, attachment, mention, or event tables changed alongside the message tail, avoiding full archive replays for routine shared-archive updates.

## 0.7.1 - 2026-05-11

### Changes

- `discrawl publish` can now narrow Git snapshots with `--public-only`, `--include-channels`, and `--exclude-channels`, while the local archive can still sync from the full bot-visible dataset.
- New installs now use OS-native config/runtime paths via crawlkit, while existing `~/.discrawl` installs keep working until users deliberately migrate.
- Moved top-level CLI parsing onto Kong while preserving Discrawl's existing command dispatch and archive lock policy.

### Maintenance

- Moved Discrawl's platform path policy back into crawlkit so config, data, cache, log, and share directory defaults stay shared across crawler apps.

### Fixes

- Kept documented command-local search flags working after the query, such as `discrawl search "term" --limit 5`. Thanks @PrinceOfEgypt.
- `help search`, `search --help`, `messages --help`, and `sql --help` now print focused command help without opening config, stores, or triggering Git snapshot auto-update.
- `discrawl sync` now warns once for newly discovered Discord Missing Access / Unknown Channel skips, then keeps repeat unavailable-channel skips out of normal logs while preserving summary counts.

## 0.7.0 - 2026-05-08

### Changes

- Added `discrawl tui`, a terminal archive browser for stored guild messages and local `@me` wiretap DMs using the shared crawlkit pane browser.
- Added crawlkit-backed `metadata --json`, `status --json`, and `doctor --json` control surfaces for launchers, automation, and CI checks.
- Published the generated documentation site at `discrawl.sh`, including command pages, install/setup docs, configuration, security notes, guides, a contact page, and social cards.
- Moved the Go module and release metadata to `github.com/openclaw/discrawl`.

### Fixes

- Made the terminal browser more useful and accurate: default guild scoping, newest-message startup, compact panes, selected-message detail panes, count-header sorting, local/remote status labels, right-click actions, Discord message URLs, row labels, direct-message pane labels, mention rendering, inline mention resolution, attachment details, and reply-context hydration without broad thread scans.
- Kept read-only commands such as `search`, `messages`, and safe `sql` usable while `tail` or another writer holds the sync lock. Thanks @PrinceOfEgypt.
- Kept `tui --help`, status, and terminal-browser reads safe for fresh or missing local databases without triggering Git snapshot auto-update.
- Kept local-only snapshot rows filtered during shared archive imports and forwarded snapshot import progress through the crawlkit import path.
- Made stale Git snapshot imports plan shard deltas from crawlkit file fingerprints or Git object identity, so routine shared-archive refreshes import changed message tail shards instead of rebuilding every table and FTS index.
- Included progress percentages in message-sync logs.
- Fixed GoReleaser version stamping after the module path move.

### Documentation

- Documented the crawlkit-backed config/status/control, snapshot, mirror, sync-state, output, and shared TUI surfaces now used on `main`.
- Clarified that Discord bot sync, desktop wiretap parsing, DM privacy filters, schema ownership, FTS/ranking, embeddings, and analytics remain app-owned.
- Aligned terminal-browser docs with the gitcrawl-style shared TUI model: channel/person/thread groups, message rows, detail/thread panes, sorting, mouse selection, right-click actions, and local/remote status chrome.
- Refreshed the repo-local `discrawl` agent skill for local Discord archive, freshness, query, boundary, TUI, verification, and read-only SQL workflows.

### Maintenance

- Migrated runtime paths, SQLite opening, archive mirror/export/import helpers, output/status wiring, and TUI plumbing onto the shared `crawlkit` infrastructure.
- Moved reusable embedding providers and vector helpers onto `crawlkit` while keeping Discrawl-owned storage, FTS, queueing, and privacy filters local.
- Updated crawlkit through `v0.4.1`, switched imports to `github.com/openclaw/crawlkit`, and added CI smoke coverage for the crawlkit control surface and merge behavior.
- Added CodeQL, verified secret scanning, protected automation owners, stale issue automation, `.editorconfig`, and `.gitattributes`.
- Added release workflow automation that dispatches the Homebrew tap formula update after GoReleaser publishes a tag.

## 0.6.6 - 2026-05-05

### Fixes

- `wiretap` now uses a fast default path for Discord Chromium cache imports: it scans cheap context files plus route-bearing HTTP cache entries, checkpoints file progress in batches, and leaves exhaustive historical cache archaeology behind `--full-cache` / `desktop.full_cache`.

## 0.6.5 - 2026-05-03

### Fixes

- Scheduled Discord backup publishing now skips redundant pre-sync snapshot imports when the workflow DB cache is warm, keeping fresh Git snapshots from getting delayed by a full archive reimport.
- `discrawl sync` now keeps Git snapshot refreshes explicit by default; use `--update=auto` or `--update=force` when you want a sync run to pull/import the shared snapshot before live Discord or desktop-cache deltas.
- Snapshot imports now emit phase/table/file progress and keep the sync lock file updated with the active phase, making long update/import runs diagnosable instead of looking hung.
- Recent-message scans are backed by a plain `messages(created_at, id)` index so archive freshness and short-window analysis queries avoid full-table scans.

## 0.6.4 - 2026-05-03

### Fixes

- `discrawl` now handles SIGINT/SIGTERM by canceling active sync/import contexts so large SQLite and FTS writes can roll back and close cleanly instead of being terminated mid-transaction.

### Maintenance

- Refreshed dependency and CI tooling pins, including GoReleaser, `go-toml`, golangci-lint, and gosec.
- Tightened CI compatibility with the latest linters and made signal-cancellation and sync fixture tests deterministic under the race detector.

## 0.6.3 - 2026-05-01

### Fixes

- Added OS keyring fallback for Discord bot-token resolution, keeping env as the first source and documenting the default keyring item. (#17)
- Clarified and locked down FTS query normalization so operator-like search terms such as `AND`, `OR`, `NOT`, `NEAR`, and `*` stay parameterized and quoted before SQLite `MATCH`. Thanks @mvanhorn.

### Maintenance

- Tightened Go linting with additional golangci-lint checks for compiler directives, host/port formatting, predeclared identifiers, missing command contexts, and related code-quality regressions.
- Updated test subprocess helpers to use test-scoped contexts and cleaned up assertions so the stricter CI suite stays green.

## 0.6.2 - 2026-05-01

### Changes

- Added `discrawl digest` for per-channel activity summaries with messages, replies, active authors, top posters, and top mentions. Thanks @mvanhorn.
- Added `discrawl analytics quiet` and `discrawl analytics trends` for finding silent top-level channels and week-over-week channel volume. Thanks @mvanhorn.

### Fixes

- `discrawl digest` now reports reply counts as `replies` instead of mislabeling reply roots as Discord threads.
- `discrawl sync` now serializes concurrent runs with a local lock, preventing two refreshes from writing the archive at the same time.
- Git snapshot imports now keep SQLite crash recovery enabled and share the same archive lock as sync, update, tail, wiretap, embed, and auto-update reads so interrupted imports are less likely to corrupt the live database.
- Git snapshot imports now recover from corrupt local FTS tables by dropping and rebuilding search indexes, and repair missing guild IDs from channel metadata so shared archive reports stay fresh.
- Channel-history sync now falls back to the channel guild when Discord omits `message.guild_id`, keeping messages, attachments, mentions, and FTS rows correctly scoped.

## 0.6.1 - 2026-04-28

### Fixes

- Repeated `sync --source wiretap` runs now skip unchanged Discord Desktop cache files and report unchanged file counts, making steady-state local-cache refreshes much faster.
- `sync --full --skip-members` now also skips member crawls when resuming incomplete stored channels, so backfills do not unexpectedly refresh the full guild member list.

### Maintenance

- Refactored sync-mode handling so routine latest syncs, `--all-channels`, `--full`, and member-refresh decisions share clearer internal paths with regression coverage.
- Refreshed Go module dependencies and CI tool/action pins, including staticcheck, gofumpt, gosec, govulncheck, gitleaks, setup-node, and GoReleaser.
- Hardened report README writes and Discord Desktop cache reads with root-scoped filesystem access to satisfy the latest gosec checks.

## 0.6.0 - 2026-04-24

### Changes

- `dms` now lists local wiretap DM conversations and can read or search one DM thread with `--with`, `--last`, and `--search`, so common DM queries no longer require raw SQL.
- `search --dm` and `messages --dm` now target the local-only `@me` archive directly and skip Git snapshot auto-update, since DMs are never imported from the shared mirror.

### Fixes

- Wiretap now infers fallback DM channel names from cached Discord user/profile data, so channels discovered only from route/message cache entries resolve to names like `Vincent K` instead of `channel-*`.
- Wiretap message output now preserves sanitized author labels in stored metadata, improving `dms` and `messages` output without storing raw desktop cache payloads.

### Maintenance

- Go module dependencies and lint rules were refreshed for the current Go toolchain, including stricter JSON marshal checks and modern simplification rules.

### Tests

- Added regression coverage for DM channel-name inference from cached profile data when Discord Desktop cache lacks explicit channel recipient metadata.
- Added coverage for local DM conversation listing/filtering, DM cleanup paths, share import/export helpers, CLI DM windows, and Discord Desktop import helper edge cases.
- CI now runs uncached test and race suites, checks `go mod tidy`, and performs a snapshot GoReleaser build before release tags.

## 0.5.1 - 2026-04-24

### Fixes

- Git snapshot export/import now keeps wiretap DMs strictly local: `@me` rows, wiretap sync state, and DM vectors are excluded from published snapshots while existing local DM rows are preserved on import.
- Publishing without `--with-embeddings` now omits old embedding manifests instead of carrying forward a stale vector bundle.

## 0.5.0 - 2026-04-24

### Changes

- `sync --source both|discord|wiretap` controls bot-token sync versus local Discord Desktop cache import; the default is `both`.
- `wiretap` imports classifiable cached Discord Desktop message payloads into the local archive, including proven DMs under synthetic guild id `@me`, without using user tokens.
- `sync` now defaults to the fast latest-message refresh path for untargeted runs; use `--all-channels` for the broad stored-channel repair sweep or `--full` for historical backfill.

## 0.4.1 - 2026-04-22

### Fixes

- existing archives that already report schema version 2 now self-heal missing embedding tables and columns before 0.4.x sync/update commands continue.

## 0.4.0 - 2026-04-22

### Changes

- semantic message search now ranks across the full compatible local vector set instead of only the newest candidate window. (#36) Thanks @GaosCode.
- hybrid message search now fuses FTS with local semantic vectors while avoiding embedding-provider calls when no local vectors exist. (#37) Thanks @GaosCode.
- local embedding providers now support OpenAI-compatible endpoints, Ollama, and llama.cpp, and `doctor` can probe the configured provider before you queue vectors
- `embed` now drains the queued embedding backlog in bounded batches, requeues safely on provider throttling, and drops stale stored vectors when messages no longer have embeddable content
- Git snapshot publishing can now opt in to backing up generated embedding vectors with `--with-embeddings` while still keeping embedding queue state local.
- Git-backed snapshot imports are now much faster on large archives by using import-only SQLite pragmas and bulk-load FTS5 settings during search index rebuilds
- `messages` and `mentions` now use composite read-path indexes so larger archives spend less time sorting/filtering common guild, channel, and author queries

### Fixes

- normalized message text is now sanitized before it reaches SQLite and FTS5, repairing malformed UTF-8 and stripping invisible/control-character noise that can poison search content
- Git-backed snapshots now keep embedding queue state and generated vectors local to each archive, so subscribers no longer inherit misleading embedding backlog metadata. (#38) Thanks @GaosCode.

### Docs

- docs now cover semantic and hybrid search setup, embedding privacy, Git snapshot behavior, and local vector rebuilds. (#39) Thanks @GaosCode.

### Tests

- Git embedding snapshot export/import now has CLI, share-package, and Docker E2E coverage.
- total Go test coverage now reaches the 85% line.

## 0.3.0 - 2026-04-21

- `sync --all` now bypasses `default_guild_id` so one run can fan out across every discovered guild without clearing the single-guild default first
- `sync --full` no longer aborts when forum thread discovery hits Discord `403 Missing Access`; inaccessible channels are skipped and marked unavailable while accessible channels continue syncing
- startup now validates and stamps SQLite schema version via `PRAGMA user_version`, and fails fast if the local DB schema is newer than the running binary
- git-backed archive sharing can now export/import compressed JSONL snapshots with manifests, subscribe to a Git repo as the data source, and run in git-only mode without Discord credentials
- `messages`, `search`, and reports can automatically refresh stale git-backed data, preferring the Git snapshot before falling back to live Discord when both sources are configured
- the Discord backup publisher workflow now syncs latest messages, publishes the archive to a private GitHub repo, serializes concurrent runs, validates required secrets, and skips the member crawl for faster updates
- the backup report workflow now updates README activity stats from the backup action and keeps those queries bounded with process timeouts
- `sync --latest-only` adds a lightweight refresh path for checking recent Discord messages without doing a full historical crawl
- repository imports now skip expensive rebuilds when the snapshot manifest is already current, and GitHub Actions persist the warmed SQLite database across runs
- the Docker git-source smoke test now verifies that a fresh install can subscribe to a repository-only archive and query messages, SQL, and reports
- CI now uses Go 1.26.2, `actions/setup-go` 6.4.0, cache actions 5.0.5, Node 24 for report generation, and refreshed SQLite dependencies

## 0.2.0 - 2026-03-26

- much faster `sync --full` behavior on large archives: incomplete backfills are auto-batched, active-thread discovery is more precise, and steady-state refreshes avoid re-scanning every archived thread once history is already complete
- `sync --since` now reliably honors the cutoff during bootstrap and full-history backfill, while still allowing a later `sync --full` without `--since` to continue older history
- full-sync progress is more resilient: slow member crawls no longer hold message sync hostage, and stale unavailable-channel markers are cleared so recovered channels can sync again
- offline member-profile search is now much richer: `members search` matches archived profile fields in addition to names
- `members show` now accepts either Discord IDs or queries and can include recent messages plus message stats for the resolved member
- archived profile extraction now surfaces stored fields like `bio`, `pronouns`, `location`, `website`, `x`, `github`, and discovered URLs when present
- `messages --sync` can do a blocking pre-query refresh for the matching channel or guild scope before reading the local archive
- `messages --hours` adds recent-hour slices without manual RFC3339 timestamps
- `messages --last` returns the newest matching rows while still printing them oldest-to-newest

## 0.1.0 - 2026-03-08

- initial public release of `discrawl`
- multi-guild Discord crawler with single-guild default UX
- local SQLite archive with FTS5 search
- commands: `init`, `sync`, `tail`, `search`, `messages`, `mentions`, `sql`, `members`, `channels`, `status`, `doctor`
- env-based bot token discovery
- resumable full-history sync, live gateway tailing, repair sync loop, targeted channel sync
- attachment-text indexing for small text-like uploads
- structured user and role mention indexing/querying
- empty-message filtering based on real searchable/displayable content instead of raw body only
- CI with lint, tests, secret scanning, and coverage enforcement
- release plumbing via GoReleaser, GitHub Actions, and Homebrew tap packaging
- sync correctness fixes for empty channels, inaccessible channels, unknown channels, and large-channel resume behavior
- SQLite/FTS performance fixes for backfill throughput and lower write amplification
