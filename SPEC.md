# discrawl Spec

This file is the build contract for an AI agent working in this repo.

Goal:

- build a local-first Discord guild crawler
- mirror all guild data the configured bot can access
- import classifiable Discord Desktop cache messages without user tokens, including DMs
- store it in SQLite
- support fast text search, semantic search, and raw SQL
- support one-shot backfill and long-running live sync

This spec is intentionally detailed so an agent can keep shipping without re-asking foundational questions.

## Product Summary

`discrawl` is a Go CLI that mirrors Discord guild data into local SQLite.

V1 scope:

- one guild at a time
- all accessible text channels
- all accessible announcement channels
- all accessible forum channels and their posts
- all accessible public threads
- all accessible private threads
- archived thread coverage
- full message history
- desktop-local import from cached Discord Desktop artifacts, with proven DMs stored under `@me`
- current member snapshot
- FTS5 search
- optional OpenAI embeddings with local vector search
- raw SQL access

Out of scope for V1:

- remote/API personal-account DM crawling
- Discord user-token automation/selfbot flows
- reactions as primary indexed entities
- attachment blob downloads by default
- cross-guild unified sync UX
- write-back or moderation actions

## Requirements Already Chosen

These are settled unless the user explicitly changes them:

- config format: `TOML`
- config location: platform-native XDG config dir, e.g.
  `${XDG_CONFIG_HOME:-~/.config}/discrawl/config.toml` on Linux
- DB location: platform-native XDG data dir, e.g.
  `${XDG_DATA_HOME:-~/.local/share}/discrawl/discrawl.db` on Linux
- cache dir: platform-native XDG cache dir, e.g. `${XDG_CACHE_HOME:-~/.cache}/discrawl/`
- log dir: platform-native XDG state dir, e.g.
  `${XDG_STATE_HOME:-~/.local/state}/discrawl/logs/`
- legacy installs with `~/.discrawl/config.toml` continue to load that config when the new default
  config file does not exist, even if XDG env vars are present
- token source: `DISCORD_BOT_TOKEN` or configured env var, then optional OS keyring fallback
- guild model: one guild in CLI UX, multi-guild-ready schema
- search: hybrid, with FTS first and embeddings optional
- embedding provider: OpenAI
- API key source: `OPENAI_API_KEY` from shell env
- message retention: current canonical row + append-only event log
- member retention: current snapshot only
- files: metadata only in DB, fetch binaries later on demand
- reactions: not important for V1
- polls: flatten into text during normalization

## Local Environment Contract

An agent should assume:

- repo path: `~/Projects/discrawl`
- shell: `zsh`
- Go is installed and modern
- user is Peter
- user keeps many secrets in `~/.profile`

### Key file paths

- platform-native Discrawl config file
- platform-native Discrawl SQLite database
- `~/.profile`

### OpenAI embeddings key

Do not store raw API keys in repo files.

Expected source:

- env var `OPENAI_API_KEY`

Typical place to discover it locally:

- `~/.profile`

The code should read the env var at runtime, not copy the value into config by default.

## Discord Data Model Notes

Important Discord facts that drive the schema:

- channels and threads are closely related; threads should be stored as channels
- forum posts are threads under a forum parent
- message history is paginated and must be backfilled incrementally
- live updates come from Gateway events, not from polling alone
- personal DMs are only supported through desktop-local cache import
- desktop cache messages without a provable channel/guild route are skipped rather than stored as unknown data
- archived public and private threads must be enumerated explicitly
- private archived thread access may require elevated bot perms like `Manage Threads`

### Entities to mirror

- guild
- categories
- channels
- threads
- members
- messages
- message lifecycle events

### Channel kinds worth preserving

- category
- text
- announcement
- forum
- thread public
- thread private
- thread announcement

Voice channels can be mirrored as metadata rows, but there is no need to crawl message history because there is none.

## Database Design

Use SQLite.

Requirements:

- WAL mode
- foreign keys on
- FTS5 enabled
- vector extension optional

### Tables

At minimum:

- `guilds`
- `channels`
- `members`
- `messages`
- `message_events`
- `sync_state`
- `embedding_jobs`
- `message_fts`

Optional once vectors are wired:

- `message_embeddings`

### `guilds`

Suggested shape:

```sql
create table guilds (
  id text primary key,
  name text not null,
  icon text,
  raw_json text not null,
  updated_at text not null
);
```

### `channels`

Threads should live in the same table.

Suggested shape:

```sql
create table channels (
  id text primary key,
  guild_id text not null,
  parent_id text,
  kind text not null,
  name text not null,
  topic text,
  position integer,
  is_nsfw integer not null default 0,
  is_archived integer not null default 0,
  is_locked integer not null default 0,
  is_private_thread integer not null default 0,
  thread_parent_id text,
  archive_timestamp text,
  raw_json text not null,
  updated_at text not null
);
```

### `members`

Suggested shape:

```sql
create table members (
  guild_id text not null,
  user_id text not null,
  username text not null,
  global_name text,
  display_name text,
  nick text,
  discriminator text,
  avatar text,
  bot integer not null default 0,
  joined_at text,
  role_ids_json text not null,
  raw_json text not null,
  updated_at text not null,
  primary key (guild_id, user_id)
);
```

### `messages`

Suggested shape:

```sql
create table messages (
  id text primary key,
  guild_id text not null,
  channel_id text not null,
  author_id text,
  message_type integer not null,
  created_at text not null,
  edited_at text,
  deleted_at text,
  content text not null,
  normalized_content text not null,
  reply_to_message_id text,
  pinned integer not null default 0,
  has_attachments integer not null default 0,
  raw_json text not null,
  updated_at text not null
);
```

### `message_events`

Suggested shape:

```sql
create table message_events (
  event_id integer primary key autoincrement,
  guild_id text not null,
  channel_id text not null,
  message_id text not null,
  event_type text not null,
  event_at text not null,
  payload_json text not null
);
```

### `sync_state`

Suggested shape:

```sql
create table sync_state (
  scope text primary key,
  cursor text,
  updated_at text not null
);
```

Examples of `scope`:

- `guild:<guild_id>:members`
- `channel:<channel_id>:messages`
- `tail:<guild_id>`

### `embedding_jobs`

Suggested shape:

```sql
create table embedding_jobs (
  message_id text primary key,
  state text not null,
  attempts integer not null default 0,
  updated_at text not null
);
```

### FTS

Recommended pattern:

- content table = `messages`
- FTS virtual table = `message_fts`
- keep it updated explicitly, not by fragile magic

Suggested columns:

- `message_id`
- `guild_id`
- `channel_id`
- `author_id`
- `author_name`
- `channel_name`
- `content`

## Search Design

### Modes

Support three modes:

- `fts`
- `semantic`
- `hybrid`

Default:

- `hybrid` when embeddings are enabled
- `fts` otherwise

### FTS behavior

FTS is mandatory.

It should be good enough that the tool is useful before embeddings exist.

Expected use cases:

- exact terms
- commands
- stack traces
- URLs
- model names
- channel names
- user names

### Semantic behavior

Embeddings are optional but planned from day one.

Recommended provider:

- OpenAI `text-embedding-3-small`

Implementation guidance:

- batch embedding jobs
- keep embedding generation out of the hot sync path
- store vectors locally
- semantic search should degrade gracefully when vectors are absent

### Vector store choice

Prefer SQLite-local vector search so the whole product stays portable.

Recommended direction:

- `sqlite-vec`

This can be wired after the base crawler and FTS system work.

## CLI Spec

Design goals:

- simple for humans
- composable for scripts
- obvious nouns and verbs
- no secrets in flags

Usage:

```text
discrawl [global flags] <command> [args]
```

### Global flags

- `-h, --help`
- `--version`
- `--config <path>`
- `--json`
- `--plain`
- `-q, --quiet`
- `-v, --verbose`
- `--no-color`

### Commands

- `init`
- `sync`
- `tail`
- `wiretap`
- `search`
- `sql`
- `members`
- `channels`
- `status`
- `doctor`

### `init`

Purpose:

- create the platform-native Discrawl config file
- discover accessible Discord guilds
- persist guild id and DB path

Expected flags:

- `--guild <id>`
- `--db <path>`
- `--with-embeddings`

### `sync`

Purpose:

- one-shot crawl

Expected flags:

- `--full`
- `--since <timestamp>`
- `--concurrency <n>`
- `--with-embeddings`

Requirements:

- idempotent
- restart-safe
- shows progress on stderr

### `tail`

Purpose:

- live sync from Gateway

Expected flags:

- `--repair-every <duration>`
- `--with-embeddings`

Requirements:

- reconnect automatically
- write checkpoints
- periodic repair sync

### `wiretap`

Purpose:

- import Discord Desktop cache artifacts into the local archive
- make cached personal DMs searchable under synthetic guild id `@me`

Expected flags:

- `--path <dir>`
- `--dry-run`
- `--watch-every <duration>`
- `--max-file-bytes <bytes>`
- `--full-cache`

Requirements:

- never use Discord user tokens
- never extract or persist auth tokens from desktop cache
- scan bounded local files only
- default to route-bearing HTTP cache entries; exhaustive Chromium cache scans require explicit full-cache mode
- store sanitized raw metadata, not full arbitrary cache blobs

### `search`

Purpose:

- query mirrored messages

Expected flags:

- `--mode fts|semantic|hybrid`
- `--channel <name-or-id>`
- `--author <name-or-id>`
- `--limit <n>`
- `--json`
- `--plain`

### `sql`

Purpose:

- run read-only SQL

Requirements:

- support query arg or stdin
- block non-read-only statements by default

### `members`

Subcommands:

- `list`
- `show <user-id>`
- `search <query>`

### `channels`

Subcommands:

- `list`
- `show <channel-id>`

### `status`

Must show:

- guild id
- guild name if known
- db path
- total channels
- total threads
- total messages
- total members
- last sync time
- last tail event time
- embedding backlog

### `doctor`

Must check:

- config file readable
- Discord token env var readable unless live access is disabled
- Discord auth valid
- guild reachable
- DB openable
- FTS present
- vector extension present if configured

## Config Spec

Format:

- TOML

Location:

- platform-native Discrawl config file

Suggested shape:

```toml
version = 1
guild_id = "1456350064065904867"
db_path = "~/.local/share/discrawl/discrawl.db"
cache_dir = "~/.cache/discrawl"
log_dir = "~/.local/state/discrawl/logs"

[discord]
token_source = "env"
token_env = "DISCORD_BOT_TOKEN"
token_keyring_service = "discrawl"
token_keyring_account = "discord_bot_token"

[sync]
concurrency = 4
repair_every = "6h"
full_history = true

[search]
default_mode = "hybrid"

[search.embeddings]
enabled = true
provider = "openai"
model = "text-embedding-3-small"
api_key_env = "OPENAI_API_KEY"
batch_size = 64
```

Config precedence:

1. flags
2. environment
3. config file

Environment variables:

- `DISCRAWL_CONFIG`
- `DISCORD_BOT_TOKEN`
- `OPENAI_API_KEY`

## Token Handling Rules

Do not:

- put bot tokens in git
- put API keys in git
- print secrets in normal logs

Do:

- load bot token from env
- fall back to the configured OS keyring item when env is empty
- load OpenAI key from env
- redact secrets in debug and doctor output

## Discord Sync Algorithm

### Initial full sync

1. load config
2. resolve token
3. fetch bot identity
4. fetch guild metadata
5. fetch guild channels
6. fetch active threads
7. enumerate archived public threads per parent channel
8. enumerate archived private threads per parent channel
9. fetch member snapshot
10. backfill messages for every crawlable channel and thread
11. normalize message content
12. upsert `messages`
13. append `message_events` where relevant
14. update FTS rows
15. enqueue embedding jobs
16. write checkpoints

### Message crawl strategy

Use REST pagination with `before`.

Rules:

- fetch newest page first for incremental runs
- fetch oldest via repeated `before` paging for full runs
- stop when no messages remain
- handle rate limits centrally

### Live tail strategy

Use Gateway events for:

- new messages
- edited messages
- deleted messages
- channel updates
- thread updates
- member updates

Tail should:

- upsert live state
- append lifecycle events
- keep retrying on disconnect
- periodically run repair sync

## Message Normalization

`normalized_content` should flatten Discord payloads into searchable text.

Include:

- message content
- embed titles and descriptions where helpful
- poll question and answers
- attachment filenames
- referenced message hints if available

Do not overcomplicate:

- reactions can be ignored
- attachment binary contents are not indexed in V1

## Member Query Design

Members matter for AI workflows.

Expected use cases:

- “who is this user”
- “find messages by this person”
- “find maintainers”
- “find everyone with a display name containing X”

At minimum, store:

- user id
- username
- display name
- nick
- roles
- bot flag

## Recommended Go Package Layout

```text
cmd/discrawl/
internal/cli/
internal/config/
internal/discord/
internal/store/
internal/search/
internal/syncer/
internal/embed/
```

Responsibilities:

- `internal/cli`: command wiring, output modes
- `internal/config`: parse and validate config
- `internal/discord`: REST + Gateway client wrappers
- `internal/store`: SQLite schema, migrations, queries
- `internal/search`: FTS and result ranking
- `internal/syncer`: full sync and repair orchestration
- `internal/embed`: embedding queue and provider integration

## Recommended Dependencies

Reasonable picks:

- Discord client: `github.com/bwmarrin/discordgo`
- TOML parser: something small and maintained
- SQLite driver: pick one path and stay consistent
- vector search: `sqlite-vec`

Guidance:

- keep dependency count low
- prefer boring stable libraries
- avoid frameworks

## Milestones

### Milestone 1

- config loader
- `init`
- `status`
- DB open + migrations

### Milestone 2

- guild metadata sync
- channel sync
- member sync

### Milestone 3

- full message backfill
- incremental checkpoints
- FTS indexing

### Milestone 4

- `search`
- `sql`
- `members`
- `channels`

### Milestone 5

- `tail`
- reconnect logic
- repair loop

### Milestone 6

- embedding queue
- vector search
- hybrid ranking

## What The Repo Must Eventually Contain

For an AI agent to finish the product without external memory, this repo should contain:

- this spec
- README with user-facing overview
- schema and migration files
- config sample
- CLI contract
- implementation package layout
- token discovery rules
- API key discovery rules
- milestone order

This file is the authoritative engineering spec for now.

## Digest

`discrawl digest` provides a per-channel activity summary over a lookback window.

Example usage:

```bash
discrawl digest
discrawl digest --since 7d
discrawl digest --since 30d --guild 123456789012345678
discrawl digest --channel general --top-n 5
discrawl --json digest --since 72h
```

Behavior:

- window defaults to `7d` when `--since` is omitted
- `--since` accepts Go durations (`72h`, `30m`) and `Nd` shorthand (`7d`, `30d`)
- `--guild` filters by `guild_id`; empty means no guild filter
- `--channel` accepts channel id or exact channel name
- per-channel metrics include `messages`, `replies`, and `active_authors`
- top posters are ranked by message count using member display fallback order: `display_name -> nick -> global_name -> username -> author_id -> unknown`
- top mentions are ranked from `mention_events` and include all target types (`user` and `role`)
- channels are sorted by message count descending, then channel name ascending
- JSON output returns a `Digest` object with channel rows and totals; plain output emits one tab-separated row per channel

## Analytics

`discrawl analytics` is a subcommand group for activity-style queries.

Example usage:

```bash
discrawl analytics
discrawl analytics quiet --since 30d
discrawl analytics quiet --guild 123456789012345678
discrawl analytics trends --weeks 8
discrawl analytics trends --weeks 12 --channel general
discrawl --json analytics quiet --since 60d
discrawl --json analytics trends --weeks 4
```

Behavior:

- `analytics quiet` defaults to `30d` lookback and supports `--guild`
- `analytics quiet` includes top-level text/announcement channels with no messages at all
- quiet rows are sorted with never-active channels first, then by longest silence
- `analytics trends` defaults to `8` weeks and supports `--guild` plus `--channel` (id or exact name)
- `analytics trends` buckets messages into Monday-start UTC weeks and zero-fills missing weeks for every returned message-capable channel
- trends rows are sorted by total messages descending, then channel name ascending
