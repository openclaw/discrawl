# Configuration

`discrawl init` writes a complete config so most users do not hand-edit anything initially. This
page documents the full shape and override rules for when you do.

## Default paths

Discrawl follows the normal per-OS storage convention instead of writing a new top-level directory
in your home folder.

On Linux and other Unix desktops, Discrawl uses XDG Base Directory paths. In practice, that means:

- config: `${XDG_CONFIG_HOME:-~/.config}/discrawl/config.toml`
- database: `${XDG_DATA_HOME:-~/.local/share}/discrawl/discrawl.db`
- Git share repo: `${XDG_DATA_HOME:-~/.local/share}/discrawl/share`
- cache: `${XDG_CACHE_HOME:-~/.cache}/discrawl/`
- logs: `${XDG_STATE_HOME:-~/.local/state}/discrawl/logs/`

On macOS, Discrawl uses the platform's `~/Library` locations:

- config: `~/Library/Application Support/discrawl/config.toml`
- database: `~/Library/Application Support/discrawl/discrawl.db`
- Git share repo: `~/Library/Application Support/discrawl/share`
- cache: `~/Library/Caches/discrawl/`
- logs: `~/Library/Application Support/discrawl/logs/`

If you set `XDG_CONFIG_HOME`, `XDG_DATA_HOME`, `XDG_CACHE_HOME`, or `XDG_STATE_HOME`, those
variables choose the new default locations on any OS.

Upgrades do not move your database automatically. Existing installs are
preserved before those new locations take over. If `~/.discrawl/config.toml`
exists and the new default config file does not, Discrawl keeps loading the
legacy config. Missing runtime fields also keep using existing legacy files or
directories under `~/.discrawl` when the new location does not exist yet. This
avoids breaking users whose desktop already sets XDG variables globally.

To migrate deliberately, copy or create the new config file first, or point
Discrawl at it with `--config` / `DISCRAWL_CONFIG`. Runtime paths switch
one-by-one: once the new database, cache, logs, or share path exists, that path
wins over the legacy `~/.discrawl` counterpart. Copy the SQLite database before
creating the new database path if you want to preserve the existing archive.

## File layout

```toml
version = 1
default_guild_id = ""
guild_ids = []
db_path = "~/.local/share/discrawl/discrawl.db" # macOS: "~/Library/Application Support/discrawl/discrawl.db"
cache_dir = "~/.cache/discrawl" # macOS: "~/Library/Caches/discrawl"
log_dir = "~/.local/state/discrawl/logs" # macOS: "~/Library/Application Support/discrawl/logs"

[discord]
token_source = "env" # use "none" for Git-only read access
token_env = "DISCORD_BOT_TOKEN"
token_keyring_service = "discrawl"
token_keyring_account = "discord_bot_token"

[sync]
source = "both" # "discord" for bot-only sync, "wiretap" for desktop-cache-only import
concurrency = 16
repair_every = "6h"
repair_offset = "0s"
include_category_ids = []
exclude_channel_ids = []
exclude_channel_kinds = []
full_history = true
attachment_text = true
attachment_media = false
max_attachment_bytes = 104857600

[desktop]
path = "~/.config/discord" # macOS default: "~/Library/Application Support/discord"
max_file_bytes = 67108864
full_cache = false

[search]
default_mode = "fts"

[search.lexical]
languages = [] # optional: "ko", "ja", "zh", "ar"
kiwi_command = "discrawl-kiwi"
kiwi_model = ""
ja_command = "discrawl-ja"
zh_command = "discrawl-zh"

[search.embeddings]
enabled = false
provider = "openai"
model = "text-embedding-3-small"
api_key_env = "OPENAI_API_KEY"
dimensions = 512 # optional OpenAI projection; omit for provider default
batch_size = 64
max_input_chars = 12000
request_timeout = "2m"
vector_backend = "exact"

[share]
remote = ""
repo_path = "~/.local/share/discrawl/share" # macOS: "~/Library/Application Support/discrawl/share"
branch = "main"
auto_update = true
update_mode = "merge" # "exact" for a dedicated snapshot-only cache
stale_after = "15m"
media = true

[share.filter]
public_only = false
include_channel_ids = []
exclude_channel_ids = []

[remote]
mode = "local" # use "cloud" for Worker-fronted remote archives
endpoint = ""
archive = ""
token_env = "DISCRAWL_REMOTE_TOKEN"
stale_after = ""
```

`concurrency` is auto-sized at `init` to `min(32, max(8, GOMAXPROCS*2))`.

## Token resolution

In order:

1. `DISCORD_BOT_TOKEN`, or the env var named in `discord.token_env`
2. OS keyring item `discrawl` / `discord_bot_token`, or the configured keyring service/account

`discrawl` accepts either raw token text or a value prefixed with `Bot `. Normalization is automatic.

Set `discord.token_source = "keyring"` if you want to require keyring lookup and skip env entirely. Set it to `"none"` for a Git-only reader.

## Override rules

- `--config <path>` beats everything
- `DISCRAWL_CONFIG=<path>` overrides the default config path
- `discord.token_source = "none"` disables live Discord access for Git-only readers
- `discord.token_source = "keyring"` skips env lookup
- `remote.mode = "cloud"` makes `status --json` and `remote ...` read the configured Worker archive without opening SQLite
- `DISCRAWL_NO_AUTO_UPDATE=1` disables Git snapshot auto-update for read commands in one process

## Notes

- `default_guild_id` is the implicit scope for `sync`, `tail`, `digest`, and `analytics` when `--guild` is not passed
- `guild_ids` is reserved for explicit multi-guild fan-out; usually you do not set this directly
- `sync.include_category_ids` limits Discord sync and tail collection to the listed categories and all channel/thread descendants; root-level and unrelated channels are skipped
- `sync.exclude_channel_ids` and `sync.exclude_channel_kinds` apply to historical sync, live tail events, and repair syncs; exclusions always win over category inclusion
- `sync.exclude_channel_kinds` accepts Discrawl kinds such as `text`, `announcement`, `forum`, `thread_public`, `thread_private`, and `thread_announcement`
- a non-zero `sync.repair_offset` aligns periodic repairs to local wall-clock boundaries; for example, `repair_every = "6h"` with `repair_offset = "2h"` targets 02:00, 08:00, 14:00, and 20:00 local time
- `[search.lexical].languages` enables opt-in multilingual FTS fields. Supported presets are Korean (`ko`, Kiwi through `github.com/codingpot/kiwigo`), Japanese (`ja`, Kagome Search through `discrawl-ja`), Chinese (`zh`, GSE CutSearch through `discrawl-zh`), and Arabic (`ar`, in-process light stemming).
- Korean, Japanese, and Chinese use separately built Go helpers so the default Discrawl binary stays small and CGO-free. Arabic is implemented in-process.
- `discrawl lexical install` reports the required helpers. It never downloads packages.
- Tokenizer helpers load lazily on first indexing or search use. Commands that only inspect metadata do not start helpers.
- Each enabled language adds an independent FTS5 table. Index and query text pass through the same tokenizer, and results from the default plus language-specific tables are merged with reciprocal rank fusion.
- After adding or changing `search.lexical.languages`, run a writer command such as `discrawl sync` once so the configured lexical tables are built. Read-only commands never mutate the archive; new and edited messages update the tables automatically during later syncs.
- changing `[search.embeddings]` provider/model/input version retargets pending jobs and resets prior attempts; existing vectors for another identity remain in SQLite but are not used for semantic search
- `[search.embeddings].dimensions` is an optional positive OpenAI projection size. Changing it requires `embed --rebuild` so stored message vectors and query vectors use the same dimensions.
- `[search.embeddings].vector_backend` accepts `exact` or optional `turbovec`; turbovec requires Python plus the `turbovec` package and embedding dimensions divisible by 8.
- changing `db_path` does not migrate existing data; copy the file yourself if you want to keep history
- `sync.attachment_media = true` makes `sync` behave like `sync --with-media`; media bytes are cached under `cache_dir/media`, and CDN `404`/other fetch failures are recorded on attachment rows
- `share.media = false` makes publish/update/auto-update omit or skip restoring cached media; `subscribe --no-media` writes this for Git-only readers. With the default `share.media = true`, publish/update include cached non-DM media as gzip-compressed snapshot files, but publish does not fetch missing Discord files by itself.
- `share.update_mode = "merge"` preserves destination-only rows during update and auto-update. Set it to `"exact"` only for a dedicated snapshot reader whose public tables must mirror the current snapshot; exact updates remove non-DM rows that the publisher omits. `subscribe --exact` writes this setting.
- `[share.filter]` narrows only `publish` output; sync can still keep a richer local archive
- `share.filter.public_only` exports only channels visible to the guild
  `@everyone` role after category/channel permission overwrites; private
  threads are excluded
- `share.filter.include_channel_ids` and
  `share.filter.exclude_channel_ids` accept Discord channel ids; exclusions win,
  and including a forum parent also includes its allowed public threads
- filtered publishes cannot write generated README reports, and remove older
  generated Discrawl share READMEs before committing
