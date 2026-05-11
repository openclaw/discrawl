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

Existing installs are preserved before those new locations take over. If `~/.discrawl/config.toml`
exists and the new default config file does not, Discrawl keeps loading the legacy config. Missing
runtime fields also keep using existing legacy files or directories under `~/.discrawl` when the
new location does not exist yet. This avoids breaking users whose desktop already sets XDG
variables globally.

To migrate deliberately, copy or create the new config file first, or point Discrawl at it with
`--config` / `DISCRAWL_CONFIG`. Runtime paths switch one-by-one: once the new database, cache,
logs, or share path exists, that path wins over the legacy `~/.discrawl` counterpart.

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
full_history = true
attachment_text = true

[desktop]
path = "~/.config/discord" # macOS default: "~/Library/Application Support/discord"
max_file_bytes = 67108864
full_cache = false

[search]
default_mode = "fts"

[search.embeddings]
enabled = false
provider = "openai"
model = "text-embedding-3-small"
api_key_env = "OPENAI_API_KEY"
batch_size = 64

[share]
remote = ""
repo_path = "~/.local/share/discrawl/share" # macOS: "~/Library/Application Support/discrawl/share"
branch = "main"
auto_update = true
stale_after = "15m"
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
- `DISCRAWL_NO_AUTO_UPDATE=1` disables Git snapshot auto-update for read commands in one process

## Notes

- `default_guild_id` is the implicit scope for `sync`, `tail`, `digest`, and `analytics` when `--guild` is not passed
- `guild_ids` is reserved for explicit multi-guild fan-out; usually you do not set this directly
- changing `[search.embeddings]` provider/model/input version retargets pending jobs and resets prior attempts; existing vectors for another identity remain in SQLite but are not used for semantic search
- changing `db_path` does not migrate existing data; copy the file yourself if you want to keep history
