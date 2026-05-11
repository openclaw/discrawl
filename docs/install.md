# Install

Discrawl is a single Go binary. Install via Homebrew or build from source.

## Homebrew

```bash
brew install steipete/tap/discrawl
discrawl --version
```

The tap auto-installs from `steipete/tap`.

## From source

Requires Go `1.26+`.

```bash
git clone https://github.com/openclaw/discrawl.git
cd discrawl
go build -o bin/discrawl ./cmd/discrawl
./bin/discrawl --version
```

If you do not put `discrawl` on `PATH`, replace `discrawl` with `./bin/discrawl` in any example below.

## Quick start (with bot token)

```bash
export DISCORD_BOT_TOKEN="your-bot-token"
discrawl init
discrawl doctor
discrawl sync --full
discrawl sync
discrawl search "panic: nil pointer"
discrawl tail
```

`init` discovers accessible guilds and writes the default XDG config file. If
exactly one guild is available, it becomes the default automatically.

`doctor` verifies the config loads, the token resolves, the bot can reach the
Gateway, and the local DB and FTS index are wired up.

## Quick start (Git-only reader)

No Discord credentials required. You read a private Git snapshot another machine published.

```bash
discrawl subscribe https://github.com/example/discord-archive.git
discrawl search "launch checklist"
discrawl messages --channel general --hours 24
```

`subscribe` writes a token-free config (`discord.token_source = "none"`) and
imports the snapshot. Read commands auto-refresh when the local snapshot is
older than `15m`.

## Default runtime paths

Discrawl follows the OS storage convention instead of writing a new top-level directory in your
home folder. Linux uses XDG Base Directory paths. macOS uses `~/Library` folders unless you set XDG
variables yourself.

- Linux config: `${XDG_CONFIG_HOME:-~/.config}/discrawl/config.toml`
- Linux database/share: `${XDG_DATA_HOME:-~/.local/share}/discrawl/`
- Linux cache: `${XDG_CACHE_HOME:-~/.cache}/discrawl/`
- Linux logs: `${XDG_STATE_HOME:-~/.local/state}/discrawl/logs/`
- macOS config/database/logs/share: `~/Library/Application Support/discrawl/`
- macOS cache: `~/Library/Caches/discrawl/`

Existing `~/.discrawl/config.toml` installs continue to load when the new default config file does
not exist, even if XDG variables are set globally by your desktop. To migrate deliberately, copy or
create the new config file first, or point Discrawl at it with `--config` / `DISCRAWL_CONFIG`.

## Next steps

- [Bot setup](bot-setup.html) - intents, permissions, token sources
- [Configuration](configuration.html) - the full TOML shape and override rules
- [`sync`](commands/sync.html) - the main archive command
