# discrawl 🛰️ — Discord history, without the scroll

[![CI](https://img.shields.io/github/actions/workflow/status/openclaw/discrawl/ci.yml?branch=main&style=flat-square&label=ci)](https://github.com/openclaw/discrawl/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/openclaw/discrawl?style=flat-square)](https://github.com/openclaw/discrawl/releases/latest)
[![Go](https://img.shields.io/badge/Go-1.27.0%2B-00ADD8?style=flat-square&logo=go&logoColor=white)](https://go.dev/)
[![License](https://img.shields.io/github/license/openclaw/discrawl?style=flat-square)](LICENSE)
[![Homebrew](https://img.shields.io/badge/Homebrew-openclaw%2Ftap-FBB040?style=flat-square&logo=homebrew&logoColor=black)](https://github.com/openclaw/homebrew-tap/blob/main/Formula/discrawl.rb)
[![Docs](https://img.shields.io/badge/docs-discrawl.sh-4C78A8?style=flat-square)](https://discrawl.sh/)

![discrawl banner](docs/assets/readme-banner.jpg)

Discrawl archives Discord guild data and classifiable Discord Desktop cache messages in local SQLite for offline search, SQL, and terminal browsing. It is for people who need durable server history, local recovery of cached direct messages, or read-only access to a shared archive.

## Install

Homebrew is the smallest path on macOS and Linux:

```bash
brew install openclaw/tap/discrawl
discrawl --version
```

Windows binaries and signed archives for all supported platforms are available from [GitHub Releases](https://github.com/openclaw/discrawl/releases/latest).

To build from source, install Go 1.27.0 or newer:

```bash
git clone https://github.com/openclaw/discrawl.git
cd discrawl
go build -o bin/discrawl ./cmd/discrawl
./bin/discrawl --version
```

See the [installation guide](docs/install.md) for Docker, runtime paths, and update checks.

## Quick start

Inspect the local Discord Desktop cache without a token or database writes:

```bash
discrawl wiretap --dry-run --json
```

If the summary finds messages, import the cache and search it:

```bash
discrawl wiretap
discrawl search "launch checklist"
discrawl search --dm "launch checklist"
```

Wiretap imports only messages already cached by Discord Desktop; it is not a complete account-history fetch. Direct messages stay local and are excluded from shared snapshots. See [wiretap](docs/commands/wiretap.md) for cache paths and scan controls.

## Choose an archive source

| Source | Setup | What it provides |
| --- | --- | --- |
| Discord bot | [Configure a bot](docs/bot-setup.md), then run [`sync`](docs/commands/sync.md) | Guilds, channels, threads, members, and bot-visible message history |
| Discord Desktop cache | Run [`wiretap`](docs/commands/wiretap.md) | Locally cached guild messages and classifiable direct messages |
| Git snapshot | Run [`subscribe`](docs/commands/subscribe.md) | Offline, token-free access to an archive published by another machine |
| Remote archive | Run [`subscribe-cloud`](docs/commands/subscribe-cloud.md) | Read-only access through a configured Worker without local SQLite |

For a bot-backed archive:

```bash
export DISCORD_BOT_TOKEN="your-bot-token"
discrawl init
discrawl doctor
discrawl sync
```

`sync` collects recent data by default. Run `discrawl sync --full` when you want a historical backfill; large guilds can take time because Discord rate-limits history requests. [`tail`](docs/commands/tail.md) keeps an archive current from Gateway events and periodic repair passes.

## Search and inspect

FTS5 search works without external services:

```bash
discrawl search "panic: nil pointer"
discrawl search --channel general --author steipete "release"
discrawl messages --channel general --hours 24
discrawl tui
```

Use [`dms`](docs/commands/dms.md), [`mentions`](docs/commands/mentions.md), [`members`](docs/commands/members.md), and [`channels`](docs/commands/channels.md) for structured browsing. [`sql`](docs/commands/sql.md) exposes read-only queries for analysis.

Semantic and hybrid search are optional. They require an embedding provider and an explicit indexing pass; FTS remains the default. See the [search modes](docs/guides/search-modes.md) and [embeddings](docs/guides/embeddings.md) guides.

## Share an archive

[`publish`](docs/commands/publish.md) writes a sharded Git snapshot that another machine can consume with `subscribe`. Readers need access to the snapshot repository, but do not need Discord credentials.

```bash
discrawl publish --check
discrawl publish
```

The preflight is read-only and reports the export scope before any snapshot is written. Published snapshots exclude wiretap direct messages and local failure history. Dedicated readers of privacy-filtered snapshots can subscribe with `--exact` so later omissions remove previously shared rows; normal subscriptions preserve richer local rows. See [Git snapshots](docs/guides/git-snapshots.md) for repository layout, filters, media handling, and update behavior.

## Automation

Discrawl exposes stable JSON for launchers, agents, and CI:

```bash
discrawl status --json
discrawl diagnostics --json
discrawl coverage --json
discrawl failures --json
```

Start with [`status`](docs/commands/status.md) and [`doctor`](docs/commands/doctor.md), then use the [command reference](docs/README.md) for the full control surface.

## Storage and security

Discrawl follows platform storage conventions: XDG directories on Linux and `~/Library` on macOS. Configuration can read the bot token from an environment variable or the OS keyring; the token is not written into snapshots.

Wiretap reads local cache files only. It does not extract a user token, call Discord as the user, or run a selfbot. Review the [configuration](docs/configuration.md), [data storage](docs/guides/data-storage.md), and [security](docs/security.md) docs before publishing an archive or enabling a remote embedding provider.

## Documentation

The full documentation lives at **[discrawl.sh](https://discrawl.sh/)**:

- [Install and setup](docs/install.md)
- [Command reference](docs/README.md)
- [Sync sources](docs/guides/sync-sources.md)
- [Search modes](docs/guides/search-modes.md)
- [Git snapshot workflows](docs/guides/git-snapshots.md)
- [Configuration](docs/configuration.md)

## Development

```bash
make build
make test
make smoke
make fmt
make lint
```

`make check` runs the complete local gate. CI also enforces race tests, coverage, dependency verification, vulnerability checks, and release checks.

## License

MIT. See [LICENSE](LICENSE).
