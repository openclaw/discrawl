# Discrawl

Mirror Discord guilds into local SQLite. Search server history without depending on Discord search. Bring a bot token, or read everything offline from a Git snapshot.

## What it does

- discovers every guild a bot can access and syncs channels, threads, members, and message history into SQLite
- maintains FTS5 indexes for fast literal search; optional embeddings for semantic and hybrid recall
- imports classifiable Discord Desktop cache messages with `wiretap`, including proven DMs under `@me`
- downloads attachment media into the local cache and includes cached non-DM media as gzip-compressed Git snapshot files
- tails the Gateway for live updates with periodic repair sweeps
- publishes the archive as sharded NDJSON snapshots in a private Git repo so readers can search offline with no Discord credentials
- exposes read-only SQL, channel/member directories, mention queries, digests, and trend analytics

## Pick your path

- **New here?** Read [Install](install.html) and run `discrawl init`.
- **Already have a bot?** Jump to [`sync`](commands/sync.html) and [`search`](commands/search.html).
- **Just want to read a shared archive?** Use [`subscribe`](commands/subscribe.html) for Git snapshots, or [`subscribe-cloud`](commands/subscribe-cloud.html) for a Worker-fronted archive - no Discord token needed.
- **Need DM search?** [`wiretap`](commands/wiretap.html) imports local Discord Desktop cache.
- **Want semantic search?** Configure [Embeddings](guides/embeddings.html), then run [`embed`](commands/embed.html).
- **Wiring an agent or launcher?** `discrawl metadata --json`, `discrawl status --json`, `discrawl diagnostics --json`, `discrawl coverage --json`, `discrawl failures --json`, `discrawl remote status`, and `discrawl doctor --json` expose the read-only crawlkit control surface.

## At a glance

```bash
export DISCORD_BOT_TOKEN="..."
discrawl init
discrawl doctor
discrawl sync --full
discrawl search "panic: nil pointer"
discrawl tail
```

[`discrawl tui`](commands/tui.html) uses the shared crawlkit terminal explorer:
channel/person/thread groups on the left, message rows in the middle, and
readable message/thread detail on the right.

## Sections

- **[Start](install.html)** - install, configure, set up the Discord bot, security notes, release history, contact
- **[Guides](guides/)** - sync sources, wiretap internals, search modes, embeddings, Git snapshots, data layout
- **[Commands](commands/)** - one page per CLI command

## Where to file issues

`https://github.com/openclaw/discrawl/issues`. See [contact](contact.html) for project links.
