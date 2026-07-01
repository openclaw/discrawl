# Security

## Tokens and credentials

- Do not commit bot tokens or API keys.
- Default config lives in your home directory, not inside the repo.
- Prefer env vars or the OS keyring for bot tokens.
- `discrawl doctor` reports the token source (env or keyring), not token contents.

## Wiretap is local-only

`wiretap` reads local Discord Desktop cache files only. It does not:

- extract, store, or print Discord auth tokens
- use a user token
- call the Discord API as your user
- run as a selfbot

Wiretap DM messages stay local. They are stored under the synthetic guild id `@me` and are never exported to:

- `publish` (Git snapshot output)
- `subscribe` / Git snapshot import
- the optional `--with-embeddings` snapshot export

A shared guild mirror refresh does not wipe local wiretap DM search either - import preserves existing local `@me` guilds, channels, messages, and attachments.

## CI

CI runs secret scanning with `gitleaks` against git history and the working tree.

## What is stored locally

- guild metadata
- channels and threads (one table)
- current member snapshot
- canonical message rows
- append-only message event records
- FTS index rows
- optional local embedding queue metadata and vectors
- local-only failure history with bounded, redacted error text

Attachment binaries are not stored in SQLite. Only attachment metadata, optional extracted text, and media cache bookkeeping are stored there. Cached files live under `cache_dir/media`. Failed CDN downloads are recorded with their HTTP status, such as `404`, instead of being retried silently forever.

The operational failure ledger is excluded from Git snapshot exports and
imports. It may contain local row identifiers and sanitized error context, so
inspect `discrawl failures --json` before sharing its output.

Set `sync.attachment_text = false` if you want to keep attachment metadata and filenames but disable attachment body fetches for text indexing.

Git snapshots include cached non-DM media files by default. Use `publish --no-media` to omit them. `publish` exports only files already in the local cache; it does not fetch missing Discord media. Published media is gzip-compressed under `media/`, while import still accepts older raw-media snapshots for backward compatibility. DM media under `@me` stays local-only.

## What is sent over the wire

With remote embedding providers, message text is sent during `discrawl embed`, and search query text is sent when using `--mode semantic` or `--mode hybrid`. Stored message text is not sent during local vector scoring.

Local providers like Ollama keep both message and query embedding on the same machine.
