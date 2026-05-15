# `attachments`

Lists attachment metadata and optionally downloads attachment media into the local cache.

## Usage

```bash
discrawl attachments --channel general --days 7
discrawl attachments --filename crash --type image --all
discrawl attachments --message 1456744319972282449
discrawl attachments fetch --channel general --days 7
discrawl attachments fetch --missing --max-bytes 104857600
discrawl --json attachments --missing --all
```

## Flags

- `--channel <id|name|#name>` - id, exact name, `#name`, or partial name match
- `--guild <id>` / `--guilds <id,id>` / `--dm` - restrict the guild scope (`--dm` is shorthand for `--guild @me`)
- `--author <name>` - restrict to one author
- `--message <id>` - restrict to one message
- `--filename <text>` - filename substring match
- `--type <text>` - content-type substring match, such as `image` or `application/pdf`
- `--hours <n>` - shorthand for "since now minus N hours"
- `--days <n>` - shorthand for "since now minus N days"
- `--since <RFC3339>` / `--before <RFC3339>` - explicit time window
- `--limit <n>` - safety limit (default 200; `--all` removes it)
- `--all` - removes the safety limit
- `--missing` - only attachments whose cached media file is absent

`attachments fetch` also accepts:

- `--force` - re-download already cached attachments
- `--max-bytes <n>` - per-attachment download cap (defaults to `[sync].max_attachment_bytes`)

## Notes

- media bytes are stored under `cache_dir/media`, not in SQLite
- SQLite stores attachment metadata, content hash, cached media path, fetch status, and errors
- `publish` backs up cached non-DM media files by default; use `publish --no-media` to omit them
- `@me` DM media is local-only and is not published to Git snapshots

## See also

- [`messages`](messages.html)
- [`publish`](publish.html)
- [`sync`](sync.html)
