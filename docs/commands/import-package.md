# `import-package`

Import the messages you sent in personal DMs and group DMs from a Discord
account data package. Reads a ZIP or an extracted directory without credentials
or network requests. Server channels are skipped.

```bash
discrawl --config personal.toml import-package --path package.zip --dry-run --json
discrawl --config personal.toml import-package --path package.zip --json
discrawl --config personal.toml dms
```

Use a separate private archive configuration for personal data. Request and
download your package through Discord's Data & Privacy settings; this command
does not request it for you. [Discord's package documentation](https://support.discord.com/hc/en-us/articles/360004957991-Your-Discord-Data-Package)
describes the available data.

## Coverage

Discord's account package contains **your sent messages**, not the replies
other people sent you. Manually deleted messages are absent. The result always
reports `coverage: "sent_only"` and `history_complete: false`; an import does not
establish complete DM history.

Combine this evidence with [Desktop cache intake](wiretap.md) when available.
Neither source proves that every incoming message was captured. Ordinary bots
cannot retrieve the user's existing person-to-person DM history.

## Supported package layout

The importer accepts this JSON layout at the package root or inside one wrapper
directory. Directory names `account` and `messages` are case-insensitive. Run a
dry run on your downloaded package before importing; automated tests use
synthetic fixtures, and provider export formats can change.

```text
account/user.json
messages/<channel-id>/channel.json
messages/<channel-id>/messages.json
```

Channel directories may also use a `c` prefix. `user.json` supplies the sender's
`id`. DM channel metadata must include `id`, numeric `type` (1 or 3), and a
`recipients` array of user IDs. Transcripts are arrays of objects with `ID`,
`Timestamp`, `Contents`, and optional `Attachments` strings. IDs may be strings
or integer JSON tokens; they are never converted through floating point.
Timestamps accept RFC 3339 or the export's space-separated timestamp with an
explicit UTC offset. Unknown formats fail with a diagnostic; older CSV exports
are not currently supported.

For a messages-only subset, explicitly supply its owner's ID:

```bash
discrawl --config personal.toml import-package \
  --path extracted-package --account-id 123456789012345678 --dry-run
```

When `account/user.json` is present, `--account-id` must match it. Names are not
used to infer identities. Account billing, email, session and other unrelated
account fields are not imported.

## Storage and retries

- DMs use the existing local-only `@me` scope. Ordinary shared Git/Cloud
  publication continues to exclude them.
- Each channel's messages commit in one transaction. A later channel failure
  leaves earlier successful channels present and reports their progress.
- Repeated imports do not duplicate messages or identical import events.
  Changed transcripts add provenance events. Existing messages, richer cache
  metadata, attachment rows and deletion tombstones are retained unchanged.
- Import events retain source account, channel, transcript SHA-256, sent-only
  coverage and the original export row. This preserves conflicting evidence
  without treating an old package as a new edit or deletion.
- Attachment links are retained in original export rows. This command does not
  fetch or back up the attachment bytes.
- Dry runs validate package contents without opening, creating or migrating
  the configured database or runtime directories.

The reader rejects unsafe ZIP paths, duplicate entries and symlinks, and bounds
ZIP entry counts and expanded size. Files are limited to 64 MiB. Large channel
transcripts must be split into separately validated packages or handled by a
future streaming importer; this command fails rather than silently truncating.

## Flags

- `--path PATH`: required ZIP file or extracted package directory.
- `--account-id ID`: confirm the sender or identify a messages-only subset.
- `--dry-run`: validate without database writes.
- `--json`: structured counts, progress and explicit coverage limits.
