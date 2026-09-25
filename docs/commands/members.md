# `members`

Browse the offline member directory built from archived profile payloads.

## Usage

```bash
discrawl members list
discrawl members show 123456789012345678
discrawl members show --messages 10 steipete
discrawl members search "peter"
discrawl members search "github"
discrawl members search "https://github.com/steipete"
```

## Subcommands

- `list` - dump the local member directory
- `show <id|query>` - show one member; if the query resolves to one match, also show recent messages
- `search <query>` - match names plus any offline profile fields present in the archived member payload

## Flags

- `show --messages <n>` - include the most recent `n` messages from that member

## Profile fields

Extracted from the archived Discord member/user payload. May include:

- `bio`
- `pronouns`
- `location`
- `website`
- `x`
- `github`
- discovered URLs

If the bot cannot see a field from Discord, `discrawl` cannot invent it. This is strictly archive-based offline data.

## Typical workflow

```bash
discrawl sync --full
discrawl members search "design engineer"
discrawl members search "github"
discrawl members show --messages 25 steipete
discrawl messages --author steipete --days 30 --all
```

## Typical `members show` output

```text
guild=1456350064065904867
user=37658261826043904
username=steipete
display=Peter Steinberger
joined=2026-03-08T16:03:14Z
bot=false
x=steipete
github=steipete
website=https://steipete.me
bio=Builds native apps and tooling.
urls=https://steipete.me, https://github.com/steipete
message_count=1284
first_message=2026-02-01T09:00:00Z
last_message=2026-03-08T15:59:58Z
```

## See also

- [`channels`](channels.html)
- [Data layout](../guides/data-storage.html)

## Incremental archive readers

Gateway member additions, profile changes and removals update `members.updated_at`.
The collector owns an ordered index on `(updated_at, guild_id, user_id)` so
read-only downstream consumers can poll changes without scanning the roster.
A normal writable open installs this index on existing archives as part of the
idempotent query-index migration; member rows, tombstones and schema version
remain unchanged. Readers should overlap timestamp boundaries, deduplicate by
the composite member key, and reconcile periodically for older/backdated changes.

Before upgrading an existing archive, read the
[member index migration and backup-first procedure](../guides/data-storage.html#member-update-cursor-index).
The first writable open builds the index synchronously. Source approval does
not make a main merge safe: the scheduled publisher checks out main and opens
its cached archive for writing. Keep the change unmerged until the separately
reviewed, backup-first publisher canary and activation qualification are complete.
