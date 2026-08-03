# `sql`

Runs read-only SQL against the local database.

## Usage

```bash
discrawl sql 'select count(*) as messages from messages'
echo 'select guild_id, count(*) from messages group by guild_id' | discrawl sql -
```

`-` reads SQL from stdin.

## Notes

- read-only - writes are blocked at the connection level
- `--unsafe --confirm` opens the escape hatch for deliberate write/admin SQL
- the schema is multi-guild ready; threads are stored as channels because that matches the Discord model
- proven DMs use the synthetic guild id `@me`
- SQLite schema migrations are versioned with `PRAGMA user_version`; startup fails fast when a local DB schema is newer than the supported binary
- a zero-row query describes the local snapshot; it cannot prove authoritative absence from Discord

Message queries that need channel metadata must start from `messages` and use a
left join so incomplete channel metadata does not silently discard archived
messages:

```sql
select m.id, m.created_at, m.channel_id, c.name as channel_name, m.content
from messages m
left join channels c on c.id = m.channel_id
where m.guild_id = 'GUILD_ID'
  and m.author_id = 'AUTHOR_ID'
  and m.content = 'EXACT PHRASE';
```

`join channels` intentionally drops messages whose channel metadata is
incomplete. `discrawl sql` warns on zero-row output when it can confirm such
orphaned references; an `undetermined` note means the catalog probe did not
complete within its bounded two-second budget or encountered a probe error, not
that referential integrity was established. The warning covers empty results
only: a non-empty inner join can still omit orphaned messages, so use the left
join or check diagnostics first. Even a `catalog.state` of `consistent` only
proves that stored messages resolve to stored channels; it does not prove source
archive coverage.

## See also

- [Data layout](../guides/data-storage.html) - what tables exist
- [`status`](status.html) - high-level archive numbers without raw SQL
