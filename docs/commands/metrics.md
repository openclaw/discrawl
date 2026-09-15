# `metrics`

Record public Discord server size and online-presence observations without a bot
token. Metrics use an explicitly configured, separate SQLite database; these
commands do not open the message/member archive, import Desktop data, or generate
embeddings.

## Usage

```bash
discrawl metrics collect --config /absolute/path/metrics.json
discrawl metrics import --config /absolute/path/metrics.json < history.ndjson
discrawl metrics status --config /absolute/path/metrics.json
discrawl help metrics
```

The metrics `--config` is a JSON file supplied after the subcommand. It is separate
from Discrawl's normal TOML archive configuration. All three commands output JSON.
`status` is read-only and reports observation/event counts, the latest observation
sequence, and the most recent observation time (`null` for an empty store).

## Configuration

```json
{
  "database": "/absolute/path/discrawl-metrics.sqlite",
  "targets": [
    {"entity": "openclaw", "target": "clawd"},
    {"entity": "hermes", "target": "nousresearch"}
  ]
}
```

`entity` is your series label; `target` is a public invite code, not an invite URL
or guild ID. Codes must be unique in the configuration. Other servers can be
configured with their own invite codes.

The database path must be absolute. `collect` and `import` create a new metrics
database if that path does not exist. Existing databases must identify their
owner as `discrawl` and metrics version as `1`; unrelated, empty, or newer-version
databases are refused before write access. `status` never creates a database.

No token or cookie is needed. The shared configuration fields `cookieJar` and
`tokenEnv` are accepted but unused by this collector.

## What is measured

Each collection makes one public `GET /api/v10/invites/{code}?with_counts=true`
request per target and records two observations:

| Metric | Discord field | Meaning |
| --- | --- | --- |
| `members` | `approximate_member_count` | Approximate total server membership |
| `online` | `approximate_presence_count` | Approximate online presence |

Both use `kind: "counter"` and `provenance: "discord_invite_approximate"`.
Here, counter means a timestamped snapshot: either value can decrease. Online
presence does **not** count active posters, messages, daily active users, or
engagement. See Discord's [Invite object and Get Invite documentation](https://docs.discord.com/developers/resources/invite#get-invite).

Zero is a valid count. Missing or invalid values, expired invites, HTTP failures,
and rate limits produce SQL `NULL`, never a fabricated zero. Valid fields and
other successful targets are retained. Such a run records `partial`, outputs
`"ok": false`, and exits nonzero. Responses are bounded in size, requests have a
30-second timeout, and redirects are refused. There is no automatic retry loop
or scheduler; invoke `collect` at the cadence appropriate for your application.

## Importing history

`import` reads one JSON object per line from stdin. Every row needs a stable,
nonempty `id` and an exact configured `entity`/`target` pair. Times use RFC 3339
(fractional seconds and offsets are accepted). Unknown fields are rejected.

```json
{"type":"metric","id":"historical-members-1","entity":"openclaw","target":"clawd","metric":"members","kind":"counter","ts":"2026-09-14T12:00:00Z","value":1200,"observed_at":"2026-09-14T12:00:00Z","provenance":"historical-import"}
{"type":"metric","id":"historical-online-1","entity":"openclaw","target":"clawd","metric":"online","kind":"counter","ts":"2026-09-14T12:00:00Z","value":null,"observed_at":"2026-09-14T12:00:00Z","provenance":"historical-import"}
{"type":"event","id":"historical-event-1","entity":"hermes","target":"nousresearch","kind":"milestone","ts":"2026-09-14T12:00:00Z","label":"Example milestone","url":"https://example.org/milestone","observed_at":"2026-09-14T12:00:00Z","provenance":"historical-import"}
```

Metric rows use `kind: "counter"` or `"daily"` and a nonnegative numeric value or
`null`. Daily rows describe a completed UTC day. Revisions require different IDs:
consumers select the latest sequence for a daily series/day instead of summing
revisions. Import preserves supplied timestamps, provenance, repeated values,
decreases, zeroes, NULLs, and events. Only an already stored ID is ignored.

Imports commit in batches of 500. If a later line is invalid, the JSON result
reports the number already committed and the command exits nonzero. Correct the
input and replay it: stored IDs make retries idempotent. A line is limited to 4 MiB.

## Storage and delivery

The metrics database contains:

- `metric_meta(key, value)`: collector owner and schema version.
- `metric_observations(sequence, id, entity, target, metric, kind, ts, value, observed_at, provenance)`.
- `metric_events(sequence, id, entity, target, kind, ts, label, url, observed_at, provenance)`.
- `metric_runs(sequence, ts, status, rows_written)`: collection attempts.

Each table's sequence advances independently; gaps are allowed. Consumers can
open this database read-only and keep separate delivery cursors for observations
and events. Import IDs are preserved; native collection derives stable IDs from
the complete observation, including observation time. Repeated collections keep
their own observations even when counts do not change.

## See also

- [`analytics`](analytics.html) for activity calculated from archived messages.
- [Data storage](../guides/data-storage.html).
