# `remote`

Reads the configured Cloudflare remote archive through the Worker API.
The Worker is deployed separately from discrawl in `openclaw/crawl-remote`
with Wrangler; discrawl stores an endpoint/archive and never deploys the
service itself.

## Usage

```bash
discrawl remote status
discrawl remote archives
discrawl remote whoami
discrawl whoami
```

## Reports

- `remote status` returns the crawlkit control status for the configured archive
- `remote archives` lists archives visible to the authenticated identity
- `remote whoami` and `whoami` report the GitHub/org identity associated with the token

## Notes

Remote commands require `[remote]` config with `mode = "cloud"`, `endpoint`,
`archive`, and a token in `remote.token_env` (default:
`DISCRAWL_REMOTE_TOKEN`). They do not open or create the local SQLite archive.

## See also

- [`subscribe-cloud`](subscribe-cloud.html)
- [`status`](status.html)

## Scheduled publication

The existing backup workflow can also publish its successful source state to D1
and R2. It saves the runtime cache before Cloud publication. A Cloud failure
therefore does not discard the Discord sync or the published Git snapshot.

Enable `DISCRAWL_CLOUD_PUBLISH_ENABLED=1` only after the backend supports
`discrawl.current-state.v1` and publisher credentials are configured. The first
run checks the archive inventory before adoption. Configure these GitHub Actions
secrets:

- `DISCRAWL_CLOUD_ENDPOINT`: the Worker URL.
- `DISCRAWL_CLOUD_ARCHIVE`: the existing archive ID.
- `DISCRAWL_CLOUD_ACCESS_CLIENT_ID` and `DISCRAWL_CLOUD_ACCESS_CLIENT_SECRET`:
  service credentials, if the endpoint also requires Cloudflare Access.

The job uses its existing `DISCORD_BACKUP_TOKEN` for Cloud login. The Worker
still checks the token's organization and team membership; an unauthorized token
fails before any Cloud data changes. No local CLI credential is copied into CI.

The workflow creates one filtered SQLite export with
`discrawl cloud publish --export-only PATH`. The output must not already exist.
This command needs no remote credentials and excludes direct messages, deleted
records, raw payloads and history. The scheduled publisher uses that same file
for D1 batches and the R2 gzip snapshot. The existing `cloud publish` flags remain
supported and also use one fixed export for both destinations.

D1 keeps one current set of rows. Unchanged key ranges are skipped; changed ranges
include updates and deletions. Failed uploads resume from server progress. Query
commands stay available as batches update in place. Readers can see newer batches
before the whole refresh finishes. The last completed SQLite snapshot remains
downloadable. R2 retains the current and previous
snapshots, plus an unfinished upload; it leaves other archives and legacy objects
alone. Public job logs contain aggregate counts, not archive IDs or message data.

Before first adoption, the publisher checks remote keys against the runtime,
including deleted source rows. If a remote key is absent, publication stops for
owner review. Do not bypass this check or reset the remote tables.
