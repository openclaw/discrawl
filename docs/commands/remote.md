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
