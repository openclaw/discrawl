# `status`

Shows local archive status, or remote archive status when `[remote]` is in
cloud read-only mode.

## Usage

```bash
discrawl status
```

## Reports

- where the local database lives
- guild count and per-guild totals
- channel and thread counts
- message totals
- latest archived message time
- whether the Git share is configured, whether its last check is stale, and whether an exact replacement is pending (`--json`)
- remote endpoint/archive metadata when `remote.mode = "cloud"`
- embeddings status if `[search.embeddings]` is enabled

## See also

- [`coverage`](coverage.html) - per-guild/channel archive readiness and wiretap skip counts
- [`diagnostics`](diagnostics.html) - read-only SQLite, WAL, freshness, and writer-lock checks
- [`doctor`](doctor.html) - liveness check (config, auth, DB, FTS wiring)
- [`remote`](remote.html) - direct Cloudflare remote archive checks
- [`report`](report.html) - Markdown activity block for the shared backup README
