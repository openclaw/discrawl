# Continuous embedding rollout, September 11, 2026

Crawlkit v0.16.0 is published through the official release pipeline; its shared
worker API merged in PR #115. Discrawl PR #217 adds opt-in live embedding and is
merged as `8f0e93b808ae84ea390a2d28875f96654343de29`. The application is built from that clean
upstream commit with no local application patches.

The Mac adapter starts `tail --embed-live` and no longer runs an hourly
stop/embed/repair/resume loop. It retains Keychain integration, process and
resource guards, startup repair, log rotation and readiness checks. Collection
filters, the embedding provider and LaunchAgent definitions are preserved.

Native worker tests cover concurrent capture, edits/deletes, expired claims,
restarts, provider failures and configured batch/time limits. A simulated
10-message/second workload with a 200 ms provider measured p95 around 0.62 s.
The full Go race suite passes; the existing publishing fixture needs Git's
`init.defaultBranch=master` on this Mac, matching Linux CI. Upstream CI passed.
The full 15 GB archive-copy migration took 8.64 s and preserved messages,
vectors and job-state counts.

Operational evidence and the latest pre-migration snapshot live in
`/Volumes/Data/Projects/openclaw-tools/backups/discrawl-live-workers-20260911`.
Schema 6 cannot be opened by the old binary. Feature rollback keeps the new
schema-compatible executable and restores the previous hourly adapter; it does
not overwrite newly captured data with an old archive. See `rollback.json` and
`verification.json` in that directory after activation.
