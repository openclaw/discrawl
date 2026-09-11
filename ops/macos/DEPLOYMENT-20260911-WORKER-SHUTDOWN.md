# Worker shutdown patch, September 11, 2026

Crawlkit PR #117 fixes the cancellation finding left in the final review of
PR #115. A controlled-clock regression reproduced 320 seconds of shutdown cleanup
for the default 64-job batch when retry persistence was canceled and release
storage was unavailable. The fixed path uses one five-second cleanup budget.
The same repair covers interrupted completion and rejected batches.

Crawlkit v0.16.1 was published by official release run `34649097014`, from
`0bb18e9865a2b8ecbd8e94924a8f6c1dbf7233f8`. Public Go proxy, checksums, and
downloaded worker source were verified. Discrawl PR #218 consumes that release;
the deployed binary is built from its clean merged commit
`66fbb8fb51b0abd0dde0ed0a99de1c60f8e17ab2`, version `0.14.1-8-g66fbb8f`.

Both follow-up PRs passed CI and final ClawSweeper review with no actionable
findings or before-merge blockers. Local validation included all 16 cancellation
cases, Crawlkit's full tests/race suite, vet and tidy, Discrawl's full race suite,
and its live-embedding integration tests. Structured review used the repository's
`autoreview --mode local --engine codex` helper on each change and returned clean.
The existing Discrawl publishing fixture used `init.defaultBranch=master` to
match Linux CI.

The Mac adapter, configuration, and LaunchAgent definitions were retained.
The `current` binary symlink now selects `upstream-66fbb8f-worker-shutdown`.
The service was restarted gracefully and performed its normal startup repair.
No database migration or archive copy was needed for this patch.

Build provenance, configuration hashes, activation details, and live capture and
embedding verification are recorded under
`/Volumes/Data/Projects/openclaw-tools/backups/worker-shutdown-fix-20260911`.

For rollback, restore `current` to `upstream-8f0e93b-live` and gracefully restart
`com.hrudolph.discrawl-tail`. Both binaries support schema 6; retain the live
archive. The older binary contains the shutdown defect this patch fixes.
