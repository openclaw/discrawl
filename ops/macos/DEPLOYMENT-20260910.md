# Upstream rebuild, September 10, 2026

The active executable is built from unmodified upstream commit
`88a05763f1f04c5171edb6ba97c049874c45e860`, version `0.14.1-5-g88a0576`, with Go
1.27.1. Embedded VCS metadata records that commit and `vcs.modified=false`.
SHA-256: `3f95938d63368d66f512b3c1b869e2d22d2aa8757eec0003c2d9ee112e2a885c`.

No local application patches were applied. Upstream already contains the index,
native tail-freshness output, and PR template. The retained Mac adapter is 248
lines and uses native status for freshness; its old SQL fallback was removed.
Only `ops/macos` differs from the upstream source checkout.

## Validation

- Upstream CI, Docker, CodeQL, and secret scanning passed for the exact commit.
- `go vet ./...` passed. Local `go test -count=1 ./...` passed except the previously
  identified publication-fixture test (`tracked remote branch origin/main is
  missing`); this does not appear in the successful upstream CI run.
- Eight adapter tests passed, including preservation of the native missing-event
  representation without issuing an extra SQL query.
- GUI-session preflight passed native status against the live archive, plus
  authentication, embeddings configuration, database opening, and FTS checks
  against a disposable clone. Native status returned `last_tail_event_at` directly.
- The initial embedding run processed 198 jobs with zero failures. A requested
  cycle processed 11 more with zero failures and resumed capture.
- Three messages captured by the new executable were verified with completed
  jobs and stored OpenAI 512-dimensional embeddings. Capture resumed as PID 31259
  and native status observed a new event at `2026-09-10T20:56:58Z`.

## Rollback and source

The binary and adapter have separate versioned releases selected by their
`current` symlinks. Prior links, configuration, adapter files, the current archive
clone with WAL/SHM, source bundle, and validation records are retained in:

`/Volumes/Data/Projects/openclaw-tools/backups/discrawl-upstream-88a0576-20260910`

The runtime schema and storage dependencies are identical to the previous
deployed build. No archive reset or schema conversion was needed. The source
branch is `ops/discrawl-upstream-88a0576-20260910`; its local commits contain only
the Mac adapter and deployment documentation. Merged PR review worktrees were
retired after validation; their Git branches and history remain available.
