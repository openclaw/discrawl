# Agent Guidance

## Changelog

Treat `CHANGELOG.md` as user-facing release history, not an internal commit log.
Entries should explain what a release lets users do, what supported behavior
changed, what failure was prevented, or what migration/privacy/compatibility
boundary matters.

When editing the changelog:

- start from the reader's task, then name commands, config keys, and internal
  systems only when they explain the user-facing behavior
- preserve concrete details such as R2, D1, SQLite mirrors, gzip chunks, privacy
  manifests, upload limits, and cache paths when they explain capabilities,
  constraints, or safety boundaries
- check the related commit, PR, or issue before removing a specific detail that
  may encode rationale
- avoid broad style churn; preserve existing tense, capitalization, punctuation,
  wrapping, and section shape unless changing them improves correctness or
  clarity
- keep low-level dependency, agent, CI, and harness notes in Maintenance when
  they do not directly affect install, publish, sync, search, archive safety, or
  user workflow
