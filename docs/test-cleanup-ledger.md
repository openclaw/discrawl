# Test cleanup ledger

## CLI fixture scaffolding

- Moved the test-only `withServices` fixture wrapper into `cli_test.go` and
  removed its redundant `withServicesAuto` forwarding layer. Fixtures still
  call the active service lifecycle with the same arguments.
- Moved `writeSyncLockMetadataFiles` into the test file. Its callers construct
  legacy or malformed lock fixtures; production writes through an open record.
- Kept all CLI fixture callers and their lock, startup, query, and error checks.
  Production retains the active locked-service and lock-record implementations.

No CLI service behavior, lock format, or release artifacts changed.

## Test-only formatters

- Removed the unused `clipWhitespace` formatter and its two assertions from
  the report helper test. No report rendering path called it.
- Removed the test-only `normalizeMessage`/`normalizeMessageParts` chain and
  its obsolete attachment-adapter test. Production derives attributed message
  parts directly through `messagetext`.
- Updated the rich-message and Unicode tests to exercise `buildMessageMutation`,
  requiring the actual archived normalized content. Their attachment fixtures
  now carry IDs so the real extraction path includes them.
- Kept raw-message round-trip checks on the real `messagetext` API.
- Retained the SQLite-backed report rendering and Components V2 round-trip/search
  tests, plus rich-message and malformed-Unicode normalization checks.

No report output, message content, schema, or release artifacts changed.

## Gateway event delivery

- Reused the existing gateway handshake helper instead of maintaining a second
  copy of its HTTP discovery and WebSocket hello/identify/ready exchange.
- Replaced the 100ms cancellation sleep in `TestTailReceivesGatewayEvents`
  with explicit callback completion. The test waits for ready, message,
  channel/thread, and member callbacks across both worker queues.
- Removed the fake socket's 50ms lifetime. It now stays open until the test
  cancels after all expected callbacks, with bounded failure deadlines.
- Retained exact callback multiplicities, message observation order, and
  handler removal assertions. The fixture uses only a local HTTP/WebSocket
  server and synthetic events; it does not contact Discord.

This is a bounded gateway-test cleanup, not a complete repository audit.
No production behavior or release artifacts changed.

## Retired manifest timestamp predicates

- Removed `ManifestAlreadyImported` and `ManifestAlreadyMerged`. Production
  import/merge paths no longer call these timestamp-only predicates.
- Replaced their integration-test uses with exact persisted marker assertions
  after actual export, merge, and forced replacement operations.
- Removed malformed-marker fixtures and predicate-only assertions. Retained
  empty-manifest handling, pending-replacement clearing, import freshness, local
  row preservation, and exact replacement checks.

No import planning, stored state format, archive data, or release artifacts changed.

## Tail ancestor events

- Removed `trackChannelExclusion`, a retired in-memory updater called only by
  the ancestor-move test. Production persists channel events and refreshes scope.
- Reworked that test to send actual channel-upsert and message-create events
  against temporary SQLite storage. It retains child-before-parent ordering and
  moves a forum into and back out of an excluded category.
- Requires exactly the two admitted message IDs in the archive, proving both
  rejection while excluded and collection after readmission.
- Retained the separate concurrent-revocation and incomplete-ancestry tests.

No production event handling, archive format, or release artifacts changed.

## Retired tail test adapters

- Removed `SetTailFailureStage`, whose only callers were tests. Tail execution
  already installs failure metadata before invoking handlers.
- Changed the stage-context test to use that active metadata context and
  `UpdateTailFailureStage`. Keep virtual-time elapsed checks, canceled-context
  rejection, frozen stages, unknown-stage normalization, and nil safety.
- Removed `IsGatewayOpenError`, whose only callers were two error-path tests.
  Both now use `errors.As` assertions on the real `GatewayOpenError` type.
  Gateway startup sanitization, error chaining, and failure-report checks remain.

No gateway behavior, failure metadata, or release artifacts changed.

## Retired config test adapters

- Removed `SearchGuildDefaults`, a constant-nil method with no production
  callers, and its only nil assertion. Effective default-guild, normalization,
  search/filter, and runtime-directory tests remain.
- Removed the test-only `RemoteEnabled` forwarding method. The existing default
  and cloud-mode checks now call `cfg.Remote.Enabled()` on the embedded
  crawlkit config.
  The cloud read-only assertion and remote config fields remain unchanged.

Validation: config race tests, module consistency, formatting, vet, independent
P0–P3 review, and full PR CI. No config format, CLI behavior, live archive data,
or release artifacts changed.

## Retired standalone event writer

- Removed `AppendMessageEvent`, whose only callers were tests. Production
  message updates and deletions append events through their transactions.
- Removed the adapter's closed-store assertion and an unused event fixture
  from the mixed sync-state/helper test. Its real message deletion remains.
- Strengthened the existing batched-write test from an event count to exact
  persisted guild, channel, message, type, and payload rows. It still checks
  both messages and now rejects incorrectly wired event contents.

No production event behavior, archive format, or release artifacts changed.

## Retired incomplete-channel filter

- Removed `IncompleteMessageChannelIDs`, called only by tests. Full sync plans
  with `AllIncompleteMessageChannelIDs`; routine sync filters provider channels
  through `FreshUnavailableChannelIDs` in the syncer.
- Removed two tests of the retired composition and its assertions in the TTL
  tests. Keep production timestamp parsing, marker-window edges, refresh,
  clearing, suffix matching, and malformed/offset timestamp coverage.
- Preserved completed-thread coverage on the active full-sync listing and
  added an incomplete thread alongside guild, voice/stage, and marker cases.
  Closed-store error coverage now calls that same production query.
- Retained real syncer entrypoint and message-sync tests for routine skip/retry,
  explicit requests, full sync, and catalog progress using temporary archives.

No sync behavior, query/schema format, live archive data, or release changes.

## Test-only guild member count

- Removed `GuildMemberCount`, whose only callers were two store tests, and its
  dedicated `CountMembersByGuild` SQL definition. Regenerated sqlc output with
  the repository's pinned generator.
- Removed the closed-store error check and rebuild-test count assertion for
  that unused API. Keep the rebuild test's actual member-search assertion,
  channel count, and message-search assertion.
- Retained the active `Members` and `Status` paths, including existing member
  count assertions and the global `CountMembers` SQL query.

No member storage, search, status, archive schema, or CLI behavior changed.
