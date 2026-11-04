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
