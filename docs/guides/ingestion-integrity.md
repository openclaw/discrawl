# Ingestion integrity and source contracts

Schema 7 adds collection-scope decisions and text provenance without rewriting
provider message bodies or source edit timestamps. Old captured evidence remains
available to appropriately authorized local/full-access readers.

## Channel scope and visibility

Channel metadata is retained independently of message collection, including
metadata for excluded categories, channels and threads. `channels.raw_json`
continues to contain Discord metadata; policy fields are separate columns:

| Column | Meaning |
| --- | --- |
| `collection_scope` | `allowed`, `excluded`, or `unknown`; default `unknown` |
| `scope_policy` | Opaque SHA-256 of the collector's configured scope policy |
| `scope_updated_at` | UTC local decision timestamp; not a provider edit time |
| `deleted_at` | UTC observation of an authoritative channel/thread deletion |
| `deletion_source` | Provenance of that deletion, such as `discord-gateway` |

`allowed` requires complete, acyclic, same-guild ancestry that passes the
configured ID/kind exclusions and optional category inclusions. Excluded
ancestors and known deletion tombstones deny collection. Missing ancestry stays
`unknown`; a scoped live collector resolves it before accepting content. Allowed
new channels can therefore become eligible without a restart. Provider failures
remain durable work rather than permission to admit an unknown descendant.

Parent changes invalidate descendant decisions atomically. Message transactions
check the persisted policy decision again, so an earlier in-memory decision
cannot authorize a write after an ancestry change. Restart seeding uses retained
guild/channel/thread metadata without resurrecting existing tombstones.

**Public consumers must require `collection_scope=allowed` on the channel and
its ancestry, no deletion tombstone, and independently proven public Discord
permissions.** Collection permission does not imply public visibility. Missing
columns/decisions/ancestry are not public. Native public-only snapshots apply the
same rule. Full-access evidence need not be deleted when its current scope is
excluded or unknown.

## Message text and dates

- `content` remains verbatim provider `content`; an embed-only message can
  legitimately have an empty value.
- `created_at` and `edited_at` remain source timestamps. Do not substitute
  `updated_at`, event receipt time or collection time for an unknown source edit.
- `updated_at` is the owning message's local mutation clock, including derived
  text and attachment-status changes.
- `text_parts_json` is an array of attributed parts, each with `kind`, `path`
  and original `text`, plus `attachment_id` or `message_id` when applicable.
- `text_version=2` identifies this projection. Version 0 means it has not been
  materialized by this projection; retained raw data remains authoritative.

Part kinds include `authored`, `embed_author`, `embed_title`,
`embed_description`, `embed_field_name`, `embed_field_value`, `embed_footer`,
`attachment_filename`, `attachment_text`, `reply_context`, `forward_context`,
`poll_question`, and `poll_answer`. Paths identify provider fields (for example
`/embeds/0/fields/1/value`) or the attachment text column. These are original
parts, not assertions that bot/embed/file text was written by the message author.

`normalized_content` combines searchable parts with Unicode normalization and
whitespace separation. It includes explicitly labelled reply/forward context
for contextual search. A serving projection must not present those contextual
parts as this message's authored body. It may project the message's own embeds
and attachments with labels/provenance. CR/LF/tab boundaries separate words;
non-whitespace controls and configured zero-width characters are removed.

## Attachment extraction

`message_attachments` has primary key `attachment_id`. Consumers must match all
of `message_id`, `guild_id` and `channel_id`, and only associate IDs present in the
current message's own `raw_json.attachments` with that current message.

Historical raw attachment references may have no `message_attachments` row.
That means optional extraction coverage is unavailable, not that the provider
reference was lost. Consumers must not invent text or infer a successful empty
extraction from an absent row. Derived-text repair uses existing extraction only;
it does not backfill missing historical attachment rows.

Existing fields are `attachment_id`, `message_id`, `guild_id`, `channel_id`,
`author_id`, `filename`, `content_type`, `size`, `url`, `proxy_url`, `text_content`,
`media_path`, `content_sha256`, `content_size`, `fetched_at`, `fetch_status`,
`fetch_error`, and `updated_at`. Media fetch fields remain separate from text:

| New column | Contract |
| --- | --- |
| `text_status` | `unknown`, `skipped`, `succeeded`, `empty`, or `failed` |
| `text_error` | Stable non-secret reason code or empty string |
| `text_attempted_at` | Nullable UTC time of the latest extraction attempt |
| `text_succeeded_at` | Nullable UTC time of the retained successful text |

Existing extraction without a receipt stays `unknown` with nullable timestamps;
no historical extraction time is invented. Unsupported/oversized content is
`skipped`. Failed transport, non-2xx or an unexpectedly empty nonempty attachment
is `failed`. An eligible, successful empty response for zero-sized content is
`empty`. Text fetching remains bounded to 256 KiB and indexing to 8 Ki characters.

Transient failures retain previous successful `text_content` and its success
timestamp. Attachment status/text and the owning message's derived text/mutation
clock commit together. Ordinary repair retries a bounded, oldest-attempt-first
set using fresh message attachment URLs. A failed latest attempt is a freshness
caveat even when useful prior text is retained. Current attachment text must not
be spliced into old message-event observations.

## Events, repair and status

Canonical create/update writes and their Gateway event commit atomically.
Single and bulk message deletions retain real deletion observations; channel and
thread deletions retain metadata tombstones. Role events update the retained
role permissions. Voice/stage text is included in REST repair when in scope.

Ordinary startup/periodic repair performs bounded exact-message failure replay,
metadata replay and attachment retry. A recovery fetch is a new `snapshot`
observation at the time it was obtained, not an invented original create/edit.
Attachment recovery also records deduplicated snapshot observations. REST
recovery cannot overwrite a newer local observation, message deletion, member
change or channel tombstone. Active-thread failures use the active-thread
endpoint, not a guild fetch that does not contain authoritative thread metadata.
Its raw payload retains the provider timestamp/edit timestamp. An exact repeat
does not append a duplicate snapshot. Missing provider content is not fabricated
and a 404 alone is not converted into a message deletion.

The failure ledger retains `resolution_reason`, distinguishing exact
reconciliation, explicit policy exclusion and terminal provider unavailability.
Unresolved ingestion and attachment-text failures make native status degraded;
worker state and `text_repair` progress remain separately visible. Historical
provider/coverage limits do not become claims of complete collection merely
because the live owner is running.

## Derived-data recovery and retained evidence

The tail owner performs bounded local text-repair batches with a policy-specific
checkpoint and fixed high-water ID. Each batch commits its checkpoint with its
changes. It neither rewinds ingestion cursors nor starts an upstream historical
backfill. Concurrent newer writes win. Excluded/unknown records remain retained
and are skipped; a completed scan reports skipped/rejected counts explicitly.

Only changed eligible text needs new embedding work. Unchanged vectors are
reused. Changed vectors are retired even when embedding generation is disabled;
stale in-flight leases cannot publish the old text's vector. Before replacement,
vectors and their prior text are retained in
`message_embedding_history`; previous successful extraction is retained in
`attachment_text_history`. These are **internal evidence tables**, not public
content projections. They include origin message/guild/channel identity for
authorization and must not bypass the original message's scope/visibility.

No migration fabricates missing source IDs, dates, parent messages or provider
deletions, and none purges already captured raw/history evidence.
