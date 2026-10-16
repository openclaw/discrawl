# Components V2 preservation and scoped recovery

The native collector retains Discord's component tree in `messages.raw_json`.
For messages with `IS_COMPONENTS_V2` (`32768`), the native text-parts path extracts
text displays, button labels and URLs, and thumbnail/gallery descriptions, with
their JSON-pointer paths. The additive part kinds are `component_text`,
`component_label`, `component_url`, and `component_description`. The text-v2
format remains unchanged. Authored `content` stays exactly as Discord supplied
it, including an empty string. Ordinary button-only messages keep their existing
normalized text; unchanged normalized text reuses existing vectors.

Older native builds dropped components while serializing the Discord SDK object.
An absent component tree cannot be reconstructed from that stored row. Check
the actual installed source and retained evidence before attempting a local
text rebuild. A version label alone does not prove that a custom build includes
this preservation fix.

Prepare recovery under the source owner's normal maintenance process:

1. Freeze a finite candidate list with message/guild/channel IDs, hashes and
   component-presence metadata. Keep message bodies out of diagnostic output.
2. Prepare and review a compatible signed build, preserving active source
   permissions, member refresh, durability and other installed fixes.
3. Take a consistent private backup and record the exact original cursors and
   installed identity before an approved graceful native cutover.
4. Rewind only approved affected channel cursors to just before their oldest
   candidate, following the scoped upstream recovery procedure. Keep unrelated
   cursors, history and exclusions intact. Existing normal startup repair can
   recover those ranges while Gateway capture runs.
5. REST pages carry their request-start observation time into the existing
   transactional revision guard. A newer Gateway mutation or deletion wins over
   a stale page, including when provider edit timestamps are absent. The guard
   covers current text and dependent projections together. A page's fetched-row
   count does not prove every row was applied: verify exact candidates afterward.
6. Verify components, attributed text, search and source frontiers. Report
   inaccessible/deleted provider responses explicitly; missing data is not a
   fabricated deletion. Coordinate downstream publication after source acceptance.

Retain binary rollback and before-state evidence. Roll back only under native
ownership, preserving observations made since the backup. Never lower a cursor
that has advanced beyond its recorded original frontier or blindly restore an
old whole database over new collection. Source recovery and downstream database
migration are separate operations; capture-feed activation holds remain separate.
