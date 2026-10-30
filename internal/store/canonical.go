package store

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"time"
)

// Canonical provider metadata.
//
// Discord delivers the same entity through two transports. Gateway events and
// REST responses disagree on transport-only fields (a Gateway message carries
// guild_id and member; REST carries public user flags), order unordered sets
// differently, and use different timestamp precision. Rewriting a row with
// whichever shape arrived last made every repair pass a content change.
//
// canonicalizeRaw produces the one stored raw_json form for an entity:
//
//   - keys are sorted (encoding/json map order) and numbers keep their
//     literal text;
//   - unordered sets are sorted (guild features, channel
//     permission_overwrites);
//   - known timestamps are truncated to millisecond precision;
//   - guild collections retain full nested metadata; live counters move to
//     entity_metrics;
//   - transport-only fields are merged with the stored row: a value that the
//     incoming transport omits, nulls or zeroes never replaces a known stored
//     value (see the per-entity merge lists below).
//
// Two raw payloads are the same content when their canonical forms are equal.
// Unknown keys are kept unchanged, so Desktop cache payloads and future
// provider fields survive.

type entityKind int

const (
	entityGuild entityKind = iota
	entityChannel
	entityMember
	entityMessage
)

var (
	// Gateway-only guild collections: preserve complete metadata.
	guildCollectionKeys = []string{"members", "presences", "voice_states", "stage_instances"}
	// The guild channel and thread lists are the restart scope catalog
	// (seedScopeMetadata): retain all fields for unknown-field safety.
	guildCatalogKeys = []string{"channels", "threads"}
	// Live guild counters. Every supplied number, including zero, is retained.
	guildMetricKeys = []string{"member_count", "approximate_member_count", "approximate_presence_count", "premium_subscription_count"}
	// Live channel counters. Relationship identifiers remain content.
	channelMetricKeys = []string{"message_count", "member_count", "total_message_sent"}
)

// canonicalizeRaw returns the raw_json to store for incoming, merged with
// stored (which may be empty or a pre-canonical legacy payload), and the live
// metrics extracted from incoming. When the canonical merge makes no edit at
// all, the incoming bytes are stored unchanged; otherwise the canonical
// encoding (sorted keys) is stored. A payload that is not a JSON object is
// returned unchanged.
func canonicalizeRaw(kind entityKind, incoming, stored, channelKind string) (string, map[string]string) {
	merged, metrics := canonicalMerge(kind, incoming, stored, channelKind)
	if merged == incoming {
		return incoming, metrics
	}
	if plain, ok := decodeRawObject(incoming); ok && encodeRawObject(plain) == merged {
		return incoming, metrics
	}
	return merged, metrics
}

// canonicalForm is the comparison form of a stored or incoming payload. In
// addition to the stored canonical rules, an absent, null, empty-list and
// empty-object value are equivalent (transports omit, null or empty unset
// collections differently).
func canonicalForm(kind entityKind, raw, channelKind string) string {
	form, _ := canonicalMerge(kind, raw, "", channelKind)
	obj, ok := decodeRawObject(form)
	if !ok {
		return form
	}
	for _, key := range []string{"reactions", "attachments", "embeds", "components", "mentions", "mention_roles", "channels", "threads", "roles"} {
		if isEmptyValue(obj[key]) {
			delete(obj, key)
		}
	}
	return encodeRawObject(obj)
}

func isEmptyValue(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case []any:
		return len(x) == 0
	case map[string]any:
		return len(x) == 0
	}
	return false
}

func canonicalMerge(kind entityKind, incoming, stored, channelKind string) (string, map[string]string) {
	in, ok := decodeRawObject(incoming)
	if !ok {
		return incoming, nil
	}
	base, _ := decodeRawObject(stored)
	if base != nil && !sameEntityIdentity(kind, in, base) {
		// Merge only two observations of the same provider entity, never a
		// placeholder or tombstone receipt.
		base = nil
	}
	metrics := map[string]string{}
	switch kind {
	case entityGuild:
		for _, key := range guildMetricKeys {
			if v, ok := metricNumber(in[key]); ok {
				metrics[key] = v
			}
			delete(in, key)
		}
		for _, key := range guildCollectionKeys {
			if base != nil {
				keepKnown(in, base, key, isEmptyList)
			}
		}
		sortStrings(in, "features")
		for _, key := range guildCatalogKeys {
			if base != nil {
				keepKnown(in, base, key, isEmptyList)
			}
			// Keep complete nested metadata; unknown fields remain content.
		}
		if base != nil {
			// GUILD_CREATE-only values that REST reports as placeholders.
			keepKnown(in, base, "joined_at", isZeroValue)
			keepKnown(in, base, "large", isZeroValue)
			keepKnown(in, base, "region", func(v any) bool { return isZeroValue(v) || v == "deprecated" })
		}
	case entityChannel:
		for _, key := range channelMetricKeys {
			if v, ok := metricNumber(in[key]); ok {
				metrics[key] = v
			}
			delete(in, key)
		}

		sortPermissionOverwrites(in)
		if meta, ok := in["thread_metadata"].(map[string]any); ok {
			normalizeTimeField(meta, "archive_timestamp")
			normalizeTimeField(meta, "create_timestamp")
		}
		if base != nil {
			keepKnown(in, base, "guild_id", isZeroValue)
			// bitrate is only provider content for voice and stage channels;
			// other kinds report 0 or 64000 depending on the producer.
			if channelKind != "voice" && channelKind != "type_13" && channelKind != "stage" {
				keepKnown(in, base, "bitrate", isZeroValue)
			}
			// invitable is only defined for private threads.
			if channelKind != "thread_private" {
				meta, _ := in["thread_metadata"].(map[string]any)
				baseMeta, _ := base["thread_metadata"].(map[string]any)
				if meta != nil && baseMeta != nil {
					keepKnown(meta, baseMeta, "invitable", isZeroValue)
				}
			}
		}
	case entityMember:
		normalizeTimeField(in, "joined_at")
		normalizeTimeField(in, "premium_since")
		normalizeTimeField(in, "communication_disabled_until")
		if base != nil {
			keepKnown(in, base, "guild_id", isZeroValue)
			keepKnown(in, base, "flags", isZeroValue)
			mergeUserFlags(in, base, "user")
		}
	case entityMessage:
		if base != nil {
			keepKnown(in, base, "guild_id", isZeroValue)
			keepKnown(in, base, "member", isZeroValue)
			mergeUserFlags(in, base, "author")
			mergeMentionMembers(in, base)
			ref, _ := in["referenced_message"].(map[string]any)
			baseRef, _ := base["referenced_message"].(map[string]any)
			if ref != nil && baseRef != nil {
				keepKnown(ref, baseRef, "guild_id", isZeroValue)
				keepKnown(ref, baseRef, "member", isZeroValue)
				mergeUserFlags(ref, baseRef, "author")
				mergeMentionMembers(ref, baseRef)
			}
		}
	}
	return encodeRawObject(in), metrics
}

// sameCanonicalRaw reports whether stored already holds the content of the
// payload to store (the result of canonicalizeRaw against stored).
func sameCanonicalRaw(kind entityKind, stored, next, channelKind string) bool {
	return canonicalForm(kind, stored, channelKind) == canonicalForm(kind, next, channelKind)
}

func decodeRawObject(raw string) (map[string]any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw[0] != '{' {
		return nil, false
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var out map[string]any
	if err := dec.Decode(&out); err != nil || out == nil {
		return nil, false
	}
	if dec.More() {
		return nil, false
	}
	return out, true
}

func encodeRawObject(v map[string]any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// Match encoding/json.Marshal, which the collectors used before.
	enc.SetEscapeHTML(true)
	if err := enc.Encode(v); err != nil {
		return "{}"
	}
	return strings.TrimSuffix(buf.String(), "\n")
}

// isZeroValue treats an absent, null, false, empty, numeric zero or zero-time
// value as "not supplied by this transport".
func isZeroValue(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case bool:
		return !x
	case string:
		return x == "" || x == "0001-01-01T00:00:00Z"
	case json.Number:
		f, err := x.Float64()
		return err == nil && f == 0
	case map[string]any:
		return len(x) == 0
	}
	return false
}

// keepKnown keeps base[key] when incoming does not supply a known value.
func keepKnown(in, base map[string]any, key string, placeholder func(any) bool) {
	if !placeholder(in[key]) {
		return
	}
	if prior, ok := base[key]; ok && !placeholder(prior) {
		in[key] = prior
	}
}

func mergeUserFlags(in, base map[string]any, key string) {
	user, _ := in[key].(map[string]any)
	baseUser, _ := base[key].(map[string]any)
	if user == nil || baseUser == nil || user["id"] != baseUser["id"] {
		return
	}
	keepKnown(user, baseUser, "flags", isZeroValue)
	keepKnown(user, baseUser, "public_flags", isZeroValue)
}

func mergeMentionMembers(in, base map[string]any) {
	mentions, _ := in["mentions"].([]any)
	baseMentions, _ := base["mentions"].([]any)
	if len(mentions) == 0 || len(baseMentions) == 0 {
		return
	}
	byID := map[any]map[string]any{}
	for _, m := range baseMentions {
		if obj, ok := m.(map[string]any); ok {
			byID[obj["id"]] = obj
		}
	}
	for _, m := range mentions {
		obj, ok := m.(map[string]any)
		if !ok {
			continue
		}
		prior := byID[obj["id"]]
		if prior == nil {
			continue
		}
		keepKnown(obj, prior, "member", isZeroValue)
		keepKnown(obj, prior, "flags", isZeroValue)
		keepKnown(obj, prior, "public_flags", isZeroValue)
	}
}

func sameEntityIdentity(kind entityKind, a, b map[string]any) bool {
	if kind == entityMember {
		ua, _ := a["user"].(map[string]any)
		ub, _ := b["user"].(map[string]any)
		return ua != nil && ub != nil && ua["id"] != nil && ua["id"] == ub["id"]
	}
	return a["id"] != nil && a["id"] == b["id"]
}

func isEmptyList(v any) bool {
	list, ok := v.([]any)
	return v == nil || (ok && len(list) == 0)
}

func metricNumber(v any) (string, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return "", false
	}
	if _, err := n.Float64(); err != nil {
		return "", false
	}
	return n.String(), true
}

func sortStrings(in map[string]any, key string) {
	values, ok := in[key].([]any)
	if !ok {
		return
	}
	slices.SortStableFunc(values, func(a, b any) int {
		as, _ := a.(string)
		bs, _ := b.(string)
		return strings.Compare(as, bs)
	})
}

func sortPermissionOverwrites(in map[string]any) {
	values, ok := in["permission_overwrites"].([]any)
	if !ok {
		return
	}
	slices.SortStableFunc(values, func(a, b any) int {
		if c := compareSnowflake(objectString(a, "id"), objectString(b, "id")); c != 0 {
			return c
		}
		return strings.Compare(objectString(a, "type"), objectString(b, "type"))
	})
}

func objectString(v any, key string) string {
	obj, _ := v.(map[string]any)
	switch x := obj[key].(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	}
	return ""
}

// compareSnowflake orders decimal snowflakes numerically and anything else
// lexically after them.
func compareSnowflake(a, b string) int {
	if len(a) != len(b) && isCanonicalDecimalSnowflake(a) && isCanonicalDecimalSnowflake(b) {
		return len(a) - len(b)
	}
	return strings.Compare(a, b)
}

func normalizeTimeField(in map[string]any, key string) {
	s, ok := in[key].(string)
	if !ok || s == "" {
		return
	}
	in[key] = normalizeProviderTime(s)
}

// normalizeProviderTime truncates a provider timestamp to the millisecond
// precision both Discord transports share and formats it as RFC 3339 UTC
// with trailing zeros trimmed (the form the collectors already stored).
// Unparseable values are returned unchanged.
func normalizeProviderTime(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return raw
	}
	if t.IsZero() {
		return raw
	}
	return t.UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano)
}
