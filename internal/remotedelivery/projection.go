// Package remotedelivery owns qualified source projection and durable delivery.
package remotedelivery

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/store"
)

const Profile = "discrawl-report-changes/v1"

var (
	ErrProjection = errors.New("invalid native report projection")
	ErrReconcile  = errors.New("native scope change requires dependency reconciliation")
	ErrOwnership  = errors.New("ownership lookup is missing or does not match the capture baseline and scope")
)

// Scope is immutable within a qualified binding. This is a private service
// profile for whole guilds, not the public-only Git snapshot permission filter.
type Scope struct{ GuildIDs []string }

type OwnershipChange struct {
	Key   string
	Owned bool
}

// OwnershipSnapshot must explicitly answer every changed key, including false
// for known absence. Missing answers are not proof that a key was never sent.
type OwnershipSnapshot struct {
	Baseline    store.NativeBaseline
	ScopeSHA256 string
	Keys        map[string]bool
	// Presence, including an empty list, proves the outbox enumerated every
	// previously owned checkpoint for this channel at Baseline.
	ChannelCheckpoints map[string][]string
}

type Projection struct {
	Changes   []remote.Change
	Ownership []OwnershipChange
}

// Layout is the receiver's versioned native-column contract, not SELECT *.
// Keep this explicit so a future native migration cannot silently reorder data.
var columns = map[string][]string{
	"messages":   strings.Fields("id guild_id channel_id author_id message_type created_at edited_at deleted_at content normalized_content reply_to_message_id pinned has_attachments raw_json updated_at text_parts_json text_version"),
	"guilds":     strings.Fields("id name icon raw_json updated_at deleted_at deletion_source deletion_reason"),
	"channels":   strings.Fields("id guild_id parent_id kind name topic position is_nsfw is_archived is_locked is_private_thread thread_parent_id archive_timestamp raw_json updated_at collection_scope deleted_at deletion_source scope_policy scope_updated_at"),
	"members":    strings.Fields("guild_id user_id username global_name display_name nick discriminator avatar bot joined_at role_ids_json raw_json updated_at deleted_at deletion_source deletion_reason"),
	"sync_state": strings.Fields("scope cursor updated_at"),
}

var keys = map[string][]string{
	"messages": {"id"}, "guilds": {"id"}, "channels": {"id"},
	"members": {"guild_id", "user_id"}, "sync_state": {"scope"},
}

var integers = map[string][]string{
	"messages": {"message_type", "pinned", "has_attachments", "text_version"},
	"channels": {"position", "is_nsfw", "is_archived", "is_locked", "is_private_thread"},
	"members":  {"bot"},
}

var nullable = map[string][]string{
	"messages":   {"author_id", "edited_at", "deleted_at", "reply_to_message_id"},
	"guilds":     {"icon", "deleted_at", "deletion_source", "deletion_reason"},
	"channels":   {"parent_id", "topic", "position", "thread_parent_id", "archive_timestamp", "deleted_at", "deletion_source", "scope_updated_at"},
	"members":    {"global_name", "display_name", "nick", "discriminator", "avatar", "joined_at", "deleted_at", "deletion_source", "deletion_reason"},
	"sync_state": {"cursor"},
}

func canonicalScope(scope Scope) (Scope, error) {
	ids := slices.Clone(scope.GuildIDs)
	slices.Sort(ids)
	if len(ids) == 0 {
		return Scope{}, ErrProjection
	}
	for i, id := range ids {
		if !validIdentity(id) || id == "@me" || (i > 0 && id == ids[i-1]) {
			return Scope{}, ErrProjection
		}
	}
	return Scope{GuildIDs: ids}, nil
}

func ScopeSHA256(scope Scope) (string, error) {
	canonical, err := canonicalScope(scope)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// NativeAnchor binds the complete cursor while fitting the shared wire limit.
// File identity remains local; this hash grants no source or seed admission.
func NativeAnchor(cursor store.NativeCursor) (string, error) {
	if cursor.Sequence < 0 || len(cursor.Generations) != len(columns) {
		return "", ErrProjection
	}
	body, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return "dcn1:" + strconv.FormatInt(cursor.Sequence, 10) + ":" + hex.EncodeToString(sum[:]), nil
}

// OwnershipKey is a local SQLite key. Preserve the full framed tuple rather
// than concatenating IDs whose delimiters or NUL bytes may collide.
func OwnershipKey(table string, key []store.NativeValue) (string, error) {
	want, ok := keys[table]
	if !ok || len(key) != len(want) {
		return "", ErrProjection
	}
	values := []string{table}
	for _, value := range key {
		if value.Kind != "text" || value.Text == "" || !utf8.ValidString(value.Text) {
			return "", ErrProjection
		}
		values = append(values, value.Text)
	}
	body, err := json.Marshal(values)
	return string(body), err
}

// ProjectCapture accepts ownership only for the changed keys, loaded by the
// outbox from its projection-equivalent qualified seed and completed captures.
// A broader seed cannot become equivalent by merely skipping later updates.
// This operation performs no I/O.
// Ownership deltas belong in the same durable transaction as the staged parts;
// they must not be applied to the acknowledged baseline before completion.
func ProjectCapture(capture store.NativeCapture, scope Scope, ownership OwnershipSnapshot) (Projection, error) {
	scope, err := canonicalScope(scope)
	if err != nil {
		return Projection{}, err
	}
	scopeHash, err := ScopeSHA256(scope)
	if err != nil {
		return Projection{}, err
	}
	from, owned := capture.From, ownership.Baseline
	if scopeHash != ownership.ScopeSHA256 || from.SourceID != owned.SourceID || from.Cursor.Sequence != owned.Cursor.Sequence || from.Cursor.Entry != owned.Cursor.Entry || !slices.Equal(from.Cursor.Generations, owned.Cursor.Generations) {
		return Projection{}, ErrOwnership
	}
	capture, err = reconcileChannelCheckpoints(capture, ownership)
	if err != nil {
		return Projection{}, err
	}
	result := Projection{Changes: []remote.Change{}}
	for _, change := range capture.Changes {
		key, err := OwnershipKey(change.Table, change.Key)
		if err != nil {
			return Projection{}, err
		}
		wasOwned, known := ownership.Keys[key]
		if !known {
			return Projection{}, ErrOwnership
		}
		included := false
		var row []remote.ChangeValue
		switch change.Operation {
		case "remove":
			if len(change.Row) != 0 {
				return Projection{}, ErrProjection
			}
		case "upsert":
			row, err = projectRow(change)
			if err != nil {
				return Projection{}, err
			}
			included, err = rowInScope(change, row, capture.CoverageGuilds, capture.ChannelCoverage, scope)
			if err != nil {
				return Projection{}, err
			}
		default:
			return Projection{}, ErrProjection
		}
		_, reconciled := capture.ChannelCoverage[change.Key[0].Text]
		if (change.Table == "channels" && wasOwned != included && !reconciled) || (change.Table == "guilds" && wasOwned && !included) {
			return Projection{}, ErrReconcile
		}
		if !included && !wasOwned {
			continue
		}
		wireKey, err := projectValues(change.Key)
		if err != nil {
			return Projection{}, err
		}
		out := remote.Change{Table: change.Table, Key: wireKey, Operation: "remove"}
		if included {
			out.Operation, out.Row = "upsert", row
		}
		result.Changes = append(result.Changes, out)
		result.Ownership = append(result.Ownership, OwnershipChange{Key: key, Owned: included})
	}
	return result, nil
}

func projectRow(change store.NativeChange) ([]remote.ChangeValue, error) {
	want := columns[change.Table]
	if len(change.Columns) != len(want) || len(change.Row) != len(want) {
		return nil, ErrProjection
	}
	byName := make(map[string]store.NativeValue, len(want))
	for i, name := range change.Columns {
		if _, duplicate := byName[name]; duplicate {
			return nil, ErrProjection
		}
		byName[name] = change.Row[i]
	}
	ordered := make([]store.NativeValue, 0, len(want))
	for _, name := range want {
		value, ok := byName[name]
		if !ok {
			return nil, ErrProjection
		}
		kind := "text"
		if slices.Contains(integers[change.Table], name) {
			kind = "integer"
		}
		if value.Kind != kind && (value.Kind != "null" || !slices.Contains(nullable[change.Table], name)) {
			return nil, ErrProjection
		}
		ordered = append(ordered, value)
	}
	for i, name := range keys[change.Table] {
		if byName[name] != change.Key[i] {
			return nil, ErrProjection
		}
	}
	return projectValues(ordered)
}

func projectValues(values []store.NativeValue) ([]remote.ChangeValue, error) {
	out := make([]remote.ChangeValue, 0, len(values))
	for _, value := range values {
		v := remote.ChangeValue{Kind: value.Kind}
		switch value.Kind {
		case "null":
		case "text":
			if !utf8.ValidString(value.Text) {
				return nil, ErrProjection
			}
			text := value.Text
			v.Value = &text
		case "integer":
			text := strconv.FormatInt(value.Integer, 10)
			v.Value = &text
		default:
			return nil, ErrProjection
		}
		out = append(out, v)
	}
	return out, nil
}

func rowInScope(change store.NativeChange, row []remote.ChangeValue, coverage map[string]string, channels map[string]store.NativeChannelCoverage, scope Scope) (bool, error) {
	field := "guild_id"
	if change.Table == "guilds" {
		field = "id"
	}
	if change.Table == "sync_state" {
		checkpoint := change.Key[0].Text
		if rest, ok := strings.CutPrefix(checkpoint, "guild:"); ok {
			guild, suffix, ok := strings.Cut(rest, ":")
			if !ok || !validIdentity(guild) || suffix == "" {
				return false, ErrProjection
			}
			return slices.Contains(scope.GuildIDs, guild), nil
		}
		if strings.HasPrefix(checkpoint, "channel:") {
			guild, present := coverage[checkpoint]
			channel, _, _ := strings.Cut(strings.TrimPrefix(checkpoint, "channel:"), ":")
			if complete, known := channels[channel]; present && guild == "" && known && !complete.Present {
				return false, nil
			}
			if !present || guild == "" {
				return false, fmt.Errorf("%w: checkpoint channel is unresolved", ErrReconcile)
			}
			return slices.Contains(scope.GuildIDs, guild), nil
		}
		// Host, worker, schema and share receipts are not report-source coverage.
		return false, nil
	}
	for i, name := range columns[change.Table] {
		if name == field {
			if row[i].Kind != "text" || row[i].Value == nil || !validIdentity(*row[i].Value) {
				return false, ErrProjection
			}
			return slices.Contains(scope.GuildIDs, *row[i].Value), nil
		}
	}
	return false, ErrProjection
}

func validIdentity(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}
