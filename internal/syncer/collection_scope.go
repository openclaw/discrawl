package syncer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
)

func (e channelExclusions) configured() bool {
	return len(e.ids) > 0 || len(e.kinds) > 0 || e.categoryScopeSet
}

func (e channelExclusions) policyID() string {
	b, err := json.Marshal(struct {
		IDs, Kinds, Categories []string
		CategoryScope          bool
	}{slices.Sorted(maps.Keys(e.ids)), slices.Sorted(maps.Keys(e.kinds)), slices.Sorted(maps.Keys(e.allowedCategoryIDs)), e.categoryScopeSet})
	if err != nil {
		panic(fmt.Sprintf("marshal string-only scope policy: %v", err))
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Missing/cyclic/cross-guild ancestry never establishes an allowed decision.
func (e channelExclusions) scope(id string, catalog map[string]store.ChannelScope) string {
	original, ok := catalog[id]
	if e.excludesID(id) {
		return "excluded"
	}
	if !ok {
		return "unknown"
	}
	seen := map[string]bool{}
	for c := original; ; {
		if c.DeletedAt != "" || e.excludesID(c.ID) || e.excludesID(c.ParentID) || e.excludesKind(c.Kind) {
			return "excluded"
		}
		if seen[c.ID] || c.GuildID == "" || c.GuildID != original.GuildID {
			return "unknown"
		}
		seen[c.ID] = true
		if c.ParentID == "" {
			break
		}
		var found bool
		c, found = catalog[c.ParentID]
		if !found {
			return "unknown"
		}
	}
	rows := make(map[string]store.ChannelRow, len(seen))
	for id := range seen {
		rows[id] = catalog[id].ChannelRow
	}
	if e.excludesStoredChannel(original.ChannelRow, rows) {
		return "excluded"
	}
	return "allowed"
}

func refreshCollectionScopes(ctx context.Context, s *store.Store, e channelExclusions) (map[string]store.ChannelScope, error) {
	rows, err := s.ScopeChannels(ctx)
	if err != nil {
		return nil, err
	}
	catalog := make(map[string]store.ChannelScope, len(rows))
	for _, r := range rows {
		catalog[r.ID] = r
	}
	policy := e.policyID()
	changes := map[string]store.ScopeDecision{}
	for _, r := range rows {
		state := e.scope(r.ID, catalog)
		if r.CollectionScope != state || r.Policy != policy {
			changes[r.ID] = store.ScopeDecision{State: state, Revision: r.Revision}
		}
	}
	if len(changes) > 0 {
		if err := s.SetChannelScopes(ctx, changes, policy); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

func (t *tailHandler) refreshScope(ctx context.Context) error {
	t.exclusionMu.Lock()
	defer t.exclusionMu.Unlock()
	catalog, err := refreshCollectionScopes(ctx, t.store, t.exclusions)
	if err != nil {
		return err
	}
	t.scopeCatalog = catalog
	t.channelScopeCatalog = make(map[string]store.ChannelRow, len(catalog))
	for id, c := range catalog {
		t.channelScopeCatalog[id] = c.ChannelRow
	}
	return nil
}

func (t *tailHandler) scopeDecision(id string) string {
	t.exclusionMu.RLock()
	defer t.exclusionMu.RUnlock()
	return t.exclusions.scope(id, t.scopeCatalog)
}

func (t *tailHandler) allowMessageChannel(ctx context.Context, guild, id string) (bool, error) {
	if !t.exclusions.configured() {
		return !t.excludeChannel(id), nil
	}
	if t.exclusions.excludesID(id) {
		return false, nil
	}
	if t.scopeDecision(id) == "unknown" {
		if err := t.refreshScope(ctx); err != nil {
			return false, err
		}
	}
	if state := t.scopeDecision(id); state != "unknown" {
		return state == "allowed", nil
	}
	if t.client == nil {
		return false, errors.New("channel ancestry unavailable")
	}
	seen := map[string]bool{}
	for next := id; next != ""; {
		if seen[next] || len(seen) >= 16 {
			return false, errors.New("invalid channel ancestry")
		}
		seen[next] = true
		// An explicitly excluded ancestor is sufficient; never fetch its content.
		if t.exclusions.excludesID(next) {
			break
		}
		started := time.Now()
		c, err := t.client.Channel(ctx, next)
		if err != nil {
			return false, fmt.Errorf("resolve channel scope: %w", err)
		}
		if c == nil || c.ID != next || c.GuildID == "" || (guild != "" && c.GuildID != guild) || !t.allowGuild(c.GuildID) {
			return false, errors.New("channel scope identity mismatch")
		}
		if err = t.store.UpsertObservedChannel(ctx, toChannelRecord(c, marshalJSONString(c, "{}")), started); err != nil {
			return false, err
		}
		next = c.ParentID
	}
	if err := t.refreshScope(ctx); err != nil {
		return false, err
	}
	state := t.scopeDecision(id)
	if state == "unknown" {
		return false, errors.New("channel ancestry remains unknown")
	}
	return state == "allowed", nil
}

func (t *tailHandler) scopePolicy() string {
	if !t.exclusions.configured() {
		return ""
	}
	return t.exclusions.policyID()
}

func (t *tailHandler) seedScopeMetadata(ctx context.Context) error {
	// Old excluded descendants may only be retained in a full GuildCreate
	// payload. Import metadata for missing identities; never resurrect a
	// tombstone or overwrite newer canonical channel metadata on restart.
	rows, err := t.store.ScopeChannels(ctx)
	if err != nil {
		return err
	}
	known := map[string]bool{}
	for _, r := range rows {
		known[r.ID] = true
	}
	guilds, err := t.store.GuildScopePayloads(ctx)
	if err != nil {
		return err
	}
	for _, record := range guilds {
		if !t.allowGuild(record.ID) {
			continue
		}
		var g discordgo.Guild
		if json.Unmarshal([]byte(record.RawJSON), &g) != nil {
			continue
		}
		for _, c := range append(g.Channels, g.Threads...) {
			if c == nil || c.ID == "" || known[c.ID] {
				continue
			}
			if c.GuildID != "" && c.GuildID != record.ID {
				continue
			}
			c.GuildID = record.ID
			if err := t.store.UpsertChannel(ctx, toChannelRecord(c, marshalJSONString(c, "{}"))); err != nil {
				return err
			}
			known[c.ID] = true
		}
	}
	return t.refreshScope(ctx)
}

func (s *Syncer) storeScopeMetadata(ctx context.Context, guildID string, channels []*discordgo.Channel, e channelExclusions, started time.Time) error {
	for i, c := range channels {
		if c == nil || c.ID == "" {
			continue
		}
		if c.GuildID != "" && c.GuildID != guildID {
			return errors.New("channel catalog guild mismatch")
		}
		observed := *c
		observed.GuildID = guildID
		c = &observed
		channels[i] = c
		if err := s.store.UpsertObservedChannel(ctx, toChannelRecord(c, marshalJSONString(c, "{}")), started); err != nil {
			return err
		}
	}
	_, err := refreshCollectionScopes(ctx, s.store, e)
	return err
}
