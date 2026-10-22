package store

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A GUILD_CREATE catalog must commit as one transaction: per-channel durable
// commits exceeded the ordered Gateway handler deadline on large guilds and
// left a partially applied catalog when the deadline cancelled the handler.
func TestUpsertChannelsCommitsCatalogAtomically(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	_, err = s.DB().ExecContext(ctx, `create temp trigger reject_boom before insert on channels when new.id='boom' begin select raise(abort,'boom'); end`)
	require.NoError(t, err)

	records := make([]ChannelRecord, 0, 501)
	for i := range 500 {
		records = append(records, ChannelRecord{ID: fmt.Sprintf("t%d", i), GuildID: "g", ParentID: "c", Kind: "thread_public", RawJSON: fmt.Sprintf(`{"id":"t%d"}`, i)})
	}
	require.Error(t, s.UpsertChannels(ctx, append(records, ChannelRecord{ID: "boom", GuildID: "g", Kind: "text", RawJSON: `{"id":"boom"}`})))
	var count int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from channels`).Scan(&count))
	require.Zero(t, count, "a failed catalog batch must not leave earlier channels applied")

	require.NoError(t, s.UpsertChannels(ctx, records))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from channels`).Scan(&count))
	require.Equal(t, 500, count)
}

func TestUpsertChannelsKeepsSingleChannelSemantics(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	for _, r := range []ChannelRecord{
		{ID: "a", GuildID: "g", Kind: "category", RawJSON: `{"id":"a"}`},
		{ID: "x", GuildID: "g", Kind: "category", RawJSON: `{"id":"x"}`},
		{ID: "c", GuildID: "g", ParentID: "a", Kind: "text", RawJSON: `{"id":"c","parent_id":"a"}`},
		{ID: "t", GuildID: "g", ParentID: "c", Kind: "thread_public", RawJSON: `{"id":"t","parent_id":"c"}`},
		{ID: "gone", GuildID: "g", ParentID: "a", Kind: "text", RawJSON: `{"id":"gone"}`},
	} {
		require.NoError(t, s.UpsertChannel(ctx, r))
	}
	rows, err := s.ScopeChannels(ctx)
	require.NoError(t, err)
	decisions := map[string]ScopeDecision{}
	for _, r := range rows {
		decisions[r.ID] = ScopeDecision{State: "allowed", Revision: r.Revision}
	}
	require.NoError(t, s.SetChannelScopes(ctx, decisions, "policy"))
	require.NoError(t, s.MarkChannelDeleted(ctx, "g", "gone", "gateway"))
	unchangedRevision, err := s.ChannelScopeRevision(ctx, "a")
	require.NoError(t, err)

	require.NoError(t, s.UpsertChannels(ctx, []ChannelRecord{
		{ID: "a", GuildID: "g", Kind: "category", RawJSON: `{"id":"a"}`},
		{ID: "c", GuildID: "g", ParentID: "x", Kind: "text", RawJSON: `{"id":"c","parent_id":"x"}`},
		{ID: "gone", GuildID: "g", ParentID: "a", Kind: "text", RawJSON: `{"id":"gone","name":"back"}`},
	}))

	revision, err := s.ChannelScopeRevision(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, unchangedRevision, revision, "identical metadata is not rewritten")
	rows, err = s.ScopeChannels(ctx)
	require.NoError(t, err)
	byID := map[string]ChannelScope{}
	for _, r := range rows {
		byID[r.ID] = r
	}
	require.Equal(t, "x", byID["c"].ParentID)
	require.Equal(t, "unknown", byID["c"].CollectionScope, "ancestry change invalidates the moved channel")
	require.Equal(t, "unknown", byID["t"].CollectionScope, "ancestry change invalidates descendants in the same batch")
	require.Empty(t, byID["gone"].DeletedAt, "an authoritative catalog observation clears a tombstone as UpsertChannel does")
}
