package share

import (
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPublicPermissionCannotOverrideUnknownOrExcludedCollectionAncestor(t *testing.T) {
	t.Parallel()
	s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertGuild(t.Context(), store.GuildRecord{ID: "g", RawJSON: `{"roles":[{"id":"g","permissions":"1024"}]}`}))
	for _, c := range []store.ChannelRecord{{ID: "parent", GuildID: "g", Kind: "category", RawJSON: `{"permission_overwrites":[]}`}, {ID: "child", GuildID: "g", ParentID: "parent", Kind: "text", RawJSON: `{"permission_overwrites":[{"id":"g","type":0,"allow":"1024"}]}`}} {
		require.NoError(t, s.UpsertChannel(t.Context(), c))
	}
	for _, state := range []string{"unknown", "excluded", "allowed"} {
		_, err = s.DB().ExecContext(t.Context(), `update channels set collection_scope=case when id='parent' then ? else 'allowed' end`, state)
		require.NoError(t, err)
		f, err := newSnapshotFilter(t.Context(), s.DB(), FilterOptions{PublicOnly: true})
		require.NoError(t, err)
		require.Equal(t, state == "allowed", f.publicChannel("child"), state)
	}
}
