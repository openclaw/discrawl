package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Forum posts store applied tag IDs; the parent forum stores their names.
func TestAttachChannelTagsResolvesAppliedTagNames(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t, ctx)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{
		ID: "f1", GuildID: "g1", Kind: "forum", Name: "support",
		RawJSON: `{"id":"f1","type":15,"available_tags":[{"id":"300","name":"bug"},{"id":"301","name":"macOS"}]}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{
		ID: "t1", GuildID: "g1", ParentID: "f1", ThreadParentID: "f1", Kind: "thread_public", Name: "crash on wake",
		RawJSON: `{"id":"t1","type":11,"parent_id":"f1","applied_tags":["301","300","399"]}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{
		ID: "t2", GuildID: "g1", ParentID: "f1", ThreadParentID: "f1", Kind: "thread_public", Name: "untagged",
		RawJSON: `{"id":"t2","type":11,"parent_id":"f1","applied_tags":[]}`,
	}))

	rows, err := s.Channels(ctx, "g1")
	require.NoError(t, err)
	require.NoError(t, s.AttachChannelTags(ctx, rows))
	tags := map[string][]string{}
	for _, row := range rows {
		tags[row.ID] = row.Tags
	}
	require.Equal(t, []string{"macOS", "bug", "399"}, tags["t1"], "names in applied order; unknown IDs kept")
	require.Nil(t, tags["t2"])
	require.Nil(t, tags["f1"])
}
