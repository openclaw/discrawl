package store

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdatedCursorIndexAddedToExistingSchemaWithoutLosingData(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, path)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessageWithOptions(ctx, MessageRecord{
		ID: "existing", GuildID: "g1", ChannelID: "c1", CreatedAt: "2026-09-01T00:00:00Z",
		Content: "preserve this message", NormalizedContent: "preserve this message", RawJSON: `{}`,
	}, WriteOptions{EnqueueEmbedding: true}))
	_, err = s.DB().ExecContext(ctx, `drop index idx_messages_channel_updated_id`)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	s, err = Open(ctx, path)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	var version int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version))
	require.Equal(t, storeSchemaVersion, version)
	require.Contains(t, indexNames(t, ctx, s.DB(), "messages"), "idx_messages_channel_updated_id")
	_, rows, err := s.ReadOnlyQuery(ctx, `select m.content,j.state from messages m join embedding_jobs j on j.message_id=m.id where m.id='existing'`)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"preserve this message", "pending"}}, rows)
}
