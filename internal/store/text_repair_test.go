package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

func TestTextRepairResumesAndReusesUnchangedVectorsWithoutChangingEvidence(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "archive.db")
	s, err := Open(ctx, path)
	require.NoError(t, err)
	for _, id := range []string{"c", "excluded"} {
		require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: id, GuildID: "g", Kind: "text", RawJSON: `{}`}))
	}
	_, err = s.DB().ExecContext(ctx, `update channels set collection_scope=case when id='c' then 'allowed' else 'excluded' end,scope_policy='p'`)
	require.NoError(t, err)
	for i, text := range []string{"alpha\nbeta", "same", "excluded\ntext"} {
		id := string(rune('1' + i))
		channel := "c"
		if i == 2 {
			channel = "excluded"
		}
		m := &discordgo.Message{ID: id, GuildID: "g", ChannelID: channel, Content: text, Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
		raw, err := json.Marshal(m)
		require.NoError(t, err)
		normalized := text
		if i == 0 {
			normalized = "alphabeta"
		}
		require.NoError(t, s.UpsertMessage(ctx, MessageRecord{ID: id, GuildID: "g", ChannelID: channel, Content: text, NormalizedContent: normalized, RawJSON: string(raw), CreatedAt: "2026-01-01T00:00:00Z"}))
		_, err = s.DB().ExecContext(ctx, `insert into message_embeddings(message_id,provider,model,input_version,dimensions,embedding_blob,embedded_at)values(?,'fixture','model','message_normalized_v1',1,x'0000803f','2026-01-01T00:00:01Z')`, id)
		require.NoError(t, err)
	}
	p, err := s.RepairMessageTextBatch(ctx, "p", 1, true)
	require.NoError(t, err)
	require.Equal(t, "1", p.LastID)
	require.Equal(t, 1, p.Changed)
	var n int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_embedding_history where message_id='1'`).Scan(&n))
	require.Equal(t, 1, n)
	require.NoError(t, s.Close())
	s, err = Open(ctx, path)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = s.RepairMessageTextBatch(canceled, "p", 1, true)
	require.Error(t, err)
	p, err = s.RepairMessageTextBatch(ctx, "p", 2, true)
	require.NoError(t, err)
	require.True(t, p.Complete)
	require.Equal(t, 1, p.Reused)
	require.Equal(t, 1, p.SkippedScope)
	var content, normalized, created, edited string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select content,normalized_content,created_at,coalesce(edited_at,'') from messages where id='1'`).Scan(&content, &normalized, &created, &edited))
	require.Equal(t, "alpha\nbeta", content)
	require.Equal(t, "alpha beta", normalized)
	require.Empty(t, edited)
	require.Equal(t, "2026-01-01T00:00:00.000000000Z", created)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from embedding_jobs`).Scan(&n))
	require.Equal(t, 1, n, "only changed eligible text is queued")
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_embeddings where message_id in ('2','3')`).Scan(&n))
	require.Equal(t, 2, n, "unchanged and excluded vectors are untouched")
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_events`).Scan(&n))
	require.Zero(t, n, "local reprocessing is not a provider event")
	p2, err := s.RepairMessageTextBatch(ctx, "p", 2, true)
	require.NoError(t, err)
	require.Equal(t, p, p2)
}
