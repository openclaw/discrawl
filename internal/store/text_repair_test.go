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

func TestTextRepairWithoutEmbeddingWorkerRetiresStaleVectorsAndLeases(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c", GuildID: "g", Kind: "text", RawJSON: `{}`}))
	_, err = s.DB().ExecContext(ctx, `update channels set collection_scope='allowed',scope_policy='p'`)
	require.NoError(t, err)
	raw, err := json.Marshal(&discordgo.Message{ID: "1", GuildID: "g", ChannelID: "c", Content: "alpha\nbeta"})
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{ID: "1", GuildID: "g", ChannelID: "c", Content: "alpha\nbeta", NormalizedContent: "alphabeta", RawJSON: string(raw)}))
	_, err = s.DB().ExecContext(ctx, `insert into message_embeddings(message_id,provider,model,input_version,dimensions,embedding_blob,embedded_at)values('1','fixture','model','v1',1,x'0000803f','2026-01-01T00:00:00Z');insert into embedding_jobs(message_id,state,updated_at,revision,lease_token)values('1','pending','2026-01-01T00:00:00Z',7,'stale-token')`)
	require.NoError(t, err)
	p, err := s.RepairMessageTextBatch(ctx, "p", 10, false)
	require.NoError(t, err)
	require.True(t, p.Complete)
	require.Equal(t, 1, p.Changed)
	var count, revision int
	var text, lease string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_embeddings`).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select normalized_content from message_embedding_history where message_id='1'`).Scan(&text))
	require.Equal(t, "alphabeta", text)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select revision,lease_token from embedding_jobs where message_id='1'`).Scan(&revision, &lease))
	require.Greater(t, revision, 7)
	require.Empty(t, lease)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from embedding_jobs`).Scan(&count))
	require.Equal(t, 1, count, "existing job retained without creating new work while disabled")
}
