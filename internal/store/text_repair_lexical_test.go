package store

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

func TestTextRepairPreservesLexicalAuthorAndChannelNames(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := OpenWithOptions(ctx, filepath.Join(t.TempDir(), "archive.db"), OpenOptions{LexicalLanguages: []string{"ar"}})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c", GuildID: "g", Kind: "text", Name: "maktaba", RawJSON: `{}`}))
	_, err = s.DB().ExecContext(ctx, `update channels set collection_scope='allowed',scope_policy='p'`)
	require.NoError(t, err)
	m := &discordgo.Message{ID: "1", GuildID: "g", ChannelID: "c", Content: "والكتاب مفيد\nللطلاب", Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Author: &discordgo.User{ID: "u", Username: "alice"}}
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	// A stale normalization forces repair to rewrite the lexical rows.
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{ID: "1", GuildID: "g", ChannelID: "c", AuthorID: "u", AuthorName: "alice", ChannelName: "maktaba", Content: m.Content, NormalizedContent: "stale", RawJSON: string(raw), CreatedAt: "2026-01-01T00:00:00Z"}))
	p, err := s.RepairMessageTextBatch(ctx, "p", 10, false)
	require.NoError(t, err)
	require.Equal(t, 1, p.Changed)
	var author, channel string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select author_name,channel_name from `+lexicalFTSTable("ar")+` where message_id='1'`).Scan(&author, &channel))
	require.Equal(t, "alice", author)
	require.Equal(t, "maktaba", channel)
	for _, opts := range []SearchOptions{{Query: "كتاب", Author: "alice", Limit: 5}, {Query: "كتاب", Channel: "maktaba", Limit: 5}} {
		results, err := s.SearchMessages(ctx, opts)
		require.NoError(t, err)
		require.Contains(t, searchResultIDs(results), "1", "repaired stemming match keeps name filters: %+v", opts)
	}
}
