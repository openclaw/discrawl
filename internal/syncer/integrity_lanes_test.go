package syncer

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestRoleAndChannelDeletionRetainEvidenceAndCloseCollectionScope(t *testing.T) {
	t.Parallel()
	s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	h := &tailHandler{store: s, exclusions: newChannelScope([]string{"blocked"}, nil, nil)}
	g := &discordgo.Guild{ID: "g", Roles: []*discordgo.Role{{ID: "g", Permissions: 1024}}, Channels: []*discordgo.Channel{{ID: "parent", Type: discordgo.ChannelTypeGuildCategory}, {ID: "c", ParentID: "parent", Type: discordgo.ChannelTypeGuildText}}}
	require.NoError(t, h.OnGuildUpsert(t.Context(), g))
	require.NoError(t, h.OnMessageCreate(t.Context(), &discordgo.Message{ID: "10", GuildID: "g", ChannelID: "c", Content: "retained evidence", Timestamp: time.Now()}))
	require.NoError(t, h.OnGuildRoleUpsert(t.Context(), &discordgo.GuildRole{GuildID: "g", Role: &discordgo.Role{ID: "g", Permissions: 0}}))
	var raw string
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select raw_json from guilds where id='g'`).Scan(&raw))
	var updated discordgo.Guild
	require.NoError(t, json.Unmarshal([]byte(raw), &updated))
	require.Zero(t, updated.Roles[0].Permissions)
	require.NoError(t, h.OnGuildRoleUpsert(t.Context(), &discordgo.GuildRole{GuildID: "g", Role: &discordgo.Role{ID: "r", Permissions: 1024}}))
	require.NoError(t, h.OnGuildRoleDelete(t.Context(), "g", "r"))
	require.NoError(t, h.OnChannelDelete(t.Context(), &discordgo.Channel{ID: "parent", GuildID: "g"}))
	var scope, content string
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select collection_scope from channels where id='c'`).Scan(&scope))
	require.Equal(t, "excluded", scope)
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select content from messages where id='10'`).Scan(&content))
	require.Equal(t, "retained evidence", content)
	// A real deletion of an uncatalogued channel must survive restart seeding.
	require.NoError(t, h.OnChannelDelete(t.Context(), &discordgo.Channel{ID: "missing", GuildID: "g"}))
	require.NoError(t, h.seedChannelExclusions(t.Context()))
	require.Equal(t, "excluded", h.scopeDecision("missing"))
}

func TestOrdinaryRepairReconcilesExactFailuresAndRecordsSnapshot(t *testing.T) {
	t.Parallel()
	s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	m := &discordgo.Message{ID: "50", GuildID: "g", ChannelID: "c", Content: "provider current content", Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Author: &discordgo.User{ID: "u"}}
	client := &fakeClient{guilds: []*discordgo.UserGuild{{ID: "g"}}, guildByID: map[string]*discordgo.Guild{"g": {ID: "g"}}, channels: map[string][]*discordgo.Channel{"g": {{ID: "c", GuildID: "g", Type: discordgo.ChannelTypeGuildText}}}, messages: map[string][]*discordgo.Message{"c": {m}}}
	require.NoError(t, s.RecordFailure(t.Context(), store.FailureRef{Operation: "tail_message", Source: "discord", GuildID: "g", ChannelID: "c", MessageID: "50", RelatedKind: "message_event", RelatedID: "update"}, errors.New("fixture failed update")))
	svc := New(client, s, nil)
	_, err = svc.runTailRepair(t.Context(), tailRepairSyncOptions([]string{"g"}, false))
	require.NoError(t, err)
	require.Positive(t, client.exactMessageCalls)
	var n int
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from failure_ledger where resolved_at is null`).Scan(&n))
	require.Zero(t, n)
	var event string
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select event_type from message_events where message_id='50'`).Scan(&event))
	require.Equal(t, "snapshot", event)
	var created string
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select created_at from messages where id='50'`).Scan(&created))
	require.Equal(t, "2026-01-01T00:00:00.000000000Z", created)
	// A failing broad channel read must not starve independent exact recovery.
	client.messageErrors = map[string]error{"c": errors.New("fixture provider unavailable")}
	require.NoError(t, s.RecordFailure(t.Context(), store.FailureRef{Operation: "tail_message", Source: "discord", GuildID: "g", ChannelID: "c", MessageID: "50", RelatedKind: "message_event", RelatedID: "update"}, errors.New("fixture second update")))
	_, _ = svc.runTailRepair(t.Context(), tailRepairSyncOptions([]string{"g"}, false))
	require.GreaterOrEqual(t, client.exactMessageCalls, 2)
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from failure_ledger where operation='tail_message' and resolved_at is null`).Scan(&n))
	require.Zero(t, n)
}

func TestVoiceAndStageRepairUsesExistingCursor(t *testing.T) {
	t.Parallel()
	for _, kind := range []discordgo.ChannelType{discordgo.ChannelTypeGuildVoice, discordgo.ChannelTypeGuildStageVoice} {
		require.True(t, isMessageChannel(&discordgo.Channel{Type: kind}))
		require.Equal(t, kind, channelTypeFromKind(channelKind(&discordgo.Channel{Type: kind})))
	}
}
