package syncer

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestBuildMessageMutationsTracksNewest(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	mutations, newest, err := buildMessageMutations(context.Background(), []*discordgo.Message{
		{
			ID:        "100",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "first",
			Timestamp: now,
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		},
		{
			ID:        "101",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "second",
			Timestamp: now,
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		},
	}, "general", "", true, true)

	require.NoError(t, err)
	require.Len(t, mutations, 2)
	require.Equal(t, "101", newest)
	require.Equal(t, "general", mutations[0].Record.ChannelName)
	require.True(t, mutations[0].Options.EnqueueEmbedding)
}

func TestFilterMessageChannelsHonorsRequestedIDs(t *testing.T) {
	t.Parallel()

	channels := []*discordgo.Channel{
		{ID: "c1", Type: discordgo.ChannelTypeGuildText},
		{ID: "c2", Type: discordgo.ChannelTypeGuildVoice},
		{ID: "t1", Type: discordgo.ChannelTypeGuildPublicThread},
	}

	filtered := filterMessageChannels(channels, []string{"t1"}, channelExclusions{})
	require.Len(t, filtered, 1)
	require.Equal(t, "t1", filtered[0].ID)
}

func TestFilterMessageChannelsHonorsCollectionExclusions(t *testing.T) {
	t.Parallel()

	channels := []*discordgo.Channel{
		{ID: "general", Type: discordgo.ChannelTypeGuildText},
		{ID: "logs", Type: discordgo.ChannelTypeGuildText},
		{ID: "news", Type: discordgo.ChannelTypeGuildNews},
		{ID: "news-thread", ParentID: "news", Type: discordgo.ChannelTypeGuildPublicThread},
	}
	exclusions := newChannelExclusions([]string{"logs"}, []string{"announcement"})

	filtered := filterMessageChannels(channels, nil, exclusions)
	require.Equal(t, []string{"general"}, channelIDs(filtered))

	filtered = filterMessageChannels(channels, []string{"logs", "news"}, exclusions)
	require.Empty(t, filtered)
}

func TestSeedChannelSyncStateUsesStoredBounds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "100",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "user",
		CreatedAt:         time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano),
		Content:           "older",
		NormalizedContent: "older",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "200",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "user",
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "newer",
		NormalizedContent: "newer",
		RawJSON:           `{}`,
	}))

	svc := New(&fakeClient{}, s, nil)
	state := channelSyncState{}
	require.NoError(t, svc.seedChannelSyncState(ctx, "c1", &state))
	require.Equal(t, "200", state.Latest)
	require.Equal(t, "100", state.BackfillCursor)
}

func TestSortChannelsOrdersByPositionThenID(t *testing.T) {
	t.Parallel()

	channels := []*discordgo.Channel{
		{ID: "b", Position: 2},
		{ID: "c", Position: 1},
		{ID: "a", Position: 1},
	}
	sortChannels(channels)
	require.Equal(t, []string{"a", "c", "b"}, []string{channels[0].ID, channels[1].ID, channels[2].ID})
}

func TestTailHandlerAllowGuild(t *testing.T) {
	t.Parallel()

	handler := &tailHandler{guilds: makeGuildSet([]string{"g1"})}
	require.True(t, handler.allowGuild("g1"))
	require.False(t, handler.allowGuild("g2"))
}

func TestTailHandlerIgnoresFilteredGuildAndIncompleteMember(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	handler := &tailHandler{guilds: makeGuildSet([]string{"g1"}), store: s}
	require.NoError(t, handler.OnMessageCreate(ctx, &discordgo.Message{ID: "m1", GuildID: "g2", ChannelID: "c1"}))
	require.NoError(t, handler.OnMessageUpdate(ctx, &discordgo.Message{ID: "m1", GuildID: "g2", ChannelID: "c1"}))
	require.NoError(t, handler.OnMessageDelete(ctx, &discordgo.MessageDelete{Message: &discordgo.Message{ID: "m1", GuildID: "g2", ChannelID: "c1"}}))
	require.NoError(t, handler.OnChannelUpsert(ctx, &discordgo.Channel{ID: "c1", GuildID: "g2"}))
	require.NoError(t, handler.OnMemberUpsert(ctx, "g1", nil))
	require.NoError(t, handler.OnMemberUpsert(ctx, "g1", &discordgo.Member{}))
	require.NoError(t, handler.OnMemberDelete(ctx, "g2", "u1"))

	status, err := s.Status(ctx, "db", "")
	require.NoError(t, err)
	require.Zero(t, status.MessageCount)
	require.Zero(t, status.ChannelCount)
	require.Zero(t, status.MemberCount)
}

func TestSyncerMemberRefreshAndCatalogDecisions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	svc := New(&fakeClient{}, s, nil)
	require.False(t, svc.shouldUseIncrementalFullCatalog(ctx, "g1"))
	require.True(t, svc.shouldRefreshMembers(ctx, "g1"))

	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.True(t, svc.shouldUseIncrementalFullCatalog(ctx, "g1"))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{}`,
	}))
	require.False(t, svc.shouldRefreshMembers(ctx, "g1"))
	require.False(t, svc.shouldRefreshMembers(ctx, "g1"))

	require.NoError(t, s.SetSyncState(ctx, guildMemberSyncSuccessScope("g1"), "bad-time"))
	require.True(t, svc.shouldRefreshMembers(ctx, "g1"))
	require.NoError(t, s.SetSyncState(ctx, guildMemberSyncSuccessScope("g1"), time.Now().UTC().Add(-2*defaultMemberRefreshInterval).Format(time.RFC3339Nano)))
	require.True(t, svc.shouldRefreshMembers(ctx, "g1"))
	svc.memberRefreshInterval = 0
	require.True(t, svc.shouldRefreshMembers(ctx, "g1"))

	require.False(t, (*Syncer)(nil).shouldUseIncrementalFullCatalog(ctx, "g1"))
	require.True(t, (*Syncer)(nil).shouldRefreshMembers(ctx, "g1"))
	require.Equal(t, "none", timeoutLabel(0))
	require.Equal(t, "1s", timeoutLabel(time.Second))
}
