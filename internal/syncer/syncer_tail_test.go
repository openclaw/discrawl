package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	discordclient "github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/store"
)

func TestNormalizeMessageIncludesRichFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	message := &discordgo.Message{
		Content: "base",
		Attachments: []*discordgo.MessageAttachment{
			{Filename: "trace.txt"},
		},
		Embeds: []*discordgo.MessageEmbed{
			{Title: "title", Description: "desc"},
		},
		ReferencedMessage: &discordgo.Message{Content: "prior"},
		Poll: &discordgo.Poll{
			Question: discordgo.PollMedia{Text: "question"},
			Answers: []discordgo.PollAnswer{
				{Media: &discordgo.PollMedia{Text: "answer"}},
			},
		},
		Timestamp: now,
	}
	content := normalizeMessage(message)
	require.Contains(t, content, "base")
	require.Contains(t, content, "trace.txt")
	require.Contains(t, content, "title")
	require.Contains(t, content, "reply:prior")
	require.Contains(t, content, "question")
	require.Contains(t, content, "answer")
}

func TestNormalizeMessageSanitizesMalformedUnicodeAndWhitespace(t *testing.T) {
	t.Parallel()

	message := &discordgo.Message{
		Content: string([]byte{'h', 'i', 0xff, ' ', 't', 'h', 'e', 'r', 'e'}) + "\u200b",
		Attachments: []*discordgo.MessageAttachment{
			{Filename: "Ｆｏｏ\u200d.txt"},
		},
		Embeds: []*discordgo.MessageEmbed{
			{Title: " spaced\u00a0out ", Description: "line\u0000break"},
		},
		ReferencedMessage: &discordgo.Message{Content: "prior reply"},
	}

	content := normalizeMessage(message)
	require.Equal(t, "hi there\nFoo.txt\nspaced out\nlinebreak\nreply:prior reply", content)
	require.NotContains(t, content, "\u200b")
	require.NotContains(t, content, "\u200d")
}

func TestTailHandlerWritesEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	handler := &tailHandler{store: s}
	msg := &discordgo.Message{
		ID:        "9",
		GuildID:   "g1",
		ChannelID: "c1",
		Content:   "tail event",
		Timestamp: time.Now().UTC(),
		Author:    &discordgo.User{ID: "u1", Username: "peter"},
	}
	require.NoError(t, handler.OnMessageCreate(ctx, msg))
	require.NoError(t, handler.OnMessageUpdate(ctx, msg))
	require.NoError(t, handler.OnMessageDelete(ctx, &discordgo.MessageDelete{Message: &discordgo.Message{
		ID:        "9",
		GuildID:   "g1",
		ChannelID: "c1",
	}}))
	require.NoError(t, handler.OnChannelUpsert(ctx, &discordgo.Channel{
		ID:      "c1",
		GuildID: "g1",
		Name:    "general",
		Type:    discordgo.ChannelTypeGuildText,
	}))
	require.NoError(t, handler.OnMemberUpsert(ctx, "g1", &discordgo.Member{
		GuildID: "g1",
		Nick:    "Peter",
		User:    &discordgo.User{ID: "u1", Username: "peter"},
	}))
	require.NoError(t, handler.OnMemberDelete(ctx, "g1", "u1"))

	status, err := s.Status(context.Background(), "db", "")
	require.NoError(t, err)
	require.Equal(t, 1, status.ChannelCount)
	require.Equal(t, 1, status.MessageCount)

	cursor, err := s.GetSyncState(context.Background(), "channel:c1:latest_message_id")
	require.NoError(t, err)
	require.Equal(t, "9", cursor)

	older := *msg
	older.ID = "8"
	require.NoError(t, handler.OnMessageCreate(ctx, &older))
	cursor, err = s.GetSyncState(context.Background(), "channel:c1:latest_message_id")
	require.NoError(t, err)
	require.Equal(t, "9", cursor)

	newer := *msg
	newer.ID = "10"
	require.NoError(t, handler.OnMessageCreate(ctx, &newer))
	cursor, err = s.GetSyncState(context.Background(), "channel:c1:latest_message_id")
	require.NoError(t, err)
	require.Equal(t, "10", cursor)
}

func TestHelpers(t *testing.T) {
	t.Parallel()

	require.Equal(t, "101", maxSnowflake("100", "101"))
	require.Equal(t, "abc", maxSnowflake("", "abc"))
	require.Equal(t, "abc", maxSnowflake("abc", ""))
	require.Equal(t, "zzz", maxSnowflake("abc", "zzz"))
	require.Equal(t, "text", channelKind(&discordgo.Channel{Type: discordgo.ChannelTypeGuildText}))
	require.Equal(t, "category", channelKind(&discordgo.Channel{Type: discordgo.ChannelTypeGuildCategory}))
	require.Equal(t, "thread_private", channelKind(&discordgo.Channel{Type: discordgo.ChannelTypeGuildPrivateThread}))
	require.Equal(t, "thread_public", channelKind(&discordgo.Channel{Type: discordgo.ChannelTypeGuildPublicThread}))
	require.Equal(t, "thread_announcement", channelKind(&discordgo.Channel{Type: discordgo.ChannelTypeGuildNewsThread}))
	require.Equal(t, "announcement", channelKind(&discordgo.Channel{Type: discordgo.ChannelTypeGuildNews}))
	require.Equal(t, "forum", channelKind(&discordgo.Channel{Type: discordgo.ChannelTypeGuildForum}))
	require.Equal(t, "voice", channelKind(&discordgo.Channel{Type: discordgo.ChannelTypeGuildVoice}))
	require.Equal(t, "type_99", channelKind(&discordgo.Channel{Type: discordgo.ChannelType(99)}))
	require.Equal(t, discordgo.ChannelTypeGuildCategory, channelTypeFromKind("category"))
	require.Equal(t, discordgo.ChannelTypeGuildNews, channelTypeFromKind("announcement"))
	require.Equal(t, discordgo.ChannelTypeGuildForum, channelTypeFromKind("forum"))
	require.Equal(t, discordgo.ChannelTypeGuildPublicThread, channelTypeFromKind("thread_public"))
	require.Equal(t, discordgo.ChannelTypeGuildPrivateThread, channelTypeFromKind("thread_private"))
	require.Equal(t, discordgo.ChannelTypeGuildNewsThread, channelTypeFromKind("thread_announcement"))
	require.Equal(t, discordgo.ChannelTypeGuildVoice, channelTypeFromKind("voice"))
	require.Equal(t, discordgo.ChannelTypeGuildText, channelTypeFromKind("unknown"))
	require.True(t, isThreadParent(&discordgo.Channel{Type: discordgo.ChannelTypeGuildForum}))
	require.True(t, isThreadParent(&discordgo.Channel{Type: discordgo.ChannelTypeGuildText}))
	require.False(t, isThreadParent(&discordgo.Channel{Type: discordgo.ChannelTypeGuildVoice}))
	require.True(t, isMessageChannel(&discordgo.Channel{Type: discordgo.ChannelTypeGuildNewsThread}))
	require.True(t, isMessageChannel(&discordgo.Channel{Type: discordgo.ChannelTypeGuildPrivateThread}))
	require.False(t, isMessageChannel(&discordgo.Channel{Type: discordgo.ChannelTypeGuildCategory}))
	require.Len(t, selectGuilds([]*discordgo.UserGuild{{ID: "g1"}, {ID: "g2"}}, []string{"g2"}), 1)
	require.Len(t, selectGuilds([]*discordgo.UserGuild{{ID: "g1"}}, nil), 1)
	require.Nil(t, makeGuildSet(nil))

	record := toChannelRecord(&discordgo.Channel{
		ID:       "t1",
		GuildID:  "g1",
		ParentID: "c1",
		Name:     "thread",
		Type:     discordgo.ChannelTypeGuildPrivateThread,
		ThreadMetadata: &discordgo.ThreadMetadata{
			Archived:         true,
			Locked:           true,
			ArchiveTimestamp: time.Now().UTC(),
		},
	}, `{}`)
	require.True(t, record.IsArchived)
	require.True(t, record.IsLocked)
	require.True(t, record.IsPrivateThread)

	sorted := mapsToSlice(map[string]*discordgo.Channel{
		"b": {ID: "b", Position: 2},
		"a": {ID: "a", Position: 1},
		"c": {ID: "c", Position: 1},
	})
	require.Equal(t, []string{"a", "c", "b"}, []string{sorted[0].ID, sorted[1].ID, sorted[2].ID})

	selected := mapsToSlice(selectStoredChannels([]store.ChannelRow{
		{ID: "c2", GuildID: "g1", Kind: "thread_private", Name: "thread", Position: 2, IsArchived: true, IsLocked: true, ArchiveTimestamp: time.Unix(10, 0).UTC()},
		{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", Position: 1},
	}, makeGuildSet([]string{"c2", "c1"})))
	require.Len(t, selected, 2)
	require.Equal(t, "c1", selected[0].ID)
	require.Nil(t, selected[0].ThreadMetadata)
	require.Equal(t, "c2", selected[1].ID)
	require.NotNil(t, selected[1].ThreadMetadata)
	require.True(t, selected[1].ThreadMetadata.Archived)

	handler := &tailHandler{guilds: makeGuildSet([]string{"g1"})}
	require.True(t, handler.allowGuild("g1"))
	require.False(t, handler.allowGuild("g2"))
	require.Empty(t, displayName(nil))
	require.Equal(t, "Nick", displayName(&discordgo.Member{Nick: "Nick", User: &discordgo.User{Username: "user"}}))
	require.Equal(t, "Global", displayName(&discordgo.Member{User: &discordgo.User{GlobalName: "Global", Username: "user"}}))
	require.Equal(t, "user", displayName(&discordgo.Member{User: &discordgo.User{Username: "user"}}))
	require.True(t, isMissingAccess(errors.New("HTTP 403 Forbidden")))
	require.True(t, isMissingAccess(errors.New("Missing Access")))
	require.False(t, isMissingAccess(errors.New("boom")))
	require.Equal(t, "missing_access", unavailableReason(errors.New("HTTP 403 Forbidden")))
	require.Equal(t, "unknown_channel", unavailableReason(errors.New("HTTP 404 Not Found, {\"message\": \"Unknown Channel\", \"code\": 10003}")))
	require.True(t, isUnknownChannel(errors.New("Unknown Channel")))
	require.False(t, isUnknownChannel(errors.New("boom")))
	require.True(t, isRetryableSyncError(context.Background(), context.DeadlineExceeded))
	require.True(t, isRetryableSyncError(context.Background(), errors.New("HTTP 503 Service Unavailable")))
	require.True(t, isRetryableSyncError(context.Background(), errors.New("stream error: stream ID 1; INTERNAL_ERROR")))
	require.False(t, isRetryableSyncError(context.Background(), context.Canceled))
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, isRetryableSyncError(canceledCtx, context.DeadlineExceeded))
}

func TestRunTail(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	handled := make(chan struct{}, 1)
	client := &fakeClient{tailHandled: handled}
	svc := New(client, s, nil)
	go func() {
		select {
		case <-handled:
		case <-time.After(time.Second):
		}
		cancel()
	}()
	err = svc.RunTail(ctx, nil, 0)
	require.True(t, err == nil || errors.Is(err, context.Canceled))

	status, err := s.Status(context.Background(), "db", "")
	require.NoError(t, err)
	require.Equal(t, 1, status.MessageCount)
	require.Equal(t, 1, client.tailCalls)
}

func TestRunTailWithRepairLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText}},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{ID: "10", GuildID: "g1", ChannelID: "c1", Content: "repair", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "u1", Username: "user"}}},
		},
	}
	svc := New(client, s, nil)
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()
	err = svc.RunTail(ctx, []string{"g1"}, 10*time.Millisecond)
	require.True(t, err == nil || errors.Is(err, context.Canceled))

	status, err := s.Status(context.Background(), "db", "")
	require.NoError(t, err)
	require.GreaterOrEqual(t, status.MessageCount, 1)
}

func TestRunTailWithRepairLoopJoinsTailOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &joiningTailClient{
		started:  make(chan struct{}),
		finished: make(chan struct{}),
		closed:   make(chan struct{}),
	}
	svc := New(client, s, nil)
	done := make(chan error, 1)
	go func() {
		done <- svc.RunTail(ctx, nil, time.Hour)
	}()

	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("tail did not start")
	}
	cancel()
	require.NoError(t, <-done)
	select {
	case <-client.finished:
	default:
		t.Fatal("RunTail returned before client.Tail finished")
	}
	select {
	case <-client.closed:
	default:
		t.Fatal("RunTail returned before closing tail client")
	}
}

func TestTailReadyCallback(t *testing.T) {
	t.Parallel()

	called := false
	svc := New(&fakeClient{}, nil, nil)
	svc.SetTailReadyCallback(func(context.Context) error {
		called = true
		return nil
	})
	handler := &tailHandler{onReady: svc.tailReady}
	require.NoError(t, handler.OnTailReady(context.Background()))
	require.True(t, called)

	handler.onReady = nil
	require.NoError(t, handler.OnTailReady(context.Background()))
}

type joiningTailClient struct {
	fakeClient
	started  chan struct{}
	finished chan struct{}
	closed   chan struct{}
}

func (c *joiningTailClient) Tail(ctx context.Context, _ discordclient.EventHandler) error {
	close(c.started)
	<-ctx.Done()
	close(c.finished)
	return nil
}

func (c *joiningTailClient) Close() error {
	close(c.closed)
	return nil
}
