package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
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
	require.NoError(t, handler.OnGuildUpsert(ctx, &discordgo.Guild{ID: "g1", Name: "Guild"}))
	require.NoError(t, handler.OnGuildDelete(ctx, &discordgo.Guild{ID: "g1", Unavailable: true}))
	var guildDeleted bool
	require.NoError(t, s.DB().QueryRowContext(ctx, `select deleted_at is not null from guilds where id = 'g1'`).Scan(&guildDeleted))
	require.False(t, guildDeleted, "temporary Discord unavailability is not a deletion")
	require.NoError(t, handler.OnGuildDelete(ctx, &discordgo.Guild{ID: "g1"}))
	var guildSource, guildReason string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select deleted_at is not null, deletion_source, deletion_reason from guilds where id = 'g1'`).Scan(&guildDeleted, &guildSource, &guildReason))
	require.True(t, guildDeleted)
	require.Equal(t, "discord-gateway", guildSource)
	require.Equal(t, "guild-delete-event", guildReason)
	require.NoError(t, handler.OnGuildUpsert(ctx, &discordgo.Guild{ID: "g1", Name: "Restored Guild"}))
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
	var memberDeleted bool
	var memberSource, memberReason string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select deleted_at is not null, deletion_source, deletion_reason from members where guild_id = 'g1' and user_id = 'u1'`).Scan(&memberDeleted, &memberSource, &memberReason))
	require.True(t, memberDeleted)
	require.Equal(t, "discord-gateway", memberSource)
	require.Equal(t, "member-remove-event", memberReason)

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

func TestTailHandlerMessageUpdateFetchesFullMessageBeforeUpsert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	createdAt := time.Now().UTC()
	original := &discordgo.Message{
		ID:        "9",
		GuildID:   "g1",
		ChannelID: "c1",
		Content:   "original <@u2>",
		Timestamp: createdAt,
		Author:    &discordgo.User{ID: "u1", Username: "peter"},
		Attachments: []*discordgo.MessageAttachment{{
			ID:          "a1",
			Filename:    "trace.txt",
			ContentType: "text/plain",
			Size:        42,
		}},
		Mentions: []*discordgo.User{{ID: "u2", Username: "shadow", GlobalName: "Shadow"}},
	}
	updated := *original
	updated.Content = "edited <@u2>"

	client := &fakeClient{
		messages: map[string][]*discordgo.Message{
			"c1": {&updated},
		},
	}
	handler := &tailHandler{store: s, client: client}

	require.NoError(t, handler.OnMessageCreate(ctx, original))
	require.NoError(t, handler.OnMessageUpdate(ctx, &discordgo.Message{
		ID:        "9",
		GuildID:   "g1",
		ChannelID: "c1",
		Content:   "edited <@u2>",
	}))

	messages, err := s.ListMessages(ctx, store.MessageListOptions{GuildIDs: []string{"g1"}, IncludeEmpty: true})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "edited <@u2>", messages[0].Content)
	require.Equal(t, "u1", messages[0].AuthorID)
	require.Equal(t, "peter", messages[0].AuthorName)
	require.Equal(t, createdAt.Format(time.RFC3339Nano), messages[0].CreatedAt.Format(time.RFC3339Nano))
	require.True(t, messages[0].HasAttachments)
	require.Equal(t, "trace.txt", messages[0].AttachmentNames)

	attachments, err := s.ListAttachments(ctx, store.AttachmentListOptions{MessageID: "9"})
	require.NoError(t, err)
	require.Len(t, attachments, 1)
	require.Equal(t, "a1", attachments[0].AttachmentID)
	require.Equal(t, "trace.txt", attachments[0].Filename)
	require.Equal(t, "u1", attachments[0].AuthorID)

	mentions, err := s.ListMentions(ctx, store.MentionListOptions{Target: "u2"})
	require.NoError(t, err)
	require.Len(t, mentions, 1)
	require.Equal(t, "u2", mentions[0].TargetID)
	require.Equal(t, "Shadow", mentions[0].TargetName)
}

func TestMessageUpdateSnapshotRejectsConflictingRefetchIdentity(t *testing.T) {
	t.Parallel()

	partial := &discordgo.Message{
		ID:        "m1",
		GuildID:   "g1",
		ChannelID: "c1",
	}
	tests := []struct {
		name    string
		full    *discordgo.Message
		wantErr string
	}{
		{
			name:    "message id",
			full:    &discordgo.Message{ID: "other", GuildID: "g1", ChannelID: "c1"},
			wantErr: "different message id",
		},
		{
			name:    "channel id",
			full:    &discordgo.Message{ID: "m1", GuildID: "g1", ChannelID: "other"},
			wantErr: "different channel id",
		},
		{
			name:    "guild id",
			full:    &discordgo.Message{ID: "m1", GuildID: "other", ChannelID: "c1"},
			wantErr: "different guild id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &tailHandler{client: &tailSnapshotClient{message: tt.full}}
			snapshot, err := handler.messageUpdateSnapshot(context.Background(), partial)
			require.Nil(t, snapshot)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestTailHandlerMessageUpdateFailureUsesSyncerRefetchedMetadata(t *testing.T) {
	tests := []struct {
		name      string
		wantKind  string
		wantStage discordclient.TailFailureStage
	}{
		{
			name:      "returned error",
			wantKind:  "returned_error",
			wantStage: discordclient.TailFailureStageCanonicalWrite,
		},
		{
			name:      "panic",
			wantKind:  "panic",
			wantStage: discordclient.TailFailureStageMessageBuild,
		},
		{
			name:      "cooperative timeout",
			wantKind:  "timeout",
			wantStage: discordclient.TailFailureStageCanonicalWrite,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var tailStore *store.Store
			var onFetch func(context.Context)
			switch tt.wantKind {
			case "returned_error":
				var err error
				tailStore, err = store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
				require.NoError(t, err)
				require.NoError(t, tailStore.Close())
			case "panic":
				var err error
				tailStore, err = store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
				require.NoError(t, err)
				t.Cleanup(func() { _ = tailStore.Close() })
			case "timeout":
				var err error
				tailStore, err = store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
				require.NoError(t, err)
				t.Cleanup(func() { _ = tailStore.Close() })
				tailStore.DB().SetMaxOpenConns(1)
				heldConn, err := tailStore.DB().Conn(ctx)
				require.NoError(t, err)
				var releaseOnce sync.Once
				release := func() {
					releaseOnce.Do(func() { _ = heldConn.Close() })
				}
				t.Cleanup(release)
				onFetch = func(taskCtx context.Context) {
					go func() {
						<-taskCtx.Done()
						time.Sleep(10 * time.Millisecond)
						release()
					}()
				}
			default:
				t.Fatalf("unsupported failure kind %q", tt.wantKind)
			}

			full := &discordgo.Message{
				ID:        "m1",
				GuildID:   "g-refetched",
				ChannelID: "c1",
				Content:   "full message",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u-refetched", Username: "test-user"},
			}
			snapshotClient := &tailSnapshotClient{
				message: full,
				onFetch: onFetch,
			}
			clientRefetches := &atomic.Int32{}
			server := newSyncerTailMessageUpdateGateway(t, full, clientRefetches)
			defer server.Close()
			restore := patchSyncerTailDiscordEndpoints(server.URL + "/api/v10/")
			defer restore()

			eventClient, err := discordclient.New("token")
			require.NoError(t, err)
			defer func() { _ = eventClient.Close() }()
			setDiscordTailHandlerTimeout(t, eventClient, 25*time.Millisecond)

			handler := &capturingTailHandler{
				tailHandler: &tailHandler{
					store:                 tailStore,
					client:                snapshotClient,
					attachmentTextEnabled: false,
				},
				cancel:        cancel,
				failures:      make(chan discordclient.TailFailure, 1),
				panicOnUpdate: tt.wantKind == "panic",
			}
			require.NoError(t, eventClient.Tail(ctx, handler))

			var failure discordclient.TailFailure
			select {
			case failure = <-handler.failures:
			default:
				t.Fatalf("tail failure was not reported before Tail returned: %v", ctx.Err())
			}
			require.Equal(t, "MESSAGE_UPDATE", failure.EventType)
			require.Equal(t, tt.wantKind, failure.Kind)
			require.Equal(t, "g-refetched", failure.GuildID)
			require.Equal(t, "c1", failure.ChannelID)
			require.Equal(t, "m1", failure.MessageID)
			require.Equal(t, "u-refetched", failure.UserID)
			require.Equal(t, tt.wantStage, failure.HandlerStage)
			require.Positive(t, failure.HandlerStageElapsed)
			require.Positive(t, failure.HandlerElapsed)
			if tt.wantKind == "timeout" {
				require.Equal(t, discordclient.TailFailureJoinJoined, failure.JoinOutcome)
				require.Positive(t, failure.JoinElapsed)
			} else {
				require.Equal(t, discordclient.TailFailureJoinNotRequired, failure.JoinOutcome)
				require.Zero(t, failure.JoinElapsed)
			}
			require.False(t, failure.ForceFallback)
			require.EqualValues(t, 1, snapshotClient.calls.Load())
			require.Zero(t, clientRefetches.Load())
		})
	}
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
	require.NoError(t, s.RecordFailure(ctx, store.FailureRef{
		Operation: tailMessageFailureOperation,
		Source:    "discord",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "5",
	}, context.DeadlineExceeded))
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
	client.mu.Lock()
	exactMessageCalls := client.exactMessageCalls
	client.mu.Unlock()
	require.Zero(t, exactMessageCalls)
	report, err := s.ListFailures(context.Background(), store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.GreaterOrEqual(t, report.UnresolvedCount, 1)
	foundTailFailure := false
	for _, failure := range report.Failures {
		if failure.Operation == tailMessageFailureOperation &&
			failure.Source == "discord" &&
			failure.GuildID == "g1" &&
			failure.ChannelID == "c1" &&
			failure.MessageID == "5" {
			foundTailFailure = true
			break
		}
	}
	require.True(t, foundTailFailure)
}

func TestPanickedGatewayMessageReplaysExactlyWithoutTailSideEffects(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()
	tailCtx, cancelTail := context.WithCancel(testCtx)
	defer cancelTail()

	s, err := store.Open(testCtx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.AdvanceChannelLatestMessageID(tailCtx, "c1", "100"))
	require.NoError(t, s.SetSyncState(tailCtx, "tail:last_event", "999"))

	const sensitivePanicText = "sensitive gateway panic token=do-not-store"
	exactFetches := &atomic.Int32{}
	exactMessage := &discordgo.Message{
		ID:        "50",
		GuildID:   "g1",
		ChannelID: "c1",
		Content:   "recovered below cursor",
		Timestamp: time.Now().UTC(),
		Author:    &discordgo.User{ID: "u1", Username: "user"},
	}
	server := newSyncerTailMessagePanicGateway(t, exactMessage, exactFetches)
	defer server.Close()
	restore := patchSyncerTailDiscordEndpoints(server.URL + "/api/v10/")
	defer restore()

	eventClient, err := discordclient.New("token")
	require.NoError(t, err)
	defer func() { _ = eventClient.Close() }()
	eventClientFailure := make(chan discordclient.TailFailure, 1)
	handler := &panicReplayTailHandler{
		tailHandler: &tailHandler{
			store:                 s,
			attachmentTextEnabled: false,
		},
		panicValue: sensitivePanicText,
		cancel:     cancelTail,
		failures:   eventClientFailure,
		started:    make(chan struct{}),
	}

	tailDone := make(chan error, 1)
	go func() {
		tailDone <- eventClient.Tail(tailCtx, handler)
	}()
	select {
	case <-handler.started:
	case <-testCtx.Done():
		t.Fatal("gateway panic handler did not start")
	}
	cancelTail()
	select {
	case err = <-tailDone:
	case <-testCtx.Done():
		t.Fatal("Tail did not return after gateway handler panic")
	}
	require.NoError(t, err)
	require.ErrorIs(t, tailCtx.Err(), context.Canceled)
	failureReport := <-eventClientFailure
	require.Equal(t, "MESSAGE_CREATE", failureReport.EventType)
	require.Equal(t, "panic", failureReport.Kind)
	require.Equal(t, "g1", failureReport.GuildID)
	require.Equal(t, "c1", failureReport.ChannelID)
	require.Equal(t, "50", failureReport.MessageID)
	require.Equal(t, "u1", failureReport.UserID)
	require.Equal(t, discordclient.TailFailureStageHandler, failureReport.HandlerStage)
	require.Positive(t, failureReport.HandlerStageElapsed)
	require.Positive(t, failureReport.HandlerElapsed)
	require.Equal(t, discordclient.TailFailureJoinJoined, failureReport.JoinOutcome)
	require.GreaterOrEqual(t, failureReport.JoinElapsed, time.Duration(0))
	require.LessOrEqual(t, failureReport.JoinElapsed, 100*time.Millisecond)
	require.False(t, failureReport.ForceFallback)

	replayCtx := context.Background()
	report, err := s.ListFailures(replayCtx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	failure := report.Failures[0]
	require.Equal(t, tailMessageFailureOperation, failure.Operation)
	require.Equal(t, "discord", failure.Source)
	require.Equal(t, "g1", failure.GuildID)
	require.Equal(t, "c1", failure.ChannelID)
	require.Equal(t, "50", failure.MessageID)
	require.Equal(t, tailMessageFailureRelatedKind, failure.RelatedKind)
	require.Equal(t, "create", failure.RelatedID)
	require.Equal(t, "errors.errorString", failure.ErrorClass)
	require.Equal(t, errTailMessageHandlerPanic.Error(), failure.ErrorMessage)
	require.NotContains(t, failure.ErrorClass, sensitivePanicText)
	require.NotContains(t, failure.ErrorMessage, sensitivePanicText)

	messages, err := s.ListMessages(replayCtx, store.MessageListOptions{
		GuildIDs:     []string{"g1"},
		IncludeEmpty: true,
	})
	require.NoError(t, err)
	require.Empty(t, messages)
	cursor, err := s.GetSyncState(replayCtx, "channel:c1:latest_message_id")
	require.NoError(t, err)
	require.Equal(t, "100", cursor)
	lastEvent, err := s.GetSyncState(replayCtx, "tail:last_event")
	require.NoError(t, err)
	require.Equal(t, "999", lastEvent)
	var eventCount int
	require.NoError(t, s.DB().QueryRowContext(replayCtx, `select count(*) from message_events`).Scan(&eventCount))
	require.Zero(t, eventCount)
	require.Zero(t, exactFetches.Load())

	svc := New(eventClient, s, nil)
	stats, err := svc.replayTailMessageFailures(replayCtx, []string{"g1"}, 10)
	require.NoError(t, err)
	require.Equal(t, tailMessageReplayStats{Candidates: 1, Recovered: 1}, stats)

	messages, err = s.ListMessages(replayCtx, store.MessageListOptions{
		GuildIDs:     []string{"g1"},
		IncludeEmpty: true,
	})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "50", messages[0].MessageID)
	require.Equal(t, "recovered below cursor", messages[0].Content)
	cursor, err = s.GetSyncState(replayCtx, "channel:c1:latest_message_id")
	require.NoError(t, err)
	require.Equal(t, "100", cursor)
	lastEvent, err = s.GetSyncState(replayCtx, "tail:last_event")
	require.NoError(t, err)
	require.Equal(t, "999", lastEvent)
	require.NoError(t, s.DB().QueryRowContext(replayCtx, `select count(*) from message_events`).Scan(&eventCount))
	require.Zero(t, eventCount)

	report, err = s.ListFailures(replayCtx, store.FailureListOptions{IncludeResolved: true}, time.Now())
	require.NoError(t, err)
	require.Zero(t, report.UnresolvedCount)
	require.Len(t, report.Failures, 1)
	require.False(t, report.Failures[0].ResolvedAt.IsZero())
	require.Equal(t, errTailMessageHandlerPanic.Error(), report.Failures[0].ErrorMessage)

	stats, err = svc.replayTailMessageFailures(replayCtx, []string{"g1"}, 10)
	require.NoError(t, err)
	require.Zero(t, stats.Candidates)
	require.EqualValues(t, 1, exactFetches.Load())
}

func TestReplayTailMessageFailuresRecoversBelowCursorWithoutTailSideEffects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.AdvanceChannelLatestMessageID(ctx, "c1", "100"))
	require.NoError(t, s.SetSyncState(ctx, "tail:last_event", "999"))
	require.NoError(t, s.RecordFailure(ctx, store.FailureRef{
		Operation:   tailMessageFailureOperation,
		Source:      "discord",
		GuildID:     "g1",
		ChannelID:   "c1",
		MessageID:   "50",
		RelatedKind: tailMessageFailureRelatedKind,
		RelatedID:   "create",
	}, context.DeadlineExceeded))

	client := &exactReplayClient{
		messages: map[string]*discordgo.Message{
			"c1/50": {
				ID:        "50",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "recovered below cursor",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			},
		},
	}
	svc := New(client, s, nil)
	stats, err := svc.replayTailMessageFailures(ctx, []string{"g1"}, 10)
	require.NoError(t, err)
	require.Equal(t, tailMessageReplayStats{Candidates: 1, Recovered: 1}, stats)

	messages, err := s.ListMessages(ctx, store.MessageListOptions{
		GuildIDs:     []string{"g1"},
		IncludeEmpty: true,
	})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "50", messages[0].MessageID)
	require.Equal(t, "recovered below cursor", messages[0].Content)

	cursor, err := s.GetSyncState(ctx, "channel:c1:latest_message_id")
	require.NoError(t, err)
	require.Equal(t, "100", cursor)
	lastEvent, err := s.GetSyncState(ctx, "tail:last_event")
	require.NoError(t, err)
	require.Equal(t, "999", lastEvent)
	var eventCount int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_events`).Scan(&eventCount))
	require.Zero(t, eventCount)

	report, err := s.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Zero(t, report.UnresolvedCount)
	require.Empty(t, report.Failures)

	stats, err = svc.replayTailMessageFailures(ctx, []string{"g1"}, 10)
	require.NoError(t, err)
	require.Zero(t, stats.Candidates)
	require.Equal(t, []string{"c1/50"}, client.calls)
}

func TestReplayTailMessageFailuresEnforcesBoundedLimit(t *testing.T) {
	t.Parallel()

	svc := New(&exactReplayClient{}, nil, nil)
	for _, limit := range []int{-1, 0, TailMessageReplayLimit + 1} {
		stats, err := svc.ReplayTailMessageFailures(context.Background(), nil, limit)
		require.ErrorContains(t, err, "tail message replay limit must be between 1 and 25")
		require.Zero(t, stats.Candidates)
	}
}

func TestReplayTailMessageFailuresDefersMissingMessagesAndRotatesCandidates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, messageID := range []string{"10", "20"} {
		require.NoError(t, s.RecordFailure(ctx, store.FailureRef{
			Operation:   tailMessageFailureOperation,
			Source:      "discord",
			GuildID:     "g1",
			ChannelID:   "c1",
			MessageID:   messageID,
			RelatedKind: tailMessageFailureRelatedKind,
			RelatedID:   "create",
		}, context.DeadlineExceeded))
	}
	_, err = s.DB().ExecContext(ctx, `
		update failure_ledger
		set last_seen_at = case message_id
			when '10' then '2026-07-13 00:00:01.000000000'
			when '20' then '2026-07-13 00:00:02.000000000'
		end
		where operation = 'tail_message'
	`)
	require.NoError(t, err)

	client := &exactReplayClient{
		messages: map[string]*discordgo.Message{
			"c1/20": {
				ID:        "20",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "second candidate",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			},
		},
	}
	svc := New(client, s, nil)
	first, err := svc.replayTailMessageFailures(ctx, []string{"g1"}, 1)
	require.NoError(t, err)
	require.Equal(t, tailMessageReplayStats{Candidates: 1, Deferred: 1}, first)

	second, err := svc.replayTailMessageFailures(ctx, []string{"g1"}, 1)
	require.NoError(t, err)
	require.Equal(t, tailMessageReplayStats{Candidates: 1, Recovered: 1}, second)
	require.Equal(t, []string{"c1/10", "c1/20"}, client.calls)

	report, err := s.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, report.UnresolvedCount)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "10", report.Failures[0].MessageID)
	require.Equal(t, 1, report.Failures[0].RetryCount)
}

func TestReplayTailMessageFailuresLeavesOutOfScopeGuildVisibleWithoutFetching(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.RecordFailure(ctx, store.FailureRef{
		Operation:   tailMessageFailureOperation,
		Source:      "discord",
		GuildID:     "g2",
		ChannelID:   "c2",
		MessageID:   "30",
		RelatedKind: tailMessageFailureRelatedKind,
		RelatedID:   "create",
	}, context.DeadlineExceeded))

	client := &exactReplayClient{}
	stats, err := New(client, s, nil).replayTailMessageFailures(ctx, []string{"g1"}, 10)
	require.NoError(t, err)
	require.Zero(t, stats.Candidates)
	require.Empty(t, client.calls)

	report, err := s.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, report.UnresolvedCount)
}

func TestReplayTailMessageFailuresRetainsFetchAndIdentityFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message *discordgo.Message
		err     error
	}{
		{
			name: "missing access",
			err:  errors.New(`HTTP 403 Forbidden, {"message":"Missing Access","code":50001}`),
		},
		{
			name: "mismatched identity",
			message: &discordgo.Message{
				ID:        "61",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "wrong message",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
			require.NoError(t, err)
			defer func() { _ = s.Close() }()
			require.NoError(t, s.RecordFailure(ctx, store.FailureRef{
				Operation:   tailMessageFailureOperation,
				Source:      "discord",
				GuildID:     "g1",
				ChannelID:   "c1",
				MessageID:   "60",
				RelatedKind: tailMessageFailureRelatedKind,
				RelatedID:   "update",
			}, context.DeadlineExceeded))

			client := &exactReplayClient{
				messages: map[string]*discordgo.Message{"c1/60": tt.message},
				errors:   map[string]error{"c1/60": tt.err},
			}
			stats, err := New(client, s, nil).replayTailMessageFailures(ctx, []string{"g1"}, 10)
			require.NoError(t, err)
			require.Equal(t, tailMessageReplayStats{Candidates: 1, Deferred: 1}, stats)

			report, err := s.ListFailures(ctx, store.FailureListOptions{}, time.Now())
			require.NoError(t, err)
			require.Equal(t, 1, report.UnresolvedCount)
			require.Len(t, report.Failures, 1)
			require.Equal(t, "60", report.Failures[0].MessageID)
			require.Equal(t, 1, report.Failures[0].RetryCount)
			var messageCount int
			require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from messages`).Scan(&messageCount))
			require.Zero(t, messageCount)
		})
	}
}

func TestReplayTailMessageFailuresDoesNotRewriteLedgerOnParentCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	ref := store.FailureRef{
		Operation:   tailMessageFailureOperation,
		Source:      "discord",
		GuildID:     "g1",
		ChannelID:   "c1",
		MessageID:   "70",
		RelatedKind: tailMessageFailureRelatedKind,
		RelatedID:   "create",
	}
	require.NoError(t, s.RecordFailure(ctx, ref, errors.New("original tail failure")))

	client := &exactReplayClient{
		errors: map[string]error{"c1/70": context.Canceled},
		onFetch: func(context.Context) {
			cancel()
		},
	}
	stats, err := New(client, s, nil).ReplayTailMessageFailures(ctx, []string{"g1"}, 10)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, stats.Candidates)
	require.Zero(t, stats.Deferred)

	report, err := s.ListFailures(context.Background(), store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, report.UnresolvedCount)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "original tail failure", report.Failures[0].ErrorMessage)
	require.Zero(t, report.Failures[0].RetryCount)
}

func TestPersistMessagePageResolvesCreateButLeavesNewerUpdateFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	for _, ref := range []store.FailureRef{
		tailMessageFailureIdentity("g1", "c1", "40", "create"),
		tailMessageFailureIdentity("g1", "c1", "40", "update"),
		tailMessageFailureIdentity("g1", "c1", "40", "delete"),
		{
			Operation: tailMessageFailureOperation,
			Source:    "discord",
			GuildID:   "g1",
			ChannelID: "c1",
			MessageID: "40",
		},
	} {
		require.NoError(t, s.RecordFailure(ctx, ref, context.DeadlineExceeded))
	}

	svc := New(&fakeClient{}, s, nil)
	// This page represents data fetched before the update failure was recorded.
	newest, err := svc.persistMessagePage(ctx, []*discordgo.Message{{
		ID:        "40",
		GuildID:   "g1",
		ChannelID: "c1",
		Content:   "normal page recovery",
		Timestamp: time.Now().UTC(),
		Author:    &discordgo.User{ID: "u1", Username: "user"},
	}}, "general", "g1", false)
	require.NoError(t, err)
	require.Equal(t, "40", newest)

	report, err := s.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 3, report.UnresolvedCount)
	require.Len(t, report.Failures, 3)
	require.ElementsMatch(t, []string{"", "delete", "update"}, []string{
		report.Failures[0].RelatedID,
		report.Failures[1].RelatedID,
		report.Failures[2].RelatedID,
	})
	var eventCount int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_events`).Scan(&eventCount))
	require.Zero(t, eventCount)
}

func TestTailHandlersResolveOnlySuccessfulEventIdentity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, eventKind := range []string{"create", "update", "delete"} {
		require.NoError(t, s.RecordFailure(
			ctx,
			tailMessageFailureIdentity("g1", "c1", "10", eventKind),
			context.DeadlineExceeded,
		))
	}
	handler := &tailHandler{store: s}
	message := &discordgo.Message{
		ID:        "10",
		GuildID:   "g1",
		ChannelID: "c1",
		Content:   "tail event",
		Timestamp: time.Now().UTC(),
		Author:    &discordgo.User{ID: "u1", Username: "user"},
	}

	require.NoError(t, handler.OnMessageCreate(ctx, message))
	requireTailFailureEventKinds(t, s, []string{"update", "delete"})
	require.NoError(t, handler.OnMessageUpdate(ctx, message))
	requireTailFailureEventKinds(t, s, []string{"delete"})
	require.NoError(t, handler.OnMessageDelete(ctx, &discordgo.MessageDelete{
		Message: &discordgo.Message{ID: "10", GuildID: "g1", ChannelID: "c1"},
	}))
	requireTailFailureEventKinds(t, s, nil)
}

func TestTailHandlerReturnedErrorIsRecordedOnlyByOuterOwner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	_, err = s.DB().ExecContext(ctx, `
		create trigger fail_tail_message_event
		before insert on message_events
		begin
			select raise(abort, 'sensitive raw handler failure token=do-not-store');
		end
	`)
	require.NoError(t, err)

	handler := &tailHandler{store: s}
	message := &discordgo.Message{
		ID:        "m1",
		GuildID:   "g1",
		ChannelID: "c1",
		Content:   "tail event",
		Timestamp: time.Now().UTC(),
		Author:    &discordgo.User{ID: "u1", Username: "user"},
	}
	require.Error(t, handler.OnMessageCreate(ctx, message))
	require.NoError(t, handler.RecordTailFailure(discordclient.TailFailure{
		EventType: "MESSAGE_CREATE",
		Kind:      "returned_error",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "m1",
	}))

	report, err := s.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, tailMessageFailureRelatedKind, report.Failures[0].RelatedKind)
	require.Equal(t, "create", report.Failures[0].RelatedID)
	require.Equal(t, errTailMessageHandlerReturned.Error(), report.Failures[0].ErrorMessage)
	require.NotContains(t, report.Failures[0].ErrorMessage, "do-not-store")
}

func TestRunTailImportsFallbackBeforeOpeningGatewayExactlyOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.PersistTailMessageFailureFallback(store.TailMessageFailureFallback{
		EventKind:   "update",
		FailureKind: "timeout",
		GuildID:     "g1",
		ChannelID:   "c1",
		MessageID:   "m1",
	}))

	client := &importObservingTailClient{store: s}
	svc := New(client, s, nil)
	require.NoError(t, svc.RunTail(ctx, nil, 0))
	require.NoError(t, svc.RunTail(ctx, nil, 0))
	require.Equal(t, []int{1, 1}, client.observedFailureCounts)

	report, err := s.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, "update", report.Failures[0].RelatedID)
	require.Zero(t, report.Failures[0].RetryCount)
}

func TestReplayTailMessageFailuresImportsFallbackBeforeCandidateSelection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.PersistTailMessageFailureFallback(store.TailMessageFailureFallback{
		EventKind:   "create",
		FailureKind: "panic",
		GuildID:     "g1",
		ChannelID:   "c1",
		MessageID:   "10",
	}))

	client := &exactReplayClient{messages: map[string]*discordgo.Message{
		"c1/10": {
			ID:        "10",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "recovered",
			Timestamp: time.Now().UTC(),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		},
	}}
	stats, err := New(client, s, nil).ReplayTailMessageFailures(ctx, []string{"g1"}, 10)
	require.NoError(t, err)
	require.Equal(t, tailMessageReplayStats{Candidates: 1, Recovered: 1}, stats)
	require.Equal(t, []string{"c1/10"}, client.calls)
}

func TestReplayTailMessageUpdateIsEventAware(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.RecordFailure(
		ctx,
		tailMessageFailureIdentity("g1", "c1", "10", "update"),
		errTailMessageHandlerReturned,
	))

	client := &exactReplayClient{messages: map[string]*discordgo.Message{
		"c1/10": {
			ID:        "10",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "updated canonical message",
			Timestamp: time.Now().UTC(),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		},
	}}
	stats, err := New(client, s, nil).ReplayTailMessageFailures(ctx, []string{"g1"}, 10)
	require.NoError(t, err)
	require.Equal(t, tailMessageReplayStats{Candidates: 1, Recovered: 1}, stats)

	messages, err := s.ListMessages(ctx, store.MessageListOptions{
		GuildIDs:     []string{"g1"},
		IncludeEmpty: true,
	})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "updated canonical message", messages[0].Content)
}

func TestReplayTailMessageDeleteDoesNotFetchOrCreateTailSideEffects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			AuthorID:          "u1",
			AuthorName:        "user",
			CreatedAt:         now,
			Content:           "delete me",
			NormalizedContent: "delete me",
			RawJSON:           `{}`,
		},
		Options: store.WriteOptions{EnqueueEmbedding: true},
	}}))
	require.NoError(t, s.AdvanceChannelLatestMessageID(ctx, "c1", "100"))
	require.NoError(t, s.SetSyncState(ctx, "tail:last_event", "999"))
	require.NoError(t, s.RecordFailure(
		ctx,
		tailMessageFailureIdentity("g1", "c1", "m1", "delete"),
		errTailMessageHandlerTimeout,
	))

	client := &exactReplayClient{}
	stats, err := New(client, s, nil).ReplayTailMessageFailures(ctx, []string{"g1"}, 10)
	require.NoError(t, err)
	require.Equal(t, tailMessageReplayStats{Candidates: 1, Recovered: 1}, stats)
	require.Empty(t, client.calls)

	var deletedAt string
	require.NoError(t, s.DB().QueryRowContext(
		ctx,
		`select coalesce(deleted_at, '') from messages where id = 'm1'`,
	).Scan(&deletedAt))
	require.NotEmpty(t, deletedAt)
	var eventCount, jobCount, searchCount int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_events`).Scan(&eventCount))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from embedding_jobs where message_id = 'm1'`).Scan(&jobCount))
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from message_fts where message_id = 'm1'`).Scan(&searchCount))
	require.Zero(t, eventCount)
	require.Zero(t, jobCount)
	require.Zero(t, searchCount)
	cursor, err := s.GetSyncState(ctx, "channel:c1:latest_message_id")
	require.NoError(t, err)
	require.Equal(t, "100", cursor)
	lastEvent, err := s.GetSyncState(ctx, "tail:last_event")
	require.NoError(t, err)
	require.Equal(t, "999", lastEvent)
	requireTailFailureEventKinds(t, s, nil)
}

func TestReplayTailMessageFailuresLeavesUnsupportedIdentitiesUntouched(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	for _, ref := range []store.FailureRef{
		{
			Operation: tailMessageFailureOperation,
			Source:    "discord",
			GuildID:   "g1",
			ChannelID: "c1",
			MessageID: "legacy",
		},
		{
			Operation:   tailMessageFailureOperation,
			Source:      "discord",
			GuildID:     "g1",
			ChannelID:   "c1",
			MessageID:   "invalid",
			RelatedKind: tailMessageFailureRelatedKind,
			RelatedID:   "unknown",
		},
	} {
		require.NoError(t, s.RecordFailure(ctx, ref, context.DeadlineExceeded))
	}

	client := &exactReplayClient{}
	svc := New(client, s, nil)
	for range 2 {
		stats, err := svc.ReplayTailMessageFailures(ctx, []string{"g1"}, 10)
		require.NoError(t, err)
		require.Equal(t, tailMessageReplayStats{}, stats)
	}
	require.Empty(t, client.calls)
	report, err := s.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 2, report.UnresolvedCount)
	require.Len(t, report.Failures, 2)
	for _, failure := range report.Failures {
		require.Equal(t, context.DeadlineExceeded.Error(), failure.ErrorMessage)
		require.Zero(t, failure.RetryCount)
	}
}

func TestReplayTailMessageFailuresSelectsOnlyValidEventAwareIdentity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	legacy := store.FailureRef{
		Operation: tailMessageFailureOperation,
		Source:    "discord",
		GuildID:   "g1",
		ChannelID: "c1",
		MessageID: "legacy",
	}
	invalid := store.FailureRef{
		Operation:   tailMessageFailureOperation,
		Source:      "discord",
		GuildID:     "g1",
		ChannelID:   "c1",
		MessageID:   "invalid",
		RelatedKind: tailMessageFailureRelatedKind,
		RelatedID:   "unknown",
	}
	eventAware := tailMessageFailureIdentity("g1", "c1", "50", "create")
	require.NoError(t, s.RecordFailure(ctx, legacy, errors.New("legacy failure")))
	require.NoError(t, s.RecordFailure(ctx, invalid, errors.New("invalid failure")))
	require.NoError(t, s.RecordFailure(ctx, eventAware, errTailMessageHandlerTimeout))
	_, err = s.DB().ExecContext(ctx, `
		update failure_ledger
		set last_seen_at = case message_id
			when 'legacy' then '2026-07-13 00:00:01.000000000'
			when 'invalid' then '2026-07-13 00:00:02.000000000'
			else '2026-07-13 00:00:03.000000000'
		end
	`)
	require.NoError(t, err)

	client := &exactReplayClient{messages: map[string]*discordgo.Message{
		"c1/50": {
			ID:        "50",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "recovered",
			Timestamp: time.Now().UTC(),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		},
	}}
	stats, err := New(client, s, nil).
		ReplayTailMessageFailures(ctx, []string{"g1"}, 1)
	require.NoError(t, err)
	require.Equal(t, tailMessageReplayStats{Candidates: 1, Recovered: 1}, stats)
	require.Equal(t, []string{"c1/50"}, client.calls)

	stats, err = New(client, s, nil).
		ReplayTailMessageFailures(ctx, []string{"g1"}, TailMessageReplayLimit)
	require.NoError(t, err)
	require.Equal(t, tailMessageReplayStats{}, stats)

	report, err := s.ListFailures(
		ctx,
		store.FailureListOptions{IncludeResolved: true},
		time.Now(),
	)
	require.NoError(t, err)
	require.Len(t, report.Failures, 3)
	for _, failure := range report.Failures {
		switch failure.MessageID {
		case "legacy":
			require.True(t, failure.ResolvedAt.IsZero())
			require.Zero(t, failure.RetryCount)
			require.Equal(t, "legacy failure", failure.ErrorMessage)
		case "invalid":
			require.True(t, failure.ResolvedAt.IsZero())
			require.Zero(t, failure.RetryCount)
			require.Equal(t, "invalid failure", failure.ErrorMessage)
		case "50":
			require.False(t, failure.ResolvedAt.IsZero())
		default:
			t.Fatalf("unexpected failure: %+v", failure)
		}
	}
}

func TestReplayTailMessageFailuresKeepsIncompleteIdentityVisible(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.RecordFailure(ctx, store.FailureRef{
		Operation:   tailMessageFailureOperation,
		Source:      "discord",
		ChannelID:   "c1",
		MessageID:   "80",
		RelatedKind: tailMessageFailureRelatedKind,
		RelatedID:   "create",
	}, context.DeadlineExceeded))

	client := &exactReplayClient{}
	stats, err := New(client, s, nil).ReplayTailMessageFailures(ctx, nil, 10)
	require.NoError(t, err)
	require.Equal(t, tailMessageReplayStats{Candidates: 1, Deferred: 1}, stats)
	require.Empty(t, client.calls)
	report, err := s.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, report.UnresolvedCount)
	require.Len(t, report.Failures, 1)
	require.Equal(t, errTailMessageReplayIncomplete.Error(), report.Failures[0].ErrorMessage)
	require.Equal(t, 1, report.Failures[0].RetryCount)
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

func TestRunTailPreservesFatalHandlerErrorDuringRequestedShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	tailErr := fmt.Errorf("%w: detached ordered handler timeout", discordclient.ErrFatalTail)
	client := &joiningTailClient{
		started:  make(chan struct{}),
		finished: make(chan struct{}),
		closed:   make(chan struct{}),
		tailErr:  tailErr,
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
	require.ErrorIs(t, <-done, discordclient.ErrFatalTail)
	select {
	case <-client.finished:
	default:
		t.Fatal("RunTail returned before the fatal tail result")
	}
}

func TestRunTailCancelsAndJoinsSerializedRepairBeforeClientClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	repairStarted := make(chan struct{})
	repairStopped := make(chan struct{})
	var startOnce sync.Once
	var stopOnce sync.Once
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	svc := New(nil, s, nil)
	svc.tailRepairJoinTimeout = time.Second
	svc.tailRepair = func(repairCtx context.Context, _ SyncOptions) (SyncStats, error) {
		active := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			previous := maxInFlight.Load()
			if active <= previous || maxInFlight.CompareAndSwap(previous, active) {
				break
			}
		}
		startOnce.Do(func() { close(repairStarted) })
		<-repairCtx.Done()
		stopOnce.Do(func() { close(repairStopped) })
		return SyncStats{}, repairCtx.Err()
	}
	client := &repairAwareTailClient{
		repairStopped: repairStopped,
		started:       make(chan struct{}),
		finished:      make(chan struct{}),
		closed:        make(chan struct{}),
	}
	svc.client = client

	done := make(chan error, 1)
	go func() {
		done <- svc.RunTail(ctx, nil, 5*time.Millisecond)
	}()
	select {
	case <-repairStarted:
	case <-time.After(time.Second):
		t.Fatal("repair did not start")
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	require.EqualValues(t, 1, maxInFlight.Load())
	require.Zero(t, inFlight.Load())
	require.False(t, client.closedBeforeRepair.Load())
	select {
	case <-client.closed:
	default:
		t.Fatal("client was not closed")
	}
}

func TestRunTailRepairJoinIsBoundedAndLogsSafeDiagnostics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	out := &lockedBuffer{}
	releaseRepair := make(chan struct{})
	repairStarted := make(chan struct{})
	repairFinished := make(chan struct{})
	svc := New(nil, s, newTestLogger(out))
	svc.tailRepairJoinTimeout = 20 * time.Millisecond
	svc.tailRepair = func(context.Context, SyncOptions) (SyncStats, error) {
		close(repairStarted)
		<-releaseRepair
		close(repairFinished)
		return SyncStats{}, errors.New("sensitive repair token=do-not-log")
	}
	client := &joiningTailClient{
		started:  make(chan struct{}),
		finished: make(chan struct{}),
		closed:   make(chan struct{}),
	}
	svc.client = client

	done := make(chan error, 1)
	go func() {
		done <- svc.RunTail(ctx, nil, 5*time.Millisecond)
	}()
	select {
	case <-repairStarted:
	case <-time.After(time.Second):
		t.Fatal("repair did not start")
	}
	cancel()
	startedAt := time.Now()
	require.ErrorIs(t, <-done, discordclient.ErrFatalTail)
	require.Less(t, time.Since(startedAt), 500*time.Millisecond)
	logged := out.String()
	require.Contains(t, logged, `msg="tail repair join completed"`)
	require.Contains(t, logged, "outcome=timed_out")
	require.Contains(t, logged, "reason=")
	require.NotContains(t, logged, "do-not-log")
	close(releaseRepair)
	select {
	case <-repairFinished:
	case <-time.After(time.Second):
		t.Fatal("repair did not finish after release")
	}
}

func TestRunTailRepairJoinTimeoutIsFatalWhenTailReturns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	out := &lockedBuffer{}
	releaseRepair := make(chan struct{})
	repairStarted := make(chan struct{})
	repairFinished := make(chan struct{})
	svc := New(nil, s, newTestLogger(out))
	svc.tailRepairJoinTimeout = 20 * time.Millisecond
	svc.tailRepair = func(context.Context, SyncOptions) (SyncStats, error) {
		close(repairStarted)
		<-releaseRepair
		close(repairFinished)
		return SyncStats{}, errors.New("sensitive repair token=do-not-log")
	}
	client := &returningTailClient{
		started: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	svc.client = client

	done := make(chan error, 1)
	go func() {
		done <- svc.RunTail(ctx, nil, 5*time.Millisecond)
	}()
	select {
	case <-client.started:
	case <-ctx.Done():
		t.Fatal("tail did not start")
	}
	select {
	case <-repairStarted:
	case <-ctx.Done():
		t.Fatal("repair did not start")
	}
	close(client.release)
	require.ErrorIs(t, <-done, discordclient.ErrFatalTail)
	select {
	case <-client.closed:
	default:
		t.Fatal("client was not closed after tail return")
	}
	logged := out.String()
	require.Contains(t, logged, `msg="tail repair join completed"`)
	require.Contains(t, logged, "outcome=timed_out")
	require.Contains(t, logged, "reason=tail_return")
	require.NotContains(t, logged, "do-not-log")

	close(releaseRepair)
	select {
	case <-repairFinished:
	case <-ctx.Done():
		t.Fatal("repair did not finish after release")
	}
}

func TestLogTailRepairResultUsesSafeFailureKinds(t *testing.T) {
	t.Parallel()

	out := &lockedBuffer{}
	svc := &Syncer{logger: newTestLogger(out)}
	svc.logTailRepairResult(tailRepairResult{})
	svc.logTailRepairResult(tailRepairResult{err: context.Canceled})
	require.Empty(t, out.String())

	svc.logTailRepairResult(tailRepairResult{
		err:     context.DeadlineExceeded,
		elapsed: 2 * time.Second,
	})
	svc.logTailRepairResult(tailRepairResult{
		err:     errors.New("sensitive repair token=do-not-log"),
		elapsed: 3 * time.Second,
	})
	logged := out.String()
	require.Contains(t, logged, "failure_kind=timeout")
	require.Contains(t, logged, "failure_kind=returned_error")
	require.Contains(t, logged, "elapsed=2s")
	require.Contains(t, logged, "elapsed=3s")
	require.NotContains(t, logged, "do-not-log")
}

func TestRunTailPreservesGatewayOpenErrorWithoutMessageFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &gatewayOpenTailClient{}
	err = New(client, s, nil).RunTail(ctx, nil, 0)
	require.Error(t, err)
	require.True(t, discordclient.IsGatewayOpenError(err))
	report, reportErr := s.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, reportErr)
	require.Empty(t, report.Failures)
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

func TestRunTailLogsSafeEventFailureMetadata(t *testing.T) {
	out := &lockedBuffer{}
	client := &reportingTailClient{
		failure: discordclient.TailFailure{
			EventType: "MESSAGE_CREATE",
			Kind:      "returned_error",
			GuildID:   "g1",
			ChannelID: "c1",
			MessageID: "m1",
			UserID:    "u1",
		},
	}
	svc := New(client, nil, newTestLogger(out))

	require.NoError(t, svc.RunTail(context.Background(), nil, 0))
	logged := out.String()
	require.Contains(t, logged, `level=WARN msg="tail event handler failed"`)
	require.Contains(t, logged, "event_type=MESSAGE_CREATE")
	require.Contains(t, logged, "failure_kind=returned_error")
	require.Contains(t, logged, "guild_id=g1")
	require.Contains(t, logged, "channel_id=c1")
	require.Contains(t, logged, "message_id=m1")
	require.Contains(t, logged, "user_id=u1")
	require.NotContains(t, logged, "err=")
}

func requireTailFailureEventKinds(t *testing.T, s *store.Store, want []string) {
	t.Helper()
	report, err := s.ListFailures(context.Background(), store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	got := make([]string, 0, len(report.Failures))
	for _, failure := range report.Failures {
		if failure.Operation == tailMessageFailureOperation && failure.Source == "discord" {
			got = append(got, failure.RelatedID)
		}
	}
	require.ElementsMatch(t, want, got)
}

type importObservingTailClient struct {
	fakeClient
	store                 *store.Store
	observedFailureCounts []int
}

func (c *importObservingTailClient) Tail(ctx context.Context, _ discordclient.EventHandler) error {
	report, err := c.store.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	if err != nil {
		return err
	}
	c.observedFailureCounts = append(c.observedFailureCounts, report.UnresolvedCount)
	return nil
}

type gatewayOpenTailClient struct {
	fakeClient
}

func (c *gatewayOpenTailClient) Tail(context.Context, discordclient.EventHandler) error {
	return &discordclient.GatewayOpenError{}
}

type repairAwareTailClient struct {
	fakeClient
	repairStopped      <-chan struct{}
	started            chan struct{}
	finished           chan struct{}
	closed             chan struct{}
	closedBeforeRepair atomic.Bool
}

func (c *repairAwareTailClient) Tail(ctx context.Context, _ discordclient.EventHandler) error {
	close(c.started)
	<-ctx.Done()
	close(c.finished)
	return nil
}

func (c *repairAwareTailClient) Close() error {
	select {
	case <-c.repairStopped:
	default:
		c.closedBeforeRepair.Store(true)
	}
	close(c.closed)
	return nil
}

type joiningTailClient struct {
	fakeClient
	started  chan struct{}
	finished chan struct{}
	closed   chan struct{}
	tailErr  error
}

func (c *joiningTailClient) Tail(ctx context.Context, _ discordclient.EventHandler) error {
	close(c.started)
	<-ctx.Done()
	close(c.finished)
	return c.tailErr
}

func (c *joiningTailClient) Close() error {
	close(c.closed)
	return nil
}

type returningTailClient struct {
	fakeClient
	started chan struct{}
	release chan struct{}
	closed  chan struct{}
}

func (c *returningTailClient) Tail(context.Context, discordclient.EventHandler) error {
	close(c.started)
	<-c.release
	return nil
}

func (c *returningTailClient) Close() error {
	close(c.closed)
	return nil
}

type exactReplayClient struct {
	fakeClient
	messages map[string]*discordgo.Message
	errors   map[string]error
	calls    []string
	onFetch  func(context.Context)
}

func (c *exactReplayClient) ChannelMessage(ctx context.Context, channelID, messageID string) (*discordgo.Message, error) {
	key := channelID + "/" + messageID
	c.calls = append(c.calls, key)
	if c.onFetch != nil {
		c.onFetch(ctx)
	}
	if err := c.errors[key]; err != nil {
		return nil, err
	}
	return c.messages[key], nil
}

type reportingTailClient struct {
	fakeClient
	failure discordclient.TailFailure
}

func (c *reportingTailClient) Tail(_ context.Context, handler discordclient.EventHandler) error {
	reporter, ok := handler.(interface {
		OnTailFailure(discordclient.TailFailure)
	})
	if !ok {
		return errors.New("tail handler does not report failures")
	}
	reporter.OnTailFailure(c.failure)
	return nil
}

type tailSnapshotClient struct {
	fakeClient
	message *discordgo.Message
	onFetch func(context.Context)
	calls   atomic.Int32
}

func (c *tailSnapshotClient) ChannelMessage(ctx context.Context, _, _ string) (*discordgo.Message, error) {
	c.calls.Add(1)
	if c.onFetch != nil {
		c.onFetch(ctx)
	}
	return c.message, nil
}

type capturingTailHandler struct {
	*tailHandler
	cancel        context.CancelFunc
	failures      chan discordclient.TailFailure
	panicOnUpdate bool
}

func (h *capturingTailHandler) OnTailFailure(failure discordclient.TailFailure) {
	select {
	case h.failures <- failure:
		h.cancel()
	default:
	}
}

func (h *capturingTailHandler) OnMessageUpdate(ctx context.Context, msg *discordgo.Message) error {
	if !h.panicOnUpdate {
		return h.tailHandler.OnMessageUpdate(ctx, msg)
	}
	snapshot, err := h.messageUpdateSnapshot(ctx, msg)
	if err != nil || snapshot == nil {
		return err
	}
	discordclient.UpdateTailFailureStage(ctx, discordclient.TailFailureStageMessageBuild)
	panic("sensitive syncer message update panic")
}

type panicReplayTailHandler struct {
	*tailHandler
	panicValue any
	cancel     context.CancelFunc
	failures   chan discordclient.TailFailure
	started    chan struct{}
	startOnce  sync.Once
}

func (h *panicReplayTailHandler) OnMessageCreate(ctx context.Context, _ *discordgo.Message) error {
	h.startOnce.Do(func() {
		close(h.started)
	})
	<-ctx.Done()
	panic(h.panicValue)
}

func (h *panicReplayTailHandler) OnTailFailure(failure discordclient.TailFailure) {
	h.failures <- failure
	h.cancel()
}

func newSyncerTailMessageUpdateGateway(
	t *testing.T,
	full *discordgo.Message,
	clientRefetches *atomic.Int32,
) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/channels/c1/messages/m1", func(w http.ResponseWriter, _ *http.Request) {
		clientRefetches.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(full)
	})
	mux.HandleFunc("/api/v10/gateway", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"url": "ws://" + r.Host + "/gateway"})
	})

	upgrader := websocket.Upgrader{}
	gatewayHandler := func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade gateway: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if err := conn.WriteJSON(map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		}); err != nil {
			t.Errorf("write hello: %v", err)
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Errorf("read identify: %v", err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "READY",
			"s":  1,
			"d": map[string]any{
				"session_id": "session",
				"user":       map[string]any{"id": "bot", "username": "bot"},
			},
		}); err != nil {
			t.Errorf("write ready: %v", err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "MESSAGE_UPDATE",
			"s":  2,
			"d": map[string]any{
				"id":         "m1",
				"channel_id": "c1",
				"content":    "edited partial",
			},
		}); err != nil {
			t.Errorf("write update: %v", err)
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}
	mux.HandleFunc("/gateway", gatewayHandler)
	mux.HandleFunc("/gateway/", gatewayHandler)
	return httptest.NewServer(mux)
}

func newSyncerTailMessagePanicGateway(
	t *testing.T,
	exactMessage *discordgo.Message,
	exactFetches *atomic.Int32,
) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/channels/c1/messages/50", func(w http.ResponseWriter, _ *http.Request) {
		exactFetches.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(exactMessage)
	})
	mux.HandleFunc("/api/v10/gateway", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"url": "ws://" + r.Host + "/gateway"})
	})

	upgrader := websocket.Upgrader{}
	gatewayHandler := func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade gateway: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if err := conn.WriteJSON(map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		}); err != nil {
			t.Errorf("write hello: %v", err)
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Errorf("read identify: %v", err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "READY",
			"s":  1,
			"d": map[string]any{
				"session_id": "session",
				"user":       map[string]any{"id": "bot", "username": "bot"},
			},
		}); err != nil {
			t.Errorf("write ready: %v", err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"op": 0,
			"t":  "MESSAGE_CREATE",
			"s":  2,
			"d": map[string]any{
				"id":         "50",
				"guild_id":   "g1",
				"channel_id": "c1",
				"content":    "gateway message",
				"timestamp":  time.Now().UTC().Format(time.RFC3339),
				"author":     map[string]any{"id": "u1", "username": "user"},
			},
		}); err != nil {
			t.Errorf("write message create: %v", err)
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}
	mux.HandleFunc("/gateway", gatewayHandler)
	mux.HandleFunc("/gateway/", gatewayHandler)
	return httptest.NewServer(mux)
}

func patchSyncerTailDiscordEndpoints(apiBase string) func() {
	oldDiscord := discordgo.EndpointDiscord
	oldAPI := discordgo.EndpointAPI
	oldChannels := discordgo.EndpointChannels
	oldGateway := discordgo.EndpointGateway
	oldChannelMessages := discordgo.EndpointChannelMessages
	oldChannelMessage := discordgo.EndpointChannelMessage

	discordgo.EndpointDiscord = apiBase[:len(apiBase)-len("api/v10/")]
	discordgo.EndpointAPI = apiBase
	discordgo.EndpointChannels = apiBase + "channels/"
	discordgo.EndpointGateway = apiBase + "gateway"
	discordgo.EndpointChannelMessages = func(channelID string) string {
		return discordgo.EndpointChannels + channelID + "/messages"
	}
	discordgo.EndpointChannelMessage = func(channelID, messageID string) string {
		return discordgo.EndpointChannelMessages(channelID) + "/" + messageID
	}

	return func() {
		discordgo.EndpointDiscord = oldDiscord
		discordgo.EndpointAPI = oldAPI
		discordgo.EndpointChannels = oldChannels
		discordgo.EndpointGateway = oldGateway
		discordgo.EndpointChannelMessages = oldChannelMessages
		discordgo.EndpointChannelMessage = oldChannelMessage
	}
}

func setDiscordTailHandlerTimeout(t *testing.T, client *discordclient.Client, timeout time.Duration) {
	t.Helper()

	field := reflect.ValueOf(client).Elem().FieldByName("tailHandlerTimeout")
	require.True(t, field.IsValid() && field.CanAddr())
	// Keep the production client API unchanged while making the cooperative timeout test fast.
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().SetInt(int64(timeout))
}
