package syncer

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestSyncFullBackfillResumesFromCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	messages := make([]*discordgo.Message, 0, 150)
	for id := 250; id >= 101; id-- {
		messages = append(messages, &discordgo.Message{
			ID:        fmt.Sprintf("%03d", id),
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   fmt.Sprintf("msg-%03d", id),
			Timestamp: time.Now().UTC(),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		})
	}

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText}},
		},
		messages: map[string][]*discordgo.Message{
			"c1": messages,
		},
		beforeErrors: map[string]map[string]error{
			"c1": {"151": context.DeadlineExceeded},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 100, stats.Messages)

	latest, err := s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "250", latest)

	backfill, err := s.GetSyncState(ctx, channelBackfillScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "151", backfill)

	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Empty(t, complete)

	delete(client.beforeErrors["c1"], "151")
	client.messages["c1"] = append([]*discordgo.Message{{
		ID:        "251",
		GuildID:   "g1",
		ChannelID: "c1",
		Content:   "msg-251",
		Timestamp: time.Now().UTC(),
		Author:    &discordgo.User{ID: "u1", Username: "user"},
	}}, client.messages["c1"]...)

	stats, err = svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 51, stats.Messages)

	latest, err = s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "251", latest)

	complete, err = s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete)

	results, err := s.SearchMessages(ctx, store.SearchOptions{Query: "msg-101", Limit: 1})
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestSyncFullBackfillDoesNotRegressLatestPointer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	// Resume state for a half-backfilled channel: head already synced up to
	// 250, backfill cursor parked at 200.
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), "250"))
	require.NoError(t, s.SetSyncState(ctx, channelBackfillScope("c1"), "200"))

	messages := make([]*discordgo.Message, 0, 250)
	for id := 250; id >= 1; id-- {
		messages = append(messages, &discordgo.Message{
			ID:        fmt.Sprintf("%03d", id),
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   fmt.Sprintf("msg-%03d", id),
			Timestamp: time.Now().UTC(),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		})
	}

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText}},
		},
		messages: map[string][]*discordgo.Message{
			"c1": messages,
		},
		beforeErrors: map[string]map[string]error{
			"c1": {"100": context.DeadlineExceeded},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 100, stats.Messages)

	// The interrupted backfill must not have clobbered the head pointer with
	// ids from the backfill region.
	latest, err := s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "250", latest)

	backfill, err := s.GetSyncState(ctx, channelBackfillScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "100", backfill)

	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Empty(t, complete)

	// The resumed pass should fetch only the remaining backfill span, not
	// re-crawl 200..250 from a regressed head pointer.
	delete(client.beforeErrors["c1"], "100")
	stats, err = svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 99, stats.Messages)

	latest, err = s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "250", latest)

	complete, err = s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete)
}

func TestSyncFullBackfillLargeResumeDoesNotRecrawlHeadSpan(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))

	const totalMessages = 1200
	headID := syntheticSnowflakeID(totalMessages)
	backfillStartID := syntheticSnowflakeID(1000)
	interruptBeforeID := syntheticSnowflakeID(900)
	for id := 1000; id <= totalMessages; id++ {
		require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
			ID:                syntheticSnowflakeID(id),
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "user",
			CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
			Content:           fmt.Sprintf("msg-%d", id),
			NormalizedContent: fmt.Sprintf("msg-%d", id),
			RawJSON:           `{}`,
		}))
	}
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), headID))
	require.NoError(t, s.SetSyncState(ctx, channelBackfillScope("c1"), backfillStartID))

	messages := make([]*discordgo.Message, 0, totalMessages)
	for id := totalMessages; id >= 1; id-- {
		messages = append(messages, &discordgo.Message{
			ID:        syntheticSnowflakeID(id),
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   fmt.Sprintf("msg-%d", id),
			Timestamp: time.Now().UTC(),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		})
	}

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText}},
		},
		messages: map[string][]*discordgo.Message{
			"c1": messages,
		},
		beforeErrors: map[string]map[string]error{
			"c1": {interruptBeforeID: context.DeadlineExceeded},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 100, stats.Messages)

	latest, err := s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Equal(t, headID, latest)
	backfill, err := s.GetSyncState(ctx, channelBackfillScope("c1"))
	require.NoError(t, err)
	require.Equal(t, interruptBeforeID, backfill)

	delete(client.beforeErrors["c1"], interruptBeforeID)
	client.mu.Lock()
	client.messageRequests = nil
	client.mu.Unlock()

	stats, err = svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 899, stats.Messages)

	client.mu.Lock()
	requests := append([]messageRequest(nil), client.messageRequests...)
	client.mu.Unlock()
	for _, req := range requests {
		if req.afterID != "" {
			require.Equal(t, headID, req.afterID)
		}
	}

	latest, err = s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Equal(t, headID, latest)
	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete)
	_, rows, err := s.ReadOnlyQuery(ctx, `select count(*) from messages where channel_id = 'c1'`)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"1200"}}, rows)
}

func syntheticSnowflakeID(offset int) string {
	return fmt.Sprintf("%018d", int64(100000000000000000+offset))
}

func TestSyncFullSeedsBackfillFromStoredMessages(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	for id := 250; id >= 151; id-- {
		require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
			ID:                fmt.Sprintf("%03d", id),
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "user",
			CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
			Content:           fmt.Sprintf("msg-%03d", id),
			NormalizedContent: fmt.Sprintf("msg-%03d", id),
			RawJSON:           `{}`,
		}))
	}

	messages := make([]*discordgo.Message, 0, 150)
	for id := 250; id >= 101; id-- {
		messages = append(messages, &discordgo.Message{
			ID:        fmt.Sprintf("%03d", id),
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   fmt.Sprintf("msg-%03d", id),
			Timestamp: time.Now().UTC(),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		})
	}

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText}},
		},
		messages: map[string][]*discordgo.Message{
			"c1": messages,
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 50, stats.Messages)

	latest, err := s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "250", latest)

	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete)
}

func TestSyncMarksLegacyLatestStateAsComplete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "250",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "user",
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "msg-250",
		NormalizedContent: "msg-250",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), "250"))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText}},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "250",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "msg-250",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 0, stats.Messages)

	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete)
}

func TestSyncMarksEmptyChannelComplete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "quiet", Type: discordgo.ChannelTypeGuildText},
			},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true})
	require.NoError(t, err)
	require.Equal(t, 0, stats.Messages)

	cursor, err := s.GetSyncState(ctx, "channel:c1:latest_message_id")
	require.NoError(t, err)
	require.Empty(t, cursor)

	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete)

	cols, rows, err := s.ReadOnlyQuery(ctx, `select count(*) from sync_state where scope = 'channel:c1:latest_message_id'`)
	require.NoError(t, err)
	require.Equal(t, []string{"count(*)"}, cols)
	require.Equal(t, [][]string{{"1"}}, rows)
}

func TestSyncSinceLimitsInitialBootstrap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Now().UTC()
	cutoff := now.Add(-3 * time.Hour)
	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText}},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {
				{
					ID:        "300",
					GuildID:   "g1",
					ChannelID: "c1",
					Content:   "msg-300",
					Timestamp: now.Add(-1 * time.Hour),
					Author:    &discordgo.User{ID: "u1", Username: "user"},
				},
				{
					ID:        "200",
					GuildID:   "g1",
					ChannelID: "c1",
					Content:   "msg-200",
					Timestamp: now.Add(-2 * time.Hour),
					Author:    &discordgo.User{ID: "u1", Username: "user"},
				},
				{
					ID:        "100",
					GuildID:   "g1",
					ChannelID: "c1",
					Content:   "msg-100",
					Timestamp: now.Add(-5 * time.Hour),
					Author:    &discordgo.User{ID: "u1", Username: "user"},
				},
			},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Since: cutoff})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Messages)

	_, rows, err := s.ReadOnlyQuery(ctx, "select id from messages order by id desc")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"300"}, {"200"}}, rows)

	latest, err := s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "300", latest)

	backfill, err := s.GetSyncState(ctx, channelBackfillScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "200", backfill)

	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Empty(t, complete)
}

func TestSyncFullSinceStopsBackfillAtCutoff(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Now().UTC()
	cutoff := now.Add(-4 * time.Hour)
	messages := []*discordgo.Message{
		{
			ID:        "300",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "msg-300",
			Timestamp: now.Add(-1 * time.Hour),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		},
		{
			ID:        "250",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "msg-250",
			Timestamp: now.Add(-2 * time.Hour),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		},
		{
			ID:        "200",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "msg-200",
			Timestamp: now.Add(-3 * time.Hour),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		},
		{
			ID:        "150",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "msg-150",
			Timestamp: now.Add(-4 * time.Hour),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		},
		{
			ID:        "100",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "msg-100",
			Timestamp: now.Add(-5 * time.Hour),
			Author:    &discordgo.User{ID: "u1", Username: "user"},
		},
	}

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText}},
		},
		messages: map[string][]*discordgo.Message{
			"c1": messages,
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, Since: cutoff, Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 4, stats.Messages)

	_, rows, err := s.ReadOnlyQuery(ctx, "select id from messages order by id desc")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"300"}, {"250"}, {"200"}, {"150"}}, rows)

	backfill, err := s.GetSyncState(ctx, channelBackfillScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "150", backfill)

	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Empty(t, complete)

	stats, err = svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)

	_, rows, err = s.ReadOnlyQuery(ctx, "select id from messages order by id desc")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"300"}, {"250"}, {"200"}, {"150"}, {"100"}}, rows)

	complete, err = s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "1", complete)
}
