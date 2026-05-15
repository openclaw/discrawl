package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultEmbedLimit(t *testing.T) {
	t.Parallel()

	require.Equal(t, 1000, DefaultEmbedLimit())
}

func TestQueryHelpersAndEmbeddingPresence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}))
	require.NoError(t, insertTestEmbedding(ctx, s, "m1", "ollama", "nomic-embed-text", []float32{1, 0}))

	ok, err := s.HasMessageEmbeddings(ctx, " ollama ", " nomic-embed-text ", "")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = s.HasMessageEmbeddings(ctx, "openai", "text-embedding-3-small", "")
	require.NoError(t, err)
	require.False(t, ok)

	require.Equal(t, 400, searchCandidateLimit(0))
	require.Equal(t, searchCandidateFloor, searchCandidateLimit(1))
	require.Equal(t, searchCandidateCap, searchCandidateLimit(1000))
	require.True(t, IsReadOnlySQL("-- hello\n/* block */\nwith rows as (select 1) select * from rows"))
	require.True(t, IsReadOnlySQL("EXPLAIN select 1"))
	require.False(t, IsReadOnlySQL("/* unfinished"))
	require.False(t, IsReadOnlySQL("delete from messages"))

	parent, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	queryCtx, queryCancel := withQueryTimeout(parent)
	defer queryCancel()
	parentDeadline, ok := parent.Deadline()
	require.True(t, ok)
	queryDeadline, ok := queryCtx.Deadline()
	require.True(t, ok)
	require.Equal(t, parentDeadline, queryDeadline)

	db, cleanup, err := (&Store{db: s.DB()}).openReadOnlyDB()
	require.NoError(t, err)
	require.Nil(t, cleanup)
	require.Same(t, s.DB(), db)
	require.NoError(t, s.CheckMessageFTS(ctx))
}

func TestSearchFallbackFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	base := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c2", GuildID: "g2", Kind: "text", Name: "random", RawJSON: `{}`}))
	for _, record := range []MessageRecord{
		{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			CreatedAt:         base.Format(time.RFC3339Nano),
			Content:           "needle alpha",
			NormalizedContent: "needle alpha",
			RawJSON:           `{"author":{"username":"Peter"}}`,
		},
		{
			ID:                "m2",
			GuildID:           "g2",
			ChannelID:         "c2",
			ChannelName:       "random",
			AuthorID:          "u2",
			AuthorName:        "Other",
			CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
			Content:           "needle beta",
			NormalizedContent: "needle beta",
			RawJSON:           `{"author":{"username":"Other"}}`,
		},
		{
			ID:                "m3",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			CreatedAt:         base.Add(2 * time.Minute).Format(time.RFC3339Nano),
			Content:           "",
			NormalizedContent: "",
			RawJSON:           `{"author":{"username":"Peter"}}`,
		},
	} {
		require.NoError(t, s.UpsertMessage(ctx, record))
	}

	results, err := s.searchFallback(ctx, SearchOptions{
		Query:    "needle",
		GuildIDs: []string{"g1"},
		Channel:  "gener",
		Author:   "Peter",
		Limit:    10,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m1", results[0].MessageID)
	require.Equal(t, "general", results[0].ChannelName)

	results, err = s.searchFallback(ctx, SearchOptions{Query: "", IncludeEmpty: true, Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 3)
}

func TestStoreMaintenanceHelpers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.NoError(t, s.RebuildSearchIndexes(ctx))
	version, err := s.schemaVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, storeSchemaVersion, version)
	require.NoError(t, s.setSchemaVersion(ctx, storeSchemaVersion))
	require.NoError(t, s.ensureFTSRowIDs(ctx))
	require.NoError(t, s.ensureMemberFTSRowIDs(ctx))
	require.NoError(t, s.ensureEmbeddingSearchIndexes(ctx))

	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.ErrorContains(t, configureFTSBulkLoad(ctx, tx, "bad_fts"), "unsupported fts table")
	require.ErrorContains(t, optimizeFTS(ctx, tx, "bad_fts"), "unsupported fts table")
	ok, err := columnExists(ctx, tx, "messages", "id")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = columnExists(ctx, tx, "messages", "missing_column")
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, tx.Rollback())

	rowID, ok := messageFTSRowID("")
	require.False(t, ok)
	require.Zero(t, rowID)
	rowID, ok = messageFTSRowID("123")
	require.True(t, ok)
	require.Equal(t, int64(123), rowID)
	rowID, ok = messageFTSRowID("not-a-snowflake")
	require.True(t, ok)
	require.NotZero(t, rowID)
	require.NotZero(t, memberFTSRowID("g1", "u1"))
	require.NoError(t, (*Store)(nil).Close())
	require.NoError(t, (&Store{}).Close())
}

func TestClosedStoreOperationsReturnErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	require.NoError(t, s.Close())

	require.Error(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1"}))
	require.Error(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1"}))
	require.Error(t, s.ReplaceMembers(ctx, "g1", nil))
	require.Error(t, s.UpsertMember(ctx, MemberRecord{GuildID: "g1", UserID: "u1"}))
	require.Error(t, s.DeleteMember(ctx, "g1", "u1"))
	require.Error(t, s.UpsertMessageWithOptions(ctx, MessageRecord{ID: "m1"}, WriteOptions{}))
	require.Error(t, s.UpsertMessages(ctx, []MessageMutation{{Record: MessageRecord{ID: "m1"}}}))
	require.NoError(t, s.UpsertMessages(ctx, nil))
	require.Error(t, s.MarkMessageDeleted(ctx, "g1", "c1", "m1", map[string]any{}))
	require.Error(t, s.AppendMessageEvent(ctx, "g1", "c1", "m1", "create", map[string]any{}))
	require.Error(t, s.SetSyncState(ctx, "scope", "cursor"))
	require.Error(t, s.DeleteSyncState(ctx, "scope"))
	_, err = s.GetSyncState(ctx, "scope")
	require.Error(t, err)
	_, _, err = s.ChannelMessageBounds(ctx, "c1")
	require.Error(t, err)
	_, err = s.SearchMessages(ctx, SearchOptions{Query: "needle", Limit: 1})
	require.Error(t, err)
	_, err = s.SearchMessagesSemantic(ctx, SemanticSearchOptions{
		QueryVector: []float32{1},
		Provider:    "ollama",
		Model:       "model",
		Dimensions:  1,
		Limit:       1,
	})
	require.Error(t, err)
	_, err = s.SearchMessagesHybrid(ctx, SearchOptions{Query: "needle", Limit: 1}, SemanticSearchOptions{
		QueryVector: []float32{1},
		Provider:    "ollama",
		Model:       "model",
		Dimensions:  1,
		Limit:       1,
	})
	require.Error(t, err)
	_, err = s.Members(ctx, "", "", 1)
	require.Error(t, err)
	_, err = s.MemberByID(ctx, "u1")
	require.Error(t, err)
	_, err = s.MemberProfile(ctx, "g1", "u1", 1)
	require.Error(t, err)
	_, err = s.ListMessages(ctx, MessageListOptions{GuildIDs: []string{"g1"}, Channel: "#general", Author: "u1", Since: time.Now().Add(-time.Hour), Before: time.Now(), Limit: 1, IncludeEmpty: true})
	require.Error(t, err)
	_, err = s.ListMentions(ctx, MentionListOptions{GuildIDs: []string{"g1"}, Channel: "#general", Author: "u1", Target: "u2", TargetType: "user", Since: time.Now().Add(-time.Hour), Before: time.Now(), Limit: 1})
	require.Error(t, err)
	_, err = s.Channels(ctx, "")
	require.Error(t, err)
	_, err = s.GuildChannelCount(ctx, "g1")
	require.Error(t, err)
	_, err = s.GuildMemberCount(ctx, "g1")
	require.Error(t, err)
	_, err = s.IncompleteMessageChannelIDs(ctx, "g1")
	require.Error(t, err)
	_, err = s.Status(ctx, "db", "g1")
	require.Error(t, err)
	_, _, err = s.Query(ctx, "select 1")
	require.Error(t, err)
	_, err = s.Exec(ctx, "delete from messages")
	require.Error(t, err)
	require.Error(t, s.RebuildSearchIndexes(ctx))
	require.NoError(t, s.CheckMessageFTS(ctx))
	_, err = s.HasMessageEmbeddings(ctx, "ollama", "model", "")
	require.Error(t, err)
	_, err = s.RequeueAllEmbeddingJobs(ctx, EmbeddingDrainOptions{Provider: "ollama", Model: "model"})
	require.Error(t, err)
	_, err = s.DrainEmbeddingJobs(ctx, &fakeEmbeddingProvider{}, EmbeddingDrainOptions{Provider: "ollama", Model: "model"})
	require.Error(t, err)
}

func TestStoreReadWriteAndSearch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "t1", GuildID: "g1", Kind: "thread_public", Name: "thread", RawJSON: `{}`}))
	require.NoError(t, s.ReplaceMembers(ctx, "g1", []MemberRecord{{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `["r1"]`,
		RawJSON:     `{"bio":"Maintainer at Example","website":"https://steipete.me","github":"steipete","twitter":"steipete"}`,
	}}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "panic: database is locked",
		NormalizedContent: "panic database is locked",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m2",
		GuildID:           "g1",
		ChannelID:         "t1",
		ChannelName:       "thread",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano),
		Content:           "rate limit discussion",
		NormalizedContent: "rate limit discussion",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m3",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(2 * time.Second).Format(time.RFC3339Nano),
		Content:           "",
		NormalizedContent: "",
		RawJSON:           `{"author":{"username":"Peter"}}`,
	}))

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "panic", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m1", results[0].MessageID)

	results, err = s.SearchMessages(ctx, SearchOptions{Query: "panic", Channel: "general", Author: "Peter", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)

	results, err = s.SearchMessages(ctx, SearchOptions{Query: "panic:error", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)

	results, err = s.SearchMessages(ctx, SearchOptions{Query: "Peter", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 2)

	results, err = s.SearchMessages(ctx, SearchOptions{Query: "Peter", Limit: 10, IncludeEmpty: true})
	require.NoError(t, err)
	require.Len(t, results, 3)

	members, err := s.Members(ctx, "g1", "pet", 10)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, "steipete", members[0].GitHubLogin)
	require.Equal(t, "steipete", members[0].XHandle)
	require.Equal(t, "https://steipete.me", members[0].Website)

	members, err = s.Members(ctx, "g1", "Maintainer", 10)
	require.NoError(t, err)
	require.Len(t, members, 1)

	profile, err := s.MemberProfile(ctx, "g1", "u1", 2)
	require.NoError(t, err)
	require.Equal(t, 3, profile.MessageCount)
	require.Len(t, profile.RecentMessages, 2)
	require.Equal(t, "steipete", profile.Member.GitHubLogin)

	channels, err := s.Channels(ctx, "g1")
	require.NoError(t, err)
	require.Len(t, channels, 2)

	status, err := s.Status(ctx, dbPath, "g1")
	require.NoError(t, err)
	require.Equal(t, 1, status.GuildCount)
	require.Equal(t, 2, status.ChannelCount)
	require.Equal(t, 1, status.ThreadCount)
	require.Equal(t, 3, status.MessageCount)
	require.Equal(t, 1, status.MemberCount)
	require.Equal(t, "Guild", status.DefaultGuildName)

	oldest, newest, err := s.ChannelMessageBounds(ctx, "c1")
	require.NoError(t, err)
	require.Equal(t, "m1", oldest)
	require.Equal(t, "m3", newest)

	messageRows, err := s.ListMessages(ctx, MessageListOptions{
		Channel: "#general",
		Since:   parseTime("2000-01-01T00:00:00Z"),
	})
	require.NoError(t, err)
	require.Len(t, messageRows, 1)
	require.Equal(t, "m1", messageRows[0].MessageID)
	require.Equal(t, "Peter", messageRows[0].AuthorName)
}

func TestListMessagesWithThreadContextAndMentionLabels(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	base := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c2", GuildID: "g2", Kind: "text", Name: "other", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "alice",
		DisplayName: "Alice",
		RoleIDsJSON: `[]`,
		RawJSON:     `{}`,
	}))
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{
		GuildID:     "g2",
		UserID:      "u1",
		Username:    "other-alice",
		DisplayName: "Other Alice",
		RoleIDsJSON: `[]`,
		RawJSON:     `{}`,
	}))
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{
		{
			Record: MessageRecord{
				ID:                "root",
				GuildID:           "g1",
				ChannelID:         "c1",
				ChannelName:       "general",
				AuthorID:          "u1",
				AuthorName:        "Alice",
				CreatedAt:         base.Format(time.RFC3339Nano),
				Content:           "root mentions <@u1> and <#c1>",
				NormalizedContent: "root mentions <@u1> and <#c1>",
				RawJSON:           `{}`,
			},
			Mentions: []MentionEventRecord{{
				MessageID:  "root",
				GuildID:    "g1",
				ChannelID:  "c1",
				AuthorID:   "u1",
				TargetType: "role",
				TargetID:   "r1",
				TargetName: "Maintainers",
				EventAt:    base.Format(time.RFC3339Nano),
			}},
		},
		{
			Record: MessageRecord{
				ID:                "reply",
				GuildID:           "g1",
				ChannelID:         "c1",
				ChannelName:       "general",
				AuthorID:          "u1",
				AuthorName:        "Alice",
				CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
				Content:           "reply to root <@&r1>",
				NormalizedContent: "reply to root <@&r1>",
				ReplyToMessageID:  "root",
				RawJSON:           `{}`,
			},
			Mentions: []MentionEventRecord{{
				MessageID:  "reply",
				GuildID:    "g1",
				ChannelID:  "c1",
				AuthorID:   "u1",
				TargetType: "role",
				TargetID:   "r1",
				TargetName: "Maintainers",
				EventAt:    base.Add(time.Minute).Format(time.RFC3339Nano),
			}},
		},
	}))

	rows, err := s.ListMessagesWithThreadContext(ctx, MessageListOptions{Channel: "general", Since: base.Add(30 * time.Second), Limit: 1})
	require.NoError(t, err)
	require.Equal(t, []string{"reply", "root"}, messageRowIDs(rows))
	require.Equal(t, "reply to root @Maintainers", rows[0].DisplayContent)
	require.Equal(t, "root mentions @Alice and #general", rows[1].DisplayContent)

	merged := mergeMessageRows(rows[:1], []MessageRow{rows[0], {MessageID: "other", GuildID: "g1", ChannelID: "c1"}})
	require.Equal(t, []string{"reply", "other"}, messageRowIDs(merged))
	require.Equal(t, "@fallback", replaceDiscordMention("<@missing>", "user", "missing", "fallback"))
	require.Equal(t, "#chan", replaceDiscordMention("<#c1>", "channel", "c1", "chan"))
	require.Equal(t, "<@u2>", replaceDiscordMention("<@u2>", "user", "", "blank"))
}

func TestSearchMessagesPrefersRecentMessageIDs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))

	base := time.Date(2026, 4, 4, 0, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "1458939673664684210",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         base.Format(time.RFC3339Nano),
		Content:           "Discord first hit",
		NormalizedContent: "discord first hit",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "1489845247147118682",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "Discord newest hit",
		NormalizedContent: "discord newest hit",
		RawJSON:           `{}`,
	}))

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "Discord", Limit: 1})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "1489845247147118682", results[0].MessageID)
	require.Contains(t, results[0].Content, "newest")
}

func TestSearchMessagesSemanticRanksAndFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	base := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g2", Name: "Other", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c2", GuildID: "g1", Kind: "text", Name: "random", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c3", GuildID: "g2", Kind: "text", Name: "other", RawJSON: `{}`}))

	semanticMessages := []MessageRecord{
		{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			MessageType:       0,
			CreatedAt:         base.Format(time.RFC3339Nano),
			Content:           "cats and databases",
			NormalizedContent: "cats and databases",
			RawJSON:           `{"author":{"username":"Alice"}}`,
		},
		{
			ID:                "m2",
			GuildID:           "g1",
			ChannelID:         "c2",
			ChannelName:       "random",
			AuthorID:          "u2",
			MessageType:       0,
			CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
			Content:           "cats but weaker",
			NormalizedContent: "cats but weaker",
			RawJSON:           `{"author":{"username":"Bob"}}`,
		},
		{
			ID:                "m3",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			MessageType:       0,
			CreatedAt:         base.Add(2 * time.Minute).Format(time.RFC3339Nano),
			Content:           "dogs",
			NormalizedContent: "dogs",
			RawJSON:           `{"author":{"username":"Alice"}}`,
		},
		{
			ID:                "m4",
			GuildID:           "g2",
			ChannelID:         "c3",
			ChannelName:       "other",
			AuthorID:          "u3",
			MessageType:       0,
			CreatedAt:         base.Add(3 * time.Minute).Format(time.RFC3339Nano),
			Content:           "other guild cats",
			NormalizedContent: "other guild cats",
			RawJSON:           `{"author":{"username":"Carol"}}`,
		},
		{
			ID:                "m5",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u4",
			MessageType:       0,
			CreatedAt:         base.Add(4 * time.Minute).Format(time.RFC3339Nano),
			Content:           "",
			NormalizedContent: "",
			RawJSON:           `{"author":{"username":"Empty"}}`,
		},
	}
	for _, message := range semanticMessages {
		require.NoError(t, s.UpsertMessage(ctx, message))
	}
	require.NoError(t, insertTestEmbedding(ctx, s, "m1", "ollama", "nomic-embed-text", []float32{1, 0}))
	require.NoError(t, insertTestEmbedding(ctx, s, "m2", "ollama", "nomic-embed-text", []float32{0.9, 0.1}))
	require.NoError(t, insertTestEmbedding(ctx, s, "m3", "ollama", "nomic-embed-text", []float32{0, 1}))
	require.NoError(t, insertTestEmbedding(ctx, s, "m4", "ollama", "nomic-embed-text", []float32{1, 0}))
	require.NoError(t, insertTestEmbedding(ctx, s, "m5", "ollama", "nomic-embed-text", []float32{1, 0}))

	results, err := s.SearchMessagesSemantic(ctx, SemanticSearchOptions{
		QueryVector:  []float32{1, 0},
		Provider:     "ollama",
		Model:        "nomic-embed-text",
		InputVersion: EmbeddingInputVersion,
		Dimensions:   2,
		GuildIDs:     []string{"g1"},
		Limit:        3,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"m1", "m2", "m3"}, searchResultIDs(results))

	results, err = s.SearchMessagesSemantic(ctx, SemanticSearchOptions{
		QueryVector:  []float32{1, 0},
		Provider:     "ollama",
		Model:        "nomic-embed-text",
		InputVersion: EmbeddingInputVersion,
		Dimensions:   2,
		GuildIDs:     []string{"g1"},
		Channel:      "general",
		Author:       "Alice",
		Limit:        10,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"m1", "m3"}, searchResultIDs(results))
	require.Equal(t, "Alice", results[0].AuthorName)
	require.Equal(t, "general", results[0].ChannelName)

	results, err = s.SearchMessagesSemantic(ctx, SemanticSearchOptions{
		QueryVector:  []float32{1, 0},
		Provider:     "ollama",
		Model:        "nomic-embed-text",
		InputVersion: EmbeddingInputVersion,
		Dimensions:   2,
		GuildIDs:     []string{"g1"},
		Channel:      "general",
		Limit:        10,
		IncludeEmpty: true,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"m5", "m1", "m3"}, searchResultIDs(results))

	results, err = s.SearchMessagesSemantic(ctx, SemanticSearchOptions{
		QueryVector:  []float32{1, 0},
		Provider:     "ollama",
		Model:        "nomic-embed-text",
		InputVersion: EmbeddingInputVersion,
		Dimensions:   2,
		GuildIDs:     []string{"g1"},
		Channel:      "missing-channel",
		Limit:        10,
	})
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestSearchMessagesSemanticScoresOlderMatchesBeyondRecentWindow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	base := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "old-best",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         base.Format(time.RFC3339Nano),
		Content:           "older best semantic match",
		NormalizedContent: "older best semantic match",
		RawJSON:           `{}`,
	}))
	require.NoError(t, insertTestEmbedding(ctx, s, "old-best", "ollama", "nomic-embed-text", []float32{1, 0}))

	for i := range searchCandidateFloor + 10 {
		messageID := "newer-weak-" + strconv.Itoa(i)
		require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
			ID:                messageID,
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         base.Add(time.Duration(i+1) * time.Minute).Format(time.RFC3339Nano),
			Content:           "newer weak semantic candidate " + strconv.Itoa(i),
			NormalizedContent: "newer weak semantic candidate " + strconv.Itoa(i),
			RawJSON:           `{}`,
		}))
		require.NoError(t, insertTestEmbedding(ctx, s, messageID, "ollama", "nomic-embed-text", []float32{0, 1}))
	}

	results, err := s.SearchMessagesSemantic(ctx, SemanticSearchOptions{
		QueryVector:  []float32{1, 0},
		Provider:     "ollama",
		Model:        "nomic-embed-text",
		InputVersion: EmbeddingInputVersion,
		Dimensions:   2,
		GuildIDs:     []string{"g1"},
		Limit:        1,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"old-best"}, searchResultIDs(results))
}

func TestSearchMessagesSemanticErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}))

	_, err = s.SearchMessagesSemantic(ctx, SemanticSearchOptions{
		QueryVector:  []float32{1, 0},
		Provider:     "ollama",
		Model:        "missing-model",
		InputVersion: EmbeddingInputVersion,
		Dimensions:   2,
		Limit:        10,
	})
	require.ErrorIs(t, err, ErrNoCompatibleEmbeddings)

	_, err = s.SearchMessagesSemantic(ctx, SemanticSearchOptions{
		QueryVector: []float32{0, 0},
		Provider:    "ollama",
		Model:       "nomic-embed-text",
		Dimensions:  2,
		Limit:       10,
	})
	require.ErrorContains(t, err, "zero vector")

	require.NoError(t, insertTestEmbeddingBlob(ctx, s, "m1", "ollama", "nomic-embed-text", 2, []byte{0, 0, 0, 0}))
	_, err = s.SearchMessagesSemantic(ctx, SemanticSearchOptions{
		QueryVector:  []float32{1, 0},
		Provider:     "ollama",
		Model:        "nomic-embed-text",
		InputVersion: EmbeddingInputVersion,
		Dimensions:   2,
		Limit:        10,
	})
	require.ErrorContains(t, err, "vector length mismatch")

	require.NoError(t, insertTestEmbedding(ctx, s, "m1", "ollama", "nomic-embed-text", []float32{0, 0}))
	_, err = s.SearchMessagesSemantic(ctx, SemanticSearchOptions{
		QueryVector:  []float32{1, 0},
		Provider:     "ollama",
		Model:        "nomic-embed-text",
		InputVersion: EmbeddingInputVersion,
		Dimensions:   2,
		Limit:        10,
	})
	require.ErrorContains(t, err, "stored embedding vector is zero")
}

func TestSearchMessagesHybridFusesAndDeduplicates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	base := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))

	messages := []MessageRecord{
		{
			ID:                "m3",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			MessageType:       0,
			CreatedAt:         base.Format(time.RFC3339Nano),
			Content:           "panic stack trace",
			NormalizedContent: "panic stack trace",
			RawJSON:           `{"author":{"username":"Alice"}}`,
		},
		{
			ID:                "m2",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u2",
			MessageType:       0,
			CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
			Content:           "database worker stalled",
			NormalizedContent: "database worker stalled",
			RawJSON:           `{"author":{"username":"Bob"}}`,
		},
		{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u3",
			MessageType:       0,
			CreatedAt:         base.Add(2 * time.Minute).Format(time.RFC3339Nano),
			Content:           "panic database lock",
			NormalizedContent: "panic database lock",
			RawJSON:           `{"author":{"username":"Carol"}}`,
		},
	}
	for _, message := range messages {
		require.NoError(t, s.UpsertMessage(ctx, message))
	}
	require.NoError(t, insertTestEmbedding(ctx, s, "m1", "ollama", "nomic-embed-text", []float32{0.9, 0.1}))
	require.NoError(t, insertTestEmbedding(ctx, s, "m2", "ollama", "nomic-embed-text", []float32{1, 0}))
	require.NoError(t, insertTestEmbedding(ctx, s, "m3", "ollama", "nomic-embed-text", []float32{0, 1}))

	results, err := s.SearchMessagesHybrid(ctx, SearchOptions{
		Query:    "lock",
		GuildIDs: []string{"g1"},
		Limit:    3,
	}, SemanticSearchOptions{
		QueryVector:  []float32{1, 0},
		Provider:     "ollama",
		Model:        "nomic-embed-text",
		InputVersion: EmbeddingInputVersion,
		Dimensions:   2,
		GuildIDs:     []string{"g1"},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"m1", "m2", "m3"}, searchResultIDs(results))
}

func TestSearchMessagesHybridTieBreaksTowardFTS(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	results := fuseSearchResults(
		[]SearchResult{{MessageID: "fts", CreatedAt: created}},
		[]SearchResult{{MessageID: "semantic", CreatedAt: created.Add(time.Hour)}},
		2,
	)
	require.Equal(t, []string{"fts", "semantic"}, searchResultIDs(results))
}

func TestSearchMessagesHybridPropagatesCorruptEmbeddings(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "panic database lock",
		NormalizedContent: "panic database lock",
		RawJSON:           `{}`,
	}))
	require.NoError(t, insertTestEmbeddingBlob(ctx, s, "m1", "ollama", "nomic-embed-text", 2, []byte{0, 0, 0, 0}))

	_, err = s.SearchMessagesHybrid(ctx, SearchOptions{
		Query: "panic",
		Limit: 10,
	}, SemanticSearchOptions{
		QueryVector:  []float32{1, 0},
		Provider:     "ollama",
		Model:        "nomic-embed-text",
		InputVersion: EmbeddingInputVersion,
		Dimensions:   2,
	})
	require.ErrorContains(t, err, "vector length mismatch")
}

func insertTestEmbedding(ctx context.Context, s *Store, messageID, provider, model string, vector []float32) error {
	blob, err := EncodeEmbeddingVector(vector)
	if err != nil {
		return err
	}
	return insertTestEmbeddingBlob(ctx, s, messageID, provider, model, len(vector), blob)
}

func insertTestEmbeddingBlob(ctx context.Context, s *Store, messageID, provider, model string, dimensions int, blob []byte) error {
	_, err := s.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values(?, ?, ?, ?, ?, ?, ?)
		on conflict(message_id, provider, model, input_version) do update set
			dimensions = excluded.dimensions,
			embedding_blob = excluded.embedding_blob,
			embedded_at = excluded.embedded_at
	`, messageID, provider, model, EmbeddingInputVersion, dimensions, blob, time.Now().UTC().Format(timeLayout))
	return err
}

func searchResultIDs(results []SearchResult) []string {
	ids := make([]string, 0, len(results))
	for _, result := range results {
		ids = append(ids, result.MessageID)
	}
	return ids
}

func messageRowIDs(rows []MessageRow) []string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.MessageID)
	}
	return ids
}

func TestCheckMessageFTSProbe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.CheckMessageFTS(ctx))

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "1458939673664684210",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "searchable text",
		NormalizedContent: "searchable text",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.CheckMessageFTS(ctx))
}

func TestRebuildSearchIndexesAndGuildCounts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"bio":"Searchable profile"}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "1458939673664684210",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "rebuildable launch text",
		NormalizedContent: "rebuildable launch text",
		RawJSON:           `{}`,
	}))

	_, err = s.DB().ExecContext(ctx, `delete from message_fts`)
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `delete from member_fts`)
	require.NoError(t, err)
	require.NoError(t, s.RebuildSearchIndexes(ctx))

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "rebuildable", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	members, err := s.Members(ctx, "g1", "Searchable", 10)
	require.NoError(t, err)
	require.Len(t, members, 1)
	channelCount, err := s.GuildChannelCount(ctx, "g1")
	require.NoError(t, err)
	require.Equal(t, 1, channelCount)
	memberCount, err := s.GuildMemberCount(ctx, "g1")
	require.NoError(t, err)
	require.Equal(t, 1, memberCount)
}

func TestOpenSetsSchemaVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	var version int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version))
	require.Equal(t, storeSchemaVersion, version)
}

func TestOpenReadOnlySchemaChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.Close())

	ro, err := OpenReadOnly(ctx, dbPath)
	require.NoError(t, err)
	status, err := ro.Status(ctx, dbPath, "")
	require.NoError(t, err)
	require.Equal(t, 1, status.GuildCount)
	require.NoError(t, ro.Close())

	future, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = future.ExecContext(ctx, `pragma user_version = 999`)
	require.NoError(t, err)
	require.NoError(t, future.Close())

	_, err = OpenReadOnly(ctx, dbPath)
	require.ErrorContains(t, err, "database schema version mismatch")
}

func TestOpenFailsOnFutureSchemaVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")

	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `pragma user_version = 999`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	_, err = Open(ctx, dbPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "newer than supported")
}

func TestOpenBackfillsMissingSchemaVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")

	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `pragma user_version = 0`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	s, err = Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	var version int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version))
	require.Equal(t, storeSchemaVersion, version)
}

func TestOpenMigratesSchemaV1ToV2(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	require.NoError(t, createV1Schema(ctx, dbPath))

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `
		insert into messages(
			id, guild_id, channel_id, message_type, created_at, content,
			normalized_content, raw_json, updated_at
		) values('m1', 'g1', 'c1', 0, '2026-01-01T00:00:00Z', 'hello', 'hello', '{}', '2026-01-01T00:00:00Z')
	`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `
		insert into embedding_jobs(message_id, state, attempts, updated_at)
		values('m1', 'pending', 1, '2026-01-01T00:00:00Z')
	`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	var version int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version))
	require.Equal(t, storeSchemaVersion, version)

	_, rows, err := s.ReadOnlyQuery(ctx, "select provider, model, input_version, last_error, locked_at from embedding_jobs where message_id = 'm1'")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"", "", "", "", ""}}, rows)

	_, rows, err = s.ReadOnlyQuery(ctx, "select count(*) from message_embeddings")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestOpenMigratesUnversionedV1SchemaToV2(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	require.NoError(t, createV1Schema(ctx, dbPath))

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `
		insert into messages(
			id, guild_id, channel_id, message_type, created_at, content,
			normalized_content, raw_json, updated_at
		) values('m1', 'g1', 'c1', 0, '2026-01-01T00:00:00Z', 'hello', 'hello', '{}', '2026-01-01T00:00:00Z')
	`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `
		insert into embedding_jobs(message_id, state, attempts, updated_at)
		values('m1', 'pending', 1, '2026-01-01T00:00:00Z')
	`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `pragma user_version = 0`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	var version int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version))
	require.Equal(t, storeSchemaVersion, version)

	_, rows, err := s.ReadOnlyQuery(ctx, "select provider, model, input_version, last_error, locked_at from embedding_jobs where message_id = 'm1'")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"", "", "", "", ""}}, rows)

	_, rows, err = s.ReadOnlyQuery(ctx, "select count(*) from message_embeddings")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestOpenHealsVersionTwoMissingEmbeddingStorage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	require.NoError(t, createV1Schema(ctx, dbPath))

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `pragma user_version = 2`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	_, rows, err := s.ReadOnlyQuery(ctx, "select provider, model, input_version, last_error, locked_at from embedding_jobs")
	require.NoError(t, err)
	require.Empty(t, rows)

	_, rows, err = s.ReadOnlyQuery(ctx, "select count(*) from message_embeddings")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestReadOnlyQueryGuards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	cols, rows, err := s.ReadOnlyQuery(ctx, "select 1 as one")
	require.NoError(t, err)
	require.Equal(t, []string{"one"}, cols)
	require.Equal(t, [][]string{{"1"}}, rows)

	_, _, err = s.ReadOnlyQuery(ctx, "delete from messages")
	require.Error(t, err)
}

func createV1Schema(ctx context.Context, path string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	stmts := []string{
		`create table guilds (
			id text primary key,
			name text not null,
			icon text,
			raw_json text not null,
			updated_at text not null
		);`,
		`create table channels (
			id text primary key,
			guild_id text not null,
			parent_id text,
			kind text not null,
			name text not null,
			topic text,
			position integer,
			is_nsfw integer not null default 0,
			is_archived integer not null default 0,
			is_locked integer not null default 0,
			is_private_thread integer not null default 0,
			thread_parent_id text,
			archive_timestamp text,
			raw_json text not null,
			updated_at text not null
		);`,
		`create table members (
			guild_id text not null,
			user_id text not null,
			username text not null,
			global_name text,
			display_name text,
			nick text,
			discriminator text,
			avatar text,
			bot integer not null default 0,
			joined_at text,
			role_ids_json text not null,
			raw_json text not null,
			updated_at text not null,
			primary key (guild_id, user_id)
		);`,
		`create table messages (
			id text primary key,
			guild_id text not null,
			channel_id text not null,
			author_id text,
			message_type integer not null,
			created_at text not null,
			edited_at text,
			deleted_at text,
			content text not null,
			normalized_content text not null,
			reply_to_message_id text,
			pinned integer not null default 0,
			has_attachments integer not null default 0,
			raw_json text not null,
			updated_at text not null
		);`,
		`create table message_events (
			event_id integer primary key autoincrement,
			guild_id text not null,
			channel_id text not null,
			message_id text not null,
			event_type text not null,
			event_at text not null,
			payload_json text not null
		);`,
		`create table message_attachments (
			attachment_id text primary key,
			message_id text not null,
			guild_id text not null,
			channel_id text not null,
			author_id text,
			filename text not null,
			content_type text,
			size integer not null default 0,
			url text,
			proxy_url text,
			text_content text not null default '',
			updated_at text not null
		);`,
		`create table mention_events (
			event_id integer primary key autoincrement,
			message_id text not null,
			guild_id text not null,
			channel_id text not null,
			author_id text,
			target_type text not null,
			target_id text not null,
			target_name text not null default '',
			event_at text not null
		);`,
		`create table sync_state (
			scope text primary key,
			cursor text,
			updated_at text not null
		);`,
		`create table embedding_jobs (
			message_id text primary key,
			state text not null,
			attempts integer not null default 0,
			updated_at text not null
		);`,
		`create virtual table message_fts using fts5(
			message_id unindexed,
			guild_id unindexed,
			channel_id unindexed,
			author_id unindexed,
			author_name,
			channel_name,
			content
		);`,
		`create virtual table member_fts using fts5(
			member_key unindexed,
			guild_id unindexed,
			user_id unindexed,
			username,
			display_name,
			profile_text
		);`,
		`pragma user_version = 1;`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func TestQueryAndExec(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	affected, err := s.Exec(ctx, `
		insert into sync_state(scope, cursor, updated_at)
		values('scope:test-exec', 'cursor-1', '2026-03-08T00:00:00Z')
	`)
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)

	cols, rows, err := s.Query(ctx, `select scope, cursor from sync_state where scope = 'scope:test-exec'`)
	require.NoError(t, err)
	require.Equal(t, []string{"scope", "cursor"}, cols)
	require.Equal(t, [][]string{{"scope:test-exec", "cursor-1"}}, rows)

	_, _, err = s.Query(ctx, "   ")
	require.Error(t, err)

	_, err = s.Exec(ctx, "   ")
	require.Error(t, err)
}

func TestUpsertAndDeleteMember(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMember(ctx, MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{}`,
	}))
	rows, err := s.MemberByID(ctx, "u1")
	require.NoError(t, err)
	require.Len(t, rows, 1)

	require.NoError(t, s.DeleteMember(ctx, "g1", "u1"))
	rows, err = s.MemberByID(ctx, "u1")
	require.NoError(t, err)
	require.Empty(t, rows)

	require.NoError(t, s.ReplaceMembers(ctx, "g1", []MemberRecord{{
		GuildID:     "g1",
		UserID:      "u2",
		Username:    "other",
		RoleIDsJSON: `[]`,
		RawJSON:     `{}`,
	}}))
	rows, err = s.Members(ctx, "g1", "", 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "u2", rows[0].UserID)
}

func TestOpenTightensDBFilePerms(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("windows does not expose unix permission bits")
	}

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	require.NoError(t, os.WriteFile(dbPath, nil, 0o644))

	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestOpenCreatesQueryIndexes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	messageIndexes := indexNames(t, ctx, s.DB(), "messages")
	require.Contains(t, messageIndexes, "idx_messages_created_id")
	require.Contains(t, messageIndexes, "idx_messages_guild_created_id")
	require.Contains(t, messageIndexes, "idx_messages_channel_created_id")
	require.Contains(t, messageIndexes, "idx_messages_author_created_id")

	mentionIndexes := indexNames(t, ctx, s.DB(), "mention_events")
	require.Contains(t, mentionIndexes, "idx_mentions_guild_event")
	require.Contains(t, mentionIndexes, "idx_mentions_channel_event")
}

func TestOpenMigratesLegacyQueryIndexes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")

	sqlDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	legacy := &Store{db: sqlDB, path: dbPath}
	require.NoError(t, legacy.applyBaselineSchema(ctx))
	require.NoError(t, legacy.setSchemaVersion(ctx, 1))
	for _, indexName := range []string{
		"idx_messages_guild_created_id",
		"idx_messages_channel_created_id",
		"idx_messages_author_created_id",
		"idx_messages_created_id",
		"idx_mentions_guild_event",
		"idx_mentions_channel_event",
	} {
		_, err = sqlDB.ExecContext(ctx, `drop index if exists `+indexName)
		require.NoError(t, err)
	}
	require.NoError(t, sqlDB.Close())

	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	version, err := s.schemaVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, storeSchemaVersion, version)
	require.Contains(t, indexNames(t, ctx, s.DB(), "messages"), "idx_messages_created_id")
	require.Contains(t, indexNames(t, ctx, s.DB(), "messages"), "idx_messages_channel_created_id")
	require.Contains(t, indexNames(t, ctx, s.DB(), "mention_events"), "idx_mentions_guild_event")
}

func indexNames(t *testing.T, ctx context.Context, db *sql.DB, table string) []string {
	t.Helper()

	rows, err := db.QueryContext(ctx, `pragma index_list(`+quoteSQLString(table)+`)`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		require.NoError(t, rows.Scan(&seq, &name, &unique, &origin, &partial))
		out = append(out, name)
	}
	require.NoError(t, rows.Err())
	return out
}

func quoteSQLString(value string) string {
	return "'" + value + "'"
}

func TestEventsSyncStateAndHelpers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NotNil(t, s.DB())
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.AppendMessageEvent(ctx, "g1", "c1", "m1", "create", map[string]string{"ok": "1"}))
	require.NoError(t, s.MarkMessageDeleted(ctx, "g1", "c1", "m1", map[string]string{"deleted": "1"}))
	require.NoError(t, s.SetSyncState(ctx, "scope:test", "cursor-1"))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		EditedAt:          time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello again",
		NormalizedContent: "hello again",
		RawJSON:           `{}`,
	}))

	cursor, err := s.GetSyncState(ctx, "scope:test")
	require.NoError(t, err)
	require.Equal(t, "cursor-1", cursor)

	require.NoError(t, s.DeleteSyncState(ctx, "scope:test"))
	cursor, err = s.GetSyncState(ctx, "scope:test")
	require.NoError(t, err)
	require.Empty(t, cursor)

	_, rows, err := s.ReadOnlyQuery(ctx, "select deleted_at from messages where id = 'm1'")
	require.NoError(t, err)
	require.NotEmpty(t, rows)

	cols, rows, err := s.ReadOnlyQuery(ctx, "pragma foreign_keys")
	require.NoError(t, err)
	require.NotEmpty(t, cols)
	require.NotEmpty(t, rows)

	require.Equal(t, "1", stringify(int64(1)))
	require.Equal(t, "value", stringify("value"))
	require.Empty(t, stringify(nil))
	require.Equal(t, "abc", stringify([]byte("abc")))
	require.True(t, parseTime(time.Now().UTC().Format(time.RFC3339Nano)).After(time.Time{}))
	require.Equal(t, "?, ?, ?", placeholders(3))

	results, err := s.searchFallback(ctx, SearchOptions{Query: "hello", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)

	empty, err := s.GetSyncState(ctx, "missing")
	require.NoError(t, err)
	require.Empty(t, empty)

	require.Equal(t, "maintainers", normalizeChannelFilter("#maintainers"))
	require.Equal(t, "maintainers", normalizeChannelFilter(" maintainers "))
	require.True(t, IsReadOnlySQL("select 1"))
	require.True(t, IsReadOnlySQL("-- comment\nselect 1"))
	require.True(t, IsReadOnlySQL("with latest as (select 1 as one) select one from latest"))
	require.False(t, IsReadOnlySQL("delete from messages"))
}

func TestIncompleteMessageChannelIDs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c2", GuildID: "g1", Kind: "thread_public", Name: "thread", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c3", GuildID: "g1", Kind: "text", Name: "restricted", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c4", GuildID: "g2", Kind: "text", Name: "other", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "v1", GuildID: "g1", Kind: "voice", Name: "voice", RawJSON: `{}`}))

	require.NoError(t, s.SetSyncState(ctx, "channel:c2:history_complete", "1"))
	require.NoError(t, s.SetSyncState(ctx, "channel:c3:unavailable", "missing_access"))

	ids, err := s.IncompleteMessageChannelIDs(ctx, "g1")
	require.NoError(t, err)
	require.Equal(t, []string{"c1"}, ids)

	ids, err = s.IncompleteMessageChannelIDs(ctx, "")
	require.NoError(t, err)
	require.Equal(t, []string{"c1", "c4"}, ids)
}

func TestListMessagesFiltersAndLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "maintainers", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c2", GuildID: "g1", Kind: "text", Name: "random", RawJSON: `{}`}))
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{}`,
	}))

	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "maintainers",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         "2026-03-01T10:00:00Z",
		Content:           "first",
		NormalizedContent: "first",
		RawJSON:           `{"author":{"username":"peter"}}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m2",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "maintainers",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         "2026-03-02T10:00:00Z",
		Content:           "second",
		NormalizedContent: "second",
		RawJSON:           `{"author":{"username":"peter"}}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m3",
		GuildID:           "g1",
		ChannelID:         "c2",
		ChannelName:       "random",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         "2026-03-03T10:00:00Z",
		Content:           "ignore",
		NormalizedContent: "ignore",
		RawJSON:           `{"author":{"username":"peter"}}`,
	}))

	rows, err := s.ListMessages(ctx, MessageListOptions{
		Channel: "#maintainer",
		Since:   parseTime("2026-03-01T12:00:00Z"),
		Author:  "Peter",
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "m2", rows[0].MessageID)

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel: "maintainers",
		Limit:   1,
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "m1", rows[0].MessageID)

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel: "maintainers",
		Last:    1,
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "m2", rows[0].MessageID)

	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m4",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "maintainers",
		AuthorID:          "u2",
		MessageType:       0,
		CreatedAt:         "2026-03-04T10:00:00Z",
		Content:           "third",
		NormalizedContent: "third",
		Pinned:            true,
		HasAttachments:    true,
		RawJSON:           `{"author":{"username":"fallback-user"}}`,
	}))

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel: "c1",
		Before:  parseTime("2026-03-04T00:00:00Z"),
	})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "m1", rows[0].MessageID)
	require.Equal(t, "m2", rows[1].MessageID)

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel: "maintainers",
		Author:  "fallback-user",
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "m4", rows[0].MessageID)
	require.Equal(t, "fallback-user", rows[0].AuthorName)
	require.Equal(t, "Guild", rows[0].GuildName)
	require.True(t, rows[0].Pinned)
	require.True(t, rows[0].HasAttachments)

	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m5",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "maintainers",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         "2026-03-05T10:00:00Z",
		Content:           "",
		NormalizedContent: "",
		RawJSON:           `{"author":{"username":"peter"}}`,
	}))

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel: "maintainers",
	})
	require.NoError(t, err)
	require.Len(t, rows, 3)

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel:      "maintainers",
		IncludeEmpty: true,
	})
	require.NoError(t, err)
	require.Len(t, rows, 4)
	require.Equal(t, "m5", rows[3].MessageID)

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel: "maintainers",
		Last:    2,
	})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "m2", rows[0].MessageID)
	require.Equal(t, "m4", rows[1].MessageID)
}

func TestListMessagesWithThreadContextHydratesReplyRoot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "root",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		MessageType:       0,
		CreatedAt:         "2026-03-01T10:00:00Z",
		Content:           "root message",
		NormalizedContent: "root message",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "reply",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u2",
		MessageType:       0,
		CreatedAt:         "2026-03-02T10:00:00Z",
		Content:           "reply message",
		NormalizedContent: "reply message",
		ReplyToMessageID:  "root",
		RawJSON:           `{}`,
	}))

	rows, err := s.ListMessagesWithThreadContext(ctx, MessageListOptions{Last: 1})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "reply", rows[0].MessageID)
	require.Equal(t, "root", rows[1].MessageID)
}

func TestNormalizeFTSQueryEdgeCases(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
		want string
	}{
		{name: "empty", raw: "", want: ""},
		{name: "whitespace-only", raw: " \t \n ", want: ""},
		{name: "single-word", raw: "needle", want: `"needle"`},
		{name: "multi-word", raw: "needle haystack", want: `"needle" "haystack"`},
		{name: "operators-as-terms", raw: "AND OR NOT NEAR", want: `"AND" "OR" "NOT" "NEAR"`},
		{name: "embedded-double-quote", raw: `say"hi`, want: `"say hi"`},
		{name: "asterisk-quoted", raw: "panic*", want: `"panic*"`},
		{name: "mixed-punctuation", raw: "alpha,(beta):gamma", want: `"alpha,(beta):gamma"`},
		{name: "unicode", raw: "café 東京", want: `"café" "東京"`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, normalizeFTSQuery(tc.raw))
		})
	}
}

func TestSearchMessagesTreatsFTSSyntaxAsTerms(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))

	for _, record := range []MessageRecord{
		{
			ID:                "and-exact",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			CreatedAt:         "2026-04-25T12:00:00Z",
			Content:           "AND",
			NormalizedContent: "AND",
			RawJSON:           `{"author":{"username":"Peter"}}`,
		},
		{
			ID:                "and-lower",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u2",
			AuthorName:        "Other",
			CreatedAt:         "2026-04-25T12:01:00Z",
			Content:           "alpha and beta",
			NormalizedContent: "alpha and beta",
			RawJSON:           `{"author":{"username":"Other"}}`,
		},
		{
			ID:                "and-absent",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u3",
			AuthorName:        "Another",
			CreatedAt:         "2026-04-25T12:02:00Z",
			Content:           "alpha beta",
			NormalizedContent: "alpha beta",
			RawJSON:           `{"author":{"username":"Another"}}`,
		},
		{
			ID:                "panic-token",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u4",
			AuthorName:        "Ops",
			CreatedAt:         "2026-04-25T12:03:00Z",
			Content:           "panic",
			NormalizedContent: "panic",
			RawJSON:           `{"author":{"username":"Ops"}}`,
		},
		{
			ID:                "panic-prefix",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u5",
			AuthorName:        "Ops",
			CreatedAt:         "2026-04-25T12:04:00Z",
			Content:           "panicked",
			NormalizedContent: "panicked",
			RawJSON:           `{"author":{"username":"Ops"}}`,
		},
		{
			ID:                "panic-star",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u6",
			AuthorName:        "Ops",
			CreatedAt:         "2026-04-25T12:05:00Z",
			Content:           "panic*",
			NormalizedContent: "panic*",
			RawJSON:           `{"author":{"username":"Ops"}}`,
		},
	} {
		require.NoError(t, s.UpsertMessage(ctx, record))
	}

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "AND", Limit: 10})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"and-exact", "and-lower"}, searchResultIDs(results))

	results, err = s.SearchMessages(ctx, SearchOptions{Query: "panic*", Limit: 10})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"panic-token", "panic-star"}, searchResultIDs(results))
}
