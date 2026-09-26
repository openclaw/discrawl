package syncer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

type scopeLookupRaceClient struct {
	*fakeClient
	onLookup func()
}

func (c *scopeLookupRaceClient) Channel(context.Context, string) (*discordgo.Channel, error) {
	c.onLookup()
	return &discordgo.Channel{ID: "new", GuildID: "g", Type: discordgo.ChannelTypeGuildText}, nil
}

func TestScopeDelayedUnknownChannelLookupCannotUndoGatewayMove(t *testing.T) {
	t.Parallel()
	s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	h := &tailHandler{store: s, exclusions: newChannelScope([]string{"blocked"}, nil, nil)}
	h.client = &scopeLookupRaceClient{fakeClient: &fakeClient{}, onLookup: func() {
		require.NoError(t, h.OnChannelUpsert(t.Context(), &discordgo.Channel{ID: "new", GuildID: "g", ParentID: "blocked", Type: discordgo.ChannelTypeGuildText}))
	}}
	require.NoError(t, h.OnMessageCreate(t.Context(), &discordgo.Message{ID: "99", GuildID: "g", ChannelID: "new", Content: "excluded", Timestamp: time.Now()}))
	var n int
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from messages`).Scan(&n))
	require.Zero(t, n)
	var parent, scope string
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select parent_id,collection_scope from channels where id='new'`).Scan(&parent, &scope))
	require.Equal(t, "blocked", parent)
	require.Equal(t, "excluded", scope)
}

func TestScopeRestartRetainsExcludedMetadataAndRejectsDescendant(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	g := &discordgo.Guild{ID: "g", Channels: []*discordgo.Channel{
		{ID: "blocked", Type: discordgo.ChannelTypeGuildCategory},
		{ID: "forum", ParentID: "blocked", Type: discordgo.ChannelTypeGuildForum},
	}, Threads: []*discordgo.Channel{{ID: "thread", ParentID: "forum", Type: discordgo.ChannelTypeGuildPublicThread}}}
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g", RawJSON: marshalJSONString(g, "{}")}))
	for range 2 {
		h := &tailHandler{store: s, guilds: makeGuildSet([]string{"g"}), exclusions: newChannelScope([]string{"blocked"}, nil, nil)}
		require.NoError(t, h.seedChannelExclusions(ctx))
		require.NoError(t, h.OnMessageCreate(ctx, &discordgo.Message{ID: "100", GuildID: "g", ChannelID: "thread", Content: "must not enter archive", Timestamp: time.Now()}))
		var count int
		require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from messages`).Scan(&count))
		require.Zero(t, count)
		var scope string
		require.NoError(t, s.DB().QueryRowContext(ctx, `select collection_scope from channels where id='thread'`).Scan(&scope))
		require.Equal(t, "excluded", scope)
	}
}

func TestScopeMoveWhileAttachmentFetchIsInFlightCannotCommit(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("evidence"))
	}))
	defer server.Close()
	s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	h := &tailHandler{store: s, attachmentTextEnabled: true, exclusions: newChannelScope([]string{"blocked"}, nil, nil)}
	require.NoError(t, h.OnChannelUpsert(t.Context(), &discordgo.Channel{ID: "c", GuildID: "g", Type: discordgo.ChannelTypeGuildText}))
	done := make(chan error, 1)
	go func() {
		done <- h.OnMessageCreate(t.Context(), &discordgo.Message{ID: "200", GuildID: "g", ChannelID: "c", Timestamp: time.Now(), Attachments: []*discordgo.MessageAttachment{{ID: "a", Filename: "a.txt", ContentType: "text/plain", URL: server.URL, Size: 8}}})
	}()
	<-started
	oldObservation := time.Now().Add(-time.Minute)
	require.NoError(t, h.OnChannelUpsert(t.Context(), &discordgo.Channel{ID: "c", GuildID: "g", ParentID: "blocked", Type: discordgo.ChannelTypeGuildText}))
	// A delayed old REST response must not overwrite the newer Gateway move.
	require.NoError(t, s.UpsertObservedChannel(t.Context(), store.ChannelRecord{ID: "c", GuildID: "g", Kind: "text", RawJSON: `{}`}, oldObservation))
	close(release)
	require.ErrorIs(t, <-done, store.ErrCollectionScope)
	var n int
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from messages`).Scan(&n))
	require.Zero(t, n)
	var parent string
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select parent_id from channels where id='c'`).Scan(&parent))
	require.Equal(t, "blocked", parent)
}

func TestScopeUnknownAllowedChannelIsResolvedWithoutRestart(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	c := &fakeClient{channelByID: map[string]*discordgo.Channel{
		"new":      {ID: "new", GuildID: "g", ParentID: "category", Type: discordgo.ChannelTypeGuildText},
		"category": {ID: "category", GuildID: "g", Type: discordgo.ChannelTypeGuildCategory},
	}}
	h := &tailHandler{store: s, client: c, exclusions: newChannelScope([]string{"blocked"}, nil, nil)}
	require.NoError(t, h.OnMessageCreate(ctx, &discordgo.Message{ID: "101", GuildID: "g", ChannelID: "new", Content: "allowed", Timestamp: time.Now()}))
	var body, scope string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select content from messages where id='101'`).Scan(&body))
	require.Equal(t, "allowed", body)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select collection_scope from channels where id='new'`).Scan(&scope))
	require.Equal(t, "allowed", scope)
	bad := &discordgo.Message{ID: "102", GuildID: "g", ChannelID: "missing", Content: "unresolved", Timestamp: time.Now()}
	require.Error(t, h.OnMessageCreate(ctx, bad))
	var n int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from messages where id='102'`).Scan(&n))
	require.Zero(t, n)
}

func TestScopeParentMoveInvalidatesInFlightDecision(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	h := &tailHandler{store: s, exclusions: newChannelScope([]string{"blocked"}, nil, nil)}
	for _, c := range []*discordgo.Channel{
		{ID: "allowed", GuildID: "g", Type: discordgo.ChannelTypeGuildCategory},
		{ID: "forum", GuildID: "g", ParentID: "allowed", Type: discordgo.ChannelTypeGuildForum},
		{ID: "thread", GuildID: "g", ParentID: "forum", Type: discordgo.ChannelTypeGuildPublicThread},
	} {
		require.NoError(t, h.OnChannelUpsert(ctx, c))
	}
	allowed, err := h.allowMessageChannel(ctx, "g", "thread")
	require.NoError(t, err)
	require.True(t, allowed)
	// Simulate a concurrent REST/catalog write after the message's preliminary
	// scope check and before its canonical transaction. The stale cache must not
	// be sufficient to commit, including after reopening the store.
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "forum", GuildID: "g", ParentID: "blocked", Kind: "forum", RawJSON: `{}`}))
	m, err := buildMessageMutation(ctx, &discordgo.Message{ID: "103", GuildID: "g", ChannelID: "thread", Content: "raced", Timestamp: time.Now()}, "", "", false, false)
	require.NoError(t, err)
	m.Options.ScopePolicy = h.scopePolicy()
	require.ErrorIs(t, s.UpsertMessages(ctx, []store.MessageMutation{m}), store.ErrCollectionScope)
	require.NoError(t, h.seedChannelExclusions(ctx))
	require.Equal(t, "excluded", h.scopeDecision("thread"))
	var n int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from messages`).Scan(&n))
	require.Zero(t, n)
}

func TestScopeDecisionsFailClosedOnIncompleteCrossGuildAndCycles(t *testing.T) {
	t.Parallel()
	e := newChannelScope([]string{"blocked"}, nil, nil)
	for _, catalog := range []map[string]store.ChannelScope{
		{"c": {ChannelRow: store.ChannelRow{ID: "c", GuildID: "g", ParentID: "missing"}}},
		{"c": {ChannelRow: store.ChannelRow{ID: "c", GuildID: "g", ParentID: "p"}}, "p": {ChannelRow: store.ChannelRow{ID: "p", GuildID: "other"}}},
		{"c": {ChannelRow: store.ChannelRow{ID: "c", GuildID: "g", ParentID: "c"}}},
	} {
		require.Equal(t, "unknown", e.scope("c", catalog))
	}
}
