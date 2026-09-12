package cli

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
)

const (
	zeroResultForumChannelID      = "1111111111111111"
	zeroResultTextChannelID       = "2222222222222222"
	zeroResultEmptyTextChannelID  = "3333333333333333"
	zeroResultAttachmentChannelID = "4444444444444444"
	zeroResultForumThreadID       = "5555555555555555"
)

func setupZeroResultStore(t *testing.T) (ctx context.Context, cfgPath string) {
	t.Helper()
	ctx = context.Background()
	dir := t.TempDir()
	cfgPath = filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID: zeroResultForumChannelID, GuildID: "g1", Kind: "forum", Name: "help-desk", RawJSON: `{}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID: zeroResultForumThreadID, GuildID: "g1", Kind: "public_thread", Name: "how-do-i",
		ParentID: zeroResultForumChannelID, ThreadParentID: zeroResultForumChannelID, RawJSON: `{}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID: zeroResultTextChannelID, GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID: zeroResultEmptyTextChannelID, GuildID: "g1", Kind: "text", Name: "quiet", RawJSON: `{}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID: zeroResultAttachmentChannelID, GuildID: "g1", Kind: "text", Name: "screenshots", RawJSON: `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         zeroResultTextChannelID,
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		CreatedAt:         "2020-01-01T00:00:00Z",
		Content:           "alpha appears here on its own",
		NormalizedContent: "alpha appears here on its own",
		RawJSON:           `{}`,
	}))
	// An attachment-only message: stored, but with no normalized content, so
	// every query drops it unless --include-empty is set.
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:             "m-attach",
		GuildID:        "g1",
		ChannelID:      zeroResultAttachmentChannelID,
		ChannelName:    "screenshots",
		AuthorID:       "u1",
		AuthorName:     "Peter",
		CreatedAt:      "2020-02-01T00:00:00Z",
		HasAttachments: true,
		RawJSON:        `{}`,
	}))
	require.NoError(t, s.Close())
	return ctx, cfgPath
}

func newZeroResultRuntime(t *testing.T, ctx context.Context, cfgPath string) (*runtime, *bytes.Buffer, func()) {
	t.Helper()
	cfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	s, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	stderr := &bytes.Buffer{}
	rt := &runtime{ctx: ctx, cfg: cfg, store: s, stdout: io.Discard, stderr: stderr, now: time.Now}
	return rt, stderr, func() { _ = s.Close() }
}

// A --channel filter that never resolved to an id stays a name fragment for
// the query, which a channel_id stats query cannot reproduce. Such a run gets
// no note rather than one counted over a wider scope.
func TestExplainEmptyResults_UnresolvedChannelNameGetsNoNote(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	rt, stderr, cleanup := newZeroResultRuntime(t, ctx, cfgPath)
	defer cleanup()

	_, ok := rt.newZeroResultScope("some-unresolved-name", []string{"g1"}, false)
	require.False(t, ok)

	scope, ok := rt.newZeroResultScope("", []string{"g1"}, false)
	require.True(t, ok)
	require.Empty(t, scope.channelID)

	scope, ok = rt.newZeroResultScope(zeroResultTextChannelID, nil, false)
	require.True(t, ok)
	require.Equal(t, zeroResultTextChannelID, scope.channelID)

	// The search entry point honours the same gate end to end.
	rt.explainEmptySearch(store.SearchOptions{Query: "alpha zulu", Channel: "some-unresolved-name", Limit: 20}, "fts")
	require.Empty(t, stderr.String())
}

// Note 1: a channel that has never had any messages -- forum case adds the
// "query its post threads" hint on top of the base note.
func TestExplainEmptyResults_ForumChannelNeverHadMessages(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "messages", "--channel", zeroResultForumChannelID,
	}, &stdout, &stderr))

	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "note: channel "+zeroResultForumChannelID+" (help-desk, kind=forum) has no messages in the local mirror")
	require.Contains(t, stderr.String(), "a forum holds its posts as separate thread channels")
	// The hint must name supported commands, not raw SQL.
	require.Contains(t, stderr.String(), "`discrawl --json channels list`")
	require.Contains(t, stderr.String(), "`discrawl messages --channel THREAD_ID`")
	require.NotContains(t, stderr.String(), "select id from channels")
}

func TestExplainEmptyResults_TextChannelNeverHadMessages(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "search", "--channel", zeroResultEmptyTextChannelID, "nothing",
	}, &stdout, &stderr))

	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "note: channel "+zeroResultEmptyTextChannelID+" (quiet, kind=text) has no messages in the local mirror")
	require.NotContains(t, stderr.String(), "a forum holds its posts")
}

// Finding 3: a channel whose only messages are empty/attachment-only. The
// channel has rows, so a stats query that looked only at deleted_at would
// call it non-empty and print nothing at all.
func TestExplainEmptyResults_AttachmentOnlyChannelPointsAtIncludeEmpty(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	for _, args := range [][]string{
		{"--config", cfgPath, "messages", "--channel", zeroResultAttachmentChannelID},
		{"--config", cfgPath, "search", "--channel", zeroResultAttachmentChannelID, "anything"},
	} {
		var stdout, stderr bytes.Buffer
		require.NoError(t, Run(ctx, args, &stdout, &stderr))
		require.Empty(t, stdout.String())
		require.Contains(t, stderr.String(), "note: all 1 messages in channel "+zeroResultAttachmentChannelID+" are empty or attachment-only")
		require.Contains(t, stderr.String(), "discrawl messages --channel "+zeroResultAttachmentChannelID+" --include-empty")
		require.NotContains(t, stderr.String(), "has no messages in the local mirror")
	}
}

// The recommendation in that note has to work: --include-empty returns the row.
func TestExplainEmptyResults_IncludeEmptyRecommendationReturnsRows(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "messages", "--channel", zeroResultAttachmentChannelID, "--include-empty",
	}, &stdout, &stderr))

	require.NotEmpty(t, stdout.String())
	require.Empty(t, stderr.String())
}

// Once --include-empty is already set, the note is about the real remaining
// cause and must not recommend the flag the user just passed.
func TestExplainEmptyResults_IncludeEmptyNoteSuppressedWhenFlagAlreadySet(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "messages", "--channel", zeroResultAttachmentChannelID,
		"--include-empty", "--author", "nobody-posted-this",
	}, &stdout, &stderr))

	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())
}

// Note 2: a --days window that excludes every message in a channel that
// does have messages.
func TestExplainEmptyResults_DaysWindowExcludesEverything(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "messages", "--channel", zeroResultTextChannelID, "--days", "7",
	}, &stdout, &stderr))

	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "note: 1 messages in scope but none within the last 7 days")
	require.Contains(t, stderr.String(), "newest: 2020-01-01T00:00:00Z")
	require.Contains(t, stderr.String(), "try without --days")
}

// Finding 4: the same window note has to fire guild-wide, with no --channel
// to hang the stats query off.
func TestExplainEmptyResults_DaysWindowGuildWide(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "messages", "--guild", "g1", "--days", "7",
	}, &stdout, &stderr))

	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "note: 1 messages in scope but none within the last 7 days")
	require.Contains(t, stderr.String(), "try without --days")
}

// Finding 5: --before is its own half of the window and gets its own note,
// including when --since is set as well.
func TestExplainEmptyResults_BeforeWindowExcludesEverything(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "messages", "--channel", zeroResultTextChannelID, "--before", "2019-01-01T00:00:00Z",
	}, &stdout, &stderr))

	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "note: 1 messages in scope but none before 2019-01-01T00:00:00Z")
	require.Contains(t, stderr.String(), "oldest: 2020-01-01T00:00:00Z")
	require.Contains(t, stderr.String(), "try without --before")

	stdout.Reset()
	stderr.Reset()
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "messages", "--channel", zeroResultTextChannelID,
		"--since", "2021-01-01T00:00:00Z", "--before", "2019-01-01T00:00:00Z",
	}, &stdout, &stderr))
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "try without --since")
	require.Contains(t, stderr.String(), "try without --before")
}

// A --before that the data does sit inside must not be blamed.
func TestExplainEmptyResults_BeforeWindowNoteSuppressedWhenDataIsInside(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "messages", "--channel", zeroResultTextChannelID,
		"--before", "2021-01-01T00:00:00Z", "--author", "nobody-posted-this",
	}, &stdout, &stderr))

	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())
}

// The date-window note must not fire when some other filter (here,
// --author) is what actually excluded the results -- the newest message is
// inside the window, so blaming --days would be a false diagnostic.
func TestExplainEmptyResults_DaysWindowNoteSuppressedWhenAuthorFilterExcludes(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	s, err := store.Open(ctx, filepath.Join(filepath.Dir(cfgPath), "discrawl.db"))
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m2",
		GuildID:           "g1",
		ChannelID:         zeroResultTextChannelID,
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		CreatedAt:         time.Now().UTC().Format(time.RFC3339),
		Content:           "fresh message",
		NormalizedContent: "fresh message",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.Close())

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "messages", "--channel", zeroResultTextChannelID, "--days", "7", "--author", "nobody-posted-this",
	}, &stdout, &stderr))

	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())
}

// Note 3: a multi-word FTS query returns nothing together, but one term
// alone matches.
func TestExplainEmptyResults_MultiTermANDHint(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "search", "alpha beta",
	}, &stdout, &stderr))

	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), `note: no message contains all 2 terms together; "alpha" alone matches`)
	require.Contains(t, stderr.String(), "Every term is required")
	// Finding 1: the recommendation has to be one this path can honour.
	require.Contains(t, stderr.String(), "search one distinctive term and narrow the result with --channel or --author")
	require.NotContains(t, stderr.String(), "exact phrase")
}

// Finding 1, the other half: the note's advice returns rows, and the advice
// it replaced does not -- quoting is not phrase search on this path.
func TestExplainEmptyResults_RecommendedSingleTermReturnsRows(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "search", "--channel", zeroResultTextChannelID, "alpha",
	}, &stdout, &stderr))
	require.NotEmpty(t, stdout.String())
	require.Empty(t, stderr.String())

	// A "phrase" whose words appear in the wrong order still matches, which
	// is why "try an exact phrase" was not a usable recommendation.
	stdout.Reset()
	stderr.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", `"own here"`}, &stdout, &stderr))
	require.NotEmpty(t, stdout.String())
}

// Finding 2: a quoted multi-word query has the same implicit-AND semantics
// as an unquoted one, so it gets the same note.
func TestExplainEmptyResults_QuotedMultiTermStillGetsANDHint(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "search", `"alpha beta"`,
	}, &stdout, &stderr))

	require.Empty(t, stdout.String())
	// The reported term is the term the index sees, with no stray quote.
	require.Contains(t, stderr.String(), `note: no message contains all 2 terms together; "alpha" alone matches`)
}

// Finding 8: the probe searches are bounded, so a long query does not issue
// one extra search per word. "alpha" is the only matching term in both
// queries below; it is reachable within the bound in the first and past the
// bound in the second, which pins the number of probes actually issued.
func TestExplainEmptyResults_TermProbesAreBounded(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)
	require.Equal(t, 4, maxZeroResultTermProbes)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "search", "zzz1 zzz2 zzz3 alpha",
	}, &stdout, &stderr))
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), `"alpha" alone matches`)

	stdout.Reset()
	stderr.Reset()
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "search", "zzz1 zzz2 zzz3 zzz4 zzz5 alpha",
	}, &stdout, &stderr))
	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())
}

// When neither term matches individually, no extra note is printed.
func TestExplainEmptyResults_MultiTermNoHintWhenNoTermMatches(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "search", "zzz yyy",
	}, &stdout, &stderr))

	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())
}

// Finding 9: a semantic search over a channel that has messages but no
// embeddings for the configured provider/model.
func TestExplainEmptyResults_SemanticWithoutEmbeddings(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	rt, stderr, cleanup := newZeroResultRuntime(t, ctx, cfgPath)
	defer cleanup()
	rt.cfg.Search.Embeddings.Enabled = true
	rt.cfg.Search.Embeddings.Provider = "openai"
	rt.cfg.Search.Embeddings.Model = "text-embedding-3-small"

	opts := store.SearchOptions{Query: "alpha", Channel: zeroResultTextChannelID, Limit: 20}
	rt.explainEmptySearch(opts, "semantic")

	require.Contains(t, stderr.String(), "note: none of the 1 messages in scope have embeddings for provider=openai model=text-embedding-3-small")
	require.Contains(t, stderr.String(), "discrawl embed")
	require.Contains(t, stderr.String(), "--mode fts")
}

// The recommended fallback in that note returns rows.
func TestExplainEmptyResults_SemanticFallbackRecommendationReturnsRows(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "search", "--mode", "fts", "--channel", zeroResultTextChannelID, "alpha",
	}, &stdout, &stderr))
	require.NotEmpty(t, stdout.String())
	require.Empty(t, stderr.String())
}

// None of the notes should ever land on stdout, or appear at all once
// results ARE returned or --json is set.
func TestExplainEmptyResults_SuppressedWhenResultsReturnedOrJSON(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "alpha"}, &stdout, &stderr))
	require.NotEmpty(t, stdout.String())
	require.Empty(t, stderr.String())

	stdout.Reset()
	stderr.Reset()
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "--json", "search", "alpha beta",
	}, &stdout, &stderr))
	require.NotEmpty(t, stdout.String())
	require.Empty(t, stderr.String())

	stdout.Reset()
	stderr.Reset()
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "--json", "messages", "--channel", zeroResultForumChannelID,
	}, &stdout, &stderr))
	require.NotEmpty(t, stdout.String())
	require.Empty(t, stderr.String())
}
