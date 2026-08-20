package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
)

const (
	zeroResultForumChannelID     = "1111111111111111"
	zeroResultTextChannelID      = "2222222222222222"
	zeroResultEmptyTextChannelID = "3333333333333333"
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
		ID: zeroResultTextChannelID, GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID: zeroResultEmptyTextChannelID, GuildID: "g1", Kind: "text", Name: "quiet", RawJSON: `{}`,
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
	require.NoError(t, s.Close())
	return ctx, cfgPath
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
	require.Contains(t, stderr.String(), "forum channels hold no messages directly")
	require.Contains(t, stderr.String(), "parent_id='"+zeroResultForumChannelID+"'")
}

func TestExplainEmptyResults_TextChannelNeverHadMessages(t *testing.T) {
	ctx, cfgPath := setupZeroResultStore(t)

	var stdout, stderr bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath, "search", "--channel", zeroResultEmptyTextChannelID, "nothing",
	}, &stdout, &stderr))

	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "note: channel "+zeroResultEmptyTextChannelID+" (quiet, kind=text) has no messages in the local mirror")
	require.NotContains(t, stderr.String(), "forum channels hold no messages directly")
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
	require.Contains(t, stderr.String(), "note: channel has 1 messages but none within the last 7 days")
	require.Contains(t, stderr.String(), "newest: 2020-01-01T00:00:00Z")
	require.Contains(t, stderr.String(), "try without --days")
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
	require.Contains(t, stderr.String(), `note: no message contains all 2 terms together; "alpha" alone matches 1 message(s)`)
	require.Contains(t, stderr.String(), "FTS terms are ANDed")
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
