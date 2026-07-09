package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/store/storedb"
	"github.com/stretchr/testify/require"
	moderncsqlite "modernc.org/sqlite"
)

var coverageObserverDriverID atomic.Uint64

type coverageQueryObservation struct {
	contextErrAtStart  error
	contextErrAtReturn error
	queryErr           error
}

type coverageObserverDriver struct {
	driver.Driver
	observations chan<- coverageQueryObservation
}

func (d *coverageObserverDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return &coverageObserverConn{Conn: conn, observations: d.observations}, nil
}

type coverageObserverConn struct {
	driver.Conn
	observations chan<- coverageQueryObservation
}

func (c *coverageObserverConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	if query != globalCoverageChannelQuery {
		return queryer.QueryContext(ctx, query, args)
	}

	observation := coverageQueryObservation{contextErrAtStart: ctx.Err()}
	rows, err := queryer.QueryContext(ctx, query, args)
	observation.contextErrAtReturn = ctx.Err()
	observation.queryErr = err
	c.observations <- observation
	return rows, err
}

func TestCoverageReportsGuildChannelAndWiretapState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild One", RawJSON: `{}`}))
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g2", Name: "Guild Two", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c2", GuildID: "g1", Kind: "text", Name: "channel-c2", RawJSON: `{"source":"discord_desktop"}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c3", GuildID: "g2", Kind: "text", Name: "empty", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "v1", GuildID: "g1", Kind: "voice", Name: "Voice", RawJSON: `{}`}))
	for _, message := range []MessageRecord{
		{ID: "deleted-early", GuildID: "g1", ChannelID: "c1", CreatedAt: "2026-05-01T10:00:00Z", Content: "deleted early", NormalizedContent: "deleted early", RawJSON: `{}`},
		{ID: "m1", GuildID: "g1", ChannelID: "c1", CreatedAt: "2026-06-01T10:00:00Z", Content: "one", NormalizedContent: "one", RawJSON: `{}`},
		{ID: "m2", GuildID: "g1", ChannelID: "c1", CreatedAt: "2026-06-02T10:00:00Z", Content: "two", NormalizedContent: "two", RawJSON: `{}`},
		{ID: "m3", GuildID: "g1", ChannelID: "c2", CreatedAt: "2026-06-03T10:00:00Z", Content: "three", NormalizedContent: "three", RawJSON: `{}`},
		{ID: "deleted-late", GuildID: "g1", ChannelID: "c1", CreatedAt: "2026-07-01T10:00:00Z", Content: "deleted late", NormalizedContent: "deleted late", RawJSON: `{}`},
	} {
		require.NoError(t, s.UpsertMessage(ctx, message))
	}
	require.NoError(t, s.MarkMessageDeleted(ctx, "g1", "c1", "deleted-early", nil))
	require.NoError(t, s.MarkMessageDeleted(ctx, "g1", "c1", "deleted-late", nil))
	require.NoError(t, s.SetSyncState(ctx, "channel:c1:history_complete", "1"))
	require.NoError(t, s.SetSyncState(ctx, "sync:last_success", "2026-06-04T10:00:00Z"))
	require.NoError(t, s.SetSyncState(ctx, "wiretap:last_import", "2026-06-04T11:00:00Z"))
	require.NoError(t, s.SetWiretapImportStats(ctx, WiretapImportStats{
		FilesScanned: 4, Messages: 3, Channels: 2, SkippedMessages: 5, SkippedChannels: 6,
		FinishedAt: time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC),
	}))
	require.NoError(t, s.RecordFailure(ctx, FailureRef{Operation: "sync_messages", Source: "discord", GuildID: "g1", ChannelID: "c1"}, errors.New("known channel failure")))
	require.NoError(t, s.RecordFailure(ctx, FailureRef{Operation: "embed_message", Source: "embeddings", MessageID: "missing"}, errors.New("unscoped failure")))

	generatedAt := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	report, err := s.Coverage(ctx, "", generatedAt)
	require.NoError(t, err)
	require.Equal(t, generatedAt, report.GeneratedAt)
	require.Equal(t, CoverageTotals{
		GuildCount: 2, MessageCount: 3, ChannelCount: 4, MessageChannelCount: 3,
		NamedChannelCount: 3, SyntheticChannelCount: 1, HistoryCompleteChannelCount: 1,
		KnownFailureCount: 2, UnscopedKnownFailureCount: 1,
	}, report.Totals)
	require.Equal(t, time.Date(2026, 6, 4, 10, 0, 0, 0, time.UTC), report.LastBotSyncAt)
	require.Equal(t, 5, report.Wiretap.SkippedMessages)
	require.Equal(t, 6, report.Wiretap.SkippedChannels)
	require.Equal(t, time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC), report.Wiretap.LastImportAt)
	require.Len(t, report.Guilds, 2)
	require.Equal(t, "g1", report.Guilds[0].ID)
	require.Equal(t, 3, report.Guilds[0].MessageCount)
	require.Equal(t, time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC), report.Guilds[0].EarliestMessageAt)
	require.Equal(t, time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC), report.Guilds[0].LatestMessageAt)
	require.Equal(t, "c1", report.Guilds[0].Channels[0].ID)
	require.NotNil(t, report.Guilds[0].Channels[0].HistoryComplete)
	require.True(t, *report.Guilds[0].Channels[0].HistoryComplete)
	require.Equal(t, 1, report.Guilds[0].KnownFailureCount)
	require.Equal(t, 1, report.Guilds[0].Channels[0].KnownFailureCount)
	require.True(t, report.Guilds[0].Channels[1].Synthetic)
	require.Equal(t, 0, report.Guilds[0].Channels[2].MessageCount)
	require.Equal(t, "v1", report.Guilds[0].Channels[2].ID)

	filtered, err := s.Coverage(ctx, "g1", generatedAt)
	require.NoError(t, err)
	require.Equal(t, 3, filtered.Totals.MessageCount)
	require.Equal(t, 3, filtered.Totals.ChannelCount)
	require.Equal(t, "g1", filtered.Guilds[0].ID)
	require.Equal(t, report.Guilds[0].Channels, filtered.Guilds[0].Channels)

	empty, err := s.Coverage(ctx, "g2", generatedAt)
	require.NoError(t, err)
	require.Equal(t, CoverageTotals{
		GuildCount: 1, ChannelCount: 1, MessageChannelCount: 1, NamedChannelCount: 1,
	}, empty.Totals)
	require.Equal(t, "c3", empty.Guilds[0].Channels[0].ID)
	require.Equal(t, 0, empty.Guilds[0].Channels[0].MessageCount)
	_, err = s.Coverage(ctx, "missing", generatedAt)
	require.ErrorContains(t, err, `guild "missing" not found`)
}

func TestCoverageQueryTimeoutsAndCancellation(t *testing.T) {
	t.Parallel()

	started := time.Now()
	globalCtx, globalCancel := withCoverageQueryTimeout(context.Background(), "")
	globalDeadline, ok := globalCtx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, started.Add(4*time.Minute), globalDeadline, time.Second)
	globalCancel()

	started = time.Now()
	filteredCtx, filteredCancel := withCoverageQueryTimeout(context.Background(), "g1")
	filteredDeadline, ok := filteredCtx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, started.Add(2*time.Minute), filteredDeadline, time.Second)
	filteredCancel()

	parentDeadline := time.Now().Add(time.Minute)
	parentCtx, parentCancel := context.WithDeadline(context.Background(), parentDeadline)
	defer parentCancel()
	childCtx, childCancel := withCoverageQueryTimeout(parentCtx, "")
	defer childCancel()
	childDeadline, ok := childCtx.Deadline()
	require.True(t, ok)
	require.Equal(t, parentDeadline, childDeadline)

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, guildID := range []string{"", "g1"} {
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := s.Coverage(canceledCtx, guildID, time.Now())
		require.ErrorIs(t, err, context.Canceled)
	}
}

func TestCoverageGlobalQueryPlanMaterializesSequentialMessageScan(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()

	rows, err := s.DB().QueryContext(ctx, "explain query plan "+globalCoverageChannelQuery)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var details []string
	for rows.Next() {
		var selectID, parentID, unused int
		var detail string
		require.NoError(t, rows.Scan(&selectID, &parentID, &unused, &detail))
		details = append(details, detail)
	}
	require.NoError(t, rows.Err())

	plan := strings.ToLower(strings.Join(details, "\n"))
	require.Contains(t, strings.ToLower(globalCoverageChannelQuery), "with message_coverage as materialized")
	require.Contains(t, strings.ToLower(globalCoverageChannelQuery), "from messages not indexed")
	require.Contains(t, plan, "materialize message_coverage")
	require.Contains(t, plan, "scan messages")
	require.NotContains(t, plan, "idx_messages_channel_id")
	require.NotContains(t, plan, "idx_messages_channel_created_id")
}

func TestCoverageGlobalQueryHonorsParentDeadlineInFlight(t *testing.T) {
	const (
		messageCount  = 300_000
		parentTimeout = 50 * time.Millisecond
	)

	s, observations := newCoverageDeadlineTestStore(t, messageCount)
	parentCtx, cancel := context.WithTimeout(context.Background(), parentTimeout)
	defer cancel()
	parentDeadline, ok := parentCtx.Deadline()
	require.True(t, ok)

	started := time.Now()
	_, err := s.Coverage(parentCtx, "", time.Now())
	elapsed := time.Since(started)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "query channel coverage")
	require.NotContains(t, err.Error(), "list coverage guilds")
	require.False(t, time.Now().Before(parentDeadline))

	select {
	case observation := <-observations:
		require.NoError(t, observation.contextErrAtStart)
		require.ErrorIs(t, observation.contextErrAtReturn, context.DeadlineExceeded)
		require.ErrorIs(t, observation.queryErr, context.DeadlineExceeded)
	default:
		t.Fatalf("global channel query was not observed; Coverage returned %v", err)
	}
	t.Logf("canceled global channel query over %d generated messages after %s", messageCount, elapsed)
}

func TestCoverageDeltaSince(t *testing.T) {
	previous := CoverageReport{Totals: CoverageTotals{MessageCount: 4, ChannelCount: 3, NamedChannelCount: 2, SyntheticChannelCount: 1}}
	current := CoverageReport{Totals: CoverageTotals{MessageCount: 7, ChannelCount: 4, NamedChannelCount: 4, SyntheticChannelCount: 0}}
	require.Equal(t, CoverageDelta{Messages: 3, Channels: 1, NamedChannels: 2, SyntheticChannels: -1}, CoverageDeltaSince(current, previous))
}

func TestSyntheticChannelClassificationUsesGeneratedPlaceholder(t *testing.T) {
	require.True(t, isSyntheticChannel("123456123456", "channel-123456"))
	require.True(t, isSyntheticChannel("123456123456", "dm-123456"))
	require.False(t, isSyntheticChannel("123456123456", "general"))
	require.False(t, isSyntheticChannel("123456123456", "channel-other"))
}

func newCoverageDeadlineTestStore(t *testing.T, messageCount int) (*Store, <-chan coverageQueryObservation) {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "coverage-deadline.db")
	seedDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = seedDB.ExecContext(ctx, `
		create table guilds (
			id text primary key,
			name text not null
		);
		create table channels (
			id text primary key,
			guild_id text not null,
			name text not null,
			kind text not null
		);
		create table messages (
			id text primary key,
			channel_id text not null,
			created_at text not null,
			deleted_at text
		);
		create table sync_state (
			scope text primary key,
			cursor text,
			updated_at text
		);
		create table failure_ledger (
			guild_id text not null default '',
			channel_id text not null default '',
			resolved_at text
		);
		insert into guilds (id, name) values ('g1', 'Guild One');
		insert into channels (id, guild_id, name, kind) values ('c1', 'g1', 'general', 'text');
	`)
	require.NoError(t, err)
	_, err = seedDB.ExecContext(ctx, `
		with
			digit(n) as (
				values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)
			),
			generated(n) as (
				select 1 + a.n + 10*b.n + 100*c.n + 1000*d.n + 10000*e.n + 100000*f.n
				from digit a
				cross join digit b
				cross join digit c
				cross join digit d
				cross join digit e
				cross join digit f
				limit ?
			)
		insert into messages (id, channel_id, created_at)
		select
			printf('message-%06d', n),
			printf('generated-channel-%06d', n),
			'2026-01-01T00:00:00Z'
		from generated
	`, messageCount)
	require.NoError(t, err)
	require.NoError(t, seedDB.Close())

	observations := make(chan coverageQueryObservation, 1)
	driverName := fmt.Sprintf("coverage-observer-%d", coverageObserverDriverID.Add(1))
	sql.Register(driverName, &coverageObserverDriver{
		Driver:       &moderncsqlite.Driver{},
		observations: observations,
	})
	db, err := sql.Open(driverName, dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	return &Store{db: db, q: storedb.New(db), path: dbPath}, observations
}
