package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestCloudPublishReplacesDeletedMessages(t *testing.T) {
	for _, live := range []int{0, 1, discrawlCloudBatchSize + 1} {
		t.Run(fmt.Sprintf("live=%d", live), func(t *testing.T) {
			ctx := context.Background()
			source, err := store.Open(ctx, filepath.Join(t.TempDir(), "source.db"))
			require.NoError(t, err)
			defer func() { _ = source.Close() }()
			for i := range live + 1 {
				require.NoError(t, source.UpsertMessage(ctx, store.MessageRecord{
					ID: fmt.Sprint(i + 1), GuildID: "guild", ChannelID: "channel",
					Content: "retained body", NormalizedContent: "retained body",
					CreatedAt: "2026-01-01T00:00:00Z", RawJSON: `{}`,
				}))
			}
			require.NoError(t, source.UpsertMessage(ctx, store.MessageRecord{
				ID: "dm", GuildID: "@me", ChannelID: "dm", Content: "local only",
				NormalizedContent: "local only", CreatedAt: "2026-01-01T00:00:00Z", RawJSON: `{}`,
			}))
			remote, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "remote.db"))
			require.NoError(t, err)
			defer func() { _ = remote.Close() }()
			_, err = remote.ExecContext(ctx, `create table messages(message_id text primary key, content text)`)
			require.NoError(t, err)
			resetIncomplete, continuations, finalizations := 0, 0, 0
			// Model the independently reviewed service contract, not its handler.
			// A small reset batch exercises exact-boundary and retry behavior.
			ingest := func(ctx context.Context, app, archive string, req crawlremote.IngestRequest) (crawlremote.IngestResult, error) {
				require.Equal(t, "discrawl", app)
				require.Equal(t, "fixture", archive)
				require.Equal(t, discrawlMessageColumns, req.Columns)
				require.NotContains(t, req.Columns, "deleted_at")
				if req.Cursor == "" {
					result, err := remote.ExecContext(ctx, `delete from messages where rowid in (select rowid from messages limit 2)`)
					require.NoError(t, err)
					deleted, err := result.RowsAffected()
					require.NoError(t, err)
					if deleted >= 2 {
						resetIncomplete++
						if len(req.Rows) > 0 {
							return crawlremote.IngestResult{}, &crawlremote.Error{Status: 409, Code: "reset_incomplete"}
						}
						return crawlremote.IngestResult{ResetIncomplete: true}, nil
					}
				} else {
					continuations++
				}
				for _, row := range req.Rows {
					_, err := remote.ExecContext(ctx, `insert or replace into messages values(?, ?)`, row[0], row[5])
					require.NoError(t, err)
				}
				if req.Final {
					finalizations++
				}
				return crawlremote.IngestResult{RowsAccepted: int64(len(req.Rows)), Complete: req.Final}, nil
			}
			publish := func(want int) {
				t.Helper()
				accepted, err := publishIngestRows(ctx, source.DB(), discrawlMessageExportSQL, ingest,
					"fixture", crawlremote.IngestManifest{App: "discrawl", SchemaName: "discrawl-cloud-v1"},
					"messages", discrawlMessageColumns, true)
				require.NoError(t, err)
				require.Equal(t, int64(want), accepted)
				count, err := countCloudRows(ctx, remote, `select count(*) from messages`)
				require.NoError(t, err)
				require.Equal(t, int64(want), count)
			}
			publish(live + 1)
			require.NoError(t, source.MarkMessageDeleted(ctx, "guild", "channel", "1", map[string]any{"id": "1"}))
			publish(live)
			var count int
			require.NoError(t, remote.QueryRowContext(ctx, `select count(*) from messages where message_id = '1' or message_id = 'dm'`).Scan(&count))
			require.Zero(t, count)
			require.Equal(t, 2, finalizations)
			if live > 0 {
				require.Positive(t, resetIncomplete)
			}
			if live > discrawlCloudBatchSize {
				require.Positive(t, continuations)
			}
			var body, deleted string
			require.NoError(t, source.DB().QueryRowContext(ctx, `select content, deleted_at from messages where id = '1'`).Scan(&body, &deleted))
			require.Equal(t, "retained body", body)
			require.NotEmpty(t, deleted)
			_, _, _, messages, err := cloudPublishCounts(ctx, source.DB())
			require.NoError(t, err)
			require.Equal(t, int64(live), messages)

			path, cleanup, err := sqliteSnapshotPath(ctx, source.DB())
			require.NoError(t, err)
			defer cleanup()
			bundle, err := sql.Open("sqlite", path)
			require.NoError(t, err)
			defer func() { _ = bundle.Close() }()
			require.NoError(t, bundle.QueryRowContext(ctx, `select count(*) from messages`).Scan(&count))
			require.Equal(t, live, count)
			require.NoError(t, bundle.QueryRowContext(ctx, `select count(*) from messages where message_id = '1' or guild_id = '@me'`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestCloudPublishCountsMatchLiveExportScope(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	for _, guild := range []string{"live", "deleted", "@me"} {
		require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: guild, Name: guild, RawJSON: `{}`}))
		require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: guild, UserID: "user", Username: "user", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	}
	_, err = s.DB().ExecContext(ctx, `update guilds set deleted_at = '2026-01-01T00:00:00Z' where id = 'deleted';
		update members set deleted_at = '2026-01-01T00:00:00Z' where guild_id = 'deleted'`)
	require.NoError(t, err)
	guilds, _, members, _, err := cloudPublishCounts(ctx, s.DB())
	require.NoError(t, err)
	require.Equal(t, int64(1), guilds)
	require.Equal(t, int64(1), members)
}

func TestCloudPublishEmptyResetFailureIsNotSuccess(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	want := errors.New("synthetic reset failure")
	calls := 0
	ingest := func(_ context.Context, _, _ string, req crawlremote.IngestRequest) (crawlremote.IngestResult, error) {
		calls++
		require.Empty(t, req.Cursor)
		require.Empty(t, req.Rows)
		if calls == 1 {
			return crawlremote.IngestResult{ResetIncomplete: true}, nil
		}
		return crawlremote.IngestResult{}, want
	}
	accepted, err := publishIngestRows(ctx, s.DB(), discrawlMessageExportSQL, ingest,
		"fixture", crawlremote.IngestManifest{App: "discrawl"}, "messages", discrawlMessageColumns, true)
	require.ErrorIs(t, err, want)
	require.Zero(t, accepted)
	require.Equal(t, 2, calls)
}
