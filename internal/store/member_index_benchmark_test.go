package store

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemberChangeIndexSyntheticProof(t *testing.T) {
	runMemberIndexProof(t, 2000)
}

// Run explicitly with -benchtime=1x. Only writable opens contribute to ns/op.
func BenchmarkMemberChangeIndexMigration(b *testing.B) {
	for _, count := range []int{200_000, 1_000_000} {
		b.Run(fmt.Sprintf("members=%d", count), func(b *testing.B) {
			b.StopTimer()
			var migration, reopen time.Duration
			var growth int64
			for range b.N {
				result := runMemberIndexProof(b, count)
				migration += result.migration
				reopen += result.reopen
				growth += result.growth
			}
			b.ReportMetric(float64(migration.Nanoseconds())/float64(b.N), "migration-open-ns/op")
			b.ReportMetric(float64(reopen.Nanoseconds())/float64(b.N), "indexed-reopen-ns/op")
			b.ReportMetric(float64(growth)/float64(b.N), "growth-bytes/op")
		})
	}
}

type memberIndexProof struct {
	migration time.Duration
	reopen    time.Duration
	growth    int64
}

func runMemberIndexProof(tb testing.TB, count int) memberIndexProof {
	tb.Helper()
	ctx := tb.Context()
	root := tb.TempDir()
	// Release each iteration's database instead of retaining b.N large fixtures.
	defer func() { require.NoError(tb, os.RemoveAll(root)) }()
	path := filepath.Join(root, "synthetic.db")
	s, err := Open(ctx, path)
	require.NoError(tb, err)
	defer func() { require.NoError(tb, s.Close()) }()
	_, err = s.DB().ExecContext(ctx, `drop index idx_members_updated_identity`)
	require.NoError(tb, err)

	// Generate fake members directly, then build the real member FTS before timing.
	// The composite identities repeat users across 100 guilds, with timestamp ties
	// and every fifth member represented by a removal tombstone.
	_, err = s.DB().ExecContext(ctx, `
		with recursive sequence(i) as (
			values(0) union all select i+1 from sequence where i+1 < ?
		)
		insert into members(
			guild_id, user_id, username, global_name, display_name, nick,
			discriminator, avatar, bot, joined_at, role_ids_json, raw_json,
			updated_at, deleted_at, deletion_source, deletion_reason
		)
		select printf('guild-%03d', i%100), printf('user-%06d', i/100),
			printf('synthetic-%06d', i/100), 'Synthetic Member',
			case when i%3 = 0 then null else 'Synthetic Display' end,
			'', '0', null, i%2, '2026-01-01T00:00:00Z', '["role-1"]',
			'{"synthetic":true}',
			strftime('%Y-%m-%dT%H:%M:%SZ', '2026-01-02', '+' || (i%1000) || ' seconds'),
			case when i%5 = 0 then '2026-01-02T01:00:00Z' end,
			case when i%5 = 0 then 'synthetic' end,
			case when i%5 = 0 then 'synthetic removal' end
		from sequence
	`, count)
	require.NoError(tb, err)
	require.NoError(tb, s.RebuildMemberSearchIndex(ctx))
	var rows, tombstones, guilds, users, timestamps int
	require.NoError(tb, s.DB().QueryRowContext(ctx, `
		select count(*), sum(deleted_at is not null),
			count(distinct guild_id), count(distinct user_id), count(distinct updated_at)
		from members
	`).Scan(&rows, &tombstones, &guilds, &users, &timestamps))
	require.Equal(tb, count, rows)
	require.Equal(tb, (count+4)/5, tombstones)
	require.Equal(tb, min(count, 100), guilds)
	require.Equal(tb, (count+99)/100, users)
	require.Equal(tb, min(count, 1000), timestamps)
	var sqliteVersion string
	require.NoError(tb, s.DB().QueryRowContext(ctx, `select sqlite_version()`).Scan(&sqliteVersion))
	beforePlan := memberIndexQueryPlan(tb, s)
	require.NotContains(tb, beforePlan, "idx_members_updated_identity")
	before := readMemberIndexState(tb, s)
	require.NoError(tb, s.Close())
	beforeFile, err := os.Stat(path)
	require.NoError(tb, err)
	require.Equal(tb, before.pages*before.pageSize, beforeFile.Size())

	if b, ok := tb.(*testing.B); ok {
		b.StartTimer()
	}
	start := time.Now()
	s, err = Open(ctx, path)
	migration := time.Since(start)
	if b, ok := tb.(*testing.B); ok {
		b.StopTimer()
	}
	require.NoError(tb, err)
	after := readMemberIndexState(tb, s)
	require.Equal(tb, before.hash, after.hash)
	require.Equal(tb, before.version, after.version)
	require.Equal(tb, before.pageSize, after.pageSize)
	afterPlan := memberIndexQueryPlan(tb, s)
	require.Contains(tb, afterPlan, "idx_members_updated_identity")
	require.NotContains(tb, afterPlan, "TEMP B-TREE")
	var integrity string
	require.NoError(tb, s.DB().QueryRowContext(ctx, `pragma quick_check`).Scan(&integrity))
	require.Equal(tb, "ok", integrity)
	require.NoError(tb, s.Close())
	afterFile, err := os.Stat(path)
	require.NoError(tb, err)
	require.Equal(tb, after.pages*after.pageSize, afterFile.Size())

	if b, ok := tb.(*testing.B); ok {
		b.StartTimer()
	}
	start = time.Now()
	s, err = Open(ctx, path)
	reopen := time.Since(start)
	if b, ok := tb.(*testing.B); ok {
		b.StopTimer()
	}
	require.NoError(tb, err)
	reopened := readMemberIndexState(tb, s)
	require.Equal(tb, after, reopened)
	require.Equal(tb, afterPlan, memberIndexQueryPlan(tb, s))
	tb.Logf("local synthetic only: go=%s sqlite=%s rows=%d tombstones=%d guilds=%d users=%d timestamps=%d",
		runtime.Version(), sqliteVersion, rows, tombstones, guilds, users, timestamps)
	tb.Logf("migration_open=%s indexed_reopen=%s page_size=%d pages_before=%d pages_after=%d file_bytes_before=%d file_bytes_after=%d growth_bytes=%d",
		migration, reopen, before.pageSize, before.pages, after.pages, beforeFile.Size(), afterFile.Size(), afterFile.Size()-beforeFile.Size())
	tb.Logf("row_sha256_before=%x row_sha256_after=%x row_sha256_reopened=%x schema_version=%d quick_check=%s",
		before.hash, after.hash, reopened.hash, after.version, integrity)
	tb.Logf("plan_before=%s plan_after=%s", beforePlan, afterPlan)
	return memberIndexProof{migration: migration, reopen: reopen, growth: afterFile.Size() - beforeFile.Size()}
}

type memberIndexState struct {
	hash     [sha256.Size]byte
	pages    int64
	pageSize int64
	version  int
}

func readMemberIndexState(tb testing.TB, s *Store) memberIndexState {
	tb.Helper()
	ctx := tb.Context()
	var state memberIndexState
	require.NoError(tb, s.DB().QueryRowContext(ctx, `pragma page_count`).Scan(&state.pages))
	require.NoError(tb, s.DB().QueryRowContext(ctx, `pragma page_size`).Scan(&state.pageSize))
	require.NoError(tb, s.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&state.version))
	require.Equal(tb, storeSchemaVersion, state.version)
	rows, err := s.DB().QueryContext(ctx, `select rowid, * from members order by guild_id, user_id`)
	require.NoError(tb, err)
	defer func() { require.NoError(tb, rows.Close()) }()
	columns, err := rows.Columns()
	require.NoError(tb, err)
	values := make([]any, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}
	hash := sha256.New()
	encoder := json.NewEncoder(hash)
	require.NoError(tb, encoder.Encode(columns))
	for rows.Next() {
		require.NoError(tb, rows.Scan(dest...))
		require.NoError(tb, encoder.Encode(values))
	}
	require.NoError(tb, rows.Err())
	copy(state.hash[:], hash.Sum(nil))
	return state
}

func memberIndexQueryPlan(tb testing.TB, s *Store) string {
	tb.Helper()
	rows, err := s.DB().QueryContext(tb.Context(), `
		explain query plan select * from members
		where updated_at >= ? order by updated_at, guild_id, user_id
	`, "2026-01-02T00:10:00Z")
	require.NoError(tb, err)
	defer func() { require.NoError(tb, rows.Close()) }()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(tb, rows.Scan(&id, &parent, &unused, &detail))
		details = append(details, detail)
	}
	require.NoError(tb, rows.Err())
	return strings.Join(details, "; ")
}
