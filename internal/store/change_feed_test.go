package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// feedCursor is what a reader persists between captures.
type feedCursor struct {
	GenerationID string
	Seq          int64
	Digest       string
}

// feedReader is the reference implementation of the reader algorithm in
// docs/guides/change-feed.md. It mirrors one table's rows.
type feedReader struct {
	table      string
	cursor     *feedCursor
	rows       map[string]string
	reconciles int
	replayed   int
}

func (r *feedReader) capture(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var generationID string
	require.NoError(t, tx.QueryRowContext(ctx, `select generation_id from change_feed_tables where table_name=?`, r.table).Scan(&generationID))
	var floor int64
	var compacted string
	require.NoError(t, tx.QueryRowContext(ctx, `select compacted_through,coalesce(compacted_entry,'') from change_feed_meta where singleton=1`).Scan(&floor, &compacted))
	head, err := changeFeedHead(ctx, tx)
	require.NoError(t, err)
	if !r.cursorValid(ctx, t, tx, generationID, floor, compacted, head) {
		r.rows = snapshotTable(ctx, t, tx, r.table)
		r.reconciles++
	} else {
		r.replay(ctx, t, tx, head)
	}
	digest := entryDigest(ctx, t, tx, head)
	if digest == "" && head == floor {
		digest = compacted
	}
	r.cursor = &feedCursor{GenerationID: generationID, Seq: head, Digest: digest}
}

func (r *feedReader) cursorValid(ctx context.Context, t *testing.T, tx *sql.Tx, generationID string, floor int64, compacted string, head int64) bool {
	c := r.cursor
	if c == nil || c.GenerationID != generationID || c.Seq < floor || c.Seq > head {
		return false
	}
	digest := entryDigest(ctx, t, tx, c.Seq)
	if digest == "" {
		// Only the floor may be absent, and it must be the entry this
		// cursor recorded (or an empty boundary on both sides).
		return c.Seq == floor && c.Digest == compacted
	}
	return digest == c.Digest
}

func (r *feedReader) replay(ctx context.Context, t *testing.T, tx *sql.Tx, head int64) {
	last := map[string]string{}
	var order []string
	func() {
		rows, err := tx.QueryContext(ctx, `select op,row_key from change_log where table_name=? and seq>? and seq<=? order by seq`, r.table, r.cursor.Seq, head)
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var op, key string
			require.NoError(t, rows.Scan(&op, &key))
			if _, ok := last[key]; !ok {
				order = append(order, key)
			}
			last[key] = op
		}
		require.NoError(t, rows.Err())
	}()
	for _, key := range order {
		row, ok := lookupRow(ctx, t, tx, r.table, key)
		if ok {
			require.NotEqual(t, ChangeOpDelete, last[key], "row present after a final delete entry: %s %s", r.table, key)
			r.rows[key] = row
		} else {
			require.Equal(t, ChangeOpDelete, last[key], "row absent without a delete entry: %s %s", r.table, key)
			delete(r.rows, key)
		}
		r.replayed++
	}
}

func entryDigest(ctx context.Context, t *testing.T, tx *sql.Tx, seq int64) string {
	t.Helper()
	var digest string
	err := tx.QueryRowContext(ctx, `select `+changeEntryDigestSQL+` from change_log where seq=?`, seq).Scan(&digest)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	require.NoError(t, err)
	return digest
}

func primaryKey(ctx context.Context, t *testing.T, q changeFeedDB, table string) []string {
	t.Helper()
	columns, err := changeFeedColumns(ctx, q, table)
	require.NoError(t, err)
	keys := make([]string, 0)
	for pk := 1; ; pk++ {
		found := false
		for _, c := range columns {
			if c.pk == pk {
				keys = append(keys, c.name)
				found = true
			}
		}
		if !found {
			return keys
		}
	}
}

// rowJSON captures every column with its storage class, the precision a
// snapshot reader observes.
func rowSelect(ctx context.Context, t *testing.T, q changeFeedDB, table string) (string, string) {
	t.Helper()
	columns, err := changeFeedColumns(ctx, q, table)
	require.NoError(t, err)
	parts := make([]string, 0, len(columns))
	for _, c := range columns {
		ref := quoteIdent(c.name)
		parts = append(parts, fmt.Sprintf(`'%s',json_array(typeof(%s),case typeof(%s) when 'blob' then hex(%s) else %s end)`, c.name, ref, ref, ref, ref))
	}
	keys := primaryKey(ctx, t, q, table)
	keyParts := make([]string, 0, len(keys))
	for _, k := range keys {
		keyParts = append(keyParts, quoteIdent(k))
	}
	return "json_object(" + strings.Join(parts, ",") + ")", "json_array(" + strings.Join(keyParts, ",") + ")"
}

func snapshotTable(ctx context.Context, t *testing.T, q changeFeedDB, table string) map[string]string {
	t.Helper()
	row, key := rowSelect(ctx, t, q, table)
	rows, err := q.QueryContext(ctx, `select `+key+`,`+row+` from `+quoteIdent(table))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		require.NoError(t, rows.Scan(&k, &v))
		out[k] = v
	}
	require.NoError(t, rows.Err())
	return out
}

func lookupRow(ctx context.Context, t *testing.T, tx *sql.Tx, table, key string) (string, bool) {
	t.Helper()
	row, _ := rowSelect(ctx, t, tx, table)
	keys := primaryKey(ctx, t, tx, table)
	where := make([]string, 0, len(keys))
	for i, k := range keys {
		where = append(where, fmt.Sprintf(`%s=json_extract(?1,'$[%d]')`, quoteIdent(k), i))
	}
	var v string
	err := tx.QueryRowContext(ctx, `select `+row+` from `+quoteIdent(table)+` where `+strings.Join(where, " and "), key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false
	}
	require.NoError(t, err)
	return v, true
}

func openChangeFeedStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

func seedChangeFeedGuild(t *testing.T, s *Store) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g", Name: "g", RawJSON: `{}`}))
	for _, c := range []string{"c1", "c2"} {
		require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: c, GuildID: "g", Kind: "text", Name: c, RawJSON: `{}`}))
	}
}

func changeFeedMutation(id, channel, content string, attachments []string, mentions int) MessageMutation {
	m := MessageMutation{Record: MessageRecord{
		ID: id, GuildID: "g", ChannelID: channel, AuthorID: "u", CreatedAt: "2026-10-28T00:00:00Z",
		Content: content, NormalizedContent: content, HasAttachments: len(attachments) > 0, RawJSON: `{}`,
	}}
	for _, a := range attachments {
		m.Attachments = append(m.Attachments, AttachmentRecord{
			AttachmentID: a, MessageID: id, GuildID: "g", ChannelID: channel, AuthorID: "u",
			Filename: a + ".txt", ContentType: "text/plain", Size: 3, TextContent: content + " " + a, TextStatus: "succeeded",
		})
	}
	for i := range mentions {
		m.Mentions = append(m.Mentions, MentionEventRecord{
			MessageID: id, GuildID: "g", ChannelID: channel, AuthorID: "u",
			TargetType: "user", TargetID: fmt.Sprintf("t%d", i), TargetName: content, EventAt: "2026-10-28T00:00:00Z",
		})
	}
	return m
}

func changeFeedEntries(t *testing.T, s *Store, table string) [][2]string {
	t.Helper()
	rows, err := s.DB().QueryContext(t.Context(), `select op,row_key from change_log where table_name=? order by seq`, table)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var out [][2]string
	for rows.Next() {
		var op, key string
		require.NoError(t, rows.Scan(&op, &key))
		out = append(out, [2]string{op, key})
	}
	require.NoError(t, rows.Err())
	return out
}

func feedGeneration(t *testing.T, s *Store, table string) (int64, string, string) {
	t.Helper()
	var generation int64
	var id, reason string
	require.NoError(t, s.DB().QueryRowContext(t.Context(), `select generation,generation_id,reason from change_feed_tables where table_name=?`, table).Scan(&generation, &id, &reason))
	return generation, id, reason
}

func lastEntries(entries [][2]string, n int) [][2]string {
	return entries[len(entries)-n:]
}

func TestChangeFeedRecordsInsertsUpdatesAndDeletes(t *testing.T) {
	t.Parallel()
	s, _ := openChangeFeedStore(t)
	ctx := t.Context()
	seedChangeFeedGuild(t, s)

	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m1", "c1", "hello", []string{"a1"}, 1)}))
	// The writer inserts the attachment with its text status in one statement.
	require.Equal(t, [][2]string{{"i", `["a1"]`}}, changeFeedEntries(t, s, "message_attachments"))
	require.Equal(t, [][2]string{{"i", `[1]`}}, changeFeedEntries(t, s, "mention_events"))

	// An identical rewrite changes no relationship row and logs nothing.
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m1", "c1", "hello", []string{"a1"}, 1)}))
	require.Len(t, changeFeedEntries(t, s, "message_attachments"), 1)
	require.Equal(t, [][2]string{{"i", `[1]`}}, changeFeedEntries(t, s, "mention_events"))

	// An edit updates relationship rows in place: the attachment keeps its
	// row and the mention keeps its event ID.
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m1", "c1", "edited", []string{"a1"}, 1)}))
	require.Equal(t, [][2]string{{"i", `["a1"]`}, {"u", `["a1"]`}}, changeFeedEntries(t, s, "message_attachments"))
	require.Equal(t, [][2]string{{"i", `[1]`}, {"u", `[1]`}}, changeFeedEntries(t, s, "mention_events"))
	// The previous successful extraction is retained once.
	require.Len(t, changeFeedEntries(t, s, "attachment_text_history"), 1)

	require.NoError(t, s.UpdateAttachmentFetchStatus(ctx, "a1", "2026-10-28T01:00:00Z", "failed", "boom"))
	require.Equal(t, [][2]string{{"u", `["a1"]`}}, lastEntries(changeFeedEntries(t, s, "message_attachments"), 1))

	// A same-value update is not a change.
	before := len(changeFeedEntries(t, s, "mention_events"))
	_, err := s.DB().ExecContext(ctx, `update mention_events set target_name=target_name`)
	require.NoError(t, err)
	require.Len(t, changeFeedEntries(t, s, "mention_events"), before)

	// A storage-class change is a change even with identical bytes.
	before = len(changeFeedEntries(t, s, "message_attachments"))
	_, err = s.DB().ExecContext(ctx, `update message_attachments set filename=cast(filename as blob)`)
	require.NoError(t, err)
	require.Len(t, changeFeedEntries(t, s, "message_attachments"), before+1)

	// A key change records the old key's deletion and the new key.
	_, err = s.DB().ExecContext(ctx, `update mention_events set event_id=100 where event_id=1`)
	require.NoError(t, err)
	require.Equal(t, [][2]string{{"d", `[1]`}, {"u", `[100]`}}, lastEntries(changeFeedEntries(t, s, "mention_events"), 2))

	// Ignored history inserts write nothing.
	historyBefore := len(changeFeedEntries(t, s, "attachment_text_history"))
	_, err = s.DB().ExecContext(ctx, `insert or ignore into attachment_text_history select * from attachment_text_history`)
	require.NoError(t, err)
	require.Len(t, changeFeedEntries(t, s, "attachment_text_history"), historyBefore)

	require.NoError(t, s.DeleteGuildData(ctx, "g"))
	require.Equal(t, [][2]string{{"d", `["a1"]`}}, lastEntries(changeFeedEntries(t, s, "message_attachments"), 1))
	require.Equal(t, [][2]string{{"d", `[100]`}}, lastEntries(changeFeedEntries(t, s, "mention_events"), 1))
}

func TestChangeFeedHistoryTables(t *testing.T) {
	t.Parallel()
	s, _ := openChangeFeedStore(t)
	ctx := t.Context()
	seedChangeFeedGuild(t, s)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m1", "c1", "first", nil, 0)}))
	_, err := s.DB().ExecContext(ctx, `insert into message_embeddings(message_id,provider,model,input_version,dimensions,embedding_blob,embedded_at) values('m1','p','m','v1',1,x'0000803f','2026-10-28T00:00:00Z')`)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m1", "c1", "second", nil, 0)}))
	require.Equal(t, [][2]string{{"i", `["m1","p","m","v1","2026-10-28T00:00:00Z"]`}}, changeFeedEntries(t, s, "message_embedding_history"))
}

func TestChangeFeedOpenIsIdempotent(t *testing.T) {
	t.Parallel()
	s, path := openChangeFeedStore(t)
	ctx := t.Context()
	var before int64
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma schema_version`).Scan(&before))
	gen, id, reason := feedGeneration(t, s, "mention_events")
	require.Equal(t, int64(1), gen)
	require.Equal(t, "enabled", reason)
	require.NoError(t, s.Close())

	reopened, err := Open(ctx, path)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	var after int64
	require.NoError(t, reopened.DB().QueryRowContext(ctx, `pragma schema_version`).Scan(&after))
	require.Equal(t, before, after, "a routine open must not change the schema")
	gen2, id2, _ := feedGeneration(t, reopened, "mention_events")
	require.Equal(t, gen, gen2)
	require.Equal(t, id, id2)
}

func TestChangeFeedBumpsGenerationWhenItCannotExpressHistory(t *testing.T) {
	t.Parallel()
	s, path := openChangeFeedStore(t)
	ctx := t.Context()
	_, firstID, _ := feedGeneration(t, s, "message_attachments")

	// A migration that adds a column changes every row without row writes.
	_, err := s.DB().ExecContext(ctx, `alter table message_attachments add column change_feed_probe text not null default 'x'`)
	require.NoError(t, err)
	// Writes while a trigger was missing are unrecorded.
	_, err = s.DB().ExecContext(ctx, `drop trigger change_feed_mention_events_ad`)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	reopened, err := Open(ctx, path)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	gen, id, reason := feedGeneration(t, reopened, "message_attachments")
	require.Equal(t, int64(2), gen)
	require.NotEqual(t, firstID, id)
	require.Equal(t, "schema_changed", reason)
	gen, _, reason = feedGeneration(t, reopened, "mention_events")
	require.Equal(t, int64(2), gen)
	require.Equal(t, "triggers_restored", reason)
	gen, _, _ = feedGeneration(t, reopened, "message_embedding_history")
	require.Equal(t, int64(1), gen)

	var triggerSQL string
	require.NoError(t, reopened.DB().QueryRowContext(ctx, `select sql from sqlite_master where name='change_feed_message_attachments_au'`).Scan(&triggerSQL))
	require.Contains(t, triggerSQL, "change_feed_probe")

	tx, err := reopened.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, BumpChangeFeedGenerationTx(ctx, tx, "snapshot_import", "mention_events"))
	require.NoError(t, BumpChangeFeedGenerationTx(ctx, tx, "snapshot_import", "messages"))
	require.Error(t, BumpChangeFeedGenerationTx(ctx, tx, "snapshot_import", "sync_state"))
	require.NoError(t, tx.Rollback())
}

func TestChangeFeedPartialFeedStartsNewGenerations(t *testing.T) {
	t.Parallel()
	for _, table := range []string{"change_log", "change_feed_meta"} {
		s, path := openChangeFeedStore(t)
		ctx := t.Context()
		seedChangeFeedGuild(t, s)
		require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m1", "c1", "x", nil, 1)}))
		// Drop triggers too, so writers keep working while the table is gone.
		for _, tracked := range ChangeFeedTables() {
			for _, suffix := range []string{"ai", "au", "ad"} {
				_, err := s.DB().ExecContext(ctx, `drop trigger `+quoteIdent("change_feed_"+tracked+"_"+suffix))
				require.NoError(t, err)
			}
		}
		_, err := s.DB().ExecContext(ctx, `drop table `+table)
		require.NoError(t, err)
		require.NoError(t, s.Close())
		reopened, err := Open(ctx, path)
		require.NoError(t, err)
		for _, tracked := range ChangeFeedTables() {
			generation, _, reason := feedGeneration(t, reopened, tracked)
			require.Equal(t, int64(2), generation, table+" "+tracked)
			require.Equal(t, "feed_restored", reason, table+" "+tracked)
		}
		require.NoError(t, reopened.Close())
	}
}

func TestChangeFeedRecoveryAfterCompactionKeepsSequencesAboveFloor(t *testing.T) {
	t.Parallel()
	s, path := openChangeFeedStore(t)
	ctx := t.Context()
	seedChangeFeedGuild(t, s)
	for i := range 5 {
		require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation(fmt.Sprintf("m%d", i), "c1", "x", nil, 1)}))
	}
	_, err := s.DB().ExecContext(ctx, `update change_log set recorded_at_ms=0`)
	require.NoError(t, err)
	_, err = s.CompactChangeFeed(ctx, time.Now(), ChangeFeedRetention)
	require.NoError(t, err)
	r := &feedReader{table: "mention_events"}
	r.capture(ctx, t, s.DB())
	head := feedHead(t, s)
	require.Equal(t, head, r.cursor.Seq)
	for _, tracked := range ChangeFeedTables() {
		for _, suffix := range []string{"ai", "au", "ad"} {
			_, err := s.DB().ExecContext(ctx, `drop trigger `+quoteIdent("change_feed_"+tracked+"_"+suffix))
			require.NoError(t, err)
		}
	}
	_, err = s.DB().ExecContext(ctx, `drop table change_log`)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	reopened, err := Open(ctx, path)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	r.capture(ctx, t, reopened.DB())
	require.Equal(t, 2, r.reconciles)
	require.NoError(t, reopened.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("late", "c1", "x", nil, 1)}))
	var seq int64
	require.NoError(t, reopened.DB().QueryRowContext(ctx, `select min(seq) from change_log`).Scan(&seq))
	require.Equal(t, head+1, seq)
	r.capture(ctx, t, reopened.DB())
	require.Equal(t, 2, r.reconciles)
	require.Equal(t, snapshotTable(ctx, t, reopened.DB(), "mention_events"), r.rows)
}

func TestChangeFeedRecoveredEmptyHeadIsAStableCursor(t *testing.T) {
	t.Parallel()
	s, path := openChangeFeedStore(t)
	ctx := t.Context()
	seedChangeFeedGuild(t, s)
	for i := range 5 {
		require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation(fmt.Sprintf("m%d", i), "c1", "x", nil, 1)}))
	}
	_, err := s.DB().ExecContext(ctx, `update change_log set recorded_at_ms=0`)
	require.NoError(t, err)
	_, err = s.CompactChangeFeed(ctx, time.Now(), ChangeFeedRetention)
	require.NoError(t, err)
	head := feedHead(t, s)
	_, err = s.DB().ExecContext(ctx, `delete from change_feed_meta`)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	reopened, err := Open(ctx, path)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	r := &feedReader{table: "mention_events"}
	r.capture(ctx, t, reopened.DB())
	r.capture(ctx, t, reopened.DB())
	r.capture(ctx, t, reopened.DB())
	require.Equal(t, 1, r.reconciles)
	require.Equal(t, head, r.cursor.Seq)
}

// A reader parked exactly on the floor still detects an in-place restore
// whose reused sequences were later compacted away.
func TestChangeFeedRestoreDetectedAfterCompactionRemovesCursorEntry(t *testing.T) {
	t.Parallel()
	s, path := openChangeFeedStore(t)
	ctx := t.Context()
	seedChangeFeedGuild(t, s)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m1", "c1", "x", nil, 1)}))
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m2", "c1", "x", nil, 1)}))
	backup := filepath.Join(t.TempDir(), "backup.db")
	_, err := s.DB().ExecContext(ctx, `vacuum into ?`, backup)
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `update mention_events set target_name='discarded' where event_id=1`)
	require.NoError(t, err)
	r := &feedReader{table: "mention_events"}
	r.capture(ctx, t, s.DB())
	require.Equal(t, feedHead(t, s), r.cursor.Seq)
	require.NoError(t, s.Close())

	body, err := os.ReadFile(backup)
	require.NoError(t, err)
	for _, suffix := range []string{"-wal", "-shm"} {
		require.NoError(t, os.RemoveAll(path+suffix))
	}
	require.NoError(t, os.WriteFile(path, body, 0o600))
	restored, err := Open(ctx, path)
	require.NoError(t, err)
	defer func() { _ = restored.Close() }()
	time.Sleep(2 * time.Millisecond)
	_, err = restored.DB().ExecContext(ctx, `update mention_events set target_name='kept' where event_id=2`)
	require.NoError(t, err)
	_, err = restored.DB().ExecContext(ctx, `update change_log set recorded_at_ms=0`)
	require.NoError(t, err)
	_, err = restored.CompactChangeFeed(ctx, time.Now(), ChangeFeedRetention)
	require.NoError(t, err)
	r.capture(ctx, t, restored.DB())
	require.Equal(t, 2, r.reconciles)
	require.Equal(t, snapshotTable(ctx, t, restored.DB(), "mention_events"), r.rows)
}

func TestChangeFeedMissingMetaRowStartsNewGenerations(t *testing.T) {
	t.Parallel()
	s, path := openChangeFeedStore(t)
	_, err := s.DB().ExecContext(t.Context(), `delete from change_feed_meta`)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	reopened, err := Open(t.Context(), path)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	for _, tracked := range ChangeFeedTables() {
		generation, _, reason := feedGeneration(t, reopened, tracked)
		require.Equal(t, int64(2), generation, tracked)
		require.Equal(t, "feed_restored", reason, tracked)
	}
}

func TestChangeFeedCompactionKeepsRetentionWindowAndNeverReusesSequences(t *testing.T) {
	t.Parallel()
	s, _ := openChangeFeedStore(t)
	ctx := t.Context()
	seedChangeFeedGuild(t, s)
	for i := range 6 {
		require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation(fmt.Sprintf("m%d", i), "c1", "x", nil, 1)}))
	}
	now := time.Now()
	old := now.Add(-ChangeFeedRetention - time.Hour).UnixMilli()
	// Entries 1-4 are old; 5-6 are inside the window. Entry 3 has a newer
	// stamp (a clock step), so only the prefix 1-2 may be removed.
	_, err := s.DB().ExecContext(ctx, `update change_log set recorded_at_ms=? where seq in (1,2,4)`, old)
	require.NoError(t, err)
	head := feedHead(t, s)
	deleted, err := s.CompactChangeFeed(ctx, now, ChangeFeedRetention)
	require.NoError(t, err)
	require.Equal(t, int64(2), deleted)
	status, err := s.ChangeFeedStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), status.CompactedThrough)
	require.Equal(t, head, status.HeadSeq)

	_, err = s.DB().ExecContext(ctx, `update change_log set recorded_at_ms=?`, old)
	require.NoError(t, err)
	deleted, err = s.CompactChangeFeed(ctx, now, ChangeFeedRetention)
	require.NoError(t, err)
	require.Equal(t, head-2, deleted)
	status, err = s.ChangeFeedStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, head, status.CompactedThrough)
	require.Equal(t, head, status.HeadSeq)

	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("late", "c1", "x", nil, 1)}))
	var seq int64
	require.NoError(t, s.DB().QueryRowContext(ctx, `select min(seq) from change_log`).Scan(&seq))
	require.Equal(t, head+1, seq)
	_, err = s.CompactChangeFeed(ctx, now, 0)
	require.Error(t, err)
}

func TestChangeFeedBlobKeyNeverAbortsWriter(t *testing.T) {
	t.Parallel()
	s, _ := openChangeFeedStore(t)
	_, err := s.DB().ExecContext(t.Context(), `insert into attachment_text_history values(x'00ff','m','g','c','sha','text',null,'2026-10-28T00:00:00Z')`)
	require.NoError(t, err)
	require.Equal(t, [][2]string{{"i", `[{"blob_hex":"00FF"},"sha"]`}}, changeFeedEntries(t, s, "attachment_text_history"))
}

// INSERT OR REPLACE (used by snapshot imports) deletes a conflicting row
// without delete triggers. That is complete only while the primary key is
// the sole uniqueness constraint: the replacement then has the same key.
func TestChangeFeedTablesHaveOnlyPrimaryKeyUniqueness(t *testing.T) {
	t.Parallel()
	s, _ := openChangeFeedStore(t)
	for _, table := range ChangeFeedTables() {
		var unique int
		require.NoError(t, s.DB().QueryRowContext(t.Context(), `select count(*) from pragma_index_list(?) where "unique"=1 and origin<>'pk'`, table).Scan(&unique))
		require.Zero(t, unique, table)
	}
	// The replacement's insert entry names the same key.
	seedChangeFeedGuild(t, s)
	require.NoError(t, s.UpsertMessages(t.Context(), []MessageMutation{changeFeedMutation("m1", "c1", "x", []string{"a1"}, 0)}))
	_, err := s.DB().ExecContext(t.Context(), `insert or replace into message_attachments select * from message_attachments`)
	require.NoError(t, err)
	entries := changeFeedEntries(t, s, "message_attachments")
	require.Equal(t, [2]string{"i", `["a1"]`}, entries[len(entries)-1])
}

// The reference reader reproduces every tracked table exactly across random
// writer activity, repairs, deletions, compaction and generation changes.
func TestChangeFeedReaderReconstructsSnapshots(t *testing.T) {
	t.Parallel()
	s, _ := openChangeFeedStore(t)
	ctx := t.Context()
	seedChangeFeedGuild(t, s)
	readers := map[string]*feedReader{}
	relationTables := ChangeFeedTables()
	for _, table := range relationTables {
		readers[table] = &feedReader{table: table}
	}
	check := func(step int) {
		for _, table := range relationTables {
			r := readers[table]
			r.capture(ctx, t, s.DB())
			tx, err := s.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			require.NoError(t, err)
			want := snapshotTable(ctx, t, tx, table)
			require.NoError(t, tx.Rollback())
			require.Equal(t, want, r.rows, "step %d table %s", step, table)
		}
	}
	rng := rand.New(rand.NewPCG(1, 28))
	contents := []string{"alpha", "beta", "gamma"}
	embedded := 0
	for step := range 400 {
		id := fmt.Sprintf("m%d", rng.IntN(40))
		channel := []string{"c1", "c2"}[rng.IntN(2)]
		switch op := rng.IntN(11); {
		case op < 5:
			var attachments []string
			for a := range rng.IntN(3) {
				attachments = append(attachments, fmt.Sprintf("%s-a%d", id, a))
			}
			mutation := changeFeedMutation(id, channel, contents[rng.IntN(len(contents))], attachments, rng.IntN(3))
			err := s.UpsertMessages(ctx, []MessageMutation{mutation})
			require.NoError(t, err)
		case op == 5:
			embedded++
			_, err := s.DB().ExecContext(ctx, `insert or ignore into message_embeddings(message_id,provider,model,input_version,dimensions,embedding_blob,embedded_at)
				select id,'p','m','v1',1,x'0000803f',? from messages where id=?`, fmt.Sprintf("2026-10-28T00:00:%02dZ", embedded%60), id)
			require.NoError(t, err)
		case op == 6:
			require.NoError(t, s.MarkMessageDeleted(ctx, "g", channel, id, map[string]string{}))
		case op == 7:
			_, err := s.DB().ExecContext(ctx, `update mention_events set target_name=target_name||'!' where event_id in (select event_id from mention_events order by random() limit 2)`)
			require.NoError(t, err)
			_, err = s.DB().ExecContext(ctx, `delete from message_attachments where attachment_id in (select attachment_id from message_attachments order by random() limit 1)`)
			require.NoError(t, err)
		case op == 8:
			require.NoError(t, s.UpdateAttachmentFetchStatus(ctx, id+"-a0", "2026-10-28T01:00:00Z", "fetched", ""))
		case op == 9:
			user := fmt.Sprintf("u%d", rng.IntN(5))
			name := contents[rng.IntN(len(contents))]
			require.NoError(t, s.UpsertMember(ctx, MemberRecord{GuildID: "g", UserID: user, Username: name, RoleIDsJSON: "[]", RawJSON: `{"nick":"` + name + `"}`}))
			require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{
				ID: channel, GuildID: "g", Kind: "text", Name: channel,
				RawJSON: fmt.Sprintf(`{"id":%q,"message_count":%d,"topic":%q}`, channel, 100+rng.IntN(4), contents[rng.IntN(2)]),
			}))
		default:
			if rng.IntN(4) == 0 {
				tx, err := s.DB().BeginTx(ctx, nil)
				require.NoError(t, err)
				require.NoError(t, BumpChangeFeedGenerationTx(ctx, tx, "test_restore", ChangeFeedTables()[rng.IntN(len(changeFeedTables))]))
				require.NoError(t, tx.Commit())
			} else {
				_, err := s.DB().ExecContext(ctx, `update change_log set recorded_at_ms=0 where seq<=(select max(seq)-20 from change_log)`)
				require.NoError(t, err)
				_, err = s.CompactChangeFeed(ctx, time.Now(), ChangeFeedRetention)
				require.NoError(t, err)
			}
		}
		if step == 200 {
			// Every table also reconciles across a generation change.
			tx, err := s.DB().BeginTx(ctx, nil)
			require.NoError(t, err)
			require.NoError(t, BumpChangeFeedGenerationTx(ctx, tx, "test_restore", ChangeFeedTables()...))
			require.NoError(t, tx.Commit())
		}
		if rng.IntN(3) == 0 {
			check(step)
		}
	}
	require.NoError(t, s.DeleteGuildData(ctx, "g"))
	check(-1)
	for _, table := range relationTables {
		r := readers[table]
		t.Logf("%s: replayed=%d reconciles=%d", table, r.replayed, r.reconciles)
		require.Positive(t, r.replayed, table)
		require.Greater(t, r.reconciles, 1, table)
	}
}

func TestChangeFeedReaderDetectsRewoundHistory(t *testing.T) {
	t.Parallel()
	s, path := openChangeFeedStore(t)
	ctx := t.Context()
	seedChangeFeedGuild(t, s)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m1", "c1", "x", nil, 1)}))
	backup := filepath.Join(t.TempDir(), "backup.db")
	_, err := s.DB().ExecContext(ctx, `vacuum into ?`, backup)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m2", "c1", "x", nil, 1)}))
	r := &feedReader{table: "mention_events"}
	r.capture(ctx, t, s.DB())
	require.Equal(t, 1, r.reconciles)
	require.Equal(t, feedHead(t, s), r.cursor.Seq)
	require.NoError(t, s.Close())

	// Restore the earlier file in place, then write again: the generation is
	// unchanged, but sequence 2 now names different content.
	body, err := os.ReadFile(backup)
	require.NoError(t, err)
	for _, suffix := range []string{"-wal", "-shm"} {
		require.NoError(t, os.RemoveAll(path+suffix))
	}
	require.NoError(t, os.WriteFile(path, body, 0o600))
	restored, err := Open(ctx, path)
	require.NoError(t, err)
	defer func() { _ = restored.Close() }()
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, restored.UpsertMessages(ctx, []MessageMutation{changeFeedMutation("m3", "c1", "x", nil, 1)}))
	r.capture(ctx, t, restored.DB())
	require.Equal(t, 2, r.reconciles)
	require.Equal(t, snapshotTable(ctx, t, restored.DB(), "mention_events"), r.rows)
}

func feedHead(t *testing.T, s *Store) int64 {
	t.Helper()
	head, err := changeFeedHead(t.Context(), s.DB())
	require.NoError(t, err)
	return head
}
