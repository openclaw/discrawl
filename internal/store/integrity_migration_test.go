package store

import (
	"bytes"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIntegrityMigrationPreservesV6RawTextVectorsAndCursors(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "v6.db")
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	old := &Store{db: db, path: path}
	require.NoError(t, old.applyBaselineSchema(ctx))
	for _, migrate := range []func() error{func() error { return old.applyAttachmentMediaMigration(ctx) }, func() error { return old.applyFailureLedgerMigration(ctx) }, func() error { return old.applyEntityTombstoneMigration(ctx) }, func() error { return old.applyEmbeddingWorkerMigration(ctx) }} {
		require.NoError(t, migrate())
	}
	_, err = db.ExecContext(ctx, `insert into channels(id,guild_id,kind,name,raw_json,updated_at)values('c','g','text','fixture','{"permission_overwrites":[]}','2026-01-01T00:00:00Z');
	insert into messages(id,guild_id,channel_id,message_type,created_at,content,normalized_content,raw_json,updated_at)values('1','g','c',0,'2026-01-01T00:00:00Z','raw body','old normalized','{"id":"1","content":"raw body"}','2026-01-01T00:00:00Z');
	insert into message_attachments(attachment_id,message_id,guild_id,channel_id,filename,text_content,updated_at)values('a','1','g','c','file.txt','retained extraction','2026-01-01T00:00:00Z');
	insert into message_embeddings(message_id,provider,model,input_version,dimensions,embedding_blob,embedded_at)values('1','fixture','model','v1',1,x'0000803f','2026-01-01T00:00:00Z');
	insert into sync_state values('channel:c:latest_message_id','1','2026-01-01T00:00:00Z');pragma user_version=6;`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	s, err := Open(ctx, path)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	var content, normalized, raw, scope, extraction, status, vector, cursor string
	var textVersion int
	var success sql.NullString
	require.NoError(t, s.DB().QueryRowContext(ctx, `select content,normalized_content,raw_json,text_version from messages where id='1'`).Scan(&content, &normalized, &raw, &textVersion))
	require.Equal(t, "raw body", content)
	require.Equal(t, "old normalized", normalized)
	require.True(t, bytes.Equal([]byte(`{"id":"1","content":"raw body"}`), []byte(raw)), "raw bytes remain unchanged")
	require.Zero(t, textVersion)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select collection_scope from channels where id='c'`).Scan(&scope))
	require.Equal(t, "unknown", scope)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select text_content,text_status,text_succeeded_at from message_attachments where attachment_id='a'`).Scan(&extraction, &status, &success))
	require.Equal(t, "retained extraction", extraction)
	require.Equal(t, "unknown", status)
	require.False(t, success.Valid)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select hex(embedding_blob) from message_embeddings where message_id='1'`).Scan(&vector))
	require.Equal(t, "0000803F", vector)
	require.NoError(t, s.DB().QueryRowContext(ctx, `select cursor from sync_state where scope='channel:c:latest_message_id'`).Scan(&cursor))
	require.Equal(t, "1", cursor)
}
