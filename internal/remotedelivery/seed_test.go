package remotedelivery

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/crawlkit/snapshot"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPreparedSeedMatchesNativeProjectionAndOwnership(t *testing.T) {
	ctx, source, path, _ := projectionFixture(t)
	for _, guild := range []string{"g1", "g2", "@me"} {
		writeMessage(t, ctx, source, "message-"+guild, guild)
	}
	writeMessage(t, ctx, source, "a\"b", "g1")
	require.NoError(t, source.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u", Username: "member", RawJSON: "{}", RoleIDsJSON: "[]"}))
	for _, scope := range []string{"guild:g1:members", "channel:cg1:history_complete", "channel:cg2:history_complete", "channel:missing:history_complete", "worker:private"} {
		require.NoError(t, source.SetSyncState(ctx, scope, "cursor\x00exact"))
	}
	_, err := source.DB().ExecContext(ctx, "UPDATE channels SET position=42 WHERE id='cg1'")
	require.NoError(t, err)
	root := filepath.Join(t.TempDir(), "seed ? #")
	prepared, qualification, err := PrepareSeed(ctx, path, root, Scope{GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, int64(7), prepared.OwnedKeysCount)
	require.Equal(t, SeedPreparationFormat, prepared.Format)
	receipt, err := os.ReadFile(filepath.Join(root, "preparation.json"))
	require.NoError(t, err)
	sum := sha256.Sum256(receipt)
	require.Equal(t, hex.EncodeToString(sum[:]), qualification)
	info, err := os.Stat(root)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	require.NoFileExists(t, filepath.Join(root, ".ownership.sqlite"))

	// Import through the actual snapshot reader, including exact SQL types.
	target, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer func() { _ = target.Close() }()
	for table := range columns {
		var ddl string
		require.NoError(t, source.DB().QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", table).Scan(&ddl))
		_, err = target.ExecContext(ctx, ddl)
		require.NoError(t, err)
	}
	_, err = snapshot.Import(ctx, snapshot.ImportOptions{DB: target, RootDir: filepath.Join(root, "snapshot")})
	require.NoError(t, err)
	var content, integerType string
	var edited sql.NullString
	var position int64
	require.NoError(t, target.QueryRowContext(ctx, "SELECT content,edited_at FROM messages WHERE id=?", "a\"b").Scan(&content, &edited))
	require.Equal(t, "text\x00preserved", content)
	require.False(t, edited.Valid)
	require.NoError(t, target.QueryRowContext(ctx, "SELECT position,typeof(position) FROM channels WHERE id='cg1'").Scan(&position, &integerType))
	require.Equal(t, int64(42), position)
	require.Equal(t, "integer", integerType)
	var messages, checkpoints int
	require.NoError(t, target.QueryRowContext(ctx, "SELECT count(*) FROM messages").Scan(&messages))
	require.NoError(t, target.QueryRowContext(ctx, "SELECT count(*) FROM sync_state").Scan(&checkpoints))
	require.Equal(t, 2, messages)
	require.Equal(t, 2, checkpoints)

	// The real outbox checks strict canonical-key ordering and its count/digest.
	f, err := os.Open(filepath.Join(root, "ownership.jsonl"))
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	outbox, err := InitOutbox(ctx, filepath.Join(t.TempDir(), "outbox.sqlite"), Registration{
		Origin: "https://archive.example.test", Archive: "fixture", BindingID: "binding", ServerCursor: "seed:complete",
		SeedManifestSHA256: prepared.SeedManifestSHA256, QualificationSHA256: qualification, OwnedKeysSHA256: prepared.OwnedKeysSHA256,
		OwnedKeysCount: prepared.OwnedKeysCount, Scope: prepared.Scope, Baseline: prepared.Baseline,
	}, func(yield func(SeedKey) error) error {
		for scanner.Scan() {
			var key SeedKey
			if err := json.Unmarshal(scanner.Bytes(), &key); err != nil {
				return err
			}
			if err := yield(key); err != nil {
				return err
			}
		}
		return scanner.Err()
	})
	require.NoError(t, err)
	require.NoError(t, outbox.Close())
	captured, err := store.CaptureNativeChanges(ctx, path, prepared.Baseline, store.NativeCaptureLimits{MaxEvents: 100, MaxBytes: 1 << 20})
	require.NoError(t, err)
	require.Equal(t, captured.From, captured.Through)

	_, _, err = PrepareSeed(ctx, path, root, prepared.Scope)
	require.Error(t, err)
	unchanged, err := os.ReadFile(filepath.Join(root, "preparation.json"))
	require.NoError(t, err)
	require.Equal(t, receipt, unchanged)
}

func TestPreparedSeedRejectsUnqualifiedSource(t *testing.T) {
	ctx, source, path, _ := projectionFixture(t)
	writeMessage(t, ctx, source, "m", "g1")
	root := filepath.Join(t.TempDir(), "invalid-blob")
	_, err := source.DB().ExecContext(ctx, "UPDATE messages SET content=x'6162' WHERE id='m'")
	require.NoError(t, err)
	_, _, err = PrepareSeed(ctx, path, root, Scope{GuildIDs: []string{"g1"}})
	require.ErrorIs(t, err, ErrProjection, "a BLOB cannot silently become report TEXT")
	require.NoFileExists(t, filepath.Join(root, "preparation.json"))
	_, err = source.DB().ExecContext(ctx, "UPDATE messages SET content='valid' WHERE id='m'")
	require.NoError(t, err)
	_, err = source.DB().ExecContext(ctx, "UPDATE channels SET position=9007199254740993 WHERE id='cg1'")
	require.NoError(t, err)
	root = filepath.Join(t.TempDir(), "large-integer")
	_, _, err = PrepareSeed(ctx, path, root, Scope{GuildIDs: []string{"g1"}})
	require.ErrorContains(t, err, "outside +/- (2^53-1)")
	require.NoFileExists(t, filepath.Join(root, "preparation.json"), "never round a value to fit the existing v1 seed format")
	_, err = source.DB().ExecContext(ctx, "UPDATE channels SET position=42 WHERE id='cg1'")
	require.NoError(t, err)
	root = filepath.Join(t.TempDir(), "missing-guild")
	_, _, err = PrepareSeed(ctx, path, root, Scope{GuildIDs: []string{"missing"}})
	require.ErrorIs(t, err, ErrProjection)
	require.NoFileExists(t, filepath.Join(root, "preparation.json"))
	root = filepath.Join(t.TempDir(), "no-deadline")
	_, _, err = PrepareSeed(context.Background(), path, root, Scope{GuildIDs: []string{"g1"}})
	require.Error(t, err)
	require.NoDirExists(t, root)
}

func TestSeedReceiptPublicationNeverReplacesExistingEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "preparation.json")
	require.NoError(t, os.WriteFile(path, []byte("existing receipt"), 0o600))
	require.Error(t, writeSeedFile(path, []byte("replacement receipt")))
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "existing receipt", string(body))
	body, err = os.ReadFile(path + ".partial")
	require.NoError(t, err)
	require.Equal(t, "replacement receipt", string(body))
}
