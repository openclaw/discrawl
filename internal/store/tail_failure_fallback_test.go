package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPersistTailMessageFailureFallbackIsCanonicalPrivateAndPoolIndependent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	conn, err := s.DB().Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	fallback := TailMessageFailureFallback{
		EventKind:   " MESSAGE_CREATE ",
		FailureKind: "RETURNED-ERROR",
		GuildID:     " g1 ",
		ChannelID:   " c1 ",
		MessageID:   " m1 ",
	}
	require.NoError(t, s.PersistTailMessageFailureFallback(fallback))

	fallbackDir, err := s.tailMessageFailureFallbackDir()
	require.NoError(t, err)
	info, err := os.Lstat(fallbackDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	if runtime.GOOS != "windows" {
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	}

	entries, err := os.ReadDir(fallbackDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.False(t, strings.HasPrefix(entries[0].Name(), tailMessageFailureFallbackTempPrefix))
	body, err := os.ReadFile(filepath.Join(fallbackDir, entries[0].Name()))
	require.NoError(t, err)
	require.JSONEq(
		t,
		`{"version":1,"event_kind":"create","failure_kind":"returned_error","guild_id":"g1","channel_id":"c1","message_id":"m1"}`,
		string(body),
	)
	require.NotContains(t, string(body), "content")
	require.NotContains(t, string(body), "payload")
	require.NotContains(t, string(body), "user")
	require.NotContains(t, string(body), "timestamp")
	require.NotContains(t, string(body), "token")
	require.NotContains(t, string(body), "cause")
	digest := sha256.Sum256(body)
	artifactID, digestHex, ok := tailMessageFailureNameParts(entries[0].Name())
	require.True(t, ok)
	require.Equal(t, hex.EncodeToString(digest[:]), digestHex)
	require.Len(t, strings.Split(artifactID, "-"), 2)
	recordInfo, err := entries[0].Info()
	require.NoError(t, err)
	require.True(t, recordInfo.Mode().IsRegular())
	if runtime.GOOS != "windows" {
		require.Equal(t, os.FileMode(0o600), recordInfo.Mode().Perm())
	}

	other, err := Open(ctx, filepath.Join(dir, "other.db"))
	require.NoError(t, err)
	defer func() { _ = other.Close() }()
	otherFallbackDir, err := other.tailMessageFailureFallbackDir()
	require.NoError(t, err)
	require.NotEqual(t, fallbackDir, otherFallbackDir)
}

func TestPersistTailMessageFailureFallbackConcurrentOccurrences(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	fallback := TailMessageFailureFallback{
		EventKind: "update", FailureKind: "panic",
		GuildID: "g1", ChannelID: "c1", MessageID: "m1",
	}
	start := make(chan struct{})
	errs := make(chan error, 16)
	var writers sync.WaitGroup
	for range 16 {
		writers.Go(func() {
			<-start
			errs <- s.PersistTailMessageFailureFallback(fallback)
		})
	}
	close(start)
	writers.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	dir, err := s.tailMessageFailureFallbackDir()
	require.NoError(t, err)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 16)
	names := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		require.False(t, strings.HasPrefix(entry.Name(), tailMessageFailureFallbackTempPrefix))
		_, _, ok := tailMessageFailureNameParts(entry.Name())
		require.True(t, ok)
		names[entry.Name()] = struct{}{}
	}
	require.Len(t, names, len(entries))
}

func TestPersistTailMessageFailureFallbackRejectsInvalidIdentityAndDirectory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	valid := TailMessageFailureFallback{
		EventKind: "create", FailureKind: "timeout",
		GuildID: "g1", ChannelID: "c1", MessageID: "m1",
	}
	for _, fallback := range []TailMessageFailureFallback{
		{EventKind: "bulk", FailureKind: "timeout", GuildID: "g1", ChannelID: "c1", MessageID: "m1"},
		{EventKind: "create", FailureKind: "raw", GuildID: "g1", ChannelID: "c1", MessageID: "m1"},
		{EventKind: "create", FailureKind: "timeout", ChannelID: "c1", MessageID: "m1"},
		{EventKind: "create", FailureKind: "timeout", GuildID: "g1", MessageID: "m1"},
		{EventKind: "create", FailureKind: "timeout", GuildID: "g1", ChannelID: "c1"},
	} {
		require.Error(t, s.PersistTailMessageFailureFallback(fallback))
	}

	fallbackDir, err := s.tailMessageFailureFallbackDir()
	require.NoError(t, err)
	target := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.Mkdir(target, 0o700))
	if err := os.Symlink(target, fallbackDir); err != nil {
		if runtime.GOOS != "windows" {
			require.NoError(t, err)
		}
	} else {
		require.ErrorContains(t, s.PersistTailMessageFailureFallback(valid), "must not be a symlink")
	}

	if runtime.GOOS != "windows" {
		insecure, err := Open(ctx, filepath.Join(t.TempDir(), "insecure.db"))
		require.NoError(t, err)
		defer func() { _ = insecure.Close() }()
		insecureDir, err := insecure.tailMessageFailureFallbackDir()
		require.NoError(t, err)
		require.NoError(t, os.Mkdir(insecureDir, 0o755))
		require.ErrorContains(t, insecure.PersistTailMessageFailureFallback(valid), "permissions must be 0700")
	}
}

func TestTailMessageFailureFallbackPathAndNameValidation(t *testing.T) {
	t.Parallel()

	var nilStore *Store
	_, err := nilStore.tailMessageFailureFallbackDir()
	require.ErrorContains(t, err, "store is required")

	for _, path := range []string{"", ":memory:", "file:"} {
		_, err := (&Store{path: path}).tailMessageFailureFallbackDir()
		require.ErrorContains(t, err, "requires a filesystem database")
	}
	_, err = openTailMessageFailureFallbackDir(filepath.Join(t.TempDir(), "missing"), false)
	require.ErrorIs(t, err, errTailMessageFailureFallbackDirAbsent)
	require.NotErrorIs(t, fmt.Errorf("verify opened directory: %w", os.ErrNotExist), errTailMessageFailureFallbackDirAbsent)
	_, err = (&Store{path: "file://remote/tmp/discrawl.db"}).tailMessageFailureFallbackDir()
	require.ErrorContains(t, err, "database path is invalid")
	_, err = (&Store{path: "file://[::1"}).tailMessageFailureFallbackDir()
	require.ErrorContains(t, err, "database path is invalid")

	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	uriPath := filepath.ToSlash(dbPath)
	if runtime.GOOS == "windows" {
		uriPath = "/" + uriPath
	}
	dbURL := (&url.URL{Scheme: "file", Host: "localhost", Path: uriPath}).String()
	got, err := (&Store{path: dbURL}).tailMessageFailureFallbackDir()
	require.NoError(t, err)
	require.Equal(t, dbPath+tailMessageFailureFallbackDirSuffix, got)

	digest := strings.Repeat("a", sha256.Size*2)
	occurrence := strings.Repeat("b", tailMessageFailureOccurrenceBytes*2)
	for _, name := range []string{
		digest + ".txt",
		strings.ToUpper(digest) + tailMessageFailureFallbackExtension,
		"bad-" + digest + tailMessageFailureFallbackExtension,
		occurrence + "-" + digest + "-extra" + tailMessageFailureFallbackExtension,
	} {
		_, _, ok := tailMessageFailureNameParts(name)
		require.False(t, ok, name)
		_, ok = tailMessageFailureReceiptScope(name)
		require.False(t, ok, name)
	}
	artifactID, parsedDigest, ok := tailMessageFailureNameParts(
		occurrence + "-" + digest + tailMessageFailureFallbackExtension,
	)
	require.True(t, ok)
	require.Equal(t, occurrence+"-"+digest, artifactID)
	require.Equal(t, digest, parsedDigest)
	require.True(t, validLowerHex(digest, sha256.Size))
	require.False(t, validLowerHex(strings.ToUpper(digest), sha256.Size))
	require.False(t, validLowerHex("aa", sha256.Size))

	_, err = normalizeTailMessageFailureFallback(TailMessageFailureFallback{
		EventKind:   "create",
		FailureKind: "timeout",
		GuildID:     strings.Repeat("g", tailMessageFailureFallbackMaxSize),
		ChannelID:   "c1",
		MessageID:   "m1",
	})
	require.ErrorContains(t, err, "exceeds size limit")

	var nilDir *tailMessageFailureFallbackDir
	require.NoError(t, nilDir.Close())
}

func TestTailMessageFailureFallbackNoopAndHookFailures(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	imported, err := s.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Zero(t, imported)

	dirPath, err := s.tailMessageFailureFallbackDir()
	require.NoError(t, err)
	createTailFailureTestDir(t, dirPath)
	imported, err = s.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Zero(t, imported)

	hookErr := errors.New("forced fallback hook failure")
	_, err = s.importTailMessageFailureFallbacks(ctx, tailMessageFailureFallbackHooks{
		afterOpen: func(string) error { return hookErr },
	})
	require.ErrorIs(t, err, hookErr)
	err = s.persistTailMessageFailureFallback(TailMessageFailureFallback{
		EventKind: "create", FailureKind: "timeout",
		GuildID: "g1", ChannelID: "c1", MessageID: "m1",
	}, tailMessageFailureFallbackHooks{
		afterOpen: func(string) error { return hookErr },
	})
	require.ErrorIs(t, err, hookErr)

	err = s.persistTailMessageFailureFallback(TailMessageFailureFallback{
		EventKind: "update", FailureKind: "panic",
		GuildID: "g1", ChannelID: "c1", MessageID: "m2",
	}, tailMessageFailureFallbackHooks{
		syncDir: func(*os.File) error { return hookErr },
	})
	require.ErrorIs(t, err, hookErr)

	dir, err := openTailMessageFailureFallbackDir(dirPath, false)
	require.NoError(t, err)
	defer func() { _ = dir.Close() }()
	require.NoError(t, os.Mkdir(filepath.Join(dirPath, tailMessageFailureFallbackTempPrefix+"dir"), 0o700))
	require.ErrorContains(t, cleanupTailMessageFailureTemps(
		dir,
		[]string{tailMessageFailureFallbackTempPrefix + "dir"},
		tailMessageFailureFallbackHooks{},
	), "must not be a directory")

	tempName := tailMessageFailureFallbackTempPrefix + "remove"
	require.NoError(t, dir.root.WriteFile(tempName, []byte("temp"), 0o600))
	err = cleanupTailMessageFailureTemps(
		dir,
		[]string{tempName},
		tailMessageFailureFallbackHooks{
			remove: func(*os.Root, string) error { return hookErr },
		},
	)
	require.ErrorIs(t, err, hookErr)

	err = cleanupTailMessageFailureTemps(
		dir,
		[]string{tempName},
		tailMessageFailureFallbackHooks{
			syncDir: func(*os.File) error { return hookErr },
		},
	)
	require.ErrorIs(t, err, hookErr)
}

func TestImportTailMessageFailureFallbacksRejectUnsafeCommittedFiles(t *testing.T) {
	t.Parallel()
	validBody := []byte(
		`{"version":1,"event_kind":"create","failure_kind":"timeout","guild_id":"g1","channel_id":"c1","message_id":"m1"}`,
	)
	tests := []struct {
		name      string
		create    func(*testing.T, string)
		errorText string
		unixOnly  bool
	}{
		{
			name: "symlink",
			create: func(t *testing.T, dir string) {
				target := filepath.Join(t.TempDir(), "target.json")
				require.NoError(t, os.WriteFile(target, validBody, 0o600))
				require.NoError(t, os.Symlink(target, filepath.Join(dir, committedTailFailureName(validBody))))
			},
			errorText: "must not be a symlink",
			unixOnly:  true,
		},
		{
			name: "nonregular",
			create: func(t *testing.T, dir string) {
				require.NoError(t, os.Mkdir(filepath.Join(dir, committedTailFailureName(validBody)), 0o700))
			},
			errorText: "must be a regular file",
		},
		{
			name: "insecure permissions",
			create: func(t *testing.T, dir string) {
				name := committedTailFailureName(validBody)
				writeTailFailureTestFile(t, dir, name, validBody, 0o600)
				path := filepath.Join(dir, name)
				require.NoError(t, os.Chmod(path, 0o644))
			},
			errorText: "permissions are insecure",
			unixOnly:  true,
		},
		{
			name: "oversize",
			create: func(t *testing.T, dir string) {
				body := []byte(strings.Repeat("x", tailMessageFailureFallbackMaxSize+1))
				writeTailFailureTestFile(t, dir, committedTailFailureName(body), body, 0o600)
			},
			errorText: "size is invalid",
		},
		{
			name: "empty",
			create: func(t *testing.T, dir string) {
				body := []byte{}
				writeTailFailureTestFile(t, dir, committedTailFailureName(body), body, 0o600)
			},
			errorText: "size is invalid",
		},
		{
			name: "digest mismatch",
			create: func(t *testing.T, dir string) {
				writeTailFailureTestFile(
					t,
					dir,
					committedTailFailureName(validBody),
					[]byte(`{"version":1}`),
					0o600,
				)
			},
			errorText: "digest does not match",
		},
		{
			name: "noncanonical json",
			create: func(t *testing.T, dir string) {
				body := []byte(
					`{ "version":1,"event_kind":"create","failure_kind":"timeout","guild_id":"g1","channel_id":"c1","message_id":"m1"}`,
				)
				writeTailFailureTestFile(t, dir, committedTailFailureName(body), body, 0o600)
			},
			errorText: "JSON is not canonical",
		},
		{
			name: "unsupported version",
			create: func(t *testing.T, dir string) {
				body := []byte(
					`{"version":2,"event_kind":"create","failure_kind":"timeout","guild_id":"g1","channel_id":"c1","message_id":"m1"}`,
				)
				writeTailFailureTestFile(t, dir, committedTailFailureName(body), body, 0o600)
			},
			errorText: "version is unsupported",
		},
		{
			name: "noncanonical event kind",
			create: func(t *testing.T, dir string) {
				body := []byte(
					`{"version":1,"event_kind":"CREATE","failure_kind":"timeout","guild_id":"g1","channel_id":"c1","message_id":"m1"}`,
				)
				writeTailFailureTestFile(t, dir, committedTailFailureName(body), body, 0o600)
			},
			errorText: "event kind is invalid",
		},
		{
			name: "noncanonical failure kind",
			create: func(t *testing.T, dir string) {
				body := []byte(
					`{"version":1,"event_kind":"create","failure_kind":"PANIC","guild_id":"g1","channel_id":"c1","message_id":"m1"}`,
				)
				writeTailFailureTestFile(t, dir, committedTailFailureName(body), body, 0o600)
			},
			errorText: "failure kind is invalid",
		},
		{
			name: "noncanonical identity",
			create: func(t *testing.T, dir string) {
				body := []byte(
					`{"version":1,"event_kind":"create","failure_kind":"timeout","guild_id":" g1","channel_id":"c1","message_id":"m1"}`,
				)
				writeTailFailureTestFile(t, dir, committedTailFailureName(body), body, 0o600)
			},
			errorText: "identity is invalid",
		},
		{
			name: "unknown field",
			create: func(t *testing.T, dir string) {
				body := []byte(
					`{"version":1,"event_kind":"create","failure_kind":"timeout","guild_id":"g1","channel_id":"c1","message_id":"m1","content":"private"}`,
				)
				writeTailFailureTestFile(t, dir, committedTailFailureName(body), body, 0o600)
			},
			errorText: "JSON is invalid",
		},
		{
			name: "trailing data",
			create: func(t *testing.T, dir string) {
				body := append(append([]byte(nil), validBody...), []byte(`{}`)...)
				writeTailFailureTestFile(t, dir, committedTailFailureName(body), body, 0o600)
			},
			errorText: "trailing data",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.unixOnly && runtime.GOOS == "windows" {
				t.Skip("POSIX permission fixture")
			}
			ctx := context.Background()
			s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
			require.NoError(t, err)
			defer func() { _ = s.Close() }()
			dir, err := s.tailMessageFailureFallbackDir()
			require.NoError(t, err)
			createTailFailureTestDir(t, dir)
			tt.create(t, dir)

			imported, err := s.ImportTailMessageFailureFallbacks(ctx)
			require.Zero(t, imported)
			require.ErrorContains(t, err, tt.errorText)
			var count int
			require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from failure_ledger`).Scan(&count))
			require.Zero(t, count)
			entries, readErr := os.ReadDir(dir)
			require.NoError(t, readErr)
			require.NotEmpty(t, entries)
		})
	}
}

func TestImportTailMessageFailureFallbacksValidatesAllBeforeMutation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.PersistTailMessageFailureFallback(TailMessageFailureFallback{
		EventKind: "create", FailureKind: "timeout",
		GuildID: "g1", ChannelID: "c1", MessageID: "m1",
	}))
	dir, err := s.tailMessageFailureFallbackDir()
	require.NoError(t, err)
	corrupt := []byte(`{"version":1}`)
	corruptName := committedTailFailureName(corrupt)
	writeTailFailureTestFile(t, dir, corruptName, corrupt, 0o600)

	imported, err := s.ImportTailMessageFailureFallbacks(ctx)
	require.Zero(t, imported)
	require.Error(t, err)
	var count int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from failure_ledger`).Scan(&count))
	require.Zero(t, count)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	_, err = os.Stat(filepath.Join(dir, corruptName))
	require.NoError(t, err)
}

func TestImportTailMessageFailureFallbacksRetainsFilesWhenDBMutationFails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.PersistTailMessageFailureFallback(TailMessageFailureFallback{
		EventKind: "delete", FailureKind: "panic",
		GuildID: "g1", ChannelID: "c1", MessageID: "m1",
	}))
	dir, err := s.tailMessageFailureFallbackDir()
	require.NoError(t, err)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	committedPath := filepath.Join(dir, entries[0].Name())
	_, err = s.DB().ExecContext(ctx, `
		create trigger fail_tail_failure_import
		before insert on failure_ledger
		begin
			select raise(abort, 'forced tail failure import failure');
		end
	`)
	require.NoError(t, err)

	imported, err := s.ImportTailMessageFailureFallbacks(ctx)
	require.Zero(t, imported)
	require.Error(t, err)
	_, err = os.Stat(committedPath)
	require.NoError(t, err)
	var count int
	require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from failure_ledger`).Scan(&count))
	require.Zero(t, count)
	assertTailFailureReceiptCount(t, s, 0)

	_, err = s.DB().ExecContext(ctx, `drop trigger fail_tail_failure_import`)
	require.NoError(t, err)
	imported, err = s.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, imported)
	assertTailFailureLedgerStatus(t, s, "m1", 0, false)
	assertTailFailureReceiptCount(t, s, 1)
}

func TestImportTailMessageFailureFallbackRecordsRejectInvalidInputsAndReceiptFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)

	imported, err := s.importTailMessageFailureFallbackRecords(ctx, nil, time.Now())
	require.NoError(t, err)
	require.Zero(t, imported)

	validRecord := tailMessageFailureFallbackRecord{
		Version:     tailMessageFailureFallbackVersion,
		EventKind:   "create",
		FailureKind: "timeout",
		GuildID:     "g1",
		ChannelID:   "c1",
		MessageID:   "m1",
	}
	validName := strings.Repeat("a", sha256.Size*2) + tailMessageFailureFallbackExtension
	_, err = s.importTailMessageFailureFallbackRecords(ctx, []committedTailMessageFailure{{
		name:   validName,
		record: tailMessageFailureFallbackRecord{Version: 99},
	}}, time.Now())
	require.ErrorContains(t, err, "version is unsupported")
	_, err = s.importTailMessageFailureFallbackRecords(ctx, []committedTailMessageFailure{{
		name:   "invalid",
		record: validRecord,
	}}, time.Now())
	require.ErrorContains(t, err, "invalid committed")

	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	invalidKind := validRecord
	invalidKind.FailureKind = "invalid"
	require.ErrorContains(
		t,
		importTailMessageFailureFallbackRecord(ctx, tx, invalidKind, time.Now()),
		"invalid tail message failure kind",
	)
	require.NoError(t, tx.Rollback())

	_, err = s.DB().ExecContext(ctx, `
		create trigger fail_tail_failure_receipt
		before insert on sync_state
		when new.scope like 'tail-message-failure-fallback:%'
		begin
			select raise(abort, 'forced receipt failure');
		end
	`)
	require.NoError(t, err)
	_, err = s.importTailMessageFailureFallbackRecords(ctx, []committedTailMessageFailure{{
		name:   validName,
		record: validRecord,
	}}, time.Now())
	require.ErrorContains(t, err, "record tail message failure fallback import receipt")

	require.NoError(t, s.Close())
	_, err = s.importTailMessageFailureFallbackRecords(ctx, []committedTailMessageFailure{{
		name:   validName,
		record: validRecord,
	}}, time.Now())
	require.ErrorContains(t, err, "begin tail message failure fallback import")
}

func TestImportTailMessageFailureFallbacksMapsEventIdentityAndCleansFiles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	for _, fallback := range []TailMessageFailureFallback{
		{EventKind: "MESSAGE_CREATE", FailureKind: "returned_error", GuildID: "g1", ChannelID: "c1", MessageID: "m1"},
		{EventKind: "MESSAGE_UPDATE", FailureKind: "panic", GuildID: "g1", ChannelID: "c1", MessageID: "m1"},
		{EventKind: "MESSAGE_DELETE", FailureKind: "timeout", GuildID: "g1", ChannelID: "c1", MessageID: "m1"},
	} {
		require.NoError(t, s.PersistTailMessageFailureFallback(fallback))
	}
	dir, err := s.tailMessageFailureFallbackDir()
	require.NoError(t, err)
	writeTailFailureTestFile(t, dir, tailMessageFailureFallbackTempPrefix+"stale", []byte("stale"), 0o600)

	imported, err := s.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, imported)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)

	rows, err := s.DB().QueryContext(ctx, `
		select operation, source, related_kind, related_id, error_class, error_message
		from failure_ledger
		order by related_id
	`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var got [][]string
	for rows.Next() {
		var values [6]string
		require.NoError(t, rows.Scan(
			&values[0],
			&values[1],
			&values[2],
			&values[3],
			&values[4],
			&values[5],
		))
		got = append(got, values[:])
	}
	require.NoError(t, rows.Err())
	require.Equal(t, [][]string{
		{"tail_message", "discord", "message_event", "create", "returned_error", "tail message handler returned an error"},
		{"tail_message", "discord", "message_event", "delete", "timeout", "tail message handler timed out"},
		{"tail_message", "discord", "message_event", "update", "panic", "tail message handler panicked"},
	}, got)
}

func TestImportTailMessageFailureFallbacksAdvancesRepeatedUnresolvedOccurrence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	fallback := TailMessageFailureFallback{
		EventKind: "create", FailureKind: "returned_error",
		GuildID: "g1", ChannelID: "c1", MessageID: "m1",
	}
	require.NoError(t, s.PersistTailMessageFailureFallback(fallback))
	imported, err := s.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, imported)

	var firstSeen string
	var firstLastSeen string
	var errorClass string
	var retryCount int
	require.NoError(t, s.DB().QueryRowContext(ctx, `
		select first_seen_at, last_seen_at, error_class, retry_count
		from failure_ledger
		where message_id = 'm1' and related_id = 'create'
	`).Scan(&firstSeen, &firstLastSeen, &errorClass, &retryCount))
	require.Equal(t, "returned_error", errorClass)
	require.Zero(t, retryCount)

	fallback.FailureKind = "panic"
	require.NoError(t, s.PersistTailMessageFailureFallback(fallback))
	imported, err = s.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, imported)

	var secondFirstSeen string
	var secondLastSeen string
	require.NoError(t, s.DB().QueryRowContext(ctx, `
		select first_seen_at, last_seen_at, error_class, retry_count
		from failure_ledger
		where message_id = 'm1' and related_id = 'create'
	`).Scan(&secondFirstSeen, &secondLastSeen, &errorClass, &retryCount))
	require.Equal(t, firstSeen, secondFirstSeen)
	require.GreaterOrEqual(t, secondLastSeen, firstLastSeen)
	require.Equal(t, "panic", errorClass)
	require.Equal(t, 1, retryCount)

	imported, err = s.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Zero(t, imported)
	assertTailFailureLedgerStatus(t, s, "m1", 1, false)
}

func TestImportTailMessageFailureFallbacksIsExactlyOnceAcrossRemovalFailure(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	fallback := TailMessageFailureFallback{
		EventKind: "delete", FailureKind: "timeout",
		GuildID: "g1", ChannelID: "c1", MessageID: "m1",
	}
	require.NoError(t, s.PersistTailMessageFailureFallback(fallback))

	imported, err := s.importTailMessageFailureFallbacks(ctx, tailMessageFailureFallbackHooks{
		remove: func(_ *os.Root, name string) error {
			if strings.HasPrefix(name, tailMessageFailureFallbackTempPrefix) {
				return errors.New("unexpected temp cleanup")
			}
			return errors.New("forced imported fallback removal failure")
		},
	})
	require.Equal(t, 1, imported)
	require.ErrorContains(t, err, "forced imported fallback removal failure")
	assertTailFailureLedgerStatus(t, s, "m1", 0, false)
	assertTailFailureReceiptCount(t, s, 1)

	require.NoError(t, s.ResolveFailureIdentity(ctx, FailureRef{
		Operation:   tailMessageFailureOperation,
		Source:      tailMessageFailureSource,
		GuildID:     "g1",
		ChannelID:   "c1",
		MessageID:   "m1",
		RelatedKind: tailMessageFailureRelated,
		RelatedID:   "delete",
	}))

	imported, err = s.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Zero(t, imported)
	assertTailFailureLedgerStatus(t, s, "m1", 0, true)

	require.NoError(t, s.PersistTailMessageFailureFallback(fallback))
	imported, err = s.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, imported)
	assertTailFailureLedgerStatus(t, s, "m1", 1, false)
	assertTailFailureReceiptCount(t, s, 2)
}

func TestImportTailMessageFailureFallbacksIsExactlyOnceAcrossDirectorySyncFailure(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	fallback := TailMessageFailureFallback{
		EventKind: "update", FailureKind: "panic",
		GuildID: "g1", ChannelID: "c1", MessageID: "m1",
	}
	require.NoError(t, s.PersistTailMessageFailureFallback(fallback))
	dir, err := s.tailMessageFailureFallbackDir()
	require.NoError(t, err)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	name := entries[0].Name()
	body, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)

	imported, err := s.importTailMessageFailureFallbacks(ctx, tailMessageFailureFallbackHooks{
		syncDir: func(*os.File) error {
			return errors.New("forced imported fallback directory sync failure")
		},
	})
	require.Equal(t, 1, imported)
	require.ErrorContains(t, err, "forced imported fallback directory sync failure")
	assertTailFailureLedgerStatus(t, s, "m1", 0, false)
	assertTailFailureReceiptCount(t, s, 1)

	require.NoError(t, s.ResolveFailureIdentity(ctx, FailureRef{
		Operation:   tailMessageFailureOperation,
		Source:      tailMessageFailureSource,
		GuildID:     "g1",
		ChannelID:   "c1",
		MessageID:   "m1",
		RelatedKind: tailMessageFailureRelated,
		RelatedID:   "update",
	}))
	writeTailFailureTestFile(t, dir, name, body, 0o600)

	imported, err = s.ImportTailMessageFailureFallbacks(ctx)
	require.NoError(t, err)
	require.Zero(t, imported)
	assertTailFailureLedgerStatus(t, s, "m1", 0, true)
	entries, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestTailMessageFailureFallbackOperationsStayAnchoredToOpenedDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows prevents replacement while the directory handle is open")
	}
	t.Run("persist", func(t *testing.T) {
		ctx := context.Background()
		s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
		require.NoError(t, err)
		defer func() { _ = s.Close() }()
		dir, err := s.tailMessageFailureFallbackDir()
		require.NoError(t, err)
		createTailFailureTestDir(t, dir)
		openedDir := dir + ".opened"

		require.NoError(t, s.persistTailMessageFailureFallback(TailMessageFailureFallback{
			EventKind: "create", FailureKind: "timeout",
			GuildID: "g1", ChannelID: "c1", MessageID: "m1",
		}, tailMessageFailureFallbackHooks{
			afterOpen: func(path string) error {
				if err := os.Rename(path, openedDir); err != nil {
					return err
				}
				return os.Mkdir(path, 0o700)
			},
		}))

		openedEntries, err := os.ReadDir(openedDir)
		require.NoError(t, err)
		require.Len(t, openedEntries, 1)
		replacementEntries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, replacementEntries)
	})

	t.Run("import", func(t *testing.T) {
		ctx := context.Background()
		s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
		require.NoError(t, err)
		defer func() { _ = s.Close() }()
		require.NoError(t, s.PersistTailMessageFailureFallback(TailMessageFailureFallback{
			EventKind: "delete", FailureKind: "returned_error",
			GuildID: "g1", ChannelID: "c1", MessageID: "m1",
		}))
		dir, err := s.tailMessageFailureFallbackDir()
		require.NoError(t, err)
		openedDir := dir + ".opened"

		imported, err := s.importTailMessageFailureFallbacks(ctx, tailMessageFailureFallbackHooks{
			afterOpen: func(path string) error {
				if err := os.Rename(path, openedDir); err != nil {
					return err
				}
				if err := os.Mkdir(path, 0o700); err != nil {
					return err
				}
				return os.WriteFile(filepath.Join(path, "redirect.json"), []byte("invalid"), 0o600)
			},
		})
		require.NoError(t, err)
		require.Equal(t, 1, imported)
		assertTailFailureLedgerStatus(t, s, "m1", 0, false)

		openedEntries, err := os.ReadDir(openedDir)
		require.NoError(t, err)
		require.Empty(t, openedEntries)
		replacementEntries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, replacementEntries, 1)
		require.Equal(t, "redirect.json", replacementEntries[0].Name())
	})
}

func TestLinkTailFailureNoReplaceRoot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer func() { _ = root.Close() }()

	require.NoError(t, root.WriteFile("old", []byte("first"), 0o600))
	require.NoError(t, linkTailFailureNoReplaceRoot(root, "old", "new"))
	_, err = root.Stat("old")
	require.ErrorIs(t, err, os.ErrNotExist)
	body, err := root.ReadFile("new")
	require.NoError(t, err)
	require.Equal(t, []byte("first"), body)

	require.NoError(t, root.WriteFile("old", []byte("second"), 0o600))
	err = linkTailFailureNoReplaceRoot(root, "old", "new")
	require.ErrorIs(t, err, os.ErrExist)
	body, err = root.ReadFile("old")
	require.NoError(t, err)
	require.Equal(t, []byte("second"), body)
}

func committedTailFailureName(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]) + tailMessageFailureFallbackExtension
}

func createTailFailureTestDir(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, createTailFailureFallbackDir(path))
}

func writeTailFailureTestFile(t *testing.T, dir, name string, body []byte, mode os.FileMode) {
	t.Helper()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer func() { _ = root.Close() }()
	dirFile, err := root.Open(".")
	require.NoError(t, err)
	defer func() { _ = dirFile.Close() }()
	require.NoError(t, root.WriteFile(name, body, mode))
	require.NoError(t, secureTailFailureFallbackTempFile(dirFile, name))
}

func assertTailFailureLedgerStatus(
	t *testing.T,
	s *Store,
	messageID string,
	retryCount int,
	resolved bool,
) {
	t.Helper()
	var storedRetryCount int
	var resolvedAt string
	require.NoError(t, s.DB().QueryRowContext(context.Background(), `
			select retry_count, coalesce(resolved_at, '')
			from failure_ledger
			where message_id = ?
	`, messageID).Scan(&storedRetryCount, &resolvedAt))
	require.Equal(t, retryCount, storedRetryCount)
	require.Equal(t, resolved, resolvedAt != "")
}

func assertTailFailureReceiptCount(t *testing.T, s *Store, want int) {
	t.Helper()
	rows, err := s.DB().QueryContext(context.Background(), `
		select scope, coalesce(cursor, '')
		from sync_state
		where scope like ?
		order by scope
	`, tailMessageFailureReceiptPrefix+"%")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var count int
	for rows.Next() {
		var scope string
		var cursor string
		require.NoError(t, rows.Scan(&scope, &cursor))
		require.True(t, strings.HasPrefix(scope, tailMessageFailureReceiptPrefix))
		artifactID := strings.TrimPrefix(scope, tailMessageFailureReceiptPrefix)
		_, _, ok := tailMessageFailureNameParts(artifactID + tailMessageFailureFallbackExtension)
		require.True(t, ok)
		require.Empty(t, cursor)
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, want, count)
}
