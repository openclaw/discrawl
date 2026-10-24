//go:build ignore

package scripts_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/share"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPrivateCheckpointReplaysNativePublication(t *testing.T) {
	ctx := context.Background()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	scripts := filepath.Dir(file)
	dir := t.TempDir()
	path := filepath.Join(dir, "native.db")
	before, nativeSyncRows := seedNativeCheckpoint(t, path)
	checkpoint := filepath.Join(dir, "checkpoint.db")
	python := exec.CommandContext(ctx, "python3", "-c", "from pathlib import Path; import sys; sys.path.insert(0,sys.argv[1]); from private_discord_checkpoint import snapshot; snapshot(Path(sys.argv[2]),Path(sys.argv[3]))", scripts, path, checkpoint)
	out, err := python.CombinedOutput()
	require.NoError(t, err, "%s", out)

	git := func(args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", args...)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
	}
	remote, repo := filepath.Join(dir, "remote.git"), filepath.Join(dir, "archive")
	git("init", "--bare", "--initial-branch=main", remote)
	git("-C", remote, "config", "core.hooksPath", filepath.Join(remote, "hooks"))
	git("init", "--initial-branch=main", repo)
	git("-C", repo, "config", "user.name", "Fixture")
	git("-C", repo, "config", "user.email", "fixture@example.invalid")
	git("-C", repo, "config", "commit.gpgsign", "false")
	git("-C", repo, "remote", "add", "origin", remote)
	git("-C", repo, "commit", "--allow-empty", "-m", "fixture")
	git("-C", repo, "push", "-u", "origin", "main")

	for index, failedPush := range []bool{false, true, false} {
		// Restore the same verified ingestion boundary, including retry and
		// crawl cursors; never import the filtered publication back into it.
		body, err := os.ReadFile(checkpoint)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, body, 0o600))
		db, err := store.Open(ctx, path)
		require.NoError(t, err)
		require.Equal(t, before, nativeCheckpointFacts(t, db))
		opts := share.Options{RepoPath: repo, Remote: remote, Branch: "main", Producer: &share.PublicationProducer{
			Repository: "openclaw/discrawl", Revision: strings.Repeat("a", 40), RunID: int64(index + 1), RunAttempt: 1,
		}}
		manifest, err := share.Export(ctx, db, opts)
		require.NoError(t, err)
		_, err = share.Commit(ctx, opts, "fixture publication")
		require.NoError(t, err)
		// Recovery uses this exact export/commit boundary without native push:
		// no import bookkeeping or new collection may precede its guarded push.
		require.Equal(t, before, nativeCheckpointFacts(t, db))
		var afterExportSyncRows int
		require.NoError(t, db.DB().QueryRowContext(ctx, "SELECT count(*) FROM sync_state").Scan(&afterExportSyncRows))
		require.Equal(t, nativeSyncRows, afterExportSyncRows)
		hook := filepath.Join(remote, "hooks", "pre-receive")
		if failedPush {
			require.NoError(t, os.WriteFile(hook, []byte("#!/bin/sh\nexit 1\n"), 0o700))
		}
		err = share.Push(ctx, opts)
		if failedPush {
			require.Error(t, err)
			require.NoError(t, os.Remove(hook))
		} else {
			require.NoError(t, err)
			require.NoError(t, share.MarkImported(ctx, db, manifest))
		}
		require.Equal(t, before, nativeCheckpointFacts(t, db))
		for _, table := range manifest.Tables {
			if table.Name == "messages" {
				require.Equal(t, 1, table.Rows)
			}
			if table.Name == "sync_state" {
				require.Equal(t, nativeSyncRows, table.Rows, "the restored ingestion boundary remains complete")
			}
		}
		require.NoError(t, db.Close())
	}
}

func TestPrivateCheckpointAutomaticAuthority(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	scripts := filepath.Dir(file)
	dir := t.TempDir()
	database, native := filepath.Join(dir, "native.db"), filepath.Join(dir, "discrawl-native")
	seedNativeCheckpoint(t, database)
	build := exec.CommandContext(ctx, "go", "build", "-o", native, "./cmd/discrawl")
	build.Dir = filepath.Dir(scripts)
	out, err := build.CombinedOutput()
	require.NoError(t, err, "%s", out)
	// Only external API/network transport is substituted. The real CLI, Git,
	// checkpoint verifier and automatic admission own every publication decision.
	proof := exec.CommandContext(ctx, "python3", filepath.Join(scripts, "private_checkpoint_authority_test.py"), database, native)
	out, err = proof.CombinedOutput()
	require.NoError(t, err, "%s", out)
	t.Logf("%s", out)
}

func seedNativeCheckpoint(t *testing.T, path string) ([]any, int) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, path)
	require.NoError(t, err)
	require.NoError(t, db.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Fixture", RawJSON: `{}`}))
	require.NoError(t, db.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "fixture", RawJSON: `{}`}))
	require.NoError(t, db.UpsertMessages(ctx, []store.MessageMutation{{Record: store.MessageRecord{
		ID: "m1", GuildID: "g1", ChannelID: "c1", AuthorID: "u1", Content: "retained fixture",
		NormalizedContent: "retained fixture", CreatedAt: "2026-01-01T00:00:00Z", RawJSON: `{}`,
	}}}))
	for _, scope := range []string{"channel:c1:latest", "channel:c1:backfill", "channel:c1:verification"} {
		require.NoError(t, db.SetSyncState(ctx, scope, "retained-progress"))
	}
	require.NoError(t, db.RecordFailure(ctx, store.FailureRef{Operation: "messages", Source: "discord", GuildID: "g1", ChannelID: "c1"}, errors.New("fixture retry")))
	before := nativeCheckpointFacts(t, db)
	var nativeSyncRows int
	require.NoError(t, db.DB().QueryRowContext(ctx, "SELECT count(*) FROM sync_state").Scan(&nativeSyncRows))
	require.NoError(t, db.Close())
	return before, nativeSyncRows
}

func nativeCheckpointFacts(t *testing.T, db *store.Store) []any {
	t.Helper()
	ctx := context.Background()
	var count int
	require.NoError(t, db.DB().QueryRowContext(ctx, "SELECT count(*) FROM messages").Scan(&count))
	result := []any{count}
	for _, scope := range []string{"channel:c1:latest", "channel:c1:backfill", "channel:c1:verification"} {
		value, err := db.GetSyncState(ctx, scope)
		require.NoError(t, err)
		result = append(result, value)
	}
	failures, err := db.ListFailures(ctx, store.FailureListOptions{}, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	return append(result, failures)
}
