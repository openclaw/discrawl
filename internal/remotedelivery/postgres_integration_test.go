//go:build integration

package remotedelivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/crawlkit/snapshot"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

type postgresFixture struct {
	Origin         string `json:"origin"`
	Cursor         string `json:"cursor"`
	ManifestSHA256 string `json:"manifest_sha256"`
	Sequence       int64  `json:"sequence"`
}

func startPostgresReceiver(t *testing.T, ctx context.Context, backend, seed, anchor, qualification string) postgresFixture {
	t.Helper()
	helper, err := filepath.Abs("testdata/postgres_receiver/main.go")
	require.NoError(t, err)
	binary := filepath.Join(t.TempDir(), "postgres-receiver")
	build := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-buildvcs=false", "-o", binary, helper)
	build.Dir = backend
	output, err := build.CombinedOutput()
	require.NoError(t, err, "%s", output)
	docker := func(args ...string) string {
		t.Helper()
		bounded, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		out, err := exec.CommandContext(bounded, "docker", args...).CombinedOutput()
		require.NoError(t, err, "fixture docker %s: %s", args[0], out)
		return strings.TrimSpace(string(out))
	}
	id := docker("run", "--detach", "--rm", "--pull=never", "--memory", "512m", "--cpus", "1",
		"--tmpfs", "/var/lib/postgresql/data:rw,nosuid,size=384m", "--label", "discrawl.test=native-delivery",
		"-p", "127.0.0.1::5432", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_USER=crawl_test", "-e", "POSTGRES_DB=crawl_test", "postgres:17-alpine")
	t.Cleanup(func() { docker("stop", "--time", "1", id) })
	address := docker("port", id, "5432/tcp")
	cmd := exec.CommandContext(ctx, binary, "postgres://crawl_test@"+address+"/crawl_test?sslmode=disable", seed, anchor, qualification)
	cmd.Dir = backend
	cmd.Stderr = os.Stderr
	input, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = input.Close()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill() // Only this test's helper, never a shared server.
			<-done
			t.Error("fixture receiver did not stop after its input closed")
		}
	})
	var ready postgresFixture
	require.NoError(t, json.NewDecoder(stdout).Decode(&ready))
	require.NotEmpty(t, ready.Origin)
	return ready
}

func TestPostgresNativeDelivery(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native capture identity is supported on macOS and Linux")
	}
	backend := os.Getenv("DISCRAWL_TEST_CRAWL_REMOTE")
	if backend == "" {
		t.Skip("set DISCRAWL_TEST_CRAWL_REMOTE to a crawl-remote checkout; requires cached postgres:17-alpine")
	}
	// Closing input must stop the child normally even after an assertion fails.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	module, err := os.ReadFile(filepath.Join(backend, "go.mod"))
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(module, []byte("module github.com/openclaw/crawl-remote\n")))
	revision, err := exec.CommandContext(ctx, "git", "-C", backend, "rev-parse", "HEAD").Output()
	require.NoError(t, err)
	dirty, err := exec.CommandContext(ctx, "git", "-C", backend, "status", "--porcelain", "--untracked-files=all").Output()
	require.NoError(t, err)
	require.Empty(t, string(dirty), "use a clean backend checkout for revision-specific proof")
	t.Logf("crawl-remote revision: %s", strings.TrimSpace(string(revision)))
	sourcePath := filepath.Join(t.TempDir(), "source.sqlite")
	source, err := store.Open(ctx, sourcePath)
	require.NoError(t, err)
	defer source.Close()
	require.NoError(t, source.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "fixture", RawJSON: "{}"}))
	require.NoError(t, source.UpsertChannel(ctx, store.ChannelRecord{ID: "cg1", GuildID: "g1", Name: "fixture", Kind: "text", RawJSON: "{}"}))
	writeMessage(t, ctx, source, "seed", "g1")
	require.NoError(t, source.SetSyncState(ctx, "channel:cg1:history_complete", "seed"))
	seedRoot := t.TempDir()
	var seedKeys []SeedKey
	_, err = snapshot.Export(ctx, snapshot.ExportOptions{DB: source.DB(), RootDir: seedRoot,
		Tables: []string{"guilds", "channels", "members", "messages", "sync_state"},
		Filter: func(table string, row map[string]any) (bool, error) {
			// The fixture has one whole-guild scope. Internal host/schema receipts
			// are not in this profile, even when the native store creates them.
			if table == "sync_state" && !strings.HasPrefix(row["scope"].(string), "channel:cg1:") && !strings.HasPrefix(row["scope"].(string), "guild:g1:") {
				return false, nil
			}
			entry := SeedKey{Table: table}
			for _, name := range keys[table] {
				entry.Key = append(entry.Key, store.NativeValue{Kind: "text", Text: row[name].(string)})
			}
			seedKeys = append(seedKeys, entry)
			return true, nil
		}})
	require.NoError(t, err)
	baseline, err := store.ObserveNativeBaseline(ctx, sourcePath)
	require.NoError(t, err)
	anchor, err := NativeAnchor(baseline.Cursor)
	require.NoError(t, err)
	qualification := sha256.Sum256([]byte("single-writer synthetic native seed"))
	qualificationSHA := hex.EncodeToString(qualification[:])
	fixture := startPostgresReceiver(t, ctx, backend, seedRoot, anchor, qualificationSHA)
	manifest, err := os.ReadFile(filepath.Join(seedRoot, snapshot.ManifestName))
	require.NoError(t, err)
	manifestHash := sha256.Sum256(manifest)
	require.Equal(t, hex.EncodeToString(manifestHash[:]), fixture.ManifestSHA256)
	reader, err := remote.NewClient(remote.Options{Endpoint: fixture.Origin, TokenProvider: remote.StaticToken("fixture-reader")})
	require.NoError(t, err)
	query := func(req remote.QueryRequest) remote.QueryResult {
		t.Helper()
		result, err := reader.Query(ctx, "discrawl", "fixture", req)
		require.NoError(t, err)
		return result
	}
	sequence := func() int64 {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, "GET", fixture.Origin+"/fixture/sequence", nil)
		require.NoError(t, err)
		response, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		var n int64
		require.Equal(t, 200, response.StatusCode)
		require.NoError(t, json.NewDecoder(response.Body).Decode(&n))
		return n
	}
	initial := query(remote.QueryRequest{Name: "discrawl.live.binding"})
	initialMessages := query(remote.QueryRequest{Name: "discrawl.live.messages", SnapshotID: initial.Stats.SnapshotID})
	require.Len(t, initialMessages.Rows, 1)
	resume := initial.Rows[0][13].(string)
	var mu sync.Mutex
	var requests [][]byte
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "fixture body read failed", 500)
			return
		}
		req, err := http.NewRequestWithContext(r.Context(), r.Method, fixture.Origin+r.URL.RequestURI(), bytes.NewReader(body))
		if err != nil {
			http.Error(w, "fixture forwarding failed", 500)
			return
		}
		req.Header = r.Header.Clone()
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, "fixture receiver failed", 502)
			return
		}
		defer response.Body.Close()
		payload, err := io.ReadAll(response.Body)
		if err != nil {
			http.Error(w, "fixture receipt read failed", 502)
			return
		}
		mu.Lock()
		requests = append(requests, body)
		drop := len(requests) == 1 && response.StatusCode == 200
		mu.Unlock()
		if drop {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(payload)
	}))
	defer proxy.Close()
	slices.SortFunc(seedKeys, func(a, b SeedKey) int {
		ak, _ := OwnershipKey(a.Table, a.Key)
		bk, _ := OwnershipKey(b.Table, b.Key)
		return strings.Compare(ak, bk)
	})
	hash := sha256.New()
	for _, entry := range seedKeys {
		key, err := OwnershipKey(entry.Table, entry.Key)
		require.NoError(t, err)
		hashOwnershipKey(hash, key)
	}
	registration := Registration{Origin: proxy.URL, Archive: "fixture", BindingID: "fixture-binding", SeedManifestSHA256: fixture.ManifestSHA256, QualificationSHA256: qualificationSHA,
		OwnedKeysSHA256: hex.EncodeToString(hash.Sum(nil)), OwnedKeysCount: int64(len(seedKeys)), Scope: Scope{GuildIDs: []string{"g1"}}, Baseline: baseline, ServerCursor: fixture.Cursor}
	outboxPath := filepath.Join(t.TempDir(), "outbox.sqlite")
	outbox, err := InitOutbox(ctx, outboxPath, registration, func(yield func(SeedKey) error) error {
		for _, entry := range seedKeys {
			if err := yield(entry); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, outbox.Close()) }()
	mutations := make([]store.MessageMutation, 501)
	for i := range mutations {
		mutations[i].Record = store.MessageRecord{ID: fmt.Sprintf("m%04d", i), GuildID: "g1", ChannelID: "cg1", CreatedAt: "2026-11-14T00:00:00Z", Content: "staged\x00☃", NormalizedContent: "staged", RawJSON: "{}"}
	}
	require.NoError(t, source.UpsertMessages(ctx, mutations))
	require.NoError(t, source.SetSyncState(ctx, "channel:cg1:history_complete", "complete\x00☃"))
	_, err = source.DB().ExecContext(ctx, `update channels set position=9007199254740993 where id='cg1'`)
	require.NoError(t, err)
	captured := capture(t, ctx, sourcePath, baseline)
	_, err = outbox.Stage(ctx, captured, 8<<20)
	require.NoError(t, err)
	part, err := outbox.NextPart(ctx)
	require.NoError(t, err)
	firstBody := append([]byte(nil), part.Body...)
	result, err := outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(1))
	require.ErrorIs(t, err, ErrDeliverySend)
	require.Equal(t, 0, result.ReceiptsRecorded)
	require.Equal(t, fixture.Sequence+1, sequence(), "remote commit survived the dropped response")
	require.Equal(t, initial.Stats.SnapshotID, query(remote.QueryRequest{Name: "discrawl.live.binding"}).Stats.SnapshotID)
	require.Equal(t, initialMessages, query(remote.QueryRequest{Name: "discrawl.live.messages", SnapshotID: initial.Stats.SnapshotID}))
	mutations[0].Record.Content = "newer than staged"
	require.NoError(t, source.UpsertMessages(ctx, mutations[:1]))
	require.NoError(t, outbox.Close())
	outbox, err = OpenOutbox(ctx, outboxPath)
	require.NoError(t, err)
	result, err = outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(1))
	require.NoError(t, err)
	require.Equal(t, DeliveryPartLimit, result.Status)
	require.Equal(t, fixture.Sequence+1, sequence(), "exact replay must not create a second commit")
	mu.Lock()
	replayedBody := append([]byte(nil), requests[1]...)
	mu.Unlock()
	require.Equal(t, firstBody, replayedBody)
	state, err := outbox.State(ctx)
	require.NoError(t, err)
	require.Equal(t, baseline, state.Baseline)
	waiting := query(remote.QueryRequest{Name: "discrawl.live.changes", Cursor: resume})
	require.Empty(t, waiting.Rows, "partial captures are invisible to incremental readers")
	result, err = outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(1))
	require.NoError(t, err)
	require.Equal(t, DeliveryComplete, result.Status)
	require.Equal(t, fixture.Sequence+2, sequence())
	state, err = outbox.State(ctx)
	require.NoError(t, err)
	require.Equal(t, captured.Through, state.Baseline)
	live := query(remote.QueryRequest{Name: "discrawl.live.binding"})
	require.NotEqual(t, initial.Stats.SnapshotID, live.Stats.SnapshotID)
	var rows [][]any
	for request := (remote.QueryRequest{Name: "discrawl.live.messages", SnapshotID: live.Stats.SnapshotID, Limit: 500}); ; {
		page := query(request)
		rows = append(rows, page.Rows...)
		if page.Cursor == "" {
			break
		}
		request.Cursor = page.Cursor
	}
	require.Len(t, rows, 502)
	foundStaged := false
	for _, row := range rows {
		if row[0] == "m0000" {
			foundStaged = true
			require.Equal(t, "staged\x00☃", row[8])
			require.Nil(t, row[6], "native NULL survives the service")
		}
	}
	require.True(t, foundStaged, "the staged message must be present, not only correct if returned")
	channels := query(remote.QueryRequest{Name: "discrawl.live.channels", SnapshotID: live.Stats.SnapshotID})
	require.Equal(t, "9007199254740993", channels.Rows[0][6])
	checkpoints := query(remote.QueryRequest{Name: "discrawl.live.sync_state", SnapshotID: live.Stats.SnapshotID})
	require.Equal(t, "complete\x00☃", checkpoints.Rows[0][1])
	require.Equal(t, initialMessages, query(remote.QueryRequest{Name: "discrawl.live.messages", SnapshotID: initial.Stats.SnapshotID}))
	var delivered [][]any
	for cursor := resume; ; {
		page := query(remote.QueryRequest{Name: "discrawl.live.changes", Cursor: cursor, Limit: 500})
		require.Equal(t, live.Stats.SnapshotID, page.Stats.SnapshotID)
		delivered = append(delivered, page.Rows...)
		if page.Cursor == "" {
			require.NotEmpty(t, page.Stats.NextCursor)
			break
		}
		cursor = page.Cursor
	}
	require.Len(t, delivered, 503, "the incremental reader must include messages and metadata once")
	// A later scope exit retracts channel/checkpoint metadata, while messages
	// still use their own guild_id. The previously staged source edit also flows.
	require.NoError(t, source.UpsertGuild(ctx, store.GuildRecord{ID: "g2", Name: "excluded", RawJSON: "{}"}))
	require.NoError(t, source.UpsertChannel(ctx, store.ChannelRecord{ID: "cg2", GuildID: "g2", Name: "excluded", Kind: "text", RawJSON: "{}"}))
	writeMessage(t, ctx, source, "excluded", "g2")
	_, err = source.DB().ExecContext(ctx, `update channels set guild_id='g2' where id='cg1'`)
	require.NoError(t, err)
	next := capture(t, ctx, sourcePath, state.Baseline)
	_, err = outbox.Stage(ctx, next, 8<<20)
	require.NoError(t, err)
	_, err = outbox.base.DB().ExecContext(ctx, `create trigger fail_final_receipt before update on binding begin select raise(abort,'fixture local receipt failure'); end`)
	require.NoError(t, err)
	result, err = outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(1))
	require.ErrorIs(t, err, ErrDeliveryState)
	require.Equal(t, 0, result.ReceiptsRecorded)
	require.Equal(t, fixture.Sequence+3, sequence())
	failed, err := outbox.State(ctx)
	require.NoError(t, err)
	require.Equal(t, state.Baseline, failed.Baseline)
	_, err = outbox.base.DB().ExecContext(ctx, `drop trigger fail_final_receipt`)
	require.NoError(t, err)
	require.NoError(t, outbox.Close())
	outbox, err = OpenOutbox(ctx, outboxPath)
	require.NoError(t, err)
	result, err = outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(1))
	require.NoError(t, err)
	require.Equal(t, DeliveryComplete, result.Status)
	require.Equal(t, fixture.Sequence+3, sequence(), "failed local receipt replays the completed remote capture")
	state, err = outbox.State(ctx)
	require.NoError(t, err)
	require.Equal(t, next.Through, state.Baseline)
	mu.Lock()
	afterFailure := append([]byte(nil), requests[len(requests)-2]...)
	afterRetry := append([]byte(nil), requests[len(requests)-1]...)
	mu.Unlock()
	require.Equal(t, afterFailure, afterRetry)
	current := query(remote.QueryRequest{Name: "discrawl.live.binding"})
	require.NotEqual(t, live.Stats.SnapshotID, current.Stats.SnapshotID)
	require.Empty(t, query(remote.QueryRequest{Name: "discrawl.live.channels", SnapshotID: current.Stats.SnapshotID}).Rows)
	require.Empty(t, query(remote.QueryRequest{Name: "discrawl.live.sync_state", SnapshotID: current.Stats.SnapshotID}).Rows)
	var latest [][]any
	for request := (remote.QueryRequest{Name: "discrawl.live.messages", SnapshotID: current.Stats.SnapshotID, Limit: 500}); ; {
		page := query(request)
		latest = append(latest, page.Rows...)
		if page.Cursor == "" {
			break
		}
		request.Cursor = page.Cursor
	}
	require.Len(t, latest, 502)
	foundLatest := false
	for _, row := range latest {
		require.NotEqual(t, "excluded", row[0])
		if row[0] == "m0000" {
			foundLatest = true
			require.Equal(t, "newer than staged", row[8])
		}
	}
	require.True(t, foundLatest, "the later message edit must be delivered")
	require.Equal(t, channels, query(remote.QueryRequest{Name: "discrawl.live.channels", SnapshotID: live.Stats.SnapshotID}), "a later scope exit must not change an older pin")
	result, err = outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(1))
	require.NoError(t, err)
	require.Equal(t, DeliveryIdle, result.Status)
	t.Logf("503 changes/two parts; lost-response replay at %d; final promotion at %d; local receipt failure/replay at %d; scope exit and old pins verified", fixture.Sequence+1, fixture.Sequence+2, fixture.Sequence+3)
}
