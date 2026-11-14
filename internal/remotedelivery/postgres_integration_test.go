//go:build integration

package remotedelivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
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

func TestPostgresNativeDelivery(t *testing.T) { testPostgresNativeDelivery(t, false) }

func TestPostgresNativeDeliveryCommand(t *testing.T) { testPostgresNativeDelivery(t, true) }

func testPostgresNativeDelivery(t *testing.T, command bool) {
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
	seedRoot := filepath.Join(t.TempDir(), "seed")
	absentConfig := filepath.Join(t.TempDir(), "absent-collector.toml")
	var binary, qualificationSHA string
	var prepared SeedPreparation
	if command {
		binary = filepath.Join(t.TempDir(), "discrawl")
		build := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-buildvcs=false", "-o", binary, "./cmd/discrawl")
		build.Dir = "../.."
		out, err := build.CombinedOutput()
		require.NoError(t, err, "%s", out)
		cmd := exec.CommandContext(ctx, binary, "--config", absentConfig, "delivery", "prepare-seed", "--source-db", sourcePath, "--output", seedRoot, "--guilds", "g1")
		out, err = cmd.Output()
		require.NoError(t, err)
		var result struct {
			QualificationSHA256 string `json:"qualification_sha256"`
		}
		require.NoError(t, json.Unmarshal(out, &result))
		qualificationSHA = result.QualificationSHA256
		body, err := os.ReadFile(filepath.Join(seedRoot, "preparation.json"))
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(body, &prepared))
		require.NoFileExists(t, absentConfig)
	} else {
		prepared, qualificationSHA, err = PrepareSeed(ctx, sourcePath, seedRoot, Scope{GuildIDs: []string{"g1"}})
		require.NoError(t, err)
	}
	baseline := prepared.Baseline
	anchor, err := NativeAnchor(baseline.Cursor)
	require.NoError(t, err)
	fixture := startPostgresReceiver(t, ctx, backend, filepath.Join(seedRoot, "snapshot"), anchor, qualificationSHA)
	require.Equal(t, prepared.SeedManifestSHA256, fixture.ManifestSHA256)
	keysFile, err := os.Open(filepath.Join(seedRoot, "ownership.jsonl"))
	require.NoError(t, err)
	var seedKeys []SeedKey
	decoder := json.NewDecoder(keysFile)
	for {
		var key SeedKey
		err := decoder.Decode(&key)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		seedKeys = append(seedKeys, key)
	}
	require.NoError(t, keysFile.Close())
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
	var rejectDelivery bool
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reject := rejectDelivery
		mu.Unlock()
		if reject {
			http.Error(w, "fixture unavailable", http.StatusServiceUnavailable)
			return
		}
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
	registration := Registration{
		Origin: proxy.URL, Archive: "fixture", BindingID: "fixture-binding", SeedManifestSHA256: fixture.ManifestSHA256, QualificationSHA256: qualificationSHA,
		OwnedKeysSHA256: prepared.OwnedKeysSHA256, OwnedKeysCount: prepared.OwnedKeysCount, Scope: prepared.Scope, Baseline: baseline, ServerCursor: fixture.Cursor,
	}
	outboxPath := filepath.Join(t.TempDir(), "outbox.sqlite")
	var runCommand func(...string) (DeliveryResult, error)
	if command {
		runCommand = func(args ...string) (DeliveryResult, error) {
			t.Helper()
			cmd := exec.CommandContext(ctx, binary, append([]string{"--config", absentConfig, "delivery"}, args...)...)
			cmd.Env = append(os.Environ(), "DISCRAWL_TEST_PUBLISHER=fixture-publisher")
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			require.NoFileExists(t, absentConfig)
			require.NotContains(t, stderr.String(), sourcePath)
			require.NotContains(t, stderr.String(), "fixture-publisher")
			var wire struct {
				Status           DeliveryStatus `json:"status"`
				CaptureID        string         `json:"capture_id"`
				ReceiptsRecorded int            `json:"receipts_recorded"`
			}
			if stdout.Len() > 0 {
				require.NoError(t, json.Unmarshal(stdout.Bytes(), &wire), "%s", stdout.Bytes())
			}
			return DeliveryResult{Status: wire.Status, CaptureID: wire.CaptureID, ReceiptsRecorded: wire.ReceiptsRecorded}, err
		}
		registrationBody, err := json.Marshal(map[string]any{"format": "discrawl-delivery-registration/v1", "registration": registration})
		require.NoError(t, err)
		registrationPath := filepath.Join(t.TempDir(), "registration.json")
		ownershipPath := filepath.Join(seedRoot, "ownership.jsonl")
		require.NoError(t, os.WriteFile(registrationPath, registrationBody, 0o600))
		result, err := runCommand("init", "--outbox", outboxPath, "--registration", registrationPath, "--ownership", ownershipPath)
		require.NoError(t, err)
		require.Equal(t, DeliveryStatus("initialized"), result.Status)
	} else {
		initialized, err := InitOutbox(ctx, outboxPath, registration, func(yield func(SeedKey) error) error {
			for _, entry := range seedKeys {
				if err := yield(entry); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)
		require.NoError(t, initialized.Close())
	}
	outbox, err := OpenOutbox(ctx, outboxPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, outbox.Close()) }()
	deliver := func(parts int) (DeliveryResult, error) {
		if command {
			return runCommand("drain", "--outbox", outboxPath, "--token-env", "DISCRAWL_TEST_PUBLISHER", "--max-parts", fmt.Sprint(parts))
		}
		return outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(parts))
	}
	stage := func(captured store.NativeCapture) {
		t.Helper()
		if command {
			result, err := runCommand("capture", "--outbox", outboxPath, "--source-db", sourcePath)
			require.NoError(t, err)
			require.Equal(t, DeliveryStatus("staged"), result.Status)
		} else {
			_, err := outbox.Stage(ctx, captured, 8<<20)
			require.NoError(t, err)
		}
	}
	mutations := make([]store.MessageMutation, 501)
	for i := range mutations {
		mutations[i].Record = store.MessageRecord{ID: fmt.Sprintf("m%04d", i), GuildID: "g1", ChannelID: "cg1", CreatedAt: "2026-11-14T00:00:00Z", Content: "staged\x00☃", NormalizedContent: "staged", RawJSON: "{}"}
	}
	require.NoError(t, source.UpsertMessages(ctx, mutations))
	require.NoError(t, source.SetSyncState(ctx, "channel:cg1:history_complete", "complete\x00☃"))
	_, err = source.DB().ExecContext(ctx, `update channels set position=9007199254740993 where id='cg1'`)
	require.NoError(t, err)
	captured := capture(t, ctx, sourcePath, baseline)
	stage(captured)
	part, err := outbox.NextPart(ctx)
	require.NoError(t, err)
	firstBody := append([]byte(nil), part.Body...)
	result, err := deliver(1)
	if command {
		require.Error(t, err)
	} else {
		require.ErrorIs(t, err, ErrDeliverySend)
	}
	require.Equal(t, 0, result.ReceiptsRecorded)
	require.Equal(t, fixture.Sequence+1, sequence(), "remote commit survived the dropped response")
	require.Equal(t, initial.Stats.SnapshotID, query(remote.QueryRequest{Name: "discrawl.live.binding"}).Stats.SnapshotID)
	require.Equal(t, initialMessages, query(remote.QueryRequest{Name: "discrawl.live.messages", SnapshotID: initial.Stats.SnapshotID}))
	mutations[0].Record.Content = "newer than staged"
	require.NoError(t, source.UpsertMessages(ctx, mutations[:1]))
	require.NoError(t, outbox.Close())
	outbox, err = OpenOutbox(ctx, outboxPath)
	require.NoError(t, err)
	result, err = deliver(1)
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
	result, err = deliver(1)
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
	stage(next)
	_, err = outbox.base.DB().ExecContext(ctx, `create trigger fail_final_receipt before update on binding begin select raise(abort,'fixture local receipt failure'); end`)
	require.NoError(t, err)
	result, err = deliver(1)
	if command {
		require.Error(t, err)
	} else {
		require.ErrorIs(t, err, ErrDeliveryState)
	}
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
	result, err = deliver(1)
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
	result, err = deliver(1)
	require.NoError(t, err)
	require.Equal(t, DeliveryIdle, result.Status)
	if command {
		// A timer invocation must resume saved bytes before reading newer native
		// state. A failed resume cannot turn into a replacement capture.
		mutations[0].Record.Content = "pending timer capture"
		require.NoError(t, source.UpsertMessages(ctx, mutations[:1]))
		pendingCapture := capture(t, ctx, sourcePath, state.Baseline)
		stage(pendingCapture)
		pendingPart, err := outbox.NextPart(ctx)
		require.NoError(t, err)
		savedBody := append([]byte(nil), pendingPart.Body...)
		beforeCycle := sequence()
		mutations[0].Record.Content = "newer timer capture"
		require.NoError(t, source.UpsertMessages(ctx, mutations[:1]))
		type cyclePhase struct{ Phase, Status string }
		runCycle := func(path string) ([]cyclePhase, error) {
			t.Helper()
			cmd := exec.CommandContext(ctx, binary, "--config", absentConfig, "delivery", "cycle", "--outbox", outboxPath, "--source-db", path, "--token-env", "DISCRAWL_TEST_PUBLISHER")
			cmd.Env = append(os.Environ(), "DISCRAWL_TEST_PUBLISHER=fixture-publisher")
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			runErr := cmd.Run()
			require.NotContains(t, stderr.String(), sourcePath)
			require.NotContains(t, stderr.String(), "fixture-publisher")
			require.NoFileExists(t, absentConfig)
			var phases []cyclePhase
			decoder := json.NewDecoder(&stdout)
			for {
				var phase cyclePhase
				err := decoder.Decode(&phase)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				phases = append(phases, phase)
			}
			return phases, runErr
		}
		mu.Lock()
		rejectDelivery = true
		mu.Unlock()
		phases, err := runCycle(filepath.Join(t.TempDir(), "absent-native.sqlite"))
		require.Error(t, err)
		require.Equal(t, []cyclePhase{{"resume", "pending"}}, phases)
		require.Equal(t, beforeCycle, sequence())
		blocked, err := outbox.State(ctx)
		require.NoError(t, err)
		require.Equal(t, state.Baseline, blocked.Baseline)
		require.NotEmpty(t, blocked.PendingID)
		mu.Lock()
		rejectDelivery = false
		mu.Unlock()
		phases, err = runCycle(sourcePath)
		require.NoError(t, err)
		require.Equal(t, []cyclePhase{{"resume", "complete"}, {"capture", "staged"}, {"deliver", "complete"}}, phases)
		require.Equal(t, beforeCycle+2, sequence())
		mu.Lock()
		require.Equal(t, savedBody, requests[len(requests)-2])
		mu.Unlock()
		found := false
		pin := query(remote.QueryRequest{Name: "discrawl.live.binding"}).Stats.SnapshotID
		for req := (remote.QueryRequest{Name: "discrawl.live.messages", SnapshotID: pin, Limit: 500}); ; {
			page := query(req)
			for _, row := range page.Rows {
				if row[0] == "m0000" {
					found = true
					require.Equal(t, "newer timer capture", row[8])
				}
			}
			if page.Cursor == "" {
				break
			}
			req.Cursor = page.Cursor
		}
		require.True(t, found)
		phases, err = runCycle(sourcePath)
		require.NoError(t, err)
		require.Equal(t, []cyclePhase{{"resume", "idle"}, {"capture", "idle"}, {"deliver", "idle"}}, phases)
		require.Equal(t, beforeCycle+2, sequence())
	}
	t.Logf("503 changes/two parts; lost-response replay at %d; final promotion at %d; local receipt failure/replay at %d; scope exit and old pins verified", fixture.Sequence+1, fixture.Sequence+2, fixture.Sequence+3)
}
