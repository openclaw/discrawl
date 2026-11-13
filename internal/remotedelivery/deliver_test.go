package remotedelivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

// This peer exercises the shared HTTP receipt contract. PostgreSQL receiver
// qualification remains a separate integration proof.
type deliveryPeer struct {
	mu       sync.Mutex
	requests [][]byte
	receipts map[string]remote.ChangeReceipt
	cursor   string
	drop     int
	after    func(http.ResponseWriter, remote.ChangeReceipt) bool
}

func newDeliveryPeer(t *testing.T) (*deliveryPeer, *httptest.Server) {
	t.Helper()
	p := &deliveryPeer{receipts: map[string]remote.ChangeReceipt{}, cursor: "seed:complete"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/apps/discrawl/archives/fixture/changes" || r.Header.Get("Authorization") != "Bearer fixture-publisher" {
			http.Error(w, "unexpected fixture request", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, remote.MaxChangeBytes))
		if err != nil {
			http.Error(w, "invalid fixture body", http.StatusBadRequest)
			return
		}
		batch, err := remote.DecodeChangeBatch(body)
		if err != nil {
			http.Error(w, "invalid fixture batch", http.StatusBadRequest)
			return
		}
		digest := sha256.Sum256(body)
		sha := hex.EncodeToString(digest[:])
		p.mu.Lock()
		p.requests = append(p.requests, body)
		receipt, replay := p.receipts[batch.BatchID()]
		if replay {
			if receipt.PayloadSHA256 != sha {
				p.mu.Unlock()
				http.Error(w, "changed replay", http.StatusConflict)
				return
			}
			receipt.Replayed = true
		} else {
			if batch.ExpectedCursor != p.cursor {
				p.mu.Unlock()
				http.Error(w, "cursor conflict", http.StatusConflict)
				return
			}
			receipt = remote.ChangeReceipt{App: batch.App, Archive: batch.Archive, BindingID: batch.BindingID, BatchID: batch.BatchID(), Cursor: batch.BatchID(), PayloadSHA256: sha, Sequence: strconv.Itoa(len(p.receipts) + 1)}
			p.receipts[batch.BatchID()] = receipt
			p.cursor = receipt.Cursor
		}
		drop := p.drop > 0
		if drop {
			p.drop--
		}
		p.mu.Unlock()
		if drop {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		if p.after != nil && p.after(w, receipt) {
			return
		}
		_ = json.NewEncoder(w).Encode(receipt)
	}))
	t.Cleanup(server.Close)
	return p, server
}

func (p *deliveryPeer) snapshot() ([][]byte, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]byte(nil), p.requests...), len(p.receipts)
}

func deliveryFixture(t *testing.T, origin string, count int) (context.Context, *store.Store, string, *Outbox, string, store.NativeCapture) {
	t.Helper()
	ctx, source, path, outbox, outboxPath, baseline := outboxFixtureAt(t, origin)
	if count == 0 {
		return ctx, source, path, outbox, outboxPath, store.NativeCapture{}
	}
	mutations := make([]store.MessageMutation, count)
	for i := range mutations {
		mutations[i].Record = store.MessageRecord{ID: fmt.Sprintf("m%04d", i), GuildID: "g1", ChannelID: "cg1", AuthorID: "u", CreatedAt: "2026-11-14T00:00:00Z", Content: "delivery", NormalizedContent: "delivery", RawJSON: "{}"}
	}
	require.NoError(t, source.UpsertMessages(ctx, mutations))
	captured := capture(t, ctx, path, baseline)
	_, err := outbox.Stage(ctx, captured, 8<<20)
	require.NoError(t, err)
	return ctx, source, path, outbox, outboxPath, captured
}

func deliveryOptions() DeliveryOptions {
	return DeliveryOptions{TokenProvider: remote.StaticToken("fixture-publisher")}
}

func deliveryLimits(parts int) DeliveryLimits {
	return DeliveryLimits{MaxParts: parts, MaxDuration: 5 * time.Second}
}

func TestDeliveryPartBudgetAndReopen(t *testing.T) {
	peer, server := newDeliveryPeer(t)
	ctx, _, _, outbox, path, captured := deliveryFixture(t, server.URL, 501)
	result, err := outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(1))
	require.NoError(t, err)
	require.Equal(t, DeliveryPartLimit, result.Status)
	require.Equal(t, 1, result.ReceiptsRecorded)
	state, err := outbox.State(ctx)
	require.NoError(t, err)
	require.True(t, sameBaseline(captured.From, state.Baseline))
	require.NoError(t, outbox.Close())
	reopened, err := OpenOutbox(ctx, path)
	require.NoError(t, err)
	defer reopened.Close()
	result, err = reopened.DeliverPending(ctx, deliveryOptions(), deliveryLimits(1))
	require.NoError(t, err)
	require.Equal(t, DeliveryComplete, result.Status)
	state, err = reopened.State(ctx)
	require.NoError(t, err)
	require.True(t, sameBaseline(captured.Through, state.Baseline))
	requests, receipts := peer.snapshot()
	require.Len(t, requests, 2)
	require.Equal(t, 2, receipts)
}

func TestDeliveryReplaysUncertainRemoteCommit(t *testing.T) {
	peer, server := newDeliveryPeer(t)
	peer.drop = 1
	ctx, _, _, outbox, _, captured := deliveryFixture(t, server.URL, 1)
	result, err := outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(2))
	require.ErrorIs(t, err, ErrDeliverySend)
	require.Equal(t, 0, result.ReceiptsRecorded)
	requests, receipts := peer.snapshot()
	require.Len(t, requests, 1, "one send per invocation; no hidden retry")
	require.Equal(t, 1, receipts, "remote commit survived the lost response")
	state, err := outbox.State(ctx)
	require.NoError(t, err)
	require.True(t, sameBaseline(captured.From, state.Baseline))
	result, err = outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(2))
	require.NoError(t, err)
	require.Equal(t, DeliveryComplete, result.Status)
	requests, receipts = peer.snapshot()
	require.Len(t, requests, 2)
	require.Equal(t, requests[0], requests[1])
	require.Equal(t, 1, receipts)
}

func TestDeliveryReplaysAfterLocalReceiptFailure(t *testing.T) {
	peer, server := newDeliveryPeer(t)
	ctx, _, _, outbox, _, _ := deliveryFixture(t, server.URL, 1)
	_, err := outbox.base.DB().ExecContext(ctx, `create trigger fail_receipt before update on binding begin select raise(abort,'private fixture data'); end`)
	require.NoError(t, err)
	result, err := outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(2))
	require.ErrorIs(t, err, ErrDeliveryState)
	require.NotContains(t, err.Error(), "private fixture data")
	require.Equal(t, 0, result.ReceiptsRecorded)
	_, err = outbox.base.DB().ExecContext(ctx, `drop trigger fail_receipt`)
	require.NoError(t, err)
	result, err = outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(2))
	require.NoError(t, err)
	require.Equal(t, DeliveryComplete, result.Status)
	requests, receipts := peer.snapshot()
	require.Len(t, requests, 2)
	require.Equal(t, requests[0], requests[1])
	require.Equal(t, 1, receipts)
}

func TestDeliveryPinsInitialCapture(t *testing.T) {
	peer, server := newDeliveryPeer(t)
	ctx, source, sourcePath, outbox, outboxPath, first := deliveryFixture(t, server.URL, 1)
	writeMessage(t, ctx, source, "next", "g1")
	next := capture(t, ctx, sourcePath, first.Through)
	other, err := OpenOutbox(ctx, outboxPath)
	require.NoError(t, err)
	defer other.Close()
	part, err := other.NextPart(ctx)
	require.NoError(t, err)
	var nextID string
	var hookErr error
	peer.after = func(_ http.ResponseWriter, receipt remote.ChangeReceipt) bool {
		hookErr = other.RecordReceipt(ctx, part.Ref, receipt)
		if hookErr == nil {
			nextID, hookErr = other.Stage(ctx, next, 1<<20)
		}
		return false
	}
	result, err := outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(64))
	require.NoError(t, err)
	require.NoError(t, hookErr)
	require.Equal(t, DeliveryComplete, result.Status)
	require.Equal(t, part.Ref.CaptureID, result.CaptureID)
	state, err := other.State(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, nextID)
	require.Equal(t, nextID, state.PendingID)
	requests, _ := peer.snapshot()
	require.Len(t, requests, 1, "successor must remain for the next invocation")
}

type deliveryTokenFunc func(context.Context) (string, error)

func (f deliveryTokenFunc) Token(ctx context.Context) (string, error) { return f(ctx) }

func TestDeliveryLimitsIdleAndCancellation(t *testing.T) {
	peer, server := newDeliveryPeer(t)
	ctx, source, path, outbox, _, _ := deliveryFixture(t, server.URL, 0)
	for _, limits := range []DeliveryLimits{{MaxParts: 0, MaxDuration: time.Second}, {MaxParts: 65, MaxDuration: time.Second}, {MaxParts: 1}} {
		_, err := outbox.DeliverPending(ctx, deliveryOptions(), limits)
		require.ErrorIs(t, err, ErrDeliveryOptions)
	}
	_, err := outbox.DeliverPending(ctx, DeliveryOptions{}, deliveryLimits(1))
	require.ErrorIs(t, err, ErrDeliveryOptions)
	result, err := outbox.DeliverPending(ctx, deliveryOptions(), deliveryLimits(1))
	require.NoError(t, err)
	require.Equal(t, DeliveryIdle, result.Status)
	state, err := outbox.State(ctx)
	require.NoError(t, err)
	writeMessage(t, ctx, source, "m", "g1")
	_, err = outbox.Stage(ctx, capture(t, ctx, path, state.Baseline), 1<<20)
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(ctx)
	options := DeliveryOptions{TokenProvider: deliveryTokenFunc(func(context.Context) (string, error) {
		cancel()
		return "", errors.New("private credential detail")
	})}
	_, err = outbox.DeliverPending(canceled, options, deliveryLimits(1))
	require.ErrorIs(t, err, context.Canceled)
	require.NotContains(t, err.Error(), "private credential detail")
	requests, _ := peer.snapshot()
	require.Empty(t, requests)
}

func TestDeliveryDeadlineRetainsPendingPart(t *testing.T) {
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		received <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	ctx, _, _, outbox, _, _ := deliveryFixture(t, server.URL, 1)
	before, err := outbox.NextPart(ctx)
	require.NoError(t, err)
	result, err := outbox.DeliverPending(ctx, deliveryOptions(), DeliveryLimits{MaxParts: 1, MaxDuration: time.Second})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, ErrDeliverySend)
	require.Equal(t, DeliveryPending, result.Status)
	require.Equal(t, 0, result.ReceiptsRecorded)
	select {
	case <-received:
	default:
		t.Fatal("request never reached the fixture server")
	}
	after, err := outbox.NextPart(ctx)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
