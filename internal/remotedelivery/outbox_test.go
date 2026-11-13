package remotedelivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func outboxFixture(t *testing.T) (context.Context, *store.Store, string, *Outbox, string, store.NativeBaseline) {
	t.Helper()
	ctx, source, path, baseline := projectionFixture(t)
	seed := []SeedKey{
		{Table: "channels", Key: []store.NativeValue{{Kind: "text", Text: "cg1"}}},
		{Table: "guilds", Key: []store.NativeValue{{Kind: "text", Text: "g1"}}},
	}
	hash := sha256.New()
	for _, value := range seed {
		key, err := OwnershipKey(value.Table, value.Key)
		require.NoError(t, err)
		hashOwnershipKey(hash, key)
	}
	r := Registration{Origin: "https://archive.example.test", Archive: "fixture", BindingID: "qualified-binding", SeedManifestSHA256: strings.Repeat("a", 64), QualificationSHA256: strings.Repeat("b", 64), OwnedKeysSHA256: hex.EncodeToString(hash.Sum(nil)), OwnedKeysCount: int64(len(seed)), Scope: Scope{GuildIDs: []string{"g1"}}, Baseline: baseline, ServerCursor: "seed:complete"}
	outboxPath := filepath.Join(t.TempDir(), "delivery.sqlite")
	outbox, err := InitOutbox(ctx, outboxPath, r, func(yield func(SeedKey) error) error {
		for _, value := range seed {
			if err := yield(value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, outbox.Close()) })
	return ctx, source, path, outbox, outboxPath, baseline
}

func receiptFor(t *testing.T, part *PendingPart) remote.ChangeReceipt {
	t.Helper()
	batch, err := remote.DecodeChangeBatch(part.Body)
	require.NoError(t, err)
	return remote.ChangeReceipt{App: batch.App, Archive: batch.Archive, BindingID: batch.BindingID, BatchID: batch.BatchID(), PayloadSHA256: part.SHA256, Sequence: strconv.Itoa(part.Ref.Index + 1), Cursor: batch.BatchID()}
}

func TestOutboxReplayAndFinalOwnershipAreAtomic(t *testing.T) {
	ctx, source, path, outbox, outboxPath, baseline := outboxFixture(t)
	mutations := make([]store.MessageMutation, 501)
	for i := range mutations {
		mutations[i] = store.MessageMutation{Record: store.MessageRecord{ID: fmt.Sprintf("m%04d", i), GuildID: "g1", ChannelID: "cg1", AuthorID: "u", CreatedAt: "2026-11-14T00:00:00Z", Content: "saved\x00payload", NormalizedContent: "saved", RawJSON: "{}"}}
	}
	require.NoError(t, source.UpsertMessages(ctx, mutations))
	captured := capture(t, ctx, path, baseline)
	id, err := outbox.Stage(ctx, captured, 8<<20)
	require.NoError(t, err)
	require.NotEmpty(t, id)
	_, err = outbox.Stage(ctx, captured, 8<<20)
	require.ErrorIs(t, err, ErrPendingCapture)
	part, err := outbox.NextPart(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, part.Ref.Index)
	saved := append([]byte(nil), part.Body...)
	// A changed native source must not change an already staged request.
	require.NoError(t, source.UpsertMessages(ctx, []store.MessageMutation{{Record: store.MessageRecord{ID: "m0000", GuildID: "g1", ChannelID: "cg1", AuthorID: "u", CreatedAt: "2026-11-14T00:00:00Z", Content: "newer", NormalizedContent: "newer", RawJSON: "{}"}}}))
	require.NoError(t, outbox.Close())
	reopened, err := OpenOutbox(ctx, outboxPath)
	require.NoError(t, err)
	defer reopened.Close()
	part, err = reopened.NextPart(ctx)
	require.NoError(t, err)
	require.Equal(t, saved, part.Body)
	firstReceipt := receiptFor(t, part)
	require.NoError(t, reopened.RecordReceipt(ctx, part.Ref, firstReceipt))
	state, err := reopened.State(ctx)
	require.NoError(t, err)
	require.True(t, sameBaseline(baseline, state.Baseline))
	key, err := OwnershipKey("messages", []store.NativeValue{{Kind: "text", Text: "m0000"}})
	require.NoError(t, err)
	var owned bool
	require.NoError(t, reopened.base.DB().QueryRowContext(ctx, `select exists(select 1 from owned_keys where key=?)`, key).Scan(&owned))
	require.False(t, owned, "partial receipts cannot advance ownership")
	part, err = reopened.NextPart(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, part.Ref.Index)
	finalReceipt := receiptFor(t, part)
	_, err = reopened.base.DB().ExecContext(ctx, `create trigger reject_finish before update on binding begin select raise(abort,'fixture failure'); end`)
	require.NoError(t, err)
	require.Error(t, reopened.RecordReceipt(ctx, part.Ref, finalReceipt))
	require.NoError(t, reopened.base.DB().QueryRowContext(ctx, `select exists(select 1 from owned_keys where key=?)`, key).Scan(&owned))
	require.False(t, owned, "failed final receipt must roll ownership back")
	stillPending, err := reopened.NextPart(ctx)
	require.NoError(t, err)
	require.Equal(t, part, stillPending)
	_, err = reopened.base.DB().ExecContext(ctx, `drop trigger reject_finish`)
	require.NoError(t, err)
	require.NoError(t, reopened.RecordReceipt(ctx, part.Ref, finalReceipt))
	finalReceipt.Replayed = true
	require.NoError(t, reopened.RecordReceipt(ctx, part.Ref, finalReceipt))
	state, err = reopened.State(ctx)
	require.NoError(t, err)
	require.Empty(t, state.PendingID)
	require.True(t, sameBaseline(captured.Through, state.Baseline))
	require.Equal(t, finalReceipt.Cursor, state.ServerCursor)
	require.NoError(t, reopened.base.DB().QueryRowContext(ctx, `select exists(select 1 from owned_keys where key=?)`, key).Scan(&owned))
	require.True(t, owned)
	_, err = source.DB().ExecContext(ctx, `delete from messages where id='m0001'`)
	require.NoError(t, err)
	next := capture(t, ctx, path, state.Baseline)
	_, err = reopened.Stage(ctx, next, 1<<20)
	require.NoError(t, err)
	part, err = reopened.NextPart(ctx)
	require.NoError(t, err)
	deletionReceipt := receiptFor(t, part)
	deletionReceipt.Sequence = "3"
	require.NoError(t, reopened.RecordReceipt(ctx, part.Ref, deletionReceipt))
	deletedKey, err := OwnershipKey("messages", []store.NativeValue{{Kind: "text", Text: "m0001"}})
	require.NoError(t, err)
	require.NoError(t, reopened.base.DB().QueryRowContext(ctx, `select exists(select 1 from owned_keys where key=?)`, deletedKey).Scan(&owned))
	require.False(t, owned, "final removal receipt must drop ownership")
	part, err = reopened.NextPart(ctx)
	require.NoError(t, err)
	require.Nil(t, part)
}

func TestOutboxConcurrentStageAndReceiptValidation(t *testing.T) {
	ctx, source, path, outbox, outboxPath, baseline := outboxFixture(t)
	writeMessage(t, ctx, source, "m", "g1")
	captured := capture(t, ctx, path, baseline)
	other, err := OpenOutbox(ctx, outboxPath)
	require.NoError(t, err)
	defer other.Close()
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	for _, candidate := range []*Outbox{outbox, other} {
		wg.Go(func() { _, err := candidate.Stage(ctx, captured, 1<<20); errors <- err })
	}
	wg.Wait()
	close(errors)
	var winners, pending int
	for err := range errors {
		if err == nil {
			winners++
		} else {
			require.ErrorIs(t, err, ErrPendingCapture)
			pending++
		}
	}
	require.Equal(t, 1, winners)
	require.Equal(t, 1, pending)
	part, err := outbox.NextPart(ctx)
	require.NoError(t, err)
	receipt := receiptFor(t, part)
	wrong := receipt
	wrong.PayloadSHA256 = strings.Repeat("0", 64)
	require.Error(t, outbox.RecordReceipt(ctx, part.Ref, wrong))
	wrong = receipt
	wrong.Sequence = "01"
	require.Error(t, outbox.RecordReceipt(ctx, part.Ref, wrong))
	require.NoError(t, outbox.RecordReceipt(ctx, part.Ref, receipt))
	_, err = outbox.Stage(ctx, captured, 1<<20)
	require.ErrorIs(t, err, ErrOutboxBaseline)
}

func TestOutboxAdmissionLimitsAndExcludedProgress(t *testing.T) {
	ctx, source, path, outbox, _, baseline := outboxFixture(t)
	writeMessage(t, ctx, source, "m", "g1")
	captured := capture(t, ctx, path, baseline)
	_, err := outbox.Stage(ctx, captured, 1)
	require.ErrorIs(t, err, ErrOutboxLimit)
	state, err := outbox.State(ctx)
	require.NoError(t, err)
	require.Empty(t, state.PendingID)
	writeMessage(t, ctx, source, "excluded", "g2")
	captured = capture(t, ctx, path, baseline)
	_, err = outbox.Stage(ctx, captured, 1<<20)
	require.NoError(t, err)
	part, err := outbox.NextPart(ctx)
	require.NoError(t, err)
	batch, err := remote.DecodeChangeBatch(part.Body)
	require.NoError(t, err)
	require.Len(t, batch.Changes, 1)
	require.Equal(t, "m", *batch.Changes[0].Key[0].Value)
	require.NoError(t, outbox.RecordReceipt(ctx, part.Ref, receiptFor(t, part)))
	writeMessage(t, ctx, source, "excluded", "g2")
	require.NoError(t, source.SetSyncState(ctx, "worker:local", "private"))
	next := capture(t, ctx, path, captured.Through)
	_, err = outbox.Stage(ctx, next, 1<<20)
	require.NoError(t, err)
	part, err = outbox.NextPart(ctx)
	require.NoError(t, err)
	batch, err = remote.DecodeChangeBatch(part.Body)
	require.NoError(t, err)
	require.Empty(t, batch.Changes, "excluded native progress uses an explicit empty batch")
}

func TestOutboxRejectsUnknownFilesWithoutMutation(t *testing.T) {
	ctx, source, path, outbox, _, _ := outboxFixture(t)
	var beforeMode string
	require.NoError(t, source.DB().QueryRowContext(ctx, `pragma journal_mode`).Scan(&beforeMode))
	info, err := os.Stat(path)
	require.NoError(t, err)
	_, err = OpenOutbox(ctx, path)
	require.ErrorIs(t, err, ErrOutboxBaseline)
	var afterMode string
	require.NoError(t, source.DB().QueryRowContext(ctx, `pragma journal_mode`).Scan(&afterMode))
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, beforeMode, afterMode)
	require.Equal(t, info.Mode(), after.Mode())
	state, err := outbox.State(ctx)
	require.NoError(t, err)
	_, err = InitOutbox(ctx, path, state.Registration, func(func(SeedKey) error) error { return nil })
	require.Error(t, err)
	_, err = InitOutbox(ctx, filepath.Join(t.TempDir(), "bad.sqlite"), state.Registration, func(func(SeedKey) error) error { return nil })
	require.ErrorIs(t, err, ErrOwnership)
}
