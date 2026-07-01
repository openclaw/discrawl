package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestTailFailureLedgerHelpers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	var nilSyncer *Syncer
	require.NoError(t, nilSyncer.recordChannelFailure(ctx, "g", "c", errors.New("ignored")))
	require.NoError(t, nilSyncer.resolveChannelFailures(ctx, "g", "c"))
	var nilHandler *tailHandler
	require.NoError(t, nilHandler.recordMessageFailure(ctx, "g", "c", "m", errors.New("ignored")))
	require.NoError(t, nilHandler.resolveMessageFailures(ctx, "g", "c", "m"))

	handler := &tailHandler{store: st}
	require.NoError(t, handler.recordMessageFailure(ctx, "g1", "c1", "m1", errors.New("gateway write failed")))
	report, err := st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Len(t, report.Failures, 1)
	require.Equal(t, tailMessageFailureOperation, report.Failures[0].Operation)
	require.Equal(t, "m1", report.Failures[0].MessageID)

	require.NoError(t, handler.resolveMessageFailures(ctx, "g1", "c1", "m1"))
	report, err = st.ListFailures(ctx, store.FailureListOptions{}, time.Now())
	require.NoError(t, err)
	require.Empty(t, report.Failures)
	require.NoError(t, handler.recordMessageFailure(ctx, "g1", "c1", "m1", nil))

	original := errors.New("original")
	require.ErrorIs(t, withFailureRecordError(original, nil), original)
	require.ErrorContains(t, withFailureRecordError(original, errors.New("ledger")), "record failure ledger: ledger")
}
