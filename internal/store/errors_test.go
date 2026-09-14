package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNormalizeContextErrorPreservesCancellationAndDatabaseCause(t *testing.T) {
	t.Parallel()
	for _, deadline := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		want := context.Canceled
		if deadline {
			ctx, cancel = context.WithDeadline(t.Context(), time.Unix(1, 0))
			t.Cleanup(cancel)
			want = context.DeadlineExceeded
		}
		sqliteErr := &codedFailureError{code: 9}
		wrapped := fmt.Errorf("open archive: %w", sqliteErr)
		got := NormalizeContextError(ctx, wrapped)
		require.ErrorIs(t, got, want)
		require.ErrorIs(t, got, wrapped)
		require.ErrorIs(t, got, sqliteErr)
		require.Same(t, got, NormalizeContextError(ctx, got), "do not duplicate an existing cancellation cause")
	}
}

func TestNormalizeContextErrorDoesNotMaskOtherFailures(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, err := range []error{nil, &codedFailureError{code: 19}, errors.New("interrupted (9)"), context.Canceled} {
		require.Equal(t, err, NormalizeContextError(ctx, err))
	}
	interrupted := &codedFailureError{code: 9}
	require.Same(t, interrupted, NormalizeContextError(t.Context(), interrupted))
}

func TestCanceledTransactionKeepsContextAfterRollback(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	st, err := Open(t.Context(), filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	tx, err := st.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	cancel()
	// Wait for the transaction to be closed, whether the context watcher won or not.
	if err := tx.Rollback(); err != nil {
		require.ErrorIs(t, err, sql.ErrTxDone)
	}
	err = tx.Commit()
	require.ErrorIs(t, err, sql.ErrTxDone)
	normalized := NormalizeContextError(ctx, err)
	require.ErrorIs(t, normalized, context.Canceled)
	require.ErrorIs(t, normalized, sql.ErrTxDone)
	require.Same(t, sql.ErrTxDone, NormalizeContextError(t.Context(), sql.ErrTxDone))
}
