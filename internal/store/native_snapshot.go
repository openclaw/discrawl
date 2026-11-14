package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/openclaw/crawlkit/snapshot"
)

// ExportNativeSnapshot binds report rows and the native cursor to one read
// transaction. A failed export may leave files, but returns no qualified baseline.
// The filter must not mutate or end its caller-owned transaction.
func ExportNativeSnapshot(ctx context.Context, path, root string, filter snapshot.RowFilterTx) (snapshot.Manifest, NativeBaseline, error) {
	if _, ok := ctx.Deadline(); !ok {
		return snapshot.Manifest{}, NativeBaseline{}, errors.New("native export requires a context deadline")
	}
	s, identity, err := openNativeSource(ctx, path)
	if err != nil {
		return snapshot.Manifest{}, NativeBaseline{}, err
	}
	defer func() { _ = s.Close() }()
	tx, err := s.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return snapshot.Manifest{}, NativeBaseline{}, err
	}
	defer func() { _ = tx.Rollback() }()
	cursor, _, err := readNativeCursor(ctx, tx, nil)
	if err != nil {
		return snapshot.Manifest{}, NativeBaseline{}, err
	}
	manifest, err := snapshot.Export(ctx, snapshot.ExportOptions{
		ReadTx: tx, RootDir: root, Tables: nativeCaptureTables, FilterTx: filter,
	})
	if err != nil {
		return snapshot.Manifest{}, NativeBaseline{}, err
	}
	if err := finishNativeSource(ctx, tx, path, identity); err != nil {
		return snapshot.Manifest{}, NativeBaseline{}, err
	}
	return manifest, NativeBaseline{SourceID: identity, Cursor: cursor}, nil
}
