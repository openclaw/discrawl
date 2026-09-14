package store

import (
	"context"
	"database/sql"
	"errors"
)

const sqliteInterruptCode = 9

// NormalizeContextError restores cancellation lost by SQLite interrupts or
// database/sql closing a canceled transaction, while retaining the original cause.
func NormalizeContextError(ctx context.Context, err error) error {
	_, category := sqliteErrorCodes(err)
	if category != sqliteInterruptCode && !errors.Is(err, sql.ErrTxDone) {
		return err
	}
	cause := ctx.Err()
	if cause == nil || errors.Is(err, cause) {
		return err
	}
	return errors.Join(cause, err)
}

type sqliteErrorCoder interface {
	Code() int
}

func sqliteErrorCodes(err error) (code, category int) {
	var coder sqliteErrorCoder
	if err == nil || !errors.As(err, &coder) {
		return 0, 0
	}
	code = coder.Code()
	return code, code & 0xff
}
