package store

import (
	"context"
	"errors"

	crawlstore "github.com/openclaw/crawlkit/store"
)

type SQLiteHealth struct {
	JournalMode   string
	SchemaVersion int
	IntegrityOK   bool
}

func InspectSQLite(ctx context.Context, path string) (SQLiteHealth, error) {
	base, err := crawlstore.OpenReadOnly(ctx, path)
	if err != nil {
		return SQLiteHealth{}, err
	}
	defer func() { _ = base.Close() }()
	db := base.DB()
	queryCtx, cancel := withQueryTimeout(ctx)
	defer cancel()

	var health SQLiteHealth
	if err := db.QueryRowContext(queryCtx, "pragma journal_mode").Scan(&health.JournalMode); err != nil {
		return SQLiteHealth{}, err
	}
	if err := db.QueryRowContext(queryCtx, "pragma user_version").Scan(&health.SchemaVersion); err != nil {
		return SQLiteHealth{}, err
	}
	rows, err := db.QueryContext(queryCtx, "pragma quick_check")
	if err != nil {
		return SQLiteHealth{}, err
	}
	defer func() { _ = rows.Close() }()
	rowCount := 0
	issueCount := 0
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return SQLiteHealth{}, err
		}
		rowCount++
		if result != "ok" {
			issueCount++
		}
	}
	if err := rows.Err(); err != nil {
		return SQLiteHealth{}, err
	}
	if rowCount == 0 {
		return SQLiteHealth{}, errors.New("SQLite quick_check returned no result")
	}
	health.IntegrityOK = rowCount == 1 && issueCount == 0
	return health, nil
}
