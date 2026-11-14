package remotedelivery

import (
	"context"
	"database/sql"
	"maps"
	"slices"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/store"
)

// Enumerate only checkpoint ownership for changed channels. These ranges use
// the same JSON encoder as OwnershipKey; raw IDs never become SQL patterns.
func readOwnedCheckpoints(ctx context.Context, tx *sql.Tx, coverage map[string]store.NativeChannelCoverage, ownership *OwnershipSnapshot, budget int64) error {
	remaining := remote.MaxChanges * remote.MaxChangeParts
	ownership.ChannelCheckpoints = map[string][]string{}
	for _, channel := range slices.Sorted(maps.Keys(coverage)) {
		prefix, err := checkpointPrefix(channel)
		if err != nil {
			return err
		}
		lower, err := checkpointKey(prefix)
		if err != nil {
			return err
		}
		upper, err := checkpointKey(prefix[:len(prefix)-1] + ";")
		if err != nil {
			return err
		}
		// Remove the closing JSON quote and array delimiter to match suffixes.
		lower, upper = lower[:len(lower)-2], upper[:len(upper)-2]
		rows, err := tx.QueryContext(ctx, `select case when length(cast(key as blob))<=? then key end
			from owned_keys where key>=? collate binary and key<? collate binary order by key collate binary`, budget, lower, upper)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		owned := []string{}
		for rows.Next() {
			if remaining == 0 {
				_ = rows.Close()
				return ErrOutboxLimit
			}
			var key sql.NullString
			if err := rows.Scan(&key); err != nil {
				_ = rows.Close()
				return err
			}
			if !key.Valid || int64(len(key.String)) > budget {
				_ = rows.Close()
				return ErrOutboxLimit
			}
			remaining--
			budget -= int64(len(key.String))
			owned = append(owned, key.String)
			ownership.Keys[key.String] = true
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return err
		}
		ownership.ChannelCheckpoints[channel] = owned
	}
	return nil
}
