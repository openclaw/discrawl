package store

import (
	"context"
	"database/sql"
	"strings"
	"unicode/utf8"
)

// A channel change can admit or withdraw checkpoints whose own rows did not
// change. Read their complete set in the original capture transaction. These
// are current rows, not invented journal events or historical row versions.
func nativeChannelCoverage(ctx context.Context, tx *sql.Tx, changes []NativeChange, remaining int, budget *int64) (map[string]NativeChannelCoverage, error) {
	result := map[string]NativeChannelCoverage{}
	for _, change := range changes {
		if change.Table != "channels" {
			continue
		}
		channel := change.Key[0].Text
		// Checkpoint scopes use ':' as their field delimiter.
		if channel == "" || strings.ContainsAny(channel, ":\x00") || !utf8.ValidString(channel) {
			return nil, ErrNativeData
		}
		entry := NativeChannelCoverage{Present: change.Operation == "upsert", Checkpoints: []NativeChange{}}
		if entry.Present {
			for i, column := range change.Columns {
				if column == "guild_id" {
					if change.Row[i].Kind != "text" || change.Row[i].Text == "" {
						return nil, ErrNativeData
					}
					entry.GuildID = change.Row[i].Text
				}
			}
			if entry.GuildID == "" {
				return nil, ErrNativeData
			}
		}
		prefix := "channel:" + channel + ":"
		// The primary-key BINARY range preserves literal '%'/'_' and avoids a
		// full sync_state scan. ';' is the immediate byte successor of ':'.
		upper := strings.TrimSuffix(prefix, ":") + ";"
		rows, err := tx.QueryContext(ctx, `select case when length(cast(scope as blob))<=? then scope end
			from sync_state where scope>=? collate binary and scope<? collate binary order by scope collate binary`, *budget, prefix, upper)
		if err != nil {
			return nil, err
		}
		defer func() { _ = rows.Close() }()
		var scopes []string
		for rows.Next() {
			if remaining == 0 {
				_ = rows.Close()
				return nil, ErrNativeLimit
			}
			var scope sql.NullString
			if err := rows.Scan(&scope); err != nil {
				_ = rows.Close()
				return nil, err
			}
			if !scope.Valid {
				_ = rows.Close()
				return nil, ErrNativeLimit
			}
			if !utf8.ValidString(scope.String) || len(scope.String) == len(prefix) {
				_ = rows.Close()
				return nil, ErrNativeData
			}
			if err := nativeTake(budget, int64(len(scope.String))); err != nil {
				_ = rows.Close()
				return nil, err
			}
			remaining--
			scopes = append(scopes, scope.String)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
		for _, scope := range scopes {
			checkpoint := NativeChange{Table: "sync_state", Operation: ChangeOpUpdate, Key: []NativeValue{{Kind: "text", Text: scope}}}
			if err := readNativeChangedRow(ctx, tx, &checkpoint, budget); err != nil {
				return nil, err
			}
			entry.Checkpoints = append(entry.Checkpoints, checkpoint)
		}
		result[channel] = entry
	}
	return result, nil
}
