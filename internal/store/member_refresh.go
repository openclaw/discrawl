package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const memberRefreshBatchSize = 256

type MemberRefreshMerge struct {
	Applied      int
	NewerSkipped int
}

// MergeObservedMembers merges a completed provider enumeration in short writer
// transactions. Gateway changes since enumeration began win, including removal
// tombstones. Absence in a paginated listing never implies provider deletion.
// A caller must not advance its full-refresh receipt unless this returns nil.
func (s *Store) MergeObservedMembers(ctx context.Context, guildID string, members []MemberRecord, started time.Time) (MemberRefreshMerge, error) {
	var result MemberRefreshMerge
	if guildID == "" || started.IsZero() {
		return result, errors.New("member refresh requires guild and observation start")
	}
	seen := make(map[string]struct{}, len(members))
	for _, m := range members {
		if m.GuildID != guildID || m.UserID == "" {
			return result, errors.New("member refresh has invalid member scope or identity")
		}
		if _, duplicate := seen[m.UserID]; duplicate {
			return result, errors.New("member refresh has duplicate member identity")
		}
		seen[m.UserID] = struct{}{}
	}
	for offset := 0; offset < len(members); offset += memberRefreshBatchSize {
		batch := members[offset:min(offset+memberRefreshBatchSize, len(members))]
		one, err := s.mergeObservedMemberBatch(ctx, batch, started)
		if err != nil {
			return result, err
		}
		result.Applied += one.Applied
		result.NewerSkipped += one.NewerSkipped
	}
	return result, ctx.Err()
}

func (s *Store) mergeObservedMemberBatch(ctx context.Context, members []MemberRecord, started time.Time) (MemberRefreshMerge, error) {
	var result MemberRefreshMerge
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer rollback(tx)
	qtx := s.q.WithTx(tx)
	for _, member := range members {
		var updated string
		err := tx.QueryRowContext(ctx, `select updated_at from members where guild_id=? and user_id=?`, member.GuildID, member.UserID).Scan(&updated)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return MemberRefreshMerge{}, err
		}
		if err == nil && parseTime(updated).After(started) {
			result.NewerSkipped++
			continue
		}
		now := time.Now().UTC().Format(timeLayout)
		if err := qtx.UpsertMember(ctx, upsertMemberParams(member, now)); err != nil {
			return MemberRefreshMerge{}, fmt.Errorf("merge observed member: %w", err)
		}
		if err := upsertMemberFTSTx(ctx, tx, member); err != nil {
			return MemberRefreshMerge{}, err
		}
		result.Applied++
	}
	if err := tx.Commit(); err != nil {
		return MemberRefreshMerge{}, err
	}
	return result, nil
}
