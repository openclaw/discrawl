package syncer

import (
	"context"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

const changeFeedCompactionEvery = time.Hour

// runChangeFeedCompaction bounds change_log growth while the tail runs. The
// store keeps every entry inside the retention window; a failed pass is
// retried on the next tick and never affects collection.
func (s *Syncer) runChangeFeedCompaction(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	compact := func() {
		deleted, err := s.store.CompactChangeFeed(ctx, time.Now(), store.ChangeFeedRetention)
		if err != nil {
			if ctx.Err() == nil && s.logger != nil {
				s.logger.Warn("change feed compaction deferred", "err", err)
			}
			return
		}
		if deleted > 0 && s.logger != nil {
			s.logger.Info("change feed compacted", "deleted", deleted)
		}
	}
	compact()
	ticker := time.NewTicker(changeFeedCompactionEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			compact()
		}
	}
}
