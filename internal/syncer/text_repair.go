package syncer

import (
	"context"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

func (s *Syncer) runTextRepair(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	ref := store.FailureRef{Operation: "repair_text", Source: "local"}
	for {
		delay := 50 * time.Millisecond
		p, err := s.store.RepairMessageTextBatch(ctx, s.channelExclusions.policyID(), 500, s.tailEmbeddings)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			_ = s.store.RecordFailure(ctx, ref, err)
			if s.logger != nil {
				s.logger.Warn("derived text repair deferred", "err", err)
			}
			delay = 30 * time.Second
		}
		if err == nil && p.Complete {
			if err := s.store.ResolveFailureWithReason(ctx, ref, "text_repair_completed"); err != nil {
				return
			}
			if s.logger != nil {
				s.logger.Info("derived text repair completed", "scanned", p.Scanned, "changed", p.Changed, "reused", p.Reused, "excluded_or_unknown", p.SkippedScope, "rejected", p.Rejected)
			}
			return
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
