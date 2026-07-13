package syncer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

const (
	TailMessageReplayLimit   = 25
	tailMessageReplayTimeout = 30 * time.Second
)

type TailMessageReplayStats struct {
	Candidates     int `json:"candidates"`
	Recovered      int `json:"recovered"`
	Deferred       int `json:"deferred"`
	PolicyDeferred int `json:"policy_deferred"`
}

type tailMessageReplayStats = TailMessageReplayStats

func (s *Syncer) ReplayTailMessageFailures(ctx context.Context, guildIDs []string, limit int) (TailMessageReplayStats, error) {
	if limit <= 0 || limit > TailMessageReplayLimit {
		return TailMessageReplayStats{}, fmt.Errorf(
			"tail message replay limit must be between 1 and %d",
			TailMessageReplayLimit,
		)
	}
	return s.replayTailMessageFailures(ctx, guildIDs, limit)
}

func (s *Syncer) replayTailMessageFailures(ctx context.Context, guildIDs []string, limit int) (tailMessageReplayStats, error) {
	stats := tailMessageReplayStats{}
	if s == nil || s.store == nil || s.client == nil {
		return stats, nil
	}
	candidates, err := s.store.ListFailureReplayCandidates(ctx, store.FailureRef{
		Operation: tailMessageFailureOperation,
		Source:    "discord",
	}, guildIDs, limit)
	if err != nil {
		return stats, err
	}
	stats.Candidates = len(candidates)
	for _, failure := range candidates {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		ref := tailMessageFailureRef(failure)
		if failure.GuildID == "" || failure.ChannelID == "" || failure.MessageID == "" {
			if err := s.recordTailMessageReplayFailure(ctx, ref, errors.New("tail message replay identity is incomplete")); err != nil {
				return stats, err
			}
			stats.Deferred++
			continue
		}
		fetchCtx, cancel := context.WithTimeout(ctx, tailMessageReplayTimeout)
		message, fetchErr := s.client.ChannelMessage(fetchCtx, failure.ChannelID, failure.MessageID)
		cancel()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return stats, ctxErr
		}
		if fetchErr != nil {
			if err := s.recordTailMessageReplayFailure(ctx, ref, fmt.Errorf("fetch exact tail message: %w", fetchErr)); err != nil {
				return stats, err
			}
			stats.Deferred++
			continue
		}
		if err := validateTailMessageReplay(failure, message); err != nil {
			if recordErr := s.recordTailMessageReplayFailure(ctx, ref, err); recordErr != nil {
				return stats, recordErr
			}
			stats.Deferred++
			continue
		}
		if message.GuildID == "" {
			message.GuildID = failure.GuildID
		}
		mutation, err := buildMessageMutation(ctx, message, "", failure.GuildID, false, s.attachmentTextEnabled)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return stats, ctxErr
			}
			if recordErr := s.recordTailMessageReplayFailure(ctx, ref, fmt.Errorf("build exact tail message mutation: %w", err)); recordErr != nil {
				return stats, recordErr
			}
			stats.Deferred++
			continue
		}
		if err := s.store.UpsertMessages(ctx, []store.MessageMutation{mutation}); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return stats, ctxErr
			}
			if recordErr := s.recordTailMessageReplayFailure(ctx, ref, fmt.Errorf("upsert exact tail message: %w", err)); recordErr != nil {
				return stats, recordErr
			}
			stats.Deferred++
			continue
		}
		if err := s.store.AdvanceChannelLatestMessageID(ctx, failure.ChannelID, failure.MessageID); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return stats, ctxErr
			}
			if recordErr := s.recordTailMessageReplayFailure(ctx, ref, fmt.Errorf("advance exact tail message cursor: %w", err)); recordErr != nil {
				return stats, recordErr
			}
			stats.Deferred++
			continue
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return stats, ctxErr
		}
		if err := s.resolveTailMessageReplay(ctx, ref); err != nil {
			return stats, err
		}
		stats.Recovered++
	}
	if stats.Candidates > 0 && s.logger != nil {
		s.logger.Info(
			"tail message replay completed",
			"candidates", stats.Candidates,
			"recovered", stats.Recovered,
			"deferred", stats.Deferred,
			"policy_deferred", stats.PolicyDeferred,
		)
	}
	return stats, nil
}

func (s *Syncer) recordTailMessageReplayFailure(ctx context.Context, ref store.FailureRef, failure error) error {
	ledgerCtx, cancel := failureLedgerContext(ctx)
	defer cancel()
	return s.store.RecordFailure(ledgerCtx, ref, failure)
}

func (s *Syncer) resolveTailMessageReplay(ctx context.Context, ref store.FailureRef) error {
	ledgerCtx, cancel := failureLedgerContext(ctx)
	defer cancel()
	return s.store.ResolveFailureIdentity(ledgerCtx, ref)
}
