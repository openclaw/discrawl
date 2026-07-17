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

var (
	errTailMessageReplayPolicyDeferred  = errors.New("tail message replay policy deferred")
	errTailMessageReplayIncomplete      = errors.New("tail message replay identity is incomplete")
	errTailMessageReplayClientMissing   = errors.New("tail message replay client is unavailable")
	errTailMessageReplayFetch           = errors.New("tail message replay exact fetch failed")
	errTailMessageReplayIdentity        = errors.New("tail message replay identity validation failed")
	errTailMessageReplayMutation        = errors.New("tail message replay mutation build failed")
	errTailMessageReplayUpsert          = errors.New("tail message replay canonical upsert failed")
	errTailMessageReplayCursor          = errors.New("tail message replay cursor advance failed")
	errTailMessageReplayDeleteCanonical = errors.New("tail message replay canonical delete failed")
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
	if s == nil || s.store == nil {
		return stats, nil
	}
	if err := s.importTailMessageFailureFallbacks(ctx); err != nil {
		return stats, err
	}
	candidates, err := s.store.ListFailureReplayCandidatesMatchingRelatedIDs(
		ctx,
		store.FailureRef{
			Operation:   tailMessageFailureOperation,
			Source:      "discord",
			RelatedKind: tailMessageFailureRelatedKind,
		},
		guildIDs,
		[]string{"create", "update", "delete"},
		limit,
	)
	if err != nil {
		return stats, err
	}
	stats.Candidates = len(candidates)
	for _, failure := range candidates {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		ref := tailMessageFailureRef(failure)
		eventKind, eventAware := tailMessageReplayEventKind(failure)
		if !eventAware {
			if err := s.recordTailMessageReplayFailure(ctx, ref, errTailMessageReplayPolicyDeferred); err != nil {
				return stats, err
			}
			stats.PolicyDeferred++
			continue
		}
		if failure.GuildID == "" || failure.ChannelID == "" || failure.MessageID == "" {
			if err := s.recordTailMessageReplayFailure(ctx, ref, errTailMessageReplayIncomplete); err != nil {
				return stats, err
			}
			stats.Deferred++
			continue
		}
		if eventKind == "delete" {
			if err := s.replayTailMessageDelete(ctx, ref); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return stats, ctxErr
				}
				if recordErr := s.recordTailMessageReplayFailure(
					ctx,
					ref,
					errTailMessageReplayDeleteCanonical,
				); recordErr != nil {
					return stats, recordErr
				}
				stats.Deferred++
				continue
			}
			stats.Recovered++
			continue
		}
		if s.client == nil {
			if err := s.recordTailMessageReplayFailure(ctx, ref, errTailMessageReplayClientMissing); err != nil {
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
			if err := s.recordTailMessageReplayFailure(ctx, ref, errTailMessageReplayFetch); err != nil {
				return stats, err
			}
			stats.Deferred++
			continue
		}
		if err := validateTailMessageReplay(failure, message); err != nil {
			if recordErr := s.recordTailMessageReplayFailure(ctx, ref, errTailMessageReplayIdentity); recordErr != nil {
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
			if recordErr := s.recordTailMessageReplayFailure(ctx, ref, errTailMessageReplayMutation); recordErr != nil {
				return stats, recordErr
			}
			stats.Deferred++
			continue
		}
		if err := s.store.UpsertMessages(ctx, []store.MessageMutation{mutation}); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return stats, ctxErr
			}
			if recordErr := s.recordTailMessageReplayFailure(ctx, ref, errTailMessageReplayUpsert); recordErr != nil {
				return stats, recordErr
			}
			stats.Deferred++
			continue
		}
		if err := s.store.AdvanceChannelLatestMessageID(ctx, failure.ChannelID, failure.MessageID); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return stats, ctxErr
			}
			if recordErr := s.recordTailMessageReplayFailure(ctx, ref, errTailMessageReplayCursor); recordErr != nil {
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

func tailMessageReplayEventKind(failure store.Failure) (string, bool) {
	if failure.RelatedKind != tailMessageFailureRelatedKind {
		return "", false
	}
	return normalizeTailMessageEventKind(failure.RelatedID)
}

func (s *Syncer) replayTailMessageDelete(ctx context.Context, ref store.FailureRef) error {
	if err := s.store.MarkMessageDeletedWithoutEvent(
		ctx,
		ref.GuildID,
		ref.ChannelID,
		ref.MessageID,
	); err != nil {
		return err
	}
	return s.resolveTailMessageReplay(ctx, ref)
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
