package syncer

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
)

// providerUnavailableResolution resolves a channel message failure whose
// channel Discord no longer serves. It is not a deletion: the channel row,
// its archived history and the unavailable marker stay in place, the marker
// keeps routine syncs from re-requesting it until its retry window expires,
// and public-only snapshots withhold the channel while the marker exists. A
// later successful crawl clears the marker and re-admits the channel.
const providerUnavailableResolution = "provider_unavailable_404"

const (
	unknownChannelLookupTimeout = 10 * time.Second
	unknownChannelReplayLimit   = 200
	unknownChannelReplayBudget  = 60 * time.Second
)

type unknownChannelVerdict int

const (
	// The exact lookup did not settle the question (transport, permission,
	// rate-limit or malformed response): the failure stays retryable.
	unknownChannelInconclusive unknownChannelVerdict = iota
	// The exact channel lookup returned the channel, so the message 404 is
	// not evidence of unavailability; retry the channel soon.
	unknownChannelExists
	// Both the message request and the exact channel lookup answered
	// Unknown Channel (HTTP 404, code 10003).
	unknownChannelConfirmed
)

// verifyUnknownChannel corroborates a message-request Unknown Channel answer
// with one exact channel lookup. A single 404 is not treated as proof.
func (s *Syncer) verifyUnknownChannel(ctx context.Context, guildID, channelID string) unknownChannelVerdict {
	if s == nil || s.client == nil || channelID == "" || ctx.Err() != nil {
		return unknownChannelInconclusive
	}
	call, cancel := context.WithTimeout(ctx, unknownChannelLookupTimeout)
	defer cancel()
	channel, err := s.client.Channel(call, channelID)
	switch {
	case confirmedDiscordMissing(err, discordgo.ErrCodeUnknownChannel):
		return unknownChannelConfirmed
	case err == nil && channel != nil && channel.ID == channelID && (channel.GuildID == "" || guildID == "" || channel.GuildID == guildID):
		return unknownChannelExists
	default:
		return unknownChannelInconclusive
	}
}

// settleUnknownChannelFailure applies the corroborated verdict to the
// channel's pending message failures. resolve resolves the pending failure
// generation with the given reason.
func (s *Syncer) settleUnknownChannelFailure(ctx context.Context, guildID, channelID string, resolve func(string) error) error {
	verdict := s.verifyUnknownChannel(ctx, guildID, channelID)
	// Ledger and marker writes must not be lost to the caller's deadline once
	// the provider answer is in hand.
	ctx, cancel := failureLedgerContext(ctx)
	defer cancel()
	switch verdict {
	case unknownChannelConfirmed:
		scope := channelMessageUnavailableScope(channelID)
		current, err := s.store.GetSyncState(ctx, scope)
		if err != nil {
			return err
		}
		if current == "" {
			// Keep exportable evidence of the skip; an existing marker keeps
			// its timestamp so the retry window is not extended.
			if err := s.store.SetSyncState(ctx, scope, "unknown_channel"); err != nil {
				return err
			}
		}
		s.logger.Info("channel message failure resolved: provider unavailable", "channel_id", channelID, "reason", providerUnavailableResolution)
		return resolve(providerUnavailableResolution)
	case unknownChannelExists:
		// Discord still serves the channel: drop only the unknown-channel
		// marker so the next routine sync retries it; the failure stays
		// pending until a crawl succeeds.
		if current, err := s.store.GetSyncState(ctx, channelMessageUnavailableScope(channelID)); err != nil {
			return err
		} else if current == "unknown_channel" {
			if err := s.clearUnavailableChannel(ctx, channelID); err != nil {
				return err
			}
		}
		s.logger.Warn("channel message failure kept: exact lookup still serves channel", "channel_id", channelID)
	}
	return nil
}

// channelFailureReplayCursorScope persists where the bounded channel failure
// lane stopped, so successive repairs rotate through every pending row rather
// than revisiting the same oldest page when earlier rows stay pending.
const channelFailureReplayCursorScope = "repair:sync_messages:after_failure_id"

// reconcileChannelMessageFailures settles pending channel message failures
// that recorded state or a corroborated provider answer supersedes, so they do
// not stay retryable indefinitely: recorded deletions (including ancestors),
// explicit policy exclusions, and Unknown Channel answers confirmed by an
// exact lookup. Other failures are left untouched. Each run handles one page
// after the persisted cursor and wraps to the start once the end is reached.
func (s *Syncer) reconcileChannelMessageFailures(ctx context.Context, guildIDs []string) error {
	if s == nil || s.store == nil {
		return nil
	}
	ref := store.FailureRef{Operation: syncMessagesFailureOperation, Source: "discord"}
	cursorText, err := s.store.GetSyncState(ctx, channelFailureReplayCursorScope)
	if err != nil {
		return err
	}
	after, _ := strconv.ParseInt(cursorText, 10, 64)
	rows, err := s.store.ListFailureReplayCandidatesAfter(ctx, ref, guildIDs, after, unknownChannelReplayLimit)
	if err != nil {
		return err
	}
	if len(rows) == 0 && after > 0 {
		after = 0
		if rows, err = s.store.ListFailureReplayCandidatesAfter(ctx, ref, guildIDs, 0, unknownChannelReplayLimit); err != nil {
			return err
		}
	}
	policy := ""
	if s.channelExclusions.configured() {
		policy = s.channelExclusions.policyID()
	}
	last := after
	defer func() {
		// Persist progress even when the budget expires mid-page.
		ledgerCtx, cancel := failureLedgerContext(ctx)
		defer cancel()
		if err := s.store.SetSyncState(ledgerCtx, channelFailureReplayCursorScope, strconv.FormatInt(last, 10)); err != nil {
			s.logger.Warn("record channel failure replay cursor", "err", err)
		}
	}()
	for _, f := range rows {
		select {
		case <-ctx.Done():
			// The budget is a soft cap; remaining rows wait for the next repair.
			return nil
		default:
		}
		if err := s.reconcileChannelMessageFailure(ctx, f, policy); err != nil {
			return err
		}
		last = f.FailureID
	}
	return nil
}

func (s *Syncer) reconcileChannelMessageFailure(ctx context.Context, f store.Failure, policy string) error {
	if f.ChannelID == "" || f.MessageID != "" {
		return nil
	}
	resolve := func(reason string) error {
		ledgerCtx, cancel := failureLedgerContext(ctx)
		defer cancel()
		return s.store.ResolveFailureGeneration(ledgerCtx, f, reason)
	}
	reason, err := s.store.ChannelFailureSupersession(ctx, f.ChannelID, policy)
	if err != nil {
		return err
	}
	if reason != "" {
		return resolve(reason)
	}
	if !isUnknownChannel(errors.New(f.ErrorMessage)) {
		return nil
	}
	return s.settleUnknownChannelFailure(ctx, f.GuildID, f.ChannelID, resolve)
}
