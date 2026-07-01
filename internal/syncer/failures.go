package syncer

import (
	"context"
	"fmt"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

const syncMessagesFailureOperation = "sync_messages"

const tailMessageFailureOperation = "tail_message"

func (s *Syncer) recordChannelFailure(ctx context.Context, guildID, channelID string, failure error) error {
	if s == nil || s.store == nil || failure == nil {
		return nil
	}
	ledgerCtx, cancel := failureLedgerContext(ctx)
	defer cancel()
	return s.store.RecordFailure(ledgerCtx, store.FailureRef{
		Operation: syncMessagesFailureOperation,
		Source:    "discord",
		GuildID:   guildID,
		ChannelID: channelID,
	}, failure)
}

func (s *Syncer) resolveChannelFailures(ctx context.Context, guildID, channelID string) error {
	if s == nil || s.store == nil {
		return nil
	}
	ledgerCtx, cancel := failureLedgerContext(ctx)
	defer cancel()
	return s.store.ResolveFailures(ledgerCtx, store.FailureRef{
		Operation: syncMessagesFailureOperation,
		Source:    "discord",
		GuildID:   guildID,
		ChannelID: channelID,
	})
}

func failureLedgerContext(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(base, 5*time.Second)
}

func withFailureRecordError(failure, recordErr error) error {
	if recordErr == nil {
		return failure
	}
	return fmt.Errorf("%w (record failure ledger: %w)", failure, recordErr)
}

func (t *tailHandler) recordMessageFailure(ctx context.Context, guildID, channelID, messageID string, failure error) error {
	if t == nil || t.store == nil || failure == nil {
		return nil
	}
	ledgerCtx, cancel := failureLedgerContext(ctx)
	defer cancel()
	return t.store.RecordFailure(ledgerCtx, store.FailureRef{
		Operation: tailMessageFailureOperation,
		Source:    "discord",
		GuildID:   guildID,
		ChannelID: channelID,
		MessageID: messageID,
	}, failure)
}

func (t *tailHandler) resolveMessageFailures(ctx context.Context, guildID, channelID, messageID string) error {
	if t == nil || t.store == nil {
		return nil
	}
	ledgerCtx, cancel := failureLedgerContext(ctx)
	defer cancel()
	return t.store.ResolveFailures(ledgerCtx, store.FailureRef{
		Operation: tailMessageFailureOperation,
		Source:    "discord",
		GuildID:   guildID,
		ChannelID: channelID,
		MessageID: messageID,
	})
}
