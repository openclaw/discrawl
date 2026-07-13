package syncer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bwmarrin/discordgo"
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

func (s *Syncer) resolveTailMessageFailuresForMessages(ctx context.Context, messages []*discordgo.Message, fallbackGuildID string) error {
	if s == nil || s.store == nil || len(messages) == 0 {
		return nil
	}
	type scope struct {
		guildID   string
		channelID string
	}
	messageIDs := map[scope][]string{}
	for _, message := range messages {
		if message == nil || message.ID == "" || message.ChannelID == "" {
			continue
		}
		guildID := message.GuildID
		if guildID == "" {
			guildID = fallbackGuildID
		}
		messageIDs[scope{guildID: guildID, channelID: message.ChannelID}] = append(
			messageIDs[scope{guildID: guildID, channelID: message.ChannelID}],
			message.ID,
		)
	}
	if len(messageIDs) == 0 {
		return nil
	}
	ledgerCtx, cancel := failureLedgerContext(ctx)
	defer cancel()
	for key, ids := range messageIDs {
		if err := s.store.ResolveMessageFailures(ledgerCtx, store.FailureRef{
			Operation: tailMessageFailureOperation,
			Source:    "discord",
			GuildID:   key.guildID,
			ChannelID: key.channelID,
		}, ids); err != nil {
			return err
		}
	}
	return nil
}

func tailMessageFailureRef(failure store.Failure) store.FailureRef {
	return store.FailureRef{
		Operation:   failure.Operation,
		Source:      failure.Source,
		GuildID:     failure.GuildID,
		ChannelID:   failure.ChannelID,
		MessageID:   failure.MessageID,
		RelatedKind: failure.RelatedKind,
		RelatedID:   failure.RelatedID,
	}
}

func validateTailMessageReplay(failure store.Failure, message *discordgo.Message) error {
	switch {
	case message == nil:
		return errors.New("exact message fetch returned no message")
	case message.ID == "":
		return errors.New("exact message fetch returned an empty message id")
	case message.ID != failure.MessageID:
		return errors.New("exact message fetch returned a different message id")
	case message.ChannelID == "":
		return errors.New("exact message fetch returned an empty channel id")
	case message.ChannelID != failure.ChannelID:
		return errors.New("exact message fetch returned a different channel id")
	case message.GuildID != "" && failure.GuildID != "" && message.GuildID != failure.GuildID:
		return errors.New("exact message fetch returned a different guild id")
	default:
		return nil
	}
}
