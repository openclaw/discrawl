package syncer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/store"
)

func (s *Syncer) RunTail(ctx context.Context, guildIDs []string, repairEvery time.Duration) error {
	handler := &tailHandler{
		guilds:                makeGuildSet(guildIDs),
		store:                 s.store,
		client:                s.client,
		attachmentTextEnabled: s.attachmentTextEnabled,
		onReady:               s.tailReady,
	}
	if repairEvery <= 0 {
		return s.client.Tail(ctx, handler)
	}
	tailCtx, cancelTail := context.WithCancel(ctx)
	defer cancelTail()
	var closeOnce sync.Once
	closeClient := func() {
		if closeable, ok := s.client.(closeableClient); ok {
			_ = closeable.Close()
		}
	}
	defer closeOnce.Do(closeClient)
	errCh := make(chan error, 2)
	go func() {
		errCh <- s.client.Tail(tailCtx, handler)
	}()
	ticker := time.NewTicker(repairEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			cancelTail()
			closeOnce.Do(closeClient)
			<-errCh
			return nil
		case err := <-errCh:
			return err
		case <-ticker.C:
			if _, err := s.Sync(ctx, SyncOptions{GuildIDs: guildIDs, Full: false, RepairReason: "tail_repair"}); err != nil {
				s.logger.Warn("repair sync failed", "err", err)
			}
		}
	}
}

type tailHandler struct {
	guilds                map[string]struct{}
	store                 *store.Store
	client                Client
	attachmentTextEnabled bool
	onReady               func(context.Context) error
}

func (t *tailHandler) OnTailReady(ctx context.Context) error {
	if t.onReady == nil {
		return nil
	}
	return t.onReady(ctx)
}

func (t *tailHandler) OnMessageCreate(ctx context.Context, msg *discordgo.Message) error {
	if !t.allowGuild(msg.GuildID) {
		return nil
	}
	mutation, err := buildMessageMutation(ctx, msg, "", "", false, t.attachmentTextEnabled)
	if err != nil {
		return withFailureRecordError(err, t.recordMessageFailure(ctx, msg.GuildID, msg.ChannelID, msg.ID, err))
	}
	if err := t.store.UpsertMessages(ctx, []store.MessageMutation{mutation}); err != nil {
		return withFailureRecordError(err, t.recordMessageFailure(ctx, msg.GuildID, msg.ChannelID, msg.ID, err))
	}
	if err := t.store.AppendMessageEvent(ctx, msg.GuildID, msg.ChannelID, msg.ID, "create", msg); err != nil {
		return withFailureRecordError(err, t.recordMessageFailure(ctx, msg.GuildID, msg.ChannelID, msg.ID, err))
	}
	if err := t.store.SetSyncState(ctx, "tail:last_event", msg.ID); err != nil {
		return withFailureRecordError(err, t.recordMessageFailure(ctx, msg.GuildID, msg.ChannelID, msg.ID, err))
	}
	if err := t.store.AdvanceChannelLatestMessageID(ctx, msg.ChannelID, msg.ID); err != nil {
		return withFailureRecordError(err, t.recordMessageFailure(ctx, msg.GuildID, msg.ChannelID, msg.ID, err))
	}
	return t.resolveMessageFailures(ctx, msg.GuildID, msg.ChannelID, msg.ID)
}

func (t *tailHandler) OnMessageUpdate(ctx context.Context, msg *discordgo.Message) error {
	if msg == nil {
		return nil
	}
	if msg.GuildID != "" && !t.allowGuild(msg.GuildID) {
		return nil
	}
	guildID, channelID, messageID := msg.GuildID, msg.ChannelID, msg.ID
	var err error
	msg, err = t.messageUpdateSnapshot(ctx, msg)
	if err != nil {
		return withFailureRecordError(err, t.recordMessageFailure(ctx, guildID, channelID, messageID, err))
	}
	if msg == nil || !t.allowGuild(msg.GuildID) {
		return nil
	}
	mutation, err := buildMessageMutation(ctx, msg, "", "", false, t.attachmentTextEnabled)
	if err != nil {
		return withFailureRecordError(err, t.recordMessageFailure(ctx, msg.GuildID, msg.ChannelID, msg.ID, err))
	}
	if err := t.store.UpsertMessages(ctx, []store.MessageMutation{mutation}); err != nil {
		return withFailureRecordError(err, t.recordMessageFailure(ctx, msg.GuildID, msg.ChannelID, msg.ID, err))
	}
	if err := t.store.AppendMessageEvent(ctx, msg.GuildID, msg.ChannelID, msg.ID, "update", msg); err != nil {
		return withFailureRecordError(err, t.recordMessageFailure(ctx, msg.GuildID, msg.ChannelID, msg.ID, err))
	}
	if err := t.store.SetSyncState(ctx, "tail:last_event", msg.ID); err != nil {
		return withFailureRecordError(err, t.recordMessageFailure(ctx, msg.GuildID, msg.ChannelID, msg.ID, err))
	}
	return t.resolveMessageFailures(ctx, msg.GuildID, msg.ChannelID, msg.ID)
}

func (t *tailHandler) messageUpdateSnapshot(ctx context.Context, msg *discordgo.Message) (*discordgo.Message, error) {
	if t.client == nil || msg.ChannelID == "" || msg.ID == "" {
		if isPartialMessageUpdate(msg) {
			return nil, nil
		}
		return msg, nil
	}
	full, err := t.client.ChannelMessage(ctx, msg.ChannelID, msg.ID)
	if err != nil {
		return nil, fmt.Errorf("fetch message update %s/%s: %w", msg.ChannelID, msg.ID, err)
	}
	if full != nil {
		if full.ID == "" {
			full.ID = msg.ID
		}
		if full.GuildID == "" {
			full.GuildID = msg.GuildID
		}
		if full.ChannelID == "" {
			full.ChannelID = msg.ChannelID
		}
		return full, nil
	}
	if isPartialMessageUpdate(msg) {
		return nil, nil
	}
	return msg, nil
}

func isPartialMessageUpdate(msg *discordgo.Message) bool {
	return msg == nil || msg.Author == nil || msg.Timestamp.IsZero()
}

func (t *tailHandler) OnMessageDelete(ctx context.Context, evt *discordgo.MessageDelete) error {
	if !t.allowGuild(evt.GuildID) {
		return nil
	}
	if err := t.store.MarkMessageDeleted(ctx, evt.GuildID, evt.ChannelID, evt.ID, evt); err != nil {
		return err
	}
	return t.store.SetSyncState(ctx, "tail:last_event", evt.ID)
}

func (t *tailHandler) OnChannelUpsert(ctx context.Context, channel *discordgo.Channel) error {
	if !t.allowGuild(channel.GuildID) {
		return nil
	}
	return t.store.UpsertChannel(ctx, toChannelRecord(channel, marshalJSONString(channel, "{}")))
}

func (t *tailHandler) OnMemberUpsert(ctx context.Context, guildID string, member *discordgo.Member) error {
	if !t.allowGuild(guildID) || member == nil || member.User == nil {
		return nil
	}
	return t.store.UpsertMember(ctx, toMemberRecord(guildID, member))
}

func (t *tailHandler) OnMemberDelete(ctx context.Context, guildID, userID string) error {
	if !t.allowGuild(guildID) {
		return nil
	}
	return t.store.DeleteMember(ctx, guildID, userID)
}

func (t *tailHandler) allowGuild(guildID string) bool {
	if len(t.guilds) == 0 {
		return true
	}
	_, ok := t.guilds[guildID]
	return ok
}
