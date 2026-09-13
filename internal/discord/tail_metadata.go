package discord

import (
	"context"
	"time"

	"github.com/bwmarrin/discordgo"
)

func newTailFailureMetadata(task tailTask) *tailFailureMetadata {
	return &tailFailureMetadata{
		guildID:   task.guildID,
		channelID: task.channelID,
		messageID: task.messageID,
		userID:    task.userID,
	}
}

func withTailFailureMetadata(ctx context.Context, metadata *tailFailureMetadata) context.Context {
	if ctx == nil || metadata == nil {
		return ctx
	}
	return context.WithValue(ctx, tailFailureMetadataContextKey{}, metadata)
}

// SetTailFailureStage installs or updates the current tail handler stage.
func SetTailFailureStage(ctx context.Context, stage TailFailureStage) context.Context {
	if ctx == nil {
		return nil
	}
	metadata, _ := ctx.Value(tailFailureMetadataContextKey{}).(*tailFailureMetadata)
	if metadata == nil {
		metadata = &tailFailureMetadata{}
		ctx = withTailFailureMetadata(ctx, metadata)
	}
	if ctx.Err() == nil {
		metadata.setStageAt(stage, time.Now())
	}
	return ctx
}

// UpdateTailFailureStage updates a stage installed by Tail or SetTailFailureStage.
func UpdateTailFailureStage(ctx context.Context, stage TailFailureStage) {
	if ctx == nil || ctx.Err() != nil {
		return
	}
	metadata, _ := ctx.Value(tailFailureMetadataContextKey{}).(*tailFailureMetadata)
	if metadata == nil {
		return
	}
	metadata.setStageAt(stage, time.Now())
}

// EnrichTailFailureMetadata adds message identifiers to the current tail event's failure report.
func EnrichTailFailureMetadata(ctx context.Context, msg *discordgo.Message) {
	if ctx == nil || ctx.Err() != nil || msg == nil {
		return
	}
	metadata, _ := ctx.Value(tailFailureMetadataContextKey{}).(*tailFailureMetadata)
	metadata.addMessage(msg)
}

type tailFailureMetadataSnapshot struct {
	guildID             string
	channelID           string
	messageID           string
	userID              string
	handlerStage        TailFailureStage
	handlerStageElapsed time.Duration
}

func (m *tailFailureMetadata) addMessage(msg *discordgo.Message) {
	if m == nil || msg == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	setTailTaskID(&m.guildID, msg.GuildID)
	setTailTaskID(&m.channelID, msg.ChannelID)
	setTailTaskID(&m.messageID, msg.ID)
	if msg.Author != nil {
		setTailTaskID(&m.userID, msg.Author.ID)
	}
}

func (m *tailFailureMetadata) setStageAt(stage TailFailureStage, startedAt time.Time) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stageFrozen {
		return
	}
	m.handlerStage = normalizeTailFailureStage(stage)
	m.stageStartedAt = startedAt
}

func (m *tailFailureMetadata) freezeStageAt(observedAt time.Time) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stageFrozen {
		return
	}
	m.stageObservedAt = observedAt
	m.stageFrozen = true
}

func (m *tailFailureMetadata) snapshot(observedAt time.Time) tailFailureMetadataSnapshot {
	if m == nil {
		return tailFailureMetadataSnapshot{handlerStage: TailFailureStageUnknown}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stageFrozen {
		observedAt = m.stageObservedAt
	}
	return tailFailureMetadataSnapshot{
		guildID:             m.guildID,
		channelID:           m.channelID,
		messageID:           m.messageID,
		userID:              m.userID,
		handlerStage:        normalizeTailFailureStage(m.handlerStage),
		handlerStageElapsed: elapsedBetween(m.stageStartedAt, observedAt),
	}
}

func normalizeTailFailureStage(stage TailFailureStage) TailFailureStage {
	switch stage {
	case TailFailureStageHandler,
		TailFailureStageMessageUpdateRefetch,
		TailFailureStageMessageBuild,
		TailFailureStageCanonicalWrite,
		TailFailureStageEventAppend,
		TailFailureStageStateUpdate,
		TailFailureStageCursorAdvance,
		TailFailureStageCanonicalDelete,
		TailFailureStageFailureResolution:
		return stage
	default:
		return TailFailureStageUnknown
	}
}
