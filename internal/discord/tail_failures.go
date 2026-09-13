package discord

import (
	"context"
	"errors"
	"sync"
	"time"
)

type TailFailureStage string

const (
	TailFailureStageUnknown              TailFailureStage = "unknown"
	TailFailureStageHandler              TailFailureStage = "handler"
	TailFailureStageMessageUpdateRefetch TailFailureStage = "message_update_refetch"
	TailFailureStageMessageBuild         TailFailureStage = "message_build"
	TailFailureStageCanonicalWrite       TailFailureStage = "canonical_write"
	TailFailureStageEventAppend          TailFailureStage = "event_append"
	TailFailureStageStateUpdate          TailFailureStage = "state_update"
	TailFailureStageCursorAdvance        TailFailureStage = "cursor_advance"
	TailFailureStageCanonicalDelete      TailFailureStage = "canonical_delete"
	TailFailureStageFailureResolution    TailFailureStage = "failure_resolution"
)

type TailFailureJoinOutcome string

const (
	TailFailureJoinNotRequired TailFailureJoinOutcome = "not_required"
	TailFailureJoinJoined      TailFailureJoinOutcome = "joined"
	TailFailureJoinTimedOut    TailFailureJoinOutcome = "timed_out"
)

type TailFailure struct {
	EventType           string
	Kind                string
	GuildID             string
	ChannelID           string
	MessageID           string
	UserID              string
	HandlerStage        TailFailureStage
	HandlerStageElapsed time.Duration
	HandlerElapsed      time.Duration
	JoinElapsed         time.Duration
	JoinOutcome         TailFailureJoinOutcome
	ForceFallback       bool
}

type tailFailureHandler interface {
	OnTailFailure(TailFailure)
}

type tailFailureRecorder interface {
	RecordTailFailure(TailFailure) error
}

type tailFailureClass string

const (
	tailFailureClassOrdered tailFailureClass = "ordered"
	tailFailureClassMember  tailFailureClass = "member"
)

type tailFailureMetadata struct {
	mu              sync.RWMutex
	guildID         string
	channelID       string
	messageID       string
	userID          string
	handlerStage    TailFailureStage
	stageStartedAt  time.Time
	stageObservedAt time.Time
	stageFrozen     bool
}

type tailFailureMetadataContextKey struct{}

type tailFailureCircuit struct {
	mu          sync.Mutex
	limit       int
	consecutive int
	opened      bool
}

func (c *tailFailureCircuit) recordFailure() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.opened || c.limit <= 0 {
		return false
	}
	c.consecutive++
	if c.consecutive < c.limit {
		return false
	}
	c.opened = true
	return true
}

func (c *tailFailureCircuit) recordSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.opened {
		c.consecutive = 0
	}
}

func recordTailFailure(handler tailFailureRecorder, failure TailFailure) (err error) {
	if handler == nil {
		return errors.New("tail failure recorder unavailable")
	}
	defer func() {
		if recover() != nil {
			err = errors.New("tail failure recorder panicked")
		}
	}()
	return handler.RecordTailFailure(failure)
}

func reportTailFailure(handler tailFailureHandler, failure TailFailure) {
	if handler == nil {
		return
	}
	handler.OnTailFailure(failure)
}

func newTailFailureFromExecution(task tailTask, execution tailTaskExecution) TailFailure {
	guildID, channelID, messageID, userID := task.guildID, task.channelID, task.messageID, task.userID
	handlerStage := TailFailureStageUnknown
	var handlerStageElapsed time.Duration
	if task.failureMetadata != nil {
		snapshot := task.failureMetadata.snapshot(execution.observedAt)
		guildID = snapshot.guildID
		channelID = snapshot.channelID
		messageID = snapshot.messageID
		userID = snapshot.userID
		handlerStage = snapshot.handlerStage
		handlerStageElapsed = snapshot.handlerStageElapsed
	}
	if handlerStageElapsed > execution.handlerElapsed {
		handlerStageElapsed = execution.handlerElapsed
	}
	return TailFailure{
		EventType:           task.eventType,
		Kind:                tailFailureKind(execution.err),
		GuildID:             guildID,
		ChannelID:           channelID,
		MessageID:           messageID,
		UserID:              userID,
		HandlerStage:        handlerStage,
		HandlerStageElapsed: handlerStageElapsed,
		HandlerElapsed:      execution.handlerElapsed,
		JoinElapsed:         execution.joinElapsed,
		JoinOutcome:         execution.joinOutcome,
		ForceFallback:       execution.forceFallback,
	}
}

func tailFailureKind(err error) string {
	var panicErr *tailHandlerPanicError
	var joinErr *tailHandlerJoinError
	switch {
	case errors.As(err, &panicErr):
		return "panic"
	case errors.As(err, &joinErr):
		return "timeout"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "returned_error"
	}
}
