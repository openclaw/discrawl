package discord

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/bwmarrin/discordgo"
)

type EventHandler interface {
	OnMessageCreate(context.Context, *discordgo.Message) error
	OnMessageUpdate(context.Context, *discordgo.Message) error
	OnMessageDelete(context.Context, *discordgo.MessageDelete) error
	OnChannelUpsert(context.Context, *discordgo.Channel) error
	OnMemberUpsert(context.Context, string, *discordgo.Member) error
	OnMemberDelete(context.Context, string, string) error
}

type guildEventHandler interface {
	OnGuildUpsert(context.Context, *discordgo.Guild) error
	OnGuildDelete(context.Context, *discordgo.Guild) error
}

type tailGuildFilter interface {
	TailAllowsGuild(string) bool
}

type TailReadyHandler interface {
	OnTailReady(context.Context) error
}

// TailEventObservation identifies a message event received from the Gateway.
// It intentionally excludes message content and author metadata.
type TailEventObservation struct {
	EventType string
	Stage     string
	Reason    string
	GuildID   string
	ChannelID string
	MessageID string
}

type tailEventObserver interface {
	OnTailEventObserved(TailEventObservation)
}

type tailTask struct {
	eventType       string
	failureClass    tailFailureClass
	guildID         string
	channelID       string
	messageID       string
	userID          string
	failureMetadata *tailFailureMetadata
	run             func(context.Context) error
}

func (c *Client) Tail(ctx context.Context, handler EventHandler) error {
	if handler == nil {
		return errors.New("missing event handler")
	}
	tailCtx, cancel := context.WithCancel(ctx)

	fatal := newTailFatalState()
	workCh := make(chan tailTask, c.tailQueueSize)
	orderedWorkCh := make(chan tailTask, c.tailQueueSize)
	failureHandler, _ := handler.(tailFailureHandler)
	failureRecorder, _ := handler.(tailFailureRecorder)
	eventObserver, _ := handler.(tailEventObserver)
	failureCircuits := map[tailFailureClass]*tailFailureCircuit{
		tailFailureClassOrdered: {limit: defaultTailHandlerFailureLimit},
		tailFailureClassMember:  {limit: defaultTailHandlerFailureLimit},
	}
	var wg sync.WaitGroup
	startWorker := func(queue <-chan tailTask) {
		wg.Go(func() {
			for {
				if tailCtx.Err() != nil {
					return
				}
				select {
				case <-tailCtx.Done():
					return
				case task := <-queue:
					if c.tailTaskDequeuedHook != nil {
						c.tailTaskDequeuedHook(tailCtx)
					}
					if tailCtx.Err() != nil {
						return
					}
					if task.run == nil {
						continue
					}
					if task.failureMetadata == nil {
						task.failureMetadata = newTailFailureMetadata(task)
					}
					execution := c.runTailTaskExecution(tailCtx, task.failureMetadata, task.run)
					if err := execution.err; err != nil {
						var deadlineErr *tailHandlerDeadlineError
						hasDeadlineErr := errors.As(err, &deadlineErr)
						var panicErr *tailHandlerPanicError
						hasPanicErr := errors.As(err, &panicErr)
						messageScoped := strings.HasPrefix(task.eventType, "MESSAGE_")
						parentJoinTimedOut := execution.parentCancellation && execution.forceFallback
						parentMessageFailure := execution.parentCancellation &&
							execution.handlerReturnedErr &&
							messageScoped
						if tailCtx.Err() != nil &&
							!hasDeadlineErr &&
							!hasPanicErr &&
							!parentJoinTimedOut &&
							!parentMessageFailure {
							return
						}

						failure := newTailFailureFromExecution(task, execution)
						if deadlineErr != nil && deadlineErr.detached {
							cancel()
						}
						if parentJoinTimedOut {
							cancel()
						}
						if messageScoped {
							if recordTailFailure(failureRecorder, failure) != nil {
								cancel()
								fatal.signal(errors.New("persist tail message failure"))
								return
							}
						}
						reportTailFailure(failureHandler, failure)
						if parentJoinTimedOut {
							fatal.signal(errors.New("tail handler did not stop after cancellation"))
							return
						}
						if deadlineErr != nil && deadlineErr.detached {
							cancel()
							fatal.signal(fmt.Errorf(
								"tail %s handler timed out for %s: %w",
								task.failureClass,
								task.eventType,
								err,
							))
							return
						}
						failureCircuit := failureCircuits[task.failureClass]
						if failureCircuit == nil {
							failureCircuit = failureCircuits[tailFailureClassOrdered]
						}
						if failureCircuit.recordFailure() {
							fatal.signal(
								fmt.Errorf(
									"tail handler circuit breaker opened after %d consecutive failures",
									defaultTailHandlerFailureLimit,
								),
							)
							cancel()
							return
						}
						continue
					}
					failureCircuit := failureCircuits[task.failureClass]
					if failureCircuit == nil {
						failureCircuit = failureCircuits[tailFailureClassOrdered]
					}
					failureCircuit.recordSuccess()
				}
			}
		})
	}
	for range c.tailWorkerCount {
		startWorker(workCh)
	}
	startWorker(orderedWorkCh)

	var removers []func()
	addHandler := func(eventHandler any) {
		removers = append(removers, c.session.AddHandler(eventHandler))
	}
	addHandler(func(_ *discordgo.Session, evt *discordgo.MessageCreate) {
		var msg *discordgo.Message
		if evt != nil {
			msg = evt.Message
		}
		task := newMessageTailTask(
			"MESSAGE_CREATE",
			func(taskCtx context.Context) error {
				return handler.OnMessageCreate(taskCtx, msg)
			},
			msg,
		)
		reportTailEventObserved(eventObserver, task, "gateway_received", "")
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, task)
	})
	addHandler(func(session *discordgo.Session, evt *discordgo.MessageUpdate) {
		var msg, before *discordgo.Message
		if evt != nil {
			msg = evt.Message
			before = evt.BeforeUpdate
		}
		task := newMessageTailTask(
			"MESSAGE_UPDATE",
			nil,
			msg,
			before,
		)
		task.run = func(taskCtx context.Context) error {
			if filter, ok := handler.(tailGuildFilter); ok && msg != nil && !filter.TailAllowsGuild(msg.GuildID) {
				reportTailEventObserved(eventObserver, task, "handler_started", "")
				reportTailEventObserved(eventObserver, task, "ignored", "guild_scope")
				return nil
			}
			var refetchErr error
			if msg != nil && msg.Content == "" {
				UpdateTailFailureStage(taskCtx, TailFailureStageMessageUpdateRefetch)
				full, err := session.ChannelMessage(msg.ChannelID, msg.ID, discordgo.WithContext(taskCtx))
				switch {
				case err != nil:
					refetchErr = fmt.Errorf("refetch message update: %w", err)
				case full != nil:
					if err := validateRefetchedMessageIdentity(msg, full); err != nil {
						refetchErr = err
					} else {
						msg = full
						EnrichTailFailureMetadata(taskCtx, full)
					}
				default:
					msg = full
				}
			}
			if msg == nil {
				return refetchErr
			}
			// A failed refetch does not suppress the partial update, but it remains
			// an event failure even when the handler accepts that recovery input.
			UpdateTailFailureStage(taskCtx, TailFailureStageHandler)
			handlerErr := handler.OnMessageUpdate(taskCtx, msg)
			if refetchErr != nil {
				UpdateTailFailureStage(taskCtx, TailFailureStageMessageUpdateRefetch)
			}
			return errors.Join(refetchErr, handlerErr)
		}
		reportTailEventObserved(eventObserver, task, "gateway_received", "")
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, task)
	})
	addHandler(func(_ *discordgo.Session, evt *discordgo.MessageDelete) {
		var msg, before *discordgo.Message
		if evt != nil {
			msg = evt.Message
			before = evt.BeforeDelete
		}
		task := newMessageTailTask(
			"MESSAGE_DELETE",
			func(taskCtx context.Context) error {
				return handler.OnMessageDelete(taskCtx, evt)
			},
			msg,
			before,
		)
		reportTailEventObserved(eventObserver, task, "gateway_received", "")
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, task)
	})
	addHandler(func(_ *discordgo.Session, evt *discordgo.ChannelCreate) {
		var channel *discordgo.Channel
		if evt != nil {
			channel = evt.Channel
		}
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, newChannelTailTask(
			"CHANNEL_CREATE",
			func(taskCtx context.Context) error {
				return handler.OnChannelUpsert(taskCtx, channel)
			},
			channel,
		))
	})
	addHandler(func(_ *discordgo.Session, evt *discordgo.ChannelUpdate) {
		var channel, before *discordgo.Channel
		if evt != nil {
			channel = evt.Channel
			before = evt.BeforeUpdate
		}
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, newChannelTailTask(
			"CHANNEL_UPDATE",
			func(taskCtx context.Context) error {
				return handler.OnChannelUpsert(taskCtx, channel)
			},
			channel,
			before,
		))
	})
	addHandler(func(_ *discordgo.Session, evt *discordgo.ThreadCreate) {
		var channel *discordgo.Channel
		if evt != nil {
			channel = evt.Channel
		}
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, newChannelTailTask(
			"THREAD_CREATE",
			func(taskCtx context.Context) error {
				return handler.OnChannelUpsert(taskCtx, channel)
			},
			channel,
		))
	})
	addHandler(func(_ *discordgo.Session, evt *discordgo.ThreadUpdate) {
		var channel, before *discordgo.Channel
		if evt != nil {
			channel = evt.Channel
			before = evt.BeforeUpdate
		}
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, newChannelTailTask(
			"THREAD_UPDATE",
			func(taskCtx context.Context) error {
				return handler.OnChannelUpsert(taskCtx, channel)
			},
			channel,
			before,
		))
	})
	addHandler(func(_ *discordgo.Session, evt *discordgo.GuildCreate) {
		guildHandler, ok := handler.(guildEventHandler)
		if !ok {
			return
		}
		var guild *discordgo.Guild
		if evt != nil {
			guild = evt.Guild
		}
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, newGuildTailTask(
			"GUILD_CREATE",
			func(taskCtx context.Context) error { return guildHandler.OnGuildUpsert(taskCtx, guild) },
			guild,
		))
	})
	addHandler(func(_ *discordgo.Session, evt *discordgo.GuildUpdate) {
		guildHandler, ok := handler.(guildEventHandler)
		if !ok {
			return
		}
		var guild *discordgo.Guild
		if evt != nil {
			guild = evt.Guild
		}
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, newGuildTailTask(
			"GUILD_UPDATE",
			func(taskCtx context.Context) error { return guildHandler.OnGuildUpsert(taskCtx, guild) },
			guild,
		))
	})
	addHandler(func(_ *discordgo.Session, evt *discordgo.GuildDelete) {
		guildHandler, ok := handler.(guildEventHandler)
		if !ok {
			return
		}
		var guild *discordgo.Guild
		if evt != nil {
			guild = evt.Guild
		}
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, newGuildTailTask(
			"GUILD_DELETE",
			func(taskCtx context.Context) error { return guildHandler.OnGuildDelete(taskCtx, guild) },
			guild,
		))
	})
	addHandler(func(_ *discordgo.Session, evt *discordgo.GuildMemberAdd) {
		var member *discordgo.Member
		if evt != nil {
			member = evt.Member
		}
		c.enqueueTailTask(tailCtx, workCh, fatal, newMemberTailTask(
			"GUILD_MEMBER_ADD",
			func(taskCtx context.Context) error {
				if member == nil {
					return handler.OnMemberUpsert(taskCtx, "", nil)
				}
				return handler.OnMemberUpsert(taskCtx, member.GuildID, member)
			},
			member,
		))
	})
	addHandler(func(_ *discordgo.Session, evt *discordgo.GuildMemberUpdate) {
		var member, before *discordgo.Member
		if evt != nil {
			before = evt.BeforeUpdate
			if evt.Member != nil {
				member = &discordgo.Member{
					GuildID:  evt.GuildID,
					Nick:     evt.Nick,
					Avatar:   evt.Avatar,
					Roles:    evt.Roles,
					JoinedAt: evt.JoinedAt,
					User:     evt.User,
				}
			}
		}
		c.enqueueTailTask(tailCtx, workCh, fatal, newMemberTailTask(
			"GUILD_MEMBER_UPDATE",
			func(taskCtx context.Context) error {
				if member == nil {
					return handler.OnMemberUpsert(taskCtx, "", nil)
				}
				return handler.OnMemberUpsert(taskCtx, member.GuildID, member)
			},
			member,
			before,
		))
	})
	addHandler(func(_ *discordgo.Session, evt *discordgo.GuildMemberRemove) {
		var member *discordgo.Member
		if evt != nil {
			member = evt.Member
		}
		if member == nil || member.User == nil {
			return
		}
		c.enqueueTailTask(tailCtx, workCh, fatal, newMemberTailTask(
			"GUILD_MEMBER_REMOVE",
			func(taskCtx context.Context) error {
				return handler.OnMemberDelete(taskCtx, member.GuildID, member.User.ID)
			},
			member,
		))
	})
	opened := false
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			cancel()
			for _, remove := range slices.Backward(removers) {
				remove()
			}
			wg.Wait()
			if opened {
				_ = c.session.Close()
			}
		})
	}
	defer cleanup()
	if err := c.session.Open(); err != nil {
		return &GatewayOpenError{cause: err}
	}
	opened = true
	if ready, ok := handler.(TailReadyHandler); ok {
		if err := ready.OnTailReady(tailCtx); err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
	case <-fatal.ready:
	}
	cleanup()
	if err := fatal.err(); err != nil {
		return err
	}
	return nil
}

func reportTailEventObserved(observer tailEventObserver, task tailTask, stage, reason string) {
	if observer == nil {
		return
	}
	observer.OnTailEventObserved(TailEventObservation{
		EventType: task.eventType,
		Stage:     stage,
		Reason:    reason,
		GuildID:   task.guildID,
		ChannelID: task.channelID,
		MessageID: task.messageID,
	})
}

func (c *Client) enqueueTailTask(
	ctx context.Context,
	workCh chan<- tailTask,
	fatal *tailFatalState,
	task tailTask,
) {
	select {
	case <-ctx.Done():
		return
	case workCh <- task:
	default:
		fatal.signal(errors.New("tail worker queue full"))
	}
}

func defaultTailWorkerCount() int {
	workers := runtime.GOMAXPROCS(0)
	switch {
	case workers < 4:
		return 4
	case workers > 16:
		return 16
	default:
		return workers
	}
}

func defaultTailQueueSize() int {
	return defaultTailWorkerCount() * 32
}
