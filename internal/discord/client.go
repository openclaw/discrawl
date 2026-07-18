package discord

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

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

type tailHandlerPanicError struct{}

type tailHandlerDeadlineError struct {
	timeout     time.Duration
	cause       error
	detached    bool
	returnedNil bool
}

type tailHandlerJoinError struct {
	cause error
}

type GatewayOpenError struct {
	cause error
}

func (e *tailHandlerPanicError) Error() string {
	return "tail handler panic"
}

func (e *tailHandlerDeadlineError) Error() string {
	switch {
	case e.detached:
		return fmt.Sprintf("tail handler timed out after %s", e.timeout)
	case e.returnedNil:
		return fmt.Sprintf("tail handler returned nil after deadline %s", e.timeout)
	default:
		return fmt.Sprintf("tail handler returned after deadline %s: %v", e.timeout, e.cause)
	}
}

func (e *tailHandlerDeadlineError) Unwrap() error {
	if e.cause != nil {
		return e.cause
	}
	return context.DeadlineExceeded
}

func (e *tailHandlerJoinError) Error() string {
	return "tail handler did not stop after cancellation"
}

func (e *tailHandlerJoinError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *GatewayOpenError) Error() string {
	return "open discord gateway"
}

func (e *GatewayOpenError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

var ErrFatalTail = errors.New("fatal tail failure")

func IsFatalTailError(err error) bool {
	return errors.Is(err, ErrFatalTail)
}

func IsGatewayOpenError(err error) bool {
	var gatewayErr *GatewayOpenError
	return errors.As(err, &gatewayErr)
}

const (
	defaultTailHandlerFailureLimit = 3
	tailHandlerCancelGrace         = 100 * time.Millisecond
)

type tailFailureCircuit struct {
	mu          sync.Mutex
	limit       int
	consecutive int
	opened      bool
}

type tailFatalState struct {
	mu    sync.Mutex
	ready chan struct{}
	errs  []error
	seen  map[string]struct{}
	once  sync.Once
}

type tailTaskResult struct {
	err         error
	completedAt time.Time
}

type tailTaskExecution struct {
	err                error
	observedAt         time.Time
	handlerElapsed     time.Duration
	joinElapsed        time.Duration
	joinOutcome        TailFailureJoinOutcome
	forceFallback      bool
	parentCancellation bool
	handlerReturnedErr bool
}

type Client struct {
	session              *discordgo.Session
	requestTimeout       time.Duration
	tailWorkerCount      int
	tailQueueSize        int
	tailHandlerTimeout   time.Duration
	tailGraceTimerHook   func()
	tailTaskDequeuedHook func(context.Context)
}

func New(token string) (*Client, error) {
	session, err := discordgo.New("Bot " + token)
	if err != nil {
		return nil, fmt.Errorf("create discord session: %w", err)
	}
	session.Identify.Intents = discordgo.IntentsGuilds |
		discordgo.IntentsGuildMessages |
		discordgo.IntentsMessageContent |
		discordgo.IntentsGuildMembers
	session.SyncEvents = true
	// discordgo logs gateway URLs and raw transport errors; callers receive
	// sanitized typed errors from this client instead.
	session.LogLevel = -1
	return &Client{
		session:            session,
		requestTimeout:     45 * time.Second,
		tailWorkerCount:    defaultTailWorkerCount(),
		tailQueueSize:      defaultTailQueueSize(),
		tailHandlerTimeout: 30 * time.Second,
	}, nil
}

func (c *Client) Close() error {
	if c == nil || c.session == nil {
		return nil
	}
	return c.session.Close()
}

func (c *Client) Self(ctx context.Context) (*discordgo.User, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.User("@me", discordgo.WithContext(reqCtx))
}

func (c *Client) Guilds(ctx context.Context) ([]*discordgo.UserGuild, error) {
	var out []*discordgo.UserGuild
	before := ""
	for {
		reqCtx, cancel := c.requestContext(ctx)
		page, err := c.session.UserGuilds(200, before, "", false, discordgo.WithContext(reqCtx))
		cancel()
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return out, nil
		}
		out = append(out, page...)
		before = page[len(page)-1].ID
		if len(page) < 200 {
			return out, nil
		}
	}
}

func (c *Client) Guild(ctx context.Context, guildID string) (*discordgo.Guild, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.Guild(guildID, discordgo.WithContext(reqCtx))
}

func (c *Client) GuildChannels(ctx context.Context, guildID string) ([]*discordgo.Channel, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.GuildChannels(guildID, discordgo.WithContext(reqCtx))
}

func (c *Client) ThreadsActive(ctx context.Context, channelID string) ([]*discordgo.Channel, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	list, err := c.session.ThreadsActive(channelID, discordgo.WithContext(reqCtx))
	if err != nil {
		return nil, err
	}
	return list.Threads, nil
}

func (c *Client) GuildThreadsActive(ctx context.Context, guildID string) ([]*discordgo.Channel, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	list, err := c.session.GuildThreadsActive(guildID, discordgo.WithContext(reqCtx))
	if err != nil {
		return nil, err
	}
	return list.Threads, nil
}

func (c *Client) ThreadsArchived(ctx context.Context, channelID string, private bool) ([]*discordgo.Channel, error) {
	var out []*discordgo.Channel
	var before *time.Time
	for {
		reqCtx, cancel := c.requestContext(ctx)
		var list *discordgo.ThreadsList
		var err error
		if private {
			list, err = c.session.ThreadsPrivateArchived(channelID, before, 100, discordgo.WithContext(reqCtx))
		} else {
			list, err = c.session.ThreadsArchived(channelID, before, 100, discordgo.WithContext(reqCtx))
		}
		cancel()
		if err != nil {
			return nil, err
		}
		if len(list.Threads) == 0 {
			return out, nil
		}
		out = append(out, list.Threads...)
		if !list.HasMore {
			return uniqueChannels(out), nil
		}
		oldest := list.Threads[len(list.Threads)-1]
		if oldest.ThreadMetadata == nil {
			return uniqueChannels(out), nil
		}
		archiveAt := oldest.ThreadMetadata.ArchiveTimestamp
		before = &archiveAt
	}
}

func (c *Client) GuildMembers(ctx context.Context, guildID string) ([]*discordgo.Member, error) {
	var out []*discordgo.Member
	after := ""
	for {
		reqCtx, cancel := c.requestContext(ctx)
		page, err := c.session.GuildMembers(guildID, after, 1000, discordgo.WithContext(reqCtx))
		cancel()
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return out, nil
		}
		out = append(out, page...)
		after = page[len(page)-1].User.ID
		if len(page) < 1000 {
			return out, nil
		}
	}
}

func (c *Client) ChannelMessages(ctx context.Context, channelID string, limit int, beforeID, afterID string) ([]*discordgo.Message, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.ChannelMessages(channelID, limit, beforeID, afterID, "", discordgo.WithContext(reqCtx))
}

func (c *Client) ChannelMessage(ctx context.Context, channelID, messageID string) (*discordgo.Message, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.ChannelMessage(channelID, messageID, discordgo.WithContext(reqCtx))
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
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, newMessageTailTask(
			"MESSAGE_CREATE",
			func(taskCtx context.Context) error {
				return handler.OnMessageCreate(taskCtx, msg)
			},
			msg,
		))
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
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, task)
	})
	addHandler(func(_ *discordgo.Session, evt *discordgo.MessageDelete) {
		var msg, before *discordgo.Message
		if evt != nil {
			msg = evt.Message
			before = evt.BeforeDelete
		}
		c.enqueueTailTask(tailCtx, orderedWorkCh, fatal, newMessageTailTask(
			"MESSAGE_DELETE",
			func(taskCtx context.Context) error {
				return handler.OnMessageDelete(taskCtx, evt)
			},
			msg,
			before,
		))
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

func newTailFatalState() *tailFatalState {
	return &tailFatalState{
		ready: make(chan struct{}),
		seen:  map[string]struct{}{},
	}
}

func (s *tailFatalState) signal(err error) {
	if s == nil || err == nil {
		return
	}
	if !IsFatalTailError(err) {
		err = fmt.Errorf("%w: %w", ErrFatalTail, err)
	}
	key := err.Error()
	s.mu.Lock()
	if _, ok := s.seen[key]; ok {
		s.mu.Unlock()
		return
	}
	s.seen[key] = struct{}{}
	s.errs = append(s.errs, err)
	s.mu.Unlock()
	s.once.Do(func() {
		close(s.ready)
	})
}

func (s *tailFatalState) err() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return errors.Join(s.errs...)
}

func (c *Client) runTailTask(ctx context.Context, task func(context.Context) error) (err error) {
	return c.runTailTaskExecution(ctx, nil, task).err
}

func (c *Client) runTailTaskExecution(
	ctx context.Context,
	metadata *tailFailureMetadata,
	task func(context.Context) error,
) tailTaskExecution {
	startedAt := time.Now()
	if metadata == nil {
		metadata = &tailFailureMetadata{}
	}
	metadata.setStageAt(TailFailureStageHandler, startedAt)

	var (
		taskCtx context.Context
		cancel  context.CancelFunc
	)
	if c.tailHandlerTimeout > 0 {
		taskCtx, cancel = context.WithDeadline(ctx, startedAt.Add(c.tailHandlerTimeout))
	} else {
		taskCtx, cancel = context.WithCancel(ctx)
	}
	taskCtx = withTailFailureMetadata(taskCtx, metadata)
	defer cancel()

	result := make(chan tailTaskResult, 1)
	go func() {
		result <- tailTaskResult{
			err:         runTailTaskSafely(taskCtx, task),
			completedAt: time.Now(),
		}
	}()

	if c.tailHandlerTimeout <= 0 {
		select {
		case result := <-result:
			if ctxErr := ctx.Err(); ctxErr != nil {
				canceledAt := time.Now()
				metadata.freezeStageAt(canceledAt)
				return tailTaskExecutionAfterParentCancellation(ctxErr, result, startedAt, canceledAt)
			}
			metadata.freezeStageAt(result.completedAt)
			return completedTailTaskExecution(result, startedAt)
		case <-ctx.Done():
			canceledAt := time.Now()
			metadata.freezeStageAt(canceledAt)
			return awaitTailTaskParentCancellationExecution(ctx.Err(), result, startedAt, canceledAt)
		}
	}

	deadline := startedAt.Add(c.tailHandlerTimeout)
	graceDeadline := deadline.Add(tailHandlerCancelGrace)
	parentDeadline, hasParentDeadline := ctx.Deadline()
	parentDeadlineBeforeLocal := hasParentDeadline && parentDeadline.Before(deadline)
	select {
	case result := <-result:
		if parentErr := tailTaskParentError(
			ctx,
			parentDeadlineBeforeLocal,
			deadline,
		); parentErr != nil {
			canceledAt := time.Now()
			metadata.freezeStageAt(canceledAt)
			return tailTaskExecutionAfterParentCancellation(parentErr, result, startedAt, canceledAt)
		}
		if result.completedAt.Before(deadline) {
			metadata.freezeStageAt(result.completedAt)
		} else {
			metadata.freezeStageAt(deadline)
		}
		return classifyTailTaskExecution(
			c.tailHandlerTimeout,
			result,
			startedAt,
			deadline,
			graceDeadline,
		)
	case <-taskCtx.Done():
		if parentErr := tailTaskParentError(
			ctx,
			parentDeadlineBeforeLocal,
			deadline,
		); parentErr != nil {
			canceledAt := time.Now()
			metadata.freezeStageAt(canceledAt)
			return awaitTailTaskParentCancellationExecution(parentErr, result, startedAt, canceledAt)
		}
		metadata.freezeStageAt(deadline)
		return c.awaitTailTaskDeadlineExecution(result, startedAt, deadline, graceDeadline)
	}
}

func tailTaskResultAfterParentCancellation(parentErr error, result tailTaskResult) error {
	return tailTaskExecutionAfterParentCancellation(
		parentErr,
		result,
		time.Time{},
		time.Now(),
	).err
}

func tailTaskExecutionAfterParentCancellation(
	parentErr error,
	result tailTaskResult,
	startedAt time.Time,
	canceledAt time.Time,
) tailTaskExecution {
	execution := tailTaskExecution{
		err:                parentErr,
		observedAt:         canceledAt,
		handlerElapsed:     elapsedBetween(startedAt, result.completedAt),
		joinElapsed:        boundedJoinElapsed(canceledAt, result.completedAt),
		joinOutcome:        TailFailureJoinJoined,
		parentCancellation: true,
		handlerReturnedErr: result.err != nil,
	}
	if result.err != nil {
		execution.err = result.err
		execution.observedAt = result.completedAt
	}
	return execution
}

func awaitTailTaskParentCancellation(parentErr error, result <-chan tailTaskResult) error {
	return awaitTailTaskParentCancellationExecution(
		parentErr,
		result,
		time.Now(),
		time.Now(),
	).err
}

func awaitTailTaskParentCancellationExecution(
	parentErr error,
	result <-chan tailTaskResult,
	startedAt time.Time,
	canceledAt time.Time,
) tailTaskExecution {
	select {
	case result := <-result:
		return tailTaskExecutionAfterParentCancellation(parentErr, result, startedAt, canceledAt)
	default:
	}
	timer := time.NewTimer(tailHandlerCancelGrace)
	defer timer.Stop()
	select {
	case result := <-result:
		return tailTaskExecutionAfterParentCancellation(parentErr, result, startedAt, canceledAt)
	case <-timer.C:
		return tailTaskExecution{
			err:                &tailHandlerJoinError{cause: parentErr},
			observedAt:         canceledAt,
			handlerElapsed:     elapsedBetween(startedAt, time.Now()),
			joinElapsed:        tailHandlerCancelGrace,
			joinOutcome:        TailFailureJoinTimedOut,
			forceFallback:      true,
			parentCancellation: true,
		}
	}
}

func tailTaskParentError(
	ctx context.Context,
	parentDeadlineBeforeLocal bool,
	localDeadline time.Time,
) error {
	parentErr := ctx.Err()
	if parentErr == nil {
		return nil
	}
	if parentDeadlineBeforeLocal || time.Now().Before(localDeadline) {
		return parentErr
	}
	return nil
}

func (c *Client) awaitTailTaskDeadline(
	result <-chan tailTaskResult,
	deadline time.Time,
	graceDeadline time.Time,
) error {
	return c.awaitTailTaskDeadlineExecution(
		result,
		time.Time{},
		deadline,
		graceDeadline,
	).err
}

func (c *Client) awaitTailTaskDeadlineExecution(
	result <-chan tailTaskResult,
	startedAt time.Time,
	deadline time.Time,
	graceDeadline time.Time,
) tailTaskExecution {
	select {
	case result := <-result:
		return classifyTailTaskExecution(
			c.tailHandlerTimeout,
			result,
			startedAt,
			deadline,
			graceDeadline,
		)
	default:
	}
	graceRemaining := time.Until(graceDeadline)
	if graceRemaining <= 0 {
		return finalTailTaskDeadlineExecution(
			c.tailHandlerTimeout,
			result,
			startedAt,
			deadline,
			graceDeadline,
		)
	}
	timer := time.NewTimer(graceRemaining)
	defer timer.Stop()
	select {
	case result := <-result:
		return classifyTailTaskExecution(
			c.tailHandlerTimeout,
			result,
			startedAt,
			deadline,
			graceDeadline,
		)
	case <-timer.C:
		if c.tailGraceTimerHook != nil {
			c.tailGraceTimerHook()
		}
		return finalTailTaskDeadlineExecution(
			c.tailHandlerTimeout,
			result,
			startedAt,
			deadline,
			graceDeadline,
		)
	}
}

func classifyTailTaskResult(
	timeout time.Duration,
	result tailTaskResult,
	deadline time.Time,
	graceDeadline time.Time,
) error {
	return classifyTailTaskExecution(
		timeout,
		result,
		time.Time{},
		deadline,
		graceDeadline,
	).err
}

func classifyTailTaskExecution(
	timeout time.Duration,
	result tailTaskResult,
	startedAt time.Time,
	deadline time.Time,
	graceDeadline time.Time,
) tailTaskExecution {
	switch {
	case result.completedAt.Before(deadline):
		return completedTailTaskExecution(result, startedAt)
	case result.completedAt.Before(graceDeadline):
		return tailTaskExecution{
			err:            tailTaskDeadlineResult(timeout, result.err),
			observedAt:     deadline,
			handlerElapsed: elapsedBetween(startedAt, result.completedAt),
			joinElapsed:    boundedJoinElapsed(deadline, result.completedAt),
			joinOutcome:    TailFailureJoinJoined,
		}
	default:
		return tailTaskExecution{
			err:            tailTaskDetachedDeadlineError(timeout),
			observedAt:     deadline,
			handlerElapsed: elapsedBetween(startedAt, graceDeadline),
			joinElapsed:    tailHandlerCancelGrace,
			joinOutcome:    TailFailureJoinJoined,
		}
	}
}

func finalTailTaskDeadlineResult(
	timeout time.Duration,
	result <-chan tailTaskResult,
	deadline time.Time,
	graceDeadline time.Time,
) error {
	return finalTailTaskDeadlineExecution(
		timeout,
		result,
		time.Time{},
		deadline,
		graceDeadline,
	).err
}

func finalTailTaskDeadlineExecution(
	timeout time.Duration,
	result <-chan tailTaskResult,
	startedAt time.Time,
	deadline time.Time,
	graceDeadline time.Time,
) tailTaskExecution {
	select {
	case result := <-result:
		return classifyTailTaskExecution(timeout, result, startedAt, deadline, graceDeadline)
	default:
		return tailTaskExecution{
			err:            tailTaskDetachedDeadlineError(timeout),
			observedAt:     deadline,
			handlerElapsed: elapsedBetween(startedAt, graceDeadline),
			joinElapsed:    tailHandlerCancelGrace,
			joinOutcome:    TailFailureJoinTimedOut,
			forceFallback:  true,
		}
	}
}

func completedTailTaskExecution(result tailTaskResult, startedAt time.Time) tailTaskExecution {
	return tailTaskExecution{
		err:                result.err,
		observedAt:         result.completedAt,
		handlerElapsed:     elapsedBetween(startedAt, result.completedAt),
		joinOutcome:        TailFailureJoinNotRequired,
		handlerReturnedErr: result.err != nil,
	}
}

func elapsedBetween(startedAt, endedAt time.Time) time.Duration {
	if startedAt.IsZero() || endedAt.IsZero() || endedAt.Before(startedAt) {
		return 0
	}
	return endedAt.Sub(startedAt)
}

func boundedJoinElapsed(startedAt, endedAt time.Time) time.Duration {
	elapsed := elapsedBetween(startedAt, endedAt)
	if elapsed > tailHandlerCancelGrace {
		return tailHandlerCancelGrace
	}
	return elapsed
}

func tailTaskDetachedDeadlineError(timeout time.Duration) error {
	return &tailHandlerDeadlineError{
		timeout:  timeout,
		cause:    context.DeadlineExceeded,
		detached: true,
	}
}

func tailTaskDeadlineResult(timeout time.Duration, err error) error {
	if err == nil {
		return &tailHandlerDeadlineError{
			timeout:     timeout,
			cause:       context.DeadlineExceeded,
			returnedNil: true,
		}
	}
	if errors.Is(err, context.Canceled) {
		err = context.DeadlineExceeded
	}
	return &tailHandlerDeadlineError{timeout: timeout, cause: err}
}

func validateRefetchedMessageIdentity(partial, full *discordgo.Message) error {
	switch {
	case partial == nil || full == nil:
		return nil
	case full.ID != "" && partial.ID != "" && full.ID != partial.ID:
		return fmt.Errorf(
			"refetched message update returned different message id: event=%s fetched=%s",
			partial.ID,
			full.ID,
		)
	case full.ChannelID != "" && partial.ChannelID != "" && full.ChannelID != partial.ChannelID:
		return fmt.Errorf(
			"refetched message update returned different channel id: event=%s fetched=%s",
			partial.ChannelID,
			full.ChannelID,
		)
	case full.GuildID != "" && partial.GuildID != "" && full.GuildID != partial.GuildID:
		return fmt.Errorf(
			"refetched message update returned different guild id: event=%s fetched=%s",
			partial.GuildID,
			full.GuildID,
		)
	default:
		return nil
	}
}

func runTailTaskSafely(ctx context.Context, task func(context.Context) error) (err error) {
	defer func() {
		if recover() != nil {
			err = &tailHandlerPanicError{}
		}
	}()
	return task(ctx)
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

func newTailFailure(task tailTask, err error) TailFailure {
	return newTailFailureFromExecution(task, tailTaskExecution{
		err:         err,
		observedAt:  time.Now(),
		joinOutcome: TailFailureJoinNotRequired,
	})
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

func newMessageTailTask(
	eventType string,
	run func(context.Context) error,
	messages ...*discordgo.Message,
) tailTask {
	task := tailTask{
		eventType:    eventType,
		failureClass: tailFailureClassOrdered,
		run:          run,
	}
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		setTailTaskID(&task.guildID, msg.GuildID)
		setTailTaskID(&task.channelID, msg.ChannelID)
		setTailTaskID(&task.messageID, msg.ID)
		if msg.Author != nil {
			setTailTaskID(&task.userID, msg.Author.ID)
		}
	}
	task.failureMetadata = newTailFailureMetadata(task)
	return task
}

func newChannelTailTask(
	eventType string,
	run func(context.Context) error,
	channels ...*discordgo.Channel,
) tailTask {
	task := tailTask{
		eventType:    eventType,
		failureClass: tailFailureClassOrdered,
		run:          run,
	}
	for _, channel := range channels {
		if channel == nil {
			continue
		}
		setTailTaskID(&task.guildID, channel.GuildID)
		setTailTaskID(&task.channelID, channel.ID)
	}
	return task
}

func newGuildTailTask(
	eventType string,
	run func(context.Context) error,
	guilds ...*discordgo.Guild,
) tailTask {
	task := tailTask{
		eventType:    eventType,
		failureClass: tailFailureClassOrdered,
		run:          run,
	}
	for _, guild := range guilds {
		if guild != nil {
			setTailTaskID(&task.guildID, guild.ID)
		}
	}
	return task
}

func newMemberTailTask(
	eventType string,
	run func(context.Context) error,
	members ...*discordgo.Member,
) tailTask {
	task := tailTask{
		eventType:    eventType,
		failureClass: tailFailureClassMember,
		run:          run,
	}
	for _, member := range members {
		if member == nil {
			continue
		}
		setTailTaskID(&task.guildID, member.GuildID)
		if member.User != nil {
			setTailTaskID(&task.userID, member.User.ID)
		}
	}
	return task
}

func setTailTaskID(dst *string, value string) {
	if *dst == "" && value != "" {
		*dst = value
	}
}

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

func uniqueChannels(in []*discordgo.Channel) []*discordgo.Channel {
	if len(in) == 0 {
		return nil
	}
	out := make([]*discordgo.Channel, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, ch := range in {
		if ch == nil {
			continue
		}
		if _, ok := seen[ch.ID]; ok {
			continue
		}
		seen[ch.ID] = struct{}{}
		out = append(out, ch)
	}
	slices.SortFunc(out, func(a, b *discordgo.Channel) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		default:
			return 0
		}
	})
	return out
}

func (c *Client) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c == nil || c.requestTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.requestTimeout)
}
