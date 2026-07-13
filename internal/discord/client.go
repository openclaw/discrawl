package discord

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
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

type TailReadyHandler interface {
	OnTailReady(context.Context) error
}

type TailFailure struct {
	EventType string
	Kind      string
	GuildID   string
	ChannelID string
	MessageID string
	UserID    string
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
	mu        sync.RWMutex
	guildID   string
	channelID string
	messageID string
	userID    string
}

type tailFailureMetadataContextKey struct{}

type tailHandlerPanicError struct {
	value any
}

type tailHandlerDeadlineError struct {
	timeout     time.Duration
	cause       error
	detached    bool
	returnedNil bool
}

func (e *tailHandlerPanicError) Error() string {
	return fmt.Sprintf("tail handler panic: %v", e.value)
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

func (e *tailHandlerDeadlineError) requiresSynchronousRecord() bool {
	return e.detached || e.returnedNil
}

var ErrFatalTail = errors.New("fatal tail failure")

func IsFatalTailError(err error) bool {
	return errors.Is(err, ErrFatalTail)
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

type Client struct {
	session            *discordgo.Session
	requestTimeout     time.Duration
	tailWorkerCount    int
	tailQueueSize      int
	tailHandlerTimeout time.Duration
	tailGraceTimerHook func()
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
				select {
				case <-tailCtx.Done():
					return
				case task := <-queue:
					if task.run == nil {
						continue
					}
					if err := c.runTailTask(tailCtx, task.run); err != nil {
						var deadlineErr *tailHandlerDeadlineError
						hasDeadlineErr := errors.As(err, &deadlineErr)
						if tailCtx.Err() != nil && !hasDeadlineErr {
							return
						}
						failure := newTailFailure(task, err)
						if deadlineErr != nil && deadlineErr.detached {
							cancel()
						}
						var recordErr error
						if deadlineErr != nil && deadlineErr.requiresSynchronousRecord() {
							recordErr = recordTailFailure(failureRecorder, failure)
						}
						reportTailFailure(failureHandler, failure)
						if deadlineErr != nil && (deadlineErr.detached || recordErr != nil) {
							cancel()
							fatalErr := fmt.Errorf(
								"tail %s handler timed out for %s: %w",
								task.failureClass,
								task.eventType,
								err,
							)
							if recordErr != nil {
								fatalErr = errors.Join(
									fatalErr,
									fmt.Errorf("persist timed-out tail failure: %w", recordErr),
								)
							}
							fatal.signal(fatalErr)
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
		task.failureMetadata = newTailFailureMetadata(task)
		metadata := task.failureMetadata
		task.run = func(taskCtx context.Context) error {
			taskCtx = withTailFailureMetadata(taskCtx, metadata)
			var refetchErr error
			if msg != nil && msg.Content == "" {
				full, err := session.ChannelMessage(msg.ChannelID, msg.ID, discordgo.WithContext(taskCtx))
				if err == nil && full != nil {
					if err := validateRefetchedMessageIdentity(msg, full); err != nil {
						refetchErr = err
					} else {
						msg = full
						EnrichTailFailureMetadata(taskCtx, full)
					}
				} else if err == nil {
					msg = full
				}
			}
			if msg == nil {
				return refetchErr
			}
			return errors.Join(refetchErr, handler.OnMessageUpdate(taskCtx, msg))
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
	defer func() {
		cancel()
		for _, remove := range slices.Backward(removers) {
			remove()
		}
		if opened {
			_ = c.session.Close()
		}
		wg.Wait()
	}()
	if err := c.session.Open(); err != nil {
		return err
	}
	opened = true
	if ready, ok := handler.(TailReadyHandler); ok {
		if err := ready.OnTailReady(tailCtx); err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
		cancel()
		wg.Wait()
		if err := fatal.err(); err != nil {
			return err
		}
		return nil
	case <-fatal.ready:
		cancel()
		wg.Wait()
		return fatal.err()
	}
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
	if c.tailHandlerTimeout <= 0 {
		taskCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		result := make(chan tailTaskResult, 1)
		go func() {
			result <- tailTaskResult{
				err:         runTailTaskSafely(taskCtx, task),
				completedAt: time.Now(),
			}
		}()
		select {
		case result := <-result:
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return result.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	deadline := time.Now().Add(c.tailHandlerTimeout)
	graceDeadline := deadline.Add(tailHandlerCancelGrace)
	parentDeadline, hasParentDeadline := ctx.Deadline()
	parentDeadlineBeforeLocal := hasParentDeadline && parentDeadline.Before(deadline)
	taskCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	result := make(chan tailTaskResult, 1)
	go func() {
		result <- tailTaskResult{
			err:         runTailTaskSafely(taskCtx, task),
			completedAt: time.Now(),
		}
	}()
	select {
	case result := <-result:
		if parentErr := tailTaskParentError(
			ctx,
			parentDeadlineBeforeLocal,
			deadline,
		); parentErr != nil {
			return parentErr
		}
		return classifyTailTaskResult(
			c.tailHandlerTimeout,
			result,
			deadline,
			graceDeadline,
		)
	case <-taskCtx.Done():
		if parentErr := tailTaskParentError(
			ctx,
			parentDeadlineBeforeLocal,
			deadline,
		); parentErr != nil {
			return parentErr
		}
		return c.awaitTailTaskDeadline(result, deadline, graceDeadline)
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
	select {
	case result := <-result:
		return classifyTailTaskResult(
			c.tailHandlerTimeout,
			result,
			deadline,
			graceDeadline,
		)
	default:
	}
	graceRemaining := time.Until(graceDeadline)
	if graceRemaining <= 0 {
		return finalTailTaskDeadlineResult(
			c.tailHandlerTimeout,
			result,
			deadline,
			graceDeadline,
		)
	}
	timer := time.NewTimer(graceRemaining)
	defer timer.Stop()
	select {
	case result := <-result:
		return classifyTailTaskResult(
			c.tailHandlerTimeout,
			result,
			deadline,
			graceDeadline,
		)
	case <-timer.C:
		if c.tailGraceTimerHook != nil {
			c.tailGraceTimerHook()
		}
		return finalTailTaskDeadlineResult(
			c.tailHandlerTimeout,
			result,
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
	switch {
	case result.completedAt.Before(deadline):
		return result.err
	case result.completedAt.Before(graceDeadline):
		return tailTaskDeadlineResult(timeout, result.err)
	default:
		return tailTaskDetachedDeadlineError(timeout)
	}
}

func finalTailTaskDeadlineResult(
	timeout time.Duration,
	result <-chan tailTaskResult,
	deadline time.Time,
	graceDeadline time.Time,
) error {
	select {
	case result := <-result:
		return classifyTailTaskResult(timeout, result, deadline, graceDeadline)
	default:
		return tailTaskDetachedDeadlineError(timeout)
	}
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
		if recovered := recover(); recovered != nil {
			err = &tailHandlerPanicError{value: recovered}
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

func recordTailFailure(handler tailFailureRecorder, failure TailFailure) error {
	if handler == nil {
		if failure.MessageID != "" {
			return errors.New("tail failure recorder unavailable")
		}
		return nil
	}
	return handler.RecordTailFailure(failure)
}

func reportTailFailure(handler tailFailureHandler, failure TailFailure) {
	if handler == nil {
		return
	}
	handler.OnTailFailure(failure)
}

func newTailFailure(task tailTask, err error) TailFailure {
	guildID, channelID, messageID, userID := task.guildID, task.channelID, task.messageID, task.userID
	if task.failureMetadata != nil {
		guildID, channelID, messageID, userID = task.failureMetadata.snapshot()
	}
	return TailFailure{
		EventType: task.eventType,
		Kind:      tailFailureKind(err),
		GuildID:   guildID,
		ChannelID: channelID,
		MessageID: messageID,
		UserID:    userID,
	}
}

func tailFailureKind(err error) string {
	var panicErr *tailHandlerPanicError
	switch {
	case errors.As(err, &panicErr):
		return "panic"
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

// EnrichTailFailureMetadata adds message identifiers to the current tail event's failure report.
func EnrichTailFailureMetadata(ctx context.Context, msg *discordgo.Message) {
	if ctx == nil || msg == nil {
		return
	}
	metadata, _ := ctx.Value(tailFailureMetadataContextKey{}).(*tailFailureMetadata)
	metadata.addMessage(msg)
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

func (m *tailFailureMetadata) snapshot() (string, string, string, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.guildID, m.channelID, m.messageID, m.userID
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
