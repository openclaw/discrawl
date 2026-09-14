package discord

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

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

const (
	defaultTailHandlerFailureLimit = 3
	tailHandlerCancelGrace         = 100 * time.Millisecond
)

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

func runTailTaskSafely(ctx context.Context, task func(context.Context) error) (err error) {
	defer func() {
		if recover() != nil {
			err = &tailHandlerPanicError{}
		}
	}()
	return task(ctx)
}
