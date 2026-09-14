package discord

import (
	"context"
	"time"
)

func (c *Client) runTailTask(ctx context.Context, task func(context.Context) error) (err error) {
	return c.runTailTaskExecution(ctx, nil, task).err
}

func tailTaskResultAfterParentCancellation(parentErr error, result tailTaskResult) error {
	return tailTaskExecutionAfterParentCancellation(
		parentErr,
		result,
		time.Time{},
		time.Now(),
	).err
}

func awaitTailTaskParentCancellation(parentErr error, result <-chan tailTaskResult) error {
	return awaitTailTaskParentCancellationExecution(
		parentErr,
		result,
		time.Now(),
		time.Now(),
	).err
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

func newTailFailure(task tailTask, err error) TailFailure {
	return newTailFailureFromExecution(task, tailTaskExecution{
		err:         err,
		observedAt:  time.Now(),
		joinOutcome: TailFailureJoinNotRequired,
	})
}
