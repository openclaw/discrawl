package discord

import (
	"context"
	"errors"
	"time"

	"github.com/bwmarrin/discordgo"
)

type MemberProgress struct {
	Pages, Members, RateLimits int
	LastUserID                 string
	ProviderWait, CallsElapsed time.Duration
}

type memberProgressKey struct{}

// WithMemberProgress observes bounded metadata only, never member bodies.
func WithMemberProgress(ctx context.Context, fn func(MemberProgress)) context.Context {
	return context.WithValue(ctx, memberProgressKey{}, fn)
}

func (c *Client) memberPage(ctx context.Context, guildID, after string, result *MemberEnumeration) ([]*discordgo.Member, error) {
	for {
		// Read the same SDK bucket used by GuildMembers, without changing its
		// remaining tokens, reset time or global backoff. The SDK rechecks it
		// on the actual request as well. Waiting here makes it cancellable.
		bucket := c.session.Ratelimiter.GetBucket(discordgo.EndpointGuildMembers(guildID))
		for {
			bucket.Lock()
			wait := c.session.Ratelimiter.GetWaitTime(bucket, 1)
			bucket.Unlock()
			if wait <= 0 {
				break
			}
			spent, err := waitForMemberBudget(ctx, wait)
			result.ProviderWait += spent
			if err != nil {
				return nil, err
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		call, cancel := c.requestContext(ctx)
		started := time.Now()
		page, err := c.session.GuildMembers(guildID, after, 1000,
			discordgo.WithContext(call), discordgo.WithRetryOnRatelimit(false))
		result.CallsElapsed += time.Since(started)
		cancel()
		var limited *discordgo.RateLimitError
		if !errors.As(err, &limited) {
			return page, err
		}
		result.RateLimits++
		if !c.session.ShouldRetryOnRateLimit || limited.RetryAfter <= 0 {
			return nil, err
		}
		// Preserve the provider's entire retry delay. A missing/invalid delay
		// is returned as an error instead of creating a rapid retry loop.
		spent, waitErr := waitForMemberBudget(ctx, limited.RetryAfter)
		result.ProviderWait += spent
		if waitErr != nil {
			return nil, waitErr
		}
	}
}

func waitForMemberBudget(ctx context.Context, wait time.Duration) (time.Duration, error) {
	started := time.Now()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return time.Since(started), ctx.Err()
	case <-timer.C:
		return time.Since(started), nil
	}
}
