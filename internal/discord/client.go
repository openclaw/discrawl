package discord

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/bwmarrin/discordgo"
)

type GatewayOpenError struct {
	cause error
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
		if len(page) < 200 {
			return out, nil
		}
		nextBefore := page[len(page)-1].ID
		if nextBefore == "" {
			return nil, errors.New("guild page missing id")
		}
		if nextBefore == before {
			return nil, errors.New("guild page cursor did not advance")
		}
		before = nextBefore
	}
}

func (c *Client) Guild(ctx context.Context, guildID string) (*discordgo.Guild, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.Guild(guildID, discordgo.WithContext(reqCtx))
}

func (c *Client) Channel(ctx context.Context, channelID string) (*discordgo.Channel, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.Channel(channelID, discordgo.WithContext(reqCtx))
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

func (c *Client) ThreadsArchived(ctx context.Context, channelID string, private bool, after time.Time) ([]*discordgo.Channel, error) {
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
		reachedAfter := false
		for _, thread := range list.Threads {
			if !after.IsZero() && thread != nil && thread.ThreadMetadata != nil && !thread.ThreadMetadata.ArchiveTimestamp.After(after) {
				reachedAfter = true
				break
			}
			out = append(out, thread)
		}
		if reachedAfter || !list.HasMore {
			return uniqueChannels(out), nil
		}
		oldest := list.Threads[len(list.Threads)-1]
		if oldest.ThreadMetadata == nil {
			return uniqueChannels(out), nil
		}
		archiveAt := oldest.ThreadMetadata.ArchiveTimestamp
		if before != nil && archiveAt.Equal(*before) {
			return nil, fmt.Errorf("channel %s archived thread page cursor did not advance", channelID)
		}
		before = &archiveAt
	}
}

func (c *Client) GuildMembers(ctx context.Context, guildID string) ([]*discordgo.Member, error) {
	result, err := c.GuildMemberEnumeration(ctx, guildID)
	return result.Members, err
}

type MemberEnumeration struct {
	Members      []*discordgo.Member
	Pages        int
	LastUserID   string
	Complete     bool
	ProviderWait time.Duration
	CallsElapsed time.Duration
	RateLimits   int
}

// GuildMemberEnumeration exposes the actual terminal pagination boundary while
// keeping incomplete member bodies out of callers' merge paths.
func (c *Client) GuildMemberEnumeration(ctx context.Context, guildID string) (MemberEnumeration, error) {
	var result MemberEnumeration
	failure := func(err error) (MemberEnumeration, error) {
		return MemberEnumeration{Pages: result.Pages, LastUserID: result.LastUserID, ProviderWait: result.ProviderWait, CallsElapsed: result.CallsElapsed, RateLimits: result.RateLimits}, err
	}
	after := ""
	var previous uint64
	for {
		page, err := c.memberPage(ctx, guildID, after, &result)
		if err != nil {
			return failure(err)
		}
		result.Pages++
		if len(page) == 0 {
			result.Complete = true
			return result, nil
		}
		for _, member := range page {
			id := memberUserID(member)
			if id == "" {
				return failure(errors.New("guild member page missing user id"))
			}
			numeric, parseErr := strconv.ParseUint(id, 10, 64)
			if parseErr != nil || numeric == 0 || strconv.FormatUint(numeric, 10) != id {
				return failure(errors.New("guild member page has invalid user id"))
			}
			if numeric <= previous {
				return failure(fmt.Errorf("guild %s member page cursor did not advance", guildID))
			}
			previous = numeric
			result.Members = append(result.Members, member)
		}
		nextAfter := lastMemberUserID(page)
		if nextAfter == "" {
			return failure(fmt.Errorf("guild %s member page missing user id", guildID))
		}
		if nextAfter == after {
			return failure(fmt.Errorf("guild %s member page cursor did not advance", guildID))
		}
		result.LastUserID = nextAfter
		if progress, ok := ctx.Value(memberProgressKey{}).(func(MemberProgress)); ok && result.Pages%25 == 0 {
			progress(MemberProgress{
				Pages: result.Pages, Members: len(result.Members), LastUserID: result.LastUserID,
				ProviderWait: result.ProviderWait, CallsElapsed: result.CallsElapsed, RateLimits: result.RateLimits,
			})
		}
		if len(page) < 1000 {
			result.Complete = true
			return result, nil
		}
		after = nextAfter
	}
}

func memberUserID(member *discordgo.Member) string {
	if member == nil || member.User == nil {
		return ""
	}
	return member.User.ID
}

func lastMemberUserID(page []*discordgo.Member) string {
	for _, member := range slices.Backward(page) {
		if id := memberUserID(member); id != "" {
			return id
		}
	}
	return ""
}

func (c *Client) ChannelMessages(ctx context.Context, channelID string, limit int, beforeID, afterID string) ([]*discordgo.Message, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.ChannelMessages(channelID, limit, beforeID, afterID, "", discordgo.WithContext(reqCtx))
}

// CanReadHistory resolves current bot permissions, including thread inheritance.
func (c *Client) CanReadHistory(ctx context.Context, channel *discordgo.Channel) (bool, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	// discordgo's permission helper prefers gateway State. A new REST session
	// avoids certifying an empty history response with stale cached permissions.
	rest, err := discordgo.New(c.session.Token)
	if err != nil {
		return false, err
	}
	rest.Client, rest.Ratelimiter = c.session.Client, c.session.Ratelimiter
	rest.UserAgent, rest.MaxRestRetries = c.session.UserAgent, c.session.MaxRestRetries
	rest.ShouldRetryOnRateLimit, rest.LogLevel = c.session.ShouldRetryOnRateLimit, -1
	fresh, err := rest.Channel(channel.ID, discordgo.WithContext(reqCtx))
	if err != nil {
		return false, err
	}
	if fresh.ID != channel.ID || fresh.GuildID != channel.GuildID || fresh.Type != channel.Type {
		return false, errors.New("channel identity changed before history permission check")
	}
	self, err := rest.User("@me", discordgo.WithContext(reqCtx))
	if err != nil {
		return false, err
	}
	permissionChannel := fresh.ID
	if fresh.IsThread() {
		if fresh.ParentID == "" {
			return false, errors.New("thread parent is unavailable")
		}
		permissionChannel = fresh.ParentID
	}
	permissions, err := rest.UserChannelPermissions(self.ID, permissionChannel, discordgo.WithContext(reqCtx))
	if err != nil {
		return false, err
	}
	required := int64(discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory)
	if channel.Type == discordgo.ChannelTypeGuildVoice || channel.Type == discordgo.ChannelTypeGuildStageVoice {
		// Discord requires CONNECT to read a voice channel's text history.
		required |= discordgo.PermissionVoiceConnect
	}
	return permissions&required == required, nil
}

func (c *Client) GuildMember(ctx context.Context, guildID, userID string) (*discordgo.Member, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.GuildMember(guildID, userID, discordgo.WithContext(reqCtx))
}

func (c *Client) ChannelMessage(ctx context.Context, channelID, messageID string) (*discordgo.Message, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.ChannelMessage(channelID, messageID, discordgo.WithContext(reqCtx))
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
