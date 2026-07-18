package syncer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	discordclient "github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/store"
)

type Client interface {
	Self(context.Context) (*discordgo.User, error)
	Guilds(context.Context) ([]*discordgo.UserGuild, error)
	Guild(context.Context, string) (*discordgo.Guild, error)
	GuildChannels(context.Context, string) ([]*discordgo.Channel, error)
	ThreadsActive(context.Context, string) ([]*discordgo.Channel, error)
	GuildThreadsActive(context.Context, string) ([]*discordgo.Channel, error)
	ThreadsArchived(context.Context, string, bool) ([]*discordgo.Channel, error)
	GuildMembers(context.Context, string) ([]*discordgo.Member, error)
	ChannelMessages(context.Context, string, int, string, string) ([]*discordgo.Message, error)
	ChannelMessage(context.Context, string, string) (*discordgo.Message, error)
	Tail(context.Context, discordclient.EventHandler) error
}

type closeableClient interface {
	Close() error
}

type Syncer struct {
	client                Client
	store                 *store.Store
	logger                *slog.Logger
	attachmentTextEnabled bool
	memberRefreshTimeout  time.Duration
	memberRefreshInterval time.Duration
	messageChannelTimeout time.Duration
	messageSyncLogEvery   time.Duration
	messageSyncWaitEvery  time.Duration
	tailReady             func(context.Context) error
	tailRepair            func(context.Context, SyncOptions) (SyncStats, error)
	tailRepairJoinTimeout time.Duration
	tailRepairMu          sync.Mutex
}

type SyncOptions struct {
	Full           bool
	GuildIDs       []string
	ChannelIDs     []string
	Concurrency    int
	Since          time.Time
	Embeddings     bool
	SkipMembers    bool
	RequireMembers bool
	LatestOnly     bool
	RepairReason   string
}

func (s *Syncer) SetTailReadyCallback(fn func(context.Context) error) {
	s.tailReady = fn
}

type SyncStats struct {
	Guilds   int `json:"guilds"`
	Channels int `json:"channels"`
	Threads  int `json:"threads"`
	Members  int `json:"members"`
	Messages int `json:"messages"`
}

const (
	fullSyncBatchSize            = 25
	defaultMemberRefreshTimeout  = 5 * time.Minute
	defaultMemberRefreshInterval = 24 * time.Hour
	defaultMessageChannelTimeout = 5 * time.Minute
	defaultMessageSyncLogEvery   = 15 * time.Second
	defaultMessageSyncWaitEvery  = 30 * time.Second
)

func New(client Client, store *store.Store, logger *slog.Logger) *Syncer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Syncer{
		client:                client,
		store:                 store,
		logger:                logger,
		attachmentTextEnabled: true,
		memberRefreshTimeout:  defaultMemberRefreshTimeout,
		memberRefreshInterval: defaultMemberRefreshInterval,
		messageChannelTimeout: defaultMessageChannelTimeout,
		messageSyncLogEvery:   defaultMessageSyncLogEvery,
		messageSyncWaitEvery:  defaultMessageSyncWaitEvery,
	}
}

func (s *Syncer) SetAttachmentTextEnabled(enabled bool) {
	s.attachmentTextEnabled = enabled
}

func (s *Syncer) DiscoverGuilds(ctx context.Context) ([]*discordgo.UserGuild, error) {
	return s.client.Guilds(ctx)
}

func (s *Syncer) Sync(ctx context.Context, opts SyncOptions) (SyncStats, error) {
	guilds, err := s.client.Guilds(ctx)
	if err != nil {
		return SyncStats{}, fmt.Errorf("list guilds: %w", err)
	}
	if missing := missingGuildIDs(guilds, opts.GuildIDs); len(missing) > 0 {
		return SyncStats{}, fmt.Errorf("requested guilds not accessible: %s", strings.Join(missing, ", "))
	}
	targets := selectGuilds(guilds, opts.GuildIDs)
	stats := SyncStats{}
	for _, guild := range targets {
		one, err := s.syncGuild(ctx, guild.ID, opts)
		if err != nil {
			return stats, err
		}
		stats.Guilds++
		stats.Channels += one.Channels
		stats.Threads += one.Threads
		stats.Members += one.Members
		stats.Messages += one.Messages
	}
	if err := s.store.SetSyncState(ctx, "sync:last_success", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return stats, err
	}
	return stats, nil
}

func (s *Syncer) syncGuild(ctx context.Context, guildID string, opts SyncOptions) (SyncStats, error) {
	if err := s.syncGuildRecord(ctx, guildID); err != nil {
		return SyncStats{}, err
	}

	stats := SyncStats{}
	catalogMode := catalogModeForSync(opts)
	if shouldResumeIncompleteFullSync(opts) {
		batched, ok, err := s.syncGuildIncompleteBatches(ctx, guildID, opts)
		if err != nil {
			return stats, err
		}
		if ok {
			stats.add(batched)
			members, err := s.refreshGuildMembersForSync(ctx, guildID, false, opts)
			if err != nil {
				return stats, err
			}
			stats.Members = members
			return stats, nil
		}
		if s.shouldUseIncrementalFullCatalog(ctx, guildID) {
			catalogMode = channelCatalogIncremental
		}
	}
	channelList, targeted, err := s.channelList(ctx, guildID, opts.ChannelIDs, catalogMode)
	if err != nil {
		return stats, err
	}
	if err := s.storeChannelList(ctx, channelList, &stats); err != nil {
		return stats, err
	}

	members, err := s.refreshGuildMembersForSync(ctx, guildID, targeted, opts)
	if err != nil {
		return stats, err
	}
	stats.Members = members
	messageCount, err := s.syncMessageChannels(ctx, guildID, channelList, opts)
	if err != nil {
		return stats, err
	}
	stats.Messages += messageCount
	return stats, nil
}

func (s *Syncer) syncGuildRecord(ctx context.Context, guildID string) error {
	guild, err := s.client.Guild(ctx, guildID)
	if err != nil {
		return fmt.Errorf("fetch guild %s: %w", guildID, err)
	}
	return s.store.UpsertGuild(ctx, store.GuildRecord{
		ID:      guild.ID,
		Name:    guild.Name,
		Icon:    guild.Icon,
		RawJSON: marshalJSONString(guild, "{}"),
	})
}

func catalogModeForSync(opts SyncOptions) channelCatalogMode {
	if opts.LatestOnly && !opts.Full && len(opts.ChannelIDs) == 0 {
		return channelCatalogIncremental
	}
	return channelCatalogFull
}

func shouldResumeIncompleteFullSync(opts SyncOptions) bool {
	return opts.Full && len(opts.ChannelIDs) == 0
}

func (s *Syncer) storeChannelList(ctx context.Context, channels []*discordgo.Channel, stats *SyncStats) error {
	for _, channel := range channels {
		record := toChannelRecord(channel, marshalJSONString(channel, "{}"))
		if err := s.store.UpsertChannel(ctx, record); err != nil {
			return err
		}
		stats.addChannel(record)
	}
	return nil
}

func (s *Syncer) refreshGuildMembersForSync(ctx context.Context, guildID string, targeted bool, opts SyncOptions) (int, error) {
	if targeted {
		if opts.RequireMembers {
			return 0, errors.New("cannot require a member refresh for a targeted channel sync")
		}
		return 0, nil
	}
	if opts.SkipMembers {
		return 0, nil
	}
	members, err := s.refreshGuildMembers(ctx, guildID, opts.RequireMembers)
	if err != nil && opts.RequireMembers {
		return 0, err
	}
	return members, nil
}

func (s *Syncer) syncGuildIncompleteBatches(ctx context.Context, guildID string, opts SyncOptions) (SyncStats, bool, error) {
	if s.store == nil {
		return SyncStats{}, false, nil
	}
	incomplete, err := s.store.IncompleteMessageChannelIDs(ctx, guildID)
	if err != nil {
		return SyncStats{}, false, err
	}
	if len(incomplete) == 0 {
		return SyncStats{}, false, nil
	}
	stats := SyncStats{}
	for start := 0; start < len(incomplete); start += fullSyncBatchSize {
		end := min(start+fullSyncBatchSize, len(incomplete))
		batchOpts := opts
		batchOpts.ChannelIDs = incomplete[start:end]
		one, err := s.syncGuild(ctx, guildID, batchOpts)
		if err != nil {
			return stats, true, err
		}
		stats.add(one)
	}
	return stats, true, nil
}

func (stats *SyncStats) add(other SyncStats) {
	stats.Guilds += other.Guilds
	stats.Channels += other.Channels
	stats.Threads += other.Threads
	stats.Members += other.Members
	stats.Messages += other.Messages
}

func (stats *SyncStats) addChannel(record store.ChannelRecord) {
	stats.Channels++
	if strings.HasPrefix(record.Kind, "thread_") {
		stats.Threads++
	}
}

func (s *Syncer) refreshGuildMembers(ctx context.Context, guildID string, force bool) (int, error) {
	if !force && !s.shouldRefreshMembers(ctx, guildID) {
		return 0, nil
	}
	memberCtx := ctx
	cancel := func() {}
	if s.memberRefreshTimeout > 0 {
		if _, ok := ctx.Deadline(); !ok {
			memberCtx, cancel = context.WithTimeout(ctx, s.memberRefreshTimeout)
		}
	}
	defer cancel()
	startedAt := time.Now()
	s.logger.Info(
		"member sync started",
		"guild_id", guildID,
		"timeout", timeoutLabel(s.memberRefreshTimeout),
	)
	members, err := s.client.GuildMembers(memberCtx, guildID)
	if err != nil {
		s.logger.Warn(
			"member crawl failed",
			"guild_id", guildID,
			"err", err,
			"elapsed", time.Since(startedAt).Round(time.Second).String(),
			"timed_out", errors.Is(err, context.DeadlineExceeded),
		)
		return 0, fmt.Errorf("crawl guild members: %w", err)
	}
	converted := make([]store.MemberRecord, 0, len(members))
	for _, member := range members {
		converted = append(converted, toMemberRecord(guildID, member))
	}
	if err := s.store.MergeMembers(ctx, guildID, converted); err != nil {
		s.logger.Warn("member merge failed", "guild_id", guildID, "err", err)
		return 0, fmt.Errorf("merge guild members: %w", err)
	}
	if s.store != nil {
		if err := s.store.SetSyncState(ctx, guildMemberSyncSuccessScope(guildID), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			s.logger.Warn("member sync state update failed", "guild_id", guildID, "err", err)
			return 0, fmt.Errorf("record guild member sync: %w", err)
		}
	}
	s.logger.Info(
		"member sync completed",
		"guild_id", guildID,
		"members", len(converted),
		"elapsed", time.Since(startedAt).Round(time.Second).String(),
	)
	return len(converted), nil
}

func (s *Syncer) shouldUseIncrementalFullCatalog(ctx context.Context, guildID string) bool {
	if s == nil || s.store == nil || guildID == "" {
		return false
	}
	count, err := s.store.GuildChannelCount(ctx, guildID)
	if err != nil {
		s.logger.Warn("channel count lookup failed", "guild_id", guildID, "err", err)
		return false
	}
	return count > 0
}

func (s *Syncer) shouldRefreshMembers(ctx context.Context, guildID string) bool {
	if s == nil || s.store == nil || guildID == "" {
		return true
	}
	scope := guildMemberSyncSuccessScope(guildID)
	lastSuccess, err := s.store.GetSyncState(ctx, scope)
	if err != nil {
		s.logger.Warn("member sync state lookup failed", "guild_id", guildID, "err", err)
		return true
	}
	if lastSuccess == "" {
		count, err := s.store.GuildMemberCount(ctx, guildID)
		if err != nil {
			s.logger.Warn("member count lookup failed", "guild_id", guildID, "err", err)
			return true
		}
		if count > 0 {
			now := time.Now().UTC().Format(time.RFC3339Nano)
			if err := s.store.SetSyncState(ctx, scope, now); err != nil {
				s.logger.Warn("member sync state seed failed", "guild_id", guildID, "err", err)
				return true
			}
			s.logger.Info(
				"member sync skipped",
				"guild_id", guildID,
				"reason", "reused_existing_snapshot",
				"members", count,
			)
			return false
		}
		return true
	}
	if s.memberRefreshInterval <= 0 {
		return true
	}
	lastAt, err := time.Parse(time.RFC3339Nano, lastSuccess)
	if err != nil {
		return true
	}
	age := time.Since(lastAt)
	if age < s.memberRefreshInterval {
		s.logger.Info(
			"member sync skipped",
			"guild_id", guildID,
			"reason", "fresh_snapshot",
			"age", age.Round(time.Second).String(),
		)
		return false
	}
	return true
}

func guildMemberSyncSuccessScope(guildID string) string {
	return "guild:" + guildID + ":members:last_success"
}

func timeoutLabel(d time.Duration) string {
	if d <= 0 {
		return "none"
	}
	return d.String()
}
