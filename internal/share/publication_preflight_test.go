package share

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPublicationPreflightRequiresSelectedPermissionEvidence(t *testing.T) {
	const public = `{"permission_overwrites":[]}`
	const denied = `{"permission_overwrites":[{"id":"guild","type":0,"deny":"1024"}]}`
	tests := []struct {
		name          string
		raw           string
		parentRaw     string
		parentKind    string
		missingParent bool
		thread        bool
		privateThread bool
		ready         bool
		allowed       bool
		includeParent bool
		excludeBad    bool
		emptyScope    bool
		noMessages    bool
	}{
		{name: "public", raw: public, ready: true, allowed: true},
		{name: "denied", raw: denied, ready: true},
		{name: "missing channel", raw: `{}`},
		{name: "null channel", raw: `{"permission_overwrites":null}`},
		{name: "malformed channel", raw: `{"permission_overwrites":"bad"}`},
		{name: "missing category evidence", raw: public, parentRaw: `{}`, parentKind: "category"},
		{name: "null category evidence", raw: public, parentRaw: `{"permission_overwrites":null}`, parentKind: "category"},
		{name: "malformed category evidence", raw: public, parentRaw: `{"permission_overwrites":"bad"}`, parentKind: "category"},
		{name: "absent category", raw: public, missingParent: true, allowed: true},
		{name: "denied category", raw: public, parentRaw: denied, parentKind: "category", ready: true},
		{name: "public thread", raw: `{}`, parentRaw: public, parentKind: "forum", thread: true, ready: true, allowed: true, includeParent: true},
		{name: "missing thread parent evidence", raw: `{}`, parentRaw: `{}`, parentKind: "forum", thread: true},
		{name: "absent thread parent", raw: `{}`, missingParent: true, thread: true},
		{name: "private thread", raw: `{}`, missingParent: true, privateThread: true, ready: true},
		{name: "excluded incomplete channel", raw: public, ready: true, allowed: true, excludeBad: true},
		{name: "empty selected scope", raw: `{}`, ready: true, emptyScope: true},
		{name: "empty channel missing evidence", raw: `{}`, noMessages: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			defer func() { _ = s.Close() }()
			require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{
				ID: "guild", Name: "Fixture",
				RawJSON: `{"roles":[{"id":"guild","permissions":"1024"}]}`,
			}))
			channel := store.ChannelRecord{ID: "selected", GuildID: "guild", Kind: "text", RawJSON: test.raw}
			if test.parentKind != "" || test.missingParent {
				channel.ParentID = "parent"
			}
			if test.thread || test.privateThread {
				channel.Kind = "thread_public"
				if test.privateThread {
					channel.Kind = "thread_private"
				}
			}
			upsertSnapshotFilterChannel(t, ctx, s, channel)
			if test.parentKind != "" {
				upsertSnapshotFilterChannel(t, ctx, s, store.ChannelRecord{
					ID: "parent", GuildID: "guild", Kind: test.parentKind, RawJSON: test.parentRaw,
				})
			}
			// Incomplete evidence outside the selected scope must not block it.
			upsertSnapshotFilterChannel(t, ctx, s, store.ChannelRecord{
				ID: "bad", GuildID: "guild", Kind: "text", RawJSON: `{}`,
			})
			if !test.noMessages {
				upsertSnapshotFilterMessage(t, ctx, s, "message", "selected", "author", "fixture")
			}
			opts := FilterOptions{PublicOnly: true, IncludeChannelIDs: []string{"selected"}}
			if test.includeParent {
				opts.IncludeChannelIDs = []string{"parent"}
			}
			if test.excludeBad {
				opts.IncludeChannelIDs = nil
				opts.ExcludeChannelIDs = []string{"bad"}
			}
			if test.emptyScope {
				opts.IncludeChannelIDs = []string{"absent"}
			}
			report, err := PreflightPublishScope(ctx, s, opts)
			require.NoError(t, err)
			require.Equal(t, test.ready, report.Ready)
			require.Len(t, report.Guilds, 1)
			require.Equal(t, "discord_metadata", report.Guilds[0].SourceHint)
			if test.allowed {
				require.Equal(t, 1, report.Messages.Allowed)
			} else {
				require.Zero(t, report.Messages.Allowed)
			}
			if !test.ready {
				require.False(t, report.Guilds[0].MetadataReady)
				require.NotEmpty(t, report.Warnings)
				require.Equal(t, "discrawl sync --source discord", report.RepairCommand)
				if test.noMessages {
					require.Equal(t, "no_matching_messages", report.EmptyReason)
				} else if !test.allowed {
					require.Equal(t, "metadata_incomplete", report.EmptyReason)
				}
			} else {
				require.Empty(t, report.Warnings)
				require.Empty(t, report.RepairCommand)
			}
			filter, err := newSnapshotFilter(ctx, s.DB(), opts)
			require.NoError(t, err)
			require.Equal(t, test.allowed, filter.allowChannelID("selected"), "export selection must not change")
		})
	}
}

func TestPublicationPreflightRequiresOrphanChannelEvidence(t *testing.T) {
	tests := []struct {
		name        string
		opts        FilterOptions
		orphan      bool
		channel     bool
		denied      bool
		ready       bool
		channels    PublishScopeCount
		messages    PublishScopeCount
		emptyReason string
	}{
		{
			name: "orphan only", opts: FilterOptions{PublicOnly: true}, orphan: true,
			messages: PublishScopeCount{Candidate: 1, Excluded: 1}, emptyReason: "metadata_incomplete",
		},
		{
			name: "public and orphan", opts: FilterOptions{PublicOnly: true}, orphan: true, channel: true,
			channels: PublishScopeCount{Candidate: 1, Allowed: 1},
			messages: PublishScopeCount{Candidate: 2, Allowed: 1, Excluded: 1},
		},
		{
			name: "known denied", opts: FilterOptions{PublicOnly: true}, channel: true, denied: true, ready: true,
			channels: PublishScopeCount{Candidate: 1, Excluded: 1},
			messages: PublishScopeCount{Candidate: 1, Excluded: 1}, emptyReason: "filters_match_no_publishable_messages",
		},
		{
			name: "include public only", opts: FilterOptions{PublicOnly: true, IncludeChannelIDs: []string{"known"}},
			orphan: true, channel: true, ready: true,
			channels: PublishScopeCount{Candidate: 1, Allowed: 1},
			messages: PublishScopeCount{Candidate: 1, Allowed: 1},
		},
		{
			name: "exclude orphan", opts: FilterOptions{PublicOnly: true, ExcludeChannelIDs: []string{"orphan"}},
			orphan: true, channel: true, ready: true,
			channels: PublishScopeCount{Candidate: 1, Allowed: 1},
			messages: PublishScopeCount{Candidate: 1, Allowed: 1},
		},
		{
			name: "include absent channel retains empty scope",
			opts: FilterOptions{PublicOnly: true, IncludeChannelIDs: []string{"orphan"}}, orphan: true, ready: true,
			emptyReason: "no_matching_messages",
		},
		{
			name: "non public selection", orphan: true, ready: true,
			messages: PublishScopeCount{Candidate: 1, Allowed: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			defer func() { _ = s.Close() }()
			require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{
				ID: "g1", Name: "Fixture",
				RawJSON: `{"roles":[{"id":"g1","permissions":"1024"}]}`,
			}))
			if test.channel {
				raw := `{"permission_overwrites":[]}`
				if test.denied {
					raw = `{"permission_overwrites":[{"id":"g1","type":0,"deny":"1024"}]}`
				}
				upsertSnapshotFilterChannel(t, ctx, s, store.ChannelRecord{
					ID: "known", GuildID: "g1", Kind: "text", RawJSON: raw,
				})
				upsertSnapshotFilterMessage(t, ctx, s, "known-message", "known", "author", "fixture")
			}
			if test.orphan {
				upsertSnapshotFilterMessage(t, ctx, s, "orphan-message", "orphan", "author", "fixture")
				var guildID string
				require.NoError(t, s.DB().QueryRowContext(ctx, `select guild_id from messages where id = 'orphan-message'`).Scan(&guildID))
				require.Equal(t, "g1", guildID)
			}
			var channelRows int
			require.NoError(t, s.DB().QueryRowContext(ctx, `select count(*) from channels where id = 'orphan'`).Scan(&channelRows))
			require.Zero(t, channelRows)

			report, err := PreflightPublishScope(ctx, s, test.opts)
			require.NoError(t, err)
			require.Equal(t, test.ready, report.Ready)
			require.Equal(t, test.channels, report.Channels)
			require.Equal(t, test.messages, report.Messages)
			require.Equal(t, test.messages.Allowed == 0, report.Empty)
			require.Equal(t, test.emptyReason, report.EmptyReason)
			require.Len(t, report.Guilds, 1)
			require.Equal(t, "g1", report.Guilds[0].GuildID)
			require.Equal(t, test.ready, report.Guilds[0].MetadataReady)
			require.Equal(t, "discord_metadata", report.Guilds[0].SourceHint)
			if test.ready {
				require.Empty(t, report.Warnings)
				require.Empty(t, report.RepairCommand)
			} else {
				require.Len(t, report.Warnings, 1)
				require.Equal(t, "discrawl sync --source discord", report.RepairCommand)
			}
			filter, err := newSnapshotFilter(ctx, s.DB(), test.opts)
			require.NoError(t, err)
			require.Equal(t, !test.opts.PublicOnly, filter.allowChannelID("orphan"), "export selection must not change")
			if test.channel {
				require.Equal(t, !test.denied, filter.allowChannelID("known"))
			}
		})
	}
}
