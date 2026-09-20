package syncer

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/discrawl/internal/share"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestTargetedSyncPreservesPublicationPermissions(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "cached", true: "mixed"}[mixed], func(t *testing.T) {
			ctx := context.Background()
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			defer func() { _ = s.Close() }()
			privateRaw := `{"id":"private","permission_overwrites":[{"id":"guild","type":0,"allow":"0","deny":"1024"}],"retained":"observation"}`
			require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
				ID: "private", GuildID: "guild", Kind: "text", Name: "private", RawJSON: privateRaw,
			}))
			require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
				ID: "unknown", GuildID: "guild", Kind: "text", Name: "unknown", RawJSON: `{}`,
			}))
			client := &fakeClient{
				guilds: []*discordgo.UserGuild{{ID: "guild", Name: "Guild"}},
				guildByID: map[string]*discordgo.Guild{
					"guild": {ID: "guild", Name: "Guild", Roles: []*discordgo.Role{{ID: "guild", Permissions: 1024}}},
				},
				channels: map[string][]*discordgo.Channel{"guild": {{
					ID: "public", GuildID: "guild", Name: "public", Type: discordgo.ChannelTypeGuildText,
					PermissionOverwrites: []*discordgo.PermissionOverwrite{},
				}}},
			}
			selected := []string{"private", "unknown"}
			if mixed {
				selected = append(selected, "public")
			} else {
				require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
					ID: "public", GuildID: "guild", Kind: "text", Name: "public",
					RawJSON: `{"permission_overwrites":[]}`,
				}))
				selected = append(selected, "public")
			}
			for _, channel := range selected {
				require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
					ID: channel, GuildID: "guild", ChannelID: channel, AuthorID: "author",
					Content: channel + " marker", NormalizedContent: channel + " marker",
					CreatedAt: "2026-01-01T00:00:00Z", RawJSON: `{}`,
				}))
			}
			_, err = New(client, s, nil).Sync(ctx, SyncOptions{GuildIDs: []string{"guild"}, ChannelIDs: selected})
			require.NoError(t, err)
			var raw string
			require.NoError(t, s.DB().QueryRowContext(ctx, `select raw_json from channels where id = 'private'`).Scan(&raw))
			require.Equal(t, privateRaw, raw)
			require.NoError(t, s.DB().QueryRowContext(ctx, `select raw_json from channels where id = 'unknown'`).Scan(&raw))
			require.Equal(t, `{}`, raw)
			if !mixed {
				require.Zero(t, client.guildChanCalls)
			}
			repo := filepath.Join(t.TempDir(), "snapshot")
			manifest, err := share.Export(ctx, s, share.Options{RepoPath: repo, Filter: share.FilterOptions{PublicOnly: true}})
			require.NoError(t, err)
			assertPublicationMarkers(t, repo, manifest, []string{"private marker", "unknown marker"}, "public marker")
		})
	}
}

func TestTargetedSyncHydratesDesktopCatalog(t *testing.T) {
	for _, tc := range []struct {
		name        string
		private     bool
		excluded    bool
		liveCatalog bool
	}{
		{name: "public"},
		{name: "private-category", private: true},
		{name: "excluded-category", excluded: true},
		{name: "live-ancestor-catalog", liveCatalog: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			defer func() { _ = s.Close() }()
			for _, channel := range []store.ChannelRecord{
				{ID: "unknown-type", GuildID: "guild", Kind: "text", RawJSON: `{"source":"discord_desktop","id":"unknown-type","guild_id":"guild"}`},
				{ID: "known-thread", GuildID: "guild", Kind: "thread_public", RawJSON: `{"source":"discord_desktop","id":"known-thread","guild_id":"guild","type":11}`},
			} {
				require.NoError(t, s.UpsertChannel(ctx, channel))
				require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
					ID: channel.ID, GuildID: "guild", ChannelID: channel.ID, AuthorID: "author",
					Content: "retained history", NormalizedContent: "retained history",
					CreatedAt: "2026-01-01T00:00:00Z", RawJSON: `{}`,
				}))
			}
			retained := map[string]string{
				"channel:unknown-type:latest_message_id": "20", "channel:unknown-type:backfill_before": "10", "channel:unknown-type:history_complete": "1",
				"channel:known-thread:latest_message_id": "20", "channel:known-thread:backfill_before": "10", "channel:known-thread:history_complete": "1",
				"channel:forum:archived_public_threads_after":  "2026-01-01T00:00:00Z",
				"channel:forum:archived_private_threads_after": "2026-01-01T00:00:00Z",
			}
			for scope, cursor := range retained {
				require.NoError(t, s.SetSyncState(ctx, scope, cursor))
			}
			_, before, err := s.ReadOnlyQuery(ctx, "select id, channel_id, content, raw_json from messages order by id")
			require.NoError(t, err)
			channelsBefore, err := s.Channels(ctx, "guild")
			require.NoError(t, err)
			category := &discordgo.Channel{ID: "category", GuildID: "guild", Type: discordgo.ChannelTypeGuildCategory, PermissionOverwrites: []*discordgo.PermissionOverwrite{}}
			if tc.private {
				category.PermissionOverwrites = []*discordgo.PermissionOverwrite{{ID: "guild", Type: discordgo.PermissionOverwriteTypeRole, Deny: 1024}}
			}
			client := &fakeClient{
				guilds: []*discordgo.UserGuild{{ID: "guild"}},
				guildByID: map[string]*discordgo.Guild{
					"guild": {ID: "guild", Roles: []*discordgo.Role{{ID: "guild", Permissions: 1024}}},
				},
				channelByID: map[string]*discordgo.Channel{
					"unknown-type": {ID: "unknown-type", GuildID: "guild", ParentID: "forum", Type: discordgo.ChannelTypeGuildPublicThread},
					"known-thread": {ID: "known-thread", GuildID: "guild", ParentID: "forum", Type: discordgo.ChannelTypeGuildPublicThread},
					"forum":        {ID: "forum", GuildID: "guild", ParentID: "category", Type: discordgo.ChannelTypeGuildForum, PermissionOverwrites: []*discordgo.PermissionOverwrite{}},
					"category":     category,
				},
			}
			if tc.liveCatalog {
				client.channels = map[string][]*discordgo.Channel{"guild": {client.channelByID["forum"], category}}
			}
			opts := SyncOptions{
				GuildIDs: []string{"guild"}, ChannelIDs: []string{"unknown-type", "known-thread"},
				Full: true, Since: time.Now().UTC(), SkipMembers: true,
			}
			if tc.excluded {
				opts.ExcludeChannelIDs = []string{"category"}
			}
			stats, err := New(client, s, nil).Sync(ctx, opts)
			require.NoError(t, err)
			require.Zero(t, stats.Messages)
			rows, err := s.Channels(ctx, "guild")
			require.NoError(t, err)
			if tc.excluded {
				require.Equal(t, channelsBefore, rows)
				require.Empty(t, client.messageCalls)
			} else {
				scope, err := share.PreflightPublishScope(ctx, s, share.FilterOptions{PublicOnly: true})
				require.NoError(t, err)
				require.True(t, scope.Ready, "hydrated catalog must contain usable permission and parent evidence")
				if tc.private {
					require.Zero(t, scope.Messages.Allowed)
				} else {
					require.Equal(t, 2, scope.Messages.Allowed)
				}
				require.Len(t, rows, 4)
				for _, row := range rows {
					if row.ID == "unknown-type" || row.ID == "known-thread" {
						require.Equal(t, "thread_public", row.Kind)
						require.Equal(t, "forum", row.ParentID)
					}
				}
			}
			wantCalls := map[string]int{"unknown-type": 1, "known-thread": 1}
			if !tc.liveCatalog {
				wantCalls["forum"], wantCalls["category"] = 1, 1
			}
			require.Equal(t, wantCalls, client.channelCalls)
			require.NotContains(t, client.messageCalls, "forum")
			require.NotContains(t, client.messageCalls, "category")
			require.Zero(t, client.threadCalls)
			require.Zero(t, client.memberCalls)
			_, after, err := s.ReadOnlyQuery(ctx, "select id, channel_id, content, raw_json from messages order by id")
			require.NoError(t, err)
			require.Equal(t, before, after)
			for key, want := range retained {
				got, err := s.GetSyncState(ctx, key)
				require.NoError(t, err)
				require.Equal(t, want, got, key)
			}
		})
	}
}

func TestTargetedSyncRejectsUnavailableDesktopMetadata(t *testing.T) {
	for _, failure := range []string{"missing target", "missing parent", "parentless thread", "wrong target", "wrong guild", "missing permissions", "parent cycle"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			require.NoError(t, err)
			defer func() { _ = s.Close() }()
			require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
				ID: "target", GuildID: "guild", Kind: "thread_public", RawJSON: `{"source":"discord_desktop","id":"target","guild_id":"guild","type":11}`,
			}))
			before, err := s.Channels(ctx, "guild")
			require.NoError(t, err)
			client := &fakeClient{
				guilds:    []*discordgo.UserGuild{{ID: "guild"}},
				guildByID: map[string]*discordgo.Guild{"guild": {ID: "guild"}},
				channelByID: map[string]*discordgo.Channel{
					"target": {ID: "target", GuildID: "guild", ParentID: "parent", Type: discordgo.ChannelTypeGuildPublicThread},
					"parent": {ID: "parent", GuildID: "guild", Type: discordgo.ChannelTypeGuildForum, PermissionOverwrites: []*discordgo.PermissionOverwrite{}},
				},
			}
			switch failure {
			case "missing target":
				delete(client.channelByID, "target")
			case "missing parent":
				delete(client.channelByID, "parent")
			case "parentless thread":
				client.channelByID["target"].ParentID = ""
			case "wrong target":
				client.channelByID["target"].ID = "other"
			case "wrong guild":
				client.channelByID["parent"].GuildID = "other"
			case "missing permissions":
				client.channelByID["parent"].PermissionOverwrites = nil
			case "parent cycle":
				client.channelByID["parent"].ParentID = "target"
			}
			_, err = New(client, s, nil).Sync(ctx, SyncOptions{GuildIDs: []string{"guild"}, ChannelIDs: []string{"target"}})
			require.Error(t, err)
			require.Contains(t, err.Error(), "metadata")
			after, err := s.Channels(ctx, "guild")
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Empty(t, client.messageCalls)
		})
	}
}

func TestGatewayGuildFilteredPublicationProjectsNestedMetadata(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	handler := &tailHandler{store: s}
	require.NoError(t, handler.OnGuildUpsert(ctx, &discordgo.Guild{
		ID: "guild", Name: "Guild",
		Roles:    []*discordgo.Role{{ID: "guild", Permissions: 1024}, {ID: "hidden-role", Name: "excluded role marker"}},
		Channels: []*discordgo.Channel{{ID: "hidden", Name: "excluded channel marker", Topic: "excluded topic marker"}},
		Members:  []*discordgo.Member{{User: &discordgo.User{ID: "hidden-user", Username: "excluded member marker"}}},
	}))
	require.NoError(t, handler.OnChannelUpsert(ctx, &discordgo.Channel{
		ID: "public", GuildID: "guild", Name: "public", Type: discordgo.ChannelTypeGuildText,
		PermissionOverwrites: []*discordgo.PermissionOverwrite{},
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID: "1", GuildID: "guild", ChannelID: "public", AuthorID: "author",
		Content: "allowed marker", NormalizedContent: "allowed marker",
		CreatedAt: "2026-01-01T00:00:00Z", RawJSON: `{}`,
	}))
	var original string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select raw_json from guilds where id = 'guild'`).Scan(&original))
	require.Contains(t, original, "excluded member marker")
	repo := filepath.Join(t.TempDir(), "snapshot")
	manifest, err := share.Export(ctx, s, share.Options{RepoPath: repo, Filter: share.FilterOptions{PublicOnly: true}})
	require.NoError(t, err)
	assertPublicationMarkers(t, repo, manifest, []string{
		"excluded channel marker", "excluded topic marker", "excluded member marker", "excluded role marker",
	}, "allowed marker")
	var retained string
	require.NoError(t, s.DB().QueryRowContext(ctx, `select raw_json from guilds where id = 'guild'`).Scan(&retained))
	require.Equal(t, original, retained)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "import.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, err = share.Import(ctx, dst, share.Options{RepoPath: repo})
	require.NoError(t, err)
	scope, err := share.PreflightPublishScope(ctx, dst, share.FilterOptions{PublicOnly: true})
	require.NoError(t, err)
	require.Equal(t, 1, scope.Messages.Allowed)
	require.True(t, scope.Ready)
}

func assertPublicationMarkers(t *testing.T, repo string, manifest share.Manifest, excluded []string, included string) {
	t.Helper()
	found := false
	for _, table := range manifest.Tables {
		for _, file := range table.Files {
			input, err := os.Open(filepath.Join(repo, file))
			require.NoError(t, err)
			reader, err := gzip.NewReader(input)
			require.NoError(t, err)
			body, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			require.NoError(t, input.Close())
			for _, marker := range excluded {
				require.NotContains(t, string(body), marker)
			}
			if strings.Contains(string(body), included) {
				found = true
			}
		}
	}
	require.True(t, found, "allowed fixture missing")
}
