package syncer

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
