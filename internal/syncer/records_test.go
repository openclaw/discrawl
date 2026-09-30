package syncer

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

// componentsV2CardPayload is a sanitized GitHub PR card sent with the
// IS_COMPONENTS_V2 flag: no content or embeds, only a component tree.
const componentsV2CardPayload = `[{
	"id": "1554528840938819725",
	"channel_id": "c1",
	"guild_id": "g1",
	"type": 20,
	"flags": 32768,
	"content": "",
	"embeds": [],
	"attachments": [],
	"mentions": [],
	"timestamp": "2026-09-28T10:15:00.000000+00:00",
	"author": {"id": "u-app", "username": "Hermit", "bot": true},
	"components": [{
		"type": 17,
		"id": 1,
		"accent_color": 3066993,
		"components": [
			{"type": 10, "id": 2, "content": "### [OPEN] PR #160926 fix(secrets): redact gateway tokens in doctor output"},
			{"type": 10, "id": 3, "content": "size: XL • P0 • needs proof"},
			{"type": 14, "id": 4, "divider": true, "spacing": 1},
			{"type": 9, "id": 5,
				"components": [{"type": 10, "id": 6, "content": "opened by contributor • +412 −37"}],
				"accessory": {"type": 11, "id": 7, "media": {"url": "https://avatars.example/u/1.png"}, "description": "contributor avatar"}},
			{"type": 12, "id": 8, "items": [{"media": {"url": "https://cdn.example/diffstat.png"}, "description": "diffstat chart"}]},
			{"type": 1, "id": 9, "components": [
				{"type": 2, "id": 10, "style": 5, "label": "View on GitHub", "url": "https://github.com/openclaw/openclaw/pull/160926"}
			]}
		]
	}]
}]`

func TestComponentsV2MessageKeepsCardTextSearchable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	var page []*discordgo.Message
	require.NoError(t, json.Unmarshal([]byte(componentsV2CardPayload), &page))
	channel := &discordgo.Channel{ID: "c1", GuildID: "g1", Name: "github", Type: discordgo.ChannelTypeGuildText}
	svc := New(&fakeClient{messages: map[string][]*discordgo.Message{"c1": page}}, s, nil)
	count, err := svc.syncMessageChannelsSerial(ctx, "g1", []*discordgo.Channel{channel}, SyncOptions{Full: true}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	var content, normalized, raw string
	require.NoError(t, s.DB().QueryRowContext(ctx,
		`select content, normalized_content, raw_json from messages where id = '1554528840938819725'`,
	).Scan(&content, &normalized, &raw))
	require.Empty(t, content, "content stays as Discord sent it")
	require.Equal(t, "### [OPEN] PR #160926 fix(secrets): redact gateway tokens in doctor output\n"+
		"size: XL • P0 • needs proof\n"+
		"opened by contributor • +412 −37\n"+
		"contributor avatar\n"+
		"diffstat chart\n"+
		"View on GitHub\n"+
		"https://github.com/openclaw/openclaw/pull/160926", normalized)

	var stored struct {
		Components []json.RawMessage `json:"components"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &stored))
	require.Len(t, stored.Components, 1)
	var reparsed discordgo.Message
	require.NoError(t, json.Unmarshal([]byte(raw), &reparsed), "raw_json components must decode as Discord components")
	require.Equal(t, normalized, normalizeMessage(&reparsed), "raw_json keeps every text-bearing component")

	for _, query := range []string{"160926", "redact gateway tokens", "needs proof"} {
		results, err := s.SearchMessages(ctx, store.SearchOptions{Query: query, Limit: 10})
		require.NoError(t, err)
		require.Len(t, results, 1, query)
		require.Equal(t, "1554528840938819725", results[0].MessageID)
	}

	classic := *page[0]
	classic.Flags = 0
	require.Empty(t, normalizeMessage(&classic), "classic message components are not message bodies")
}

func TestToMemberRecordSkipsNilUser(t *testing.T) {
	t.Parallel()

	var rec store.MemberRecord
	require.NotPanics(t, func() {
		rec = toMemberRecord("g1", &discordgo.Member{User: nil})
	})
	require.Empty(t, rec.UserID)

	require.NotPanics(t, func() {
		rec = toMemberRecord("g1", nil)
	})
	require.Empty(t, rec.UserID)

	rec = toMemberRecord("g1", &discordgo.Member{
		User: &discordgo.User{ID: "u1", Username: "peter", GlobalName: "Peter"},
		Nick: "Pete",
	})
	require.Equal(t, "g1", rec.GuildID)
	require.Equal(t, "u1", rec.UserID)
	require.Equal(t, "peter", rec.Username)
	require.Equal(t, "Peter", rec.GlobalName)
	require.Equal(t, "Pete", rec.DisplayName)
}

func TestRefreshGuildMembersSkipsNilUser(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	client := &fakeClient{
		members: map[string][]*discordgo.Member{
			"g1": {
				{GuildID: "g1", User: &discordgo.User{ID: "u1", Username: "peter"}},
				{GuildID: "g1", User: nil},
				nil,
			},
		},
	}
	svc := New(client, s, nil)

	var count int
	require.NotPanics(t, func() {
		count, err = svc.refreshGuildMembers(ctx, "g1", true)
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)

	status, err := s.Status(ctx, "db", "")
	require.NoError(t, err)
	require.Equal(t, 1, status.MemberCount)

	rows, err := s.Members(ctx, "g1", "", 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "u1", rows[0].UserID)
}
