package messagetext

import (
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

func TestComponentsV2TextKeepsAttributedPaths(t *testing.T) {
	t.Parallel()
	thumb, media := "thumbnail alt", "gallery alt"
	m := &discordgo.Message{ID: "m", Flags: discordgo.MessageFlagsIsComponentsV2, Components: []discordgo.MessageComponent{
		&discordgo.Container{Components: []discordgo.MessageComponent{
			&discordgo.TextDisplay{Content: "container text"},
			&discordgo.Section{
				Components: []discordgo.MessageComponent{&discordgo.TextDisplay{Content: "section text"}},
				Accessory:  &discordgo.Thumbnail{Description: &thumb},
			},
			&discordgo.MediaGallery{Items: []discordgo.MediaGalleryItem{{Description: &media}, {}}},
		}},
		&discordgo.ActionsRow{Components: []discordgo.MessageComponent{&discordgo.Button{Label: "Open", URL: "https://example.test"}}},
		(*discordgo.TextDisplay)(nil),
	}}
	got := map[string]Part{}
	for _, p := range Parts(m, nil) {
		got[p.Path] = p
	}
	require.Equal(t, Part{Kind: "component_text", Path: "/components/0/components/0/content", Text: "container text"}, got["/components/0/components/0/content"])
	require.Equal(t, "section text", got["/components/0/components/1/components/0/content"].Text)
	require.Equal(t, "component_description", got["/components/0/components/1/accessory/description"].Kind)
	require.Equal(t, media, got["/components/0/components/2/items/0/description"].Text)
	require.Equal(t, "Open", got["/components/1/components/0/label"].Text)
	require.Equal(t, "component_url", got["/components/1/components/0/url"].Kind)
	require.NotContains(t, got, "/components/2/content", "nil components contribute nothing")
}

func TestComponentsIgnoredWithoutComponentsV2Flag(t *testing.T) {
	t.Parallel()
	m := &discordgo.Message{ID: "m", Components: []discordgo.MessageComponent{&discordgo.TextDisplay{Content: "legacy"}}}
	require.Empty(t, Parts(m, nil))
}
