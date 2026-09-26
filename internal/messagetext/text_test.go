package messagetext

import (
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

func TestRichTextPreservesProvenanceAndWhitespaceBoundaries(t *testing.T) {
	t.Parallel()
	m := &discordgo.Message{ID: "m", Content: "alpha\nbeta\tgamma", Embeds: []*discordgo.MessageEmbed{{Author: &discordgo.MessageEmbedAuthor{Name: "Embed author"}, Title: "Title", Description: "Description", Fields: []*discordgo.MessageEmbedField{{Name: "Field", Value: "one\r\ntwo"}}, Footer: &discordgo.MessageEmbedFooter{Text: "Footer"}}}, ReferencedMessage: &discordgo.Message{ID: "parent", Content: "Someone else's words"}}
	p := Parts(m, []Attachment{{ID: "a", Filename: "trace.txt", Text: "retained\ntext"}})
	require.Equal(t, "alpha\nbeta\tgamma", p[0].Text, "provenance text remains verbatim")
	for _, want := range []string{"alpha beta gamma", "Embed author", "Field", "one two", "Footer", "retained text", "reply:Someone else's words"} {
		require.Contains(t, Normalize(p), want)
	}
	var quoted Part
	for _, part := range p {
		if part.Kind == "reply_context" {
			quoted = part
		}
		if part.AttachmentID != "" {
			require.Equal(t, "a", part.AttachmentID)
		}
	}
	require.Equal(t, "parent", quoted.MessageID)
	require.Equal(t, "/referenced_message/content", quoted.Path)
	require.Equal(t, "Foo.txt", Sanitize("Ｆｏｏ\u200d.txt"))
	require.Equal(t, "linebreak", Sanitize("line\x00break"))
	require.Empty(t, Normalize(Parts(nil, nil)))
}

func TestForwardContextIsNeverAuthoredAndPollRemainsAttributed(t *testing.T) {
	t.Parallel()
	m := &discordgo.Message{MessageReference: &discordgo.MessageReference{Type: discordgo.MessageReferenceTypeForward}, ReferencedMessage: &discordgo.Message{ID: "original", Content: "forwarded words"}, Poll: &discordgo.Poll{Question: discordgo.PollMedia{Text: "question"}, Answers: []discordgo.PollAnswer{{Media: &discordgo.PollMedia{Text: "answer"}}}}}
	p := Parts(m, nil)
	require.Equal(t, "forward_context", p[0].Kind)
	require.Equal(t, "original", p[0].MessageID)
	require.Contains(t, Normalize(p), "forward:forwarded words")
	for _, part := range p {
		require.NotEqual(t, "authored", part.Kind)
	}
	require.Equal(t, "poll_question", p[1].Kind)
	require.Equal(t, "poll_answer", p[2].Kind)
}
