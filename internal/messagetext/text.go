// Package messagetext derives search text while retaining source attribution.
package messagetext

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/bwmarrin/discordgo"
	"golang.org/x/text/unicode/norm"
)

const Version = 2

type Part struct {
	Kind         string `json:"kind"`
	Path         string `json:"path"`
	Text         string `json:"text"`
	AttachmentID string `json:"attachment_id,omitempty"`
	MessageID    string `json:"message_id,omitempty"`
}

type Attachment struct{ ID, Filename, Text string }

// Parts preserve provider text verbatim. Normalization is a separate operation;
// callers must not present reply_context as text authored by this message.
func Parts(m *discordgo.Message, attachments []Attachment) []Part {
	parts := []Part{}
	add := func(kind, path, text string) {
		if text != "" {
			parts = append(parts, Part{Kind: kind, Path: path, Text: text})
		}
	}
	if m == nil {
		return parts
	}
	add("authored", "/content", m.Content)
	if attachments == nil {
		for _, a := range m.Attachments {
			if a != nil {
				attachments = append(attachments, Attachment{ID: a.ID, Filename: a.Filename})
			}
		}
	}
	for i, a := range attachments {
		for _, v := range []struct{ kind, path, text string }{{"attachment_filename", fmt.Sprintf("/attachments/%d/filename", i), a.Filename}, {"attachment_text", "message_attachments/text_content", a.Text}} {
			if v.text != "" {
				parts = append(parts, Part{Kind: v.kind, Path: v.path, Text: v.text, AttachmentID: a.ID})
			}
		}
	}
	for i, e := range m.Embeds {
		if e == nil {
			continue
		}
		base := fmt.Sprintf("/embeds/%d", i)
		if e.Author != nil {
			add("embed_author", base+"/author/name", e.Author.Name)
		}
		add("embed_title", base+"/title", e.Title)
		add("embed_description", base+"/description", e.Description)
		for j, f := range e.Fields {
			if f != nil {
				add("embed_field_name", fmt.Sprintf("%s/fields/%d/name", base, j), f.Name)
				add("embed_field_value", fmt.Sprintf("%s/fields/%d/value", base, j), f.Value)
			}
		}
		if e.Footer != nil {
			add("embed_footer", base+"/footer/text", e.Footer.Text)
		}
	}
	if m.ReferencedMessage != nil && m.ReferencedMessage.Content != "" {
		kind := "reply_context"
		if m.MessageReference != nil && m.MessageReference.Type == discordgo.MessageReferenceTypeForward {
			kind = "forward_context"
		}
		parts = append(parts, Part{Kind: kind, Path: "/referenced_message/content", Text: m.ReferencedMessage.Content, MessageID: m.ReferencedMessage.ID})
	}
	if m.Poll != nil {
		add("poll_question", "/poll/question/text", m.Poll.Question.Text)
		for i, a := range m.Poll.Answers {
			if a.Media != nil {
				add("poll_answer", fmt.Sprintf("/poll/answers/%d/poll_media/text", i), a.Media.Text)
			}
		}
	}
	return parts
}

func Normalize(parts []Part) string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		text := Sanitize(p.Text)
		if text == "" {
			continue
		}
		switch p.Kind {
		case "reply_context":
			text = "reply:" + text
		case "forward_context":
			text = "forward:" + text
		}
		out = append(out, text)
	}
	return strings.Join(out, "\n")
}

func Sanitize(raw string) string {
	raw = norm.NFKC.String(strings.ToValidUTF8(raw, ""))
	var b strings.Builder
	space := false
	for _, r := range raw {
		switch {
		case unicode.IsSpace(r):
			space = b.Len() > 0
		case unicode.IsControl(r) || r == '\u200b' || r == '\u200c' || r == '\u200d' || r == '\ufeff':
			continue
		default:
			if space {
				b.WriteByte(' ')
				space = false
			}
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}
