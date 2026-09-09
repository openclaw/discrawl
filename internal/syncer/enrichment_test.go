package syncer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/share"
	"github.com/openclaw/discrawl/internal/store"
)

func TestBuildMessageMutationIncludesAttachmentTextAndMentions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("stack trace line one\nstack trace line two"))
	}))
	defer server.Close()

	previousClient := attachmentHTTPClient
	attachmentHTTPClient = server.Client()
	t.Cleanup(func() {
		attachmentHTTPClient = previousClient
	})

	now := time.Now().UTC()
	mutation, err := buildMessageMutation(context.Background(), &discordgo.Message{
		ID:        "m1",
		GuildID:   "g1",
		ChannelID: "c1",
		Content:   "",
		Timestamp: now,
		Author:    &discordgo.User{ID: "u1", Username: "peter"},
		Attachments: []*discordgo.MessageAttachment{{
			ID:          "a1",
			Filename:    "trace.txt",
			ContentType: "text/plain",
			URL:         server.URL,
			Size:        64,
		}},
		Mentions: []*discordgo.User{{
			ID:         "u2",
			Username:   "shadow",
			GlobalName: "Shadow",
		}},
		MentionRoles: []string{"r1"},
	}, "maintainers", "", false, true)
	require.NoError(t, err)
	require.Len(t, mutation.Attachments, 1)
	require.Equal(t, "trace.txt", mutation.Attachments[0].Filename)
	require.Contains(t, mutation.Attachments[0].TextContent, "stack trace")
	require.Contains(t, mutation.Record.NormalizedContent, "trace.txt")
	require.Contains(t, mutation.Record.NormalizedContent, "stack trace line one")
	require.Len(t, mutation.Mentions, 2)
	require.Equal(t, "user", mutation.Mentions[0].TargetType)
	require.Equal(t, "u2", mutation.Mentions[0].TargetID)
	require.Equal(t, "Shadow", mutation.Mentions[0].TargetName)
	require.Equal(t, "role", mutation.Mentions[1].TargetType)
	require.Equal(t, "r1", mutation.Mentions[1].TargetID)
}

func TestBuildMessageMutationFallsBackToChannelGuildID(t *testing.T) {
	now := time.Now().UTC()
	mutation, err := buildMessageMutation(context.Background(), &discordgo.Message{
		ID:        "m1",
		ChannelID: "c1",
		Content:   "missing guild id from channel history",
		Timestamp: now,
		Author:    &discordgo.User{ID: "u1", Username: "peter"},
		Attachments: []*discordgo.MessageAttachment{{
			ID:       "a1",
			Filename: "trace.txt",
		}},
		Mentions: []*discordgo.User{{ID: "u2", Username: "shadow"}},
		MentionRoles: []string{
			"r1",
		},
	}, "maintainers", "g1", false, false)
	require.NoError(t, err)
	require.Equal(t, "g1", mutation.Record.GuildID)
	require.Equal(t, "g1", mutation.Attachments[0].GuildID)
	require.Equal(t, "g1", mutation.Mentions[0].GuildID)
	require.Equal(t, "g1", mutation.Mentions[1].GuildID)
}

func TestShouldFetchAttachmentText(t *testing.T) {
	t.Parallel()

	require.True(t, shouldFetchAttachmentText(&discordgo.MessageAttachment{Filename: "trace.txt"}))
	require.True(t, shouldFetchAttachmentText(&discordgo.MessageAttachment{Filename: "payload.json"}))
	require.True(t, shouldFetchAttachmentText(&discordgo.MessageAttachment{ContentType: "text/plain"}))
	require.False(t, shouldFetchAttachmentText(&discordgo.MessageAttachment{Filename: "script.sh", ContentType: "text/plain"}))
	require.False(t, shouldFetchAttachmentText(&discordgo.MessageAttachment{Filename: "photo.png", ContentType: "image/png"}))
}

func TestBuildMessageMutationSkipsBinaryResponseEvenWhenAttachmentLooksTextual(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte{0x7f, 'E', 'L', 'F', 0x02, 0x01})
	}))
	defer server.Close()

	previousClient := attachmentHTTPClient
	attachmentHTTPClient = server.Client()
	t.Cleanup(func() {
		attachmentHTTPClient = previousClient
	})

	mutation, err := buildMessageMutation(context.Background(), &discordgo.Message{
		ID:        "m1",
		GuildID:   "g1",
		ChannelID: "c1",
		Timestamp: time.Now().UTC(),
		Author:    &discordgo.User{ID: "u1", Username: "peter"},
		Attachments: []*discordgo.MessageAttachment{{
			ID:       "a1",
			Filename: "trace.txt",
			URL:      server.URL,
		}},
	}, "maintainers", "", false, true)
	require.NoError(t, err)
	require.Len(t, mutation.Attachments, 1)
	require.Empty(t, mutation.Attachments[0].TextContent)
	require.Contains(t, mutation.Record.NormalizedContent, "trace.txt")
	require.NotContains(t, mutation.Record.NormalizedContent, "ELF")
}

func TestBuildMessageMutationSkipsOversizedAttachmentResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", "999999")
		_, _ = w.Write([]byte("should not be indexed"))
	}))
	defer server.Close()

	previousClient := attachmentHTTPClient
	attachmentHTTPClient = server.Client()
	t.Cleanup(func() {
		attachmentHTTPClient = previousClient
	})

	mutation, err := buildMessageMutation(context.Background(), &discordgo.Message{
		ID:        "m1",
		GuildID:   "g1",
		ChannelID: "c1",
		Timestamp: time.Now().UTC(),
		Author:    &discordgo.User{ID: "u1", Username: "peter"},
		Attachments: []*discordgo.MessageAttachment{{
			ID:          "a1",
			Filename:    "trace.txt",
			ContentType: "text/plain",
			URL:         server.URL,
		}},
	}, "maintainers", "", false, true)
	require.NoError(t, err)
	require.Len(t, mutation.Attachments, 1)
	require.Empty(t, mutation.Attachments[0].TextContent)
	require.NotContains(t, mutation.Record.NormalizedContent, "should not be indexed")
}

func TestBuildMessageMutationRespectsAttachmentTextOptOut(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("stack trace line one"))
	}))
	defer server.Close()

	previousClient := attachmentHTTPClient
	attachmentHTTPClient = server.Client()
	t.Cleanup(func() {
		attachmentHTTPClient = previousClient
	})

	mutation, err := buildMessageMutation(context.Background(), &discordgo.Message{
		ID:        "m1",
		GuildID:   "g1",
		ChannelID: "c1",
		Timestamp: time.Now().UTC(),
		Author:    &discordgo.User{ID: "u1", Username: "peter"},
		Attachments: []*discordgo.MessageAttachment{{
			ID:          "a1",
			Filename:    "trace.txt",
			ContentType: "text/plain",
			URL:         server.URL,
		}},
	}, "maintainers", "", false, false)
	require.NoError(t, err)
	require.Len(t, mutation.Attachments, 1)
	require.Empty(t, mutation.Attachments[0].TextContent)
	require.Contains(t, mutation.Record.NormalizedContent, "trace.txt")
	require.NotContains(t, mutation.Record.NormalizedContent, "stack trace line one")
}

func TestClampTextUTF8ByteBoundaries(t *testing.T) {
	for _, character := range []string{"\u00e9", "\u20ac", "\U0001f642", "\ufffd"} {
		for prefixBytes := attachmentIndexMaxChars - len(character) - 1; prefixBytes <= attachmentIndexMaxChars+1; prefixBytes++ {
			t.Run(fmt.Sprintf("%U/prefix-%d", []rune(character)[0], prefixBytes), func(t *testing.T) {
				prefix := strings.Repeat("a", prefixBytes)
				input := prefix + character + "zzzz"
				want := prefix
				switch {
				case prefixBytes >= attachmentIndexMaxChars:
					want = strings.Repeat("a", attachmentIndexMaxChars)
				case prefixBytes+len(character) <= attachmentIndexMaxChars:
					want += character + strings.Repeat("z", attachmentIndexMaxChars-prefixBytes-len(character))
				}
				got := clampText(input, attachmentIndexMaxChars)
				require.Equal(t, want, got)
				require.True(t, utf8.ValidString(got))
				require.LessOrEqual(t, len(got), attachmentIndexMaxChars)
				require.True(t, strings.HasPrefix(input, got))
			})
		}
	}
}

func TestClampTextPreservesTrimmingAndInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
		limit int
		want  string
	}{
		{"empty", "", 1, ""},
		{"short", " \tshort\n", 20, "short"},
		{"ascii", "abcdef", 3, "abc"},
		{"trim-before-and-after", "  ab  cd  ", 4, "ab"},
		{"zero-limit", " full text ", 0, "full text"},
		{"negative-limit", " full text ", -1, "full text"},
		{"character-exceeds-limit", "\u20ac", 2, ""},
		{"unicode-whitespace", "\u2003\u00e9\u2003", 2, "\u00e9"},
		{"valid-replacement", " \ufffd value ", 3, "\ufffd"},
		{"invalid-short", " bad\xfftext ", 20, "bad\xfftext"},
		{"invalid-prefix", "bad\xfftext", 4, "bad\xff"},
		{"invalid-cut", "a\xc3\xa9\xff", 2, "a\xc3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, clampText(tc.input, tc.limit))
		})
	}
}

func TestAttachmentUTF8FetchStorePublicationRoundTrip(t *testing.T) {
	for _, character := range []string{"\u00e9", "\u20ac", "\U0001f642", "\ufffd"} {
		t.Run(fmt.Sprintf("%U", []rune(character)[0]), func(t *testing.T) {
			want := "\ufffd" + strings.Repeat("a", attachmentIndexMaxChars-len("\ufffd")-1)
			input := " \t" + want + character + " tail\n"
			mutation := attachmentTextMutation(t, input, "text/plain; charset=utf-8")
			require.Len(t, mutation.Attachments, 1)
			require.Equal(t, want, mutation.Attachments[0].TextContent)
			src, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "source.db"))
			require.NoError(t, err)
			defer func() { _ = src.Close() }()
			require.NoError(t, src.UpsertMessages(t.Context(), []store.MessageMutation{mutation}))
			var stored string
			require.NoError(t, src.DB().QueryRowContext(t.Context(),
				`select text_content from message_attachments where attachment_id = 'attachment'`).Scan(&stored))
			require.Equal(t, []byte(want), []byte(stored))

			repo := filepath.Join(t.TempDir(), "publication")
			_, err = share.Export(t.Context(), src, share.Options{RepoPath: repo})
			require.NoError(t, err)
			dst, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "destination.db"))
			require.NoError(t, err)
			defer func() { _ = dst.Close() }()
			_, err = share.Import(t.Context(), dst, share.Options{RepoPath: repo})
			require.NoError(t, err)
			require.NoError(t, dst.DB().QueryRowContext(t.Context(),
				`select text_content from message_attachments where attachment_id = 'attachment'`).Scan(&stored))
			require.Equal(t, []byte(want), []byte(stored))
		})
	}
}

func TestAttachmentFetchDoesNotReplaceInvalidSourceText(t *testing.T) {
	input := "declared Latin-1 caf\xe9"
	mutation := attachmentTextMutation(t, input, "text/plain; charset=iso-8859-1")
	require.Len(t, mutation.Attachments, 1)
	require.Equal(t, input, mutation.Attachments[0].TextContent)
	require.False(t, utf8.ValidString(mutation.Attachments[0].TextContent))
}

type attachmentTextRoundTripper func(*http.Request) (*http.Response, error)

func (f attachmentTextRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func attachmentTextMutation(t *testing.T, text, contentType string) store.MessageMutation {
	t.Helper()
	previous := attachmentHTTPClient
	calls := 0
	attachmentHTTPClient = &http.Client{Transport: attachmentTextRoundTripper(func(req *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {contentType}},
			Body: io.NopCloser(strings.NewReader(text)), ContentLength: int64(len(text)), Request: req,
		}, nil
	})}
	t.Cleanup(func() { attachmentHTTPClient = previous })
	mutation, err := buildMessageMutation(t.Context(), &discordgo.Message{
		ID: "message", GuildID: "guild", ChannelID: "channel",
		Timestamp: time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC),
		Author:    &discordgo.User{ID: "author", Username: "fixture"},
		Attachments: []*discordgo.MessageAttachment{{
			ID: "attachment", Filename: "fixture.txt", ContentType: contentType,
			URL: "https://example.invalid/fixture.txt", Size: len(text),
		}},
	}, "fixture", "", false, true)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	return mutation
}
