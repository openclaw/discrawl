package syncer

import (
	"bufio"
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"

	"github.com/openclaw/discrawl/internal/store"
)

const (
	attachmentFetchMaxBytes = 256 << 10
	attachmentIndexMaxChars = 8 << 10
)

var attachmentHTTPClient = &http.Client{Timeout: 5 * time.Second}

func buildMessageMutation(
	ctx context.Context,
	message *discordgo.Message,
	channelName string,
	fallbackGuildID string,
	embeddings bool,
	attachmentText bool,
) (store.MessageMutation, error) {
	guildID := effectiveMessageGuildID(message, fallbackGuildID)
	attachments, attachmentParts, err := extractAttachments(ctx, message, guildID, attachmentText)
	if err != nil {
		return store.MessageMutation{}, err
	}
	normalized := normalizeMessageParts(message, attachmentParts)
	record := toMessageRecord(message, channelName, guildID, normalized)
	return store.MessageMutation{
		Record:      record,
		EventType:   "upsert",
		PayloadJSON: record.RawJSON,
		Options: store.WriteOptions{
			EnqueueEmbedding: embeddings,
		},
		Attachments: attachments,
		Mentions:    extractMentions(message, guildID),
	}, nil
}

func extractAttachments(ctx context.Context, message *discordgo.Message, guildID string, attachmentText bool) ([]store.AttachmentRecord, []string, error) {
	if message == nil || len(message.Attachments) == 0 {
		return nil, nil, nil
	}
	records := make([]store.AttachmentRecord, 0, len(message.Attachments))
	parts := make([]string, 0, len(message.Attachments)*2)
	for _, attachment := range message.Attachments {
		if attachment == nil || attachment.ID == "" {
			continue
		}
		record := store.AttachmentRecord{
			AttachmentID: attachment.ID,
			MessageID:    message.ID,
			GuildID:      guildID,
			ChannelID:    message.ChannelID,
			Filename:     attachment.Filename,
			ContentType:  attachment.ContentType,
			Size:         int64(attachment.Size),
			URL:          attachment.URL,
			ProxyURL:     attachment.ProxyURL,
		}
		if message.Author != nil {
			record.AuthorID = message.Author.ID
		}
		if name := strings.TrimSpace(attachment.Filename); name != "" {
			parts = append(parts, name)
		}
		if attachmentText && shouldFetchAttachmentText(attachment) && attachment.URL != "" {
			text, err := fetchAttachmentText(ctx, attachment.URL)
			if err != nil {
				text = ""
			}
			if text != "" {
				record.TextContent = text
				parts = append(parts, text)
			}
		}
		records = append(records, record)
	}
	return records, parts, nil
}

func extractMentions(message *discordgo.Message, guildID string) []store.MentionEventRecord {
	if message == nil {
		return nil
	}
	eventAt := message.Timestamp.UTC().Format(time.RFC3339Nano)
	authorID := ""
	if message.Author != nil {
		authorID = message.Author.ID
	}
	mentions := make([]store.MentionEventRecord, 0, len(message.Mentions)+len(message.MentionRoles))
	seen := make(map[string]struct{}, len(message.Mentions)+len(message.MentionRoles))
	for _, user := range message.Mentions {
		if user == nil || user.ID == "" {
			continue
		}
		key := "user:" + user.ID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		targetName := strings.TrimSpace(user.GlobalName)
		if targetName == "" {
			targetName = user.Username
		}
		mentions = append(mentions, store.MentionEventRecord{
			MessageID:  message.ID,
			GuildID:    guildID,
			ChannelID:  message.ChannelID,
			AuthorID:   authorID,
			TargetType: "user",
			TargetID:   user.ID,
			TargetName: targetName,
			EventAt:    eventAt,
		})
	}
	for _, roleID := range message.MentionRoles {
		roleID = strings.TrimSpace(roleID)
		if roleID == "" {
			continue
		}
		key := "role:" + roleID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		mentions = append(mentions, store.MentionEventRecord{
			MessageID:  message.ID,
			GuildID:    guildID,
			ChannelID:  message.ChannelID,
			AuthorID:   authorID,
			TargetType: "role",
			TargetID:   roleID,
			EventAt:    eventAt,
		})
	}
	return mentions
}

func shouldFetchAttachmentText(attachment *discordgo.MessageAttachment) bool {
	if attachment == nil {
		return false
	}
	if isBlockedAttachment(attachment.Filename, attachment.ContentType) {
		return false
	}
	contentType := strings.ToLower(strings.TrimSpace(attachment.ContentType))
	if strings.HasPrefix(contentType, "text/") || contentType == "application/json" {
		return true
	}
	switch strings.ToLower(filepath.Ext(strings.TrimSpace(attachment.Filename))) {
	case ".txt", ".md", ".log", ".csv", ".json", ".yaml", ".yml", ".xml":
		return true
	default:
		return false
	}
}

func fetchAttachmentText(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := attachmentHTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", nil
	}
	if resp.ContentLength > attachmentFetchMaxBytes {
		return "", nil
	}
	contentType := normalizedMediaType(resp.Header.Get("Content-Type"))
	if contentType != "" && !isAllowedFetchedContentType(contentType) {
		return "", nil
	}
	reader := bufio.NewReader(resp.Body)
	peek, err := reader.Peek(512)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	if len(peek) != 0 && !isAllowedFetchedContentType(normalizedMediaType(http.DetectContentType(peek))) {
		return "", nil
	}
	body, err := io.ReadAll(io.LimitReader(reader, attachmentFetchMaxBytes+1))
	if err != nil {
		return "", err
	}
	if len(body) > attachmentFetchMaxBytes {
		return "", nil
	}
	return clampText(string(body), attachmentIndexMaxChars), nil
}

func isBlockedAttachment(filename, contentType string) bool {
	switch strings.ToLower(filepath.Ext(strings.TrimSpace(filename))) {
	case ".app", ".bat", ".cmd", ".com", ".dll", ".dmg", ".exe", ".jar", ".msi", ".pkg", ".ps1", ".scr", ".sh":
		return true
	}
	switch normalizedMediaType(contentType) {
	case "application/java-archive",
		"application/octet-stream",
		"application/vnd.microsoft.portable-executable",
		"application/x-apple-diskimage",
		"application/x-dosexec",
		"application/x-msdownload",
		"application/x-msi",
		"application/x-sh":
		return true
	default:
		return false
	}
}

func isAllowedFetchedContentType(contentType string) bool {
	if contentType == "" {
		return false
	}
	if strings.HasPrefix(contentType, "text/") {
		return true
	}
	switch contentType {
	case "application/json", "application/xml", "text/xml":
		return true
	default:
		return false
	}
}

func normalizedMediaType(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err == nil {
		return strings.ToLower(mediaType)
	}
	return strings.ToLower(strings.TrimSpace(strings.Split(value, ";")[0]))
}

func clampText(text string, limit int) string {
	text = strings.TrimSpace(text)
	if limit <= 0 || len(text) <= limit {
		return text
	}
	// Keep the byte ceiling without repairing already-invalid source text.
	if utf8.ValidString(text) {
		for limit > 0 && !utf8.RuneStart(text[limit]) {
			limit--
		}
	}
	return strings.TrimSpace(text[:limit])
}
