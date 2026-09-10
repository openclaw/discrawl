package report

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	FieldNotesStartMarker  = "<!-- discrawl-field-notes:start -->"
	FieldNotesEndMarker    = "<!-- discrawl-field-notes:end -->"
	FieldNotesMarkdownPath = "reports/latest-field-notes.md"
	FieldNotesJSONPath     = "reports/latest-field-notes.json"
)

type FieldNotes struct {
	Version                 int                `json:"version"`
	Generator               string             `json:"generator"`
	Scope                   string             `json:"scope"`
	Coverage                string             `json:"coverage"`
	GeneratedAt             time.Time          `json:"generated_at"`
	LatestMessageAt         *time.Time         `json:"latest_message_at"`
	LatestMessageAgeSeconds *int64             `json:"latest_message_age_seconds"`
	WindowAnchorAt          time.Time          `json:"window_anchor_at"`
	TimestampStatus         string             `json:"timestamp_status"`
	Totals                  FieldNotesTotals   `json:"totals"`
	Windows                 []FieldNotesWindow `json:"windows"`
}

type FieldNotesTotals struct {
	Messages int `json:"messages"`
	Channels int `json:"channels"`
	Members  int `json:"member_records"`
}

type FieldNotesWindow struct {
	Since                   time.Time `json:"since"`
	Messages                int       `json:"messages"`
	ActiveAuthors           int       `json:"active_authors"`
	ActiveChannels          int       `json:"active_channels"`
	MessagesWithAttachments int       `json:"messages_with_attachments"`
}

// RenderFieldNotes consumes the same published report as the activity block.
// It deliberately excludes rankings, names, identifiers and message content.
func RenderFieldNotes(activity ActivityReport) ([]byte, []byte, error) {
	notes := FieldNotes{
		Version: 1, Generator: "deterministic", Scope: "published-non-dm",
		Coverage: "unknown", GeneratedAt: activity.GeneratedAt.UTC(),
		WindowAnchorAt: activity.GeneratedAt.UTC(), TimestampStatus: "unavailable",
		Totals:  FieldNotesTotals{activity.TotalMessages, activity.TotalChannels, activity.TotalMembers},
		Windows: make([]FieldNotesWindow, 0, len(activity.Windows)),
	}
	if activity.TotalMessages == 0 {
		notes.TimestampStatus = "empty"
	} else if !activity.LatestMessageAt.IsZero() {
		latest := activity.LatestMessageAt.UTC()
		notes.LatestMessageAt = &latest
		notes.WindowAnchorAt = latest
		notes.TimestampStatus = "observed"
		if latest.After(notes.GeneratedAt) {
			notes.TimestampStatus = "future"
		} else {
			age := int64(notes.GeneratedAt.Sub(latest) / time.Second)
			notes.LatestMessageAgeSeconds = &age
		}
	}
	for _, window := range activity.Windows {
		notes.Windows = append(notes.Windows, FieldNotesWindow{
			Since: window.Since.UTC(), Messages: window.Messages,
			ActiveAuthors: window.ActiveAuthors, ActiveChannels: window.ActiveChannels,
			MessagesWithAttachments: window.Attachments,
		})
	}
	body, err := json.MarshalIndent(notes, "", "  ")
	if err != nil {
		return nil, nil, err
	}
	var md strings.Builder
	fmt.Fprintf(&md, "## Field Notes\n\nGenerated at: %s\n\n", notes.GeneratedAt.Format(time.RFC3339))
	md.WriteString("Deterministic aggregate observations.\n")
	md.WriteString("Scope: published non-DM archive; private guild channels may be included.\n")
	md.WriteString("Coverage: unknown. These counts do not establish complete history or a successful sync.\n\n")
	fmt.Fprintf(&md, "Observed archive: %d messages, %d recorded channels, %d member records.\n\n",
		notes.Totals.Messages, notes.Totals.Channels, notes.Totals.Members)
	switch notes.TimestampStatus {
	case "empty":
		md.WriteString("No archived non-DM messages observed; this does not establish that Discord had no activity.\n")
		md.WriteString("Intervals use the report generation time because there is no latest message.\n")
	case "unavailable":
		md.WriteString("Latest archived message timestamp unavailable. Intervals use the report generation time fallback; invalid message timestamps may be omitted from counts.\n")
	case "future":
		fmt.Fprintf(&md, "Latest archived message: %s, later than report generation. Timestamp age is unavailable; intervals retain this future anchor.\n",
			notes.LatestMessageAt.Format(time.RFC3339))
	default:
		fmt.Fprintf(&md, "Latest archived message: %s (%d seconds before generation). This is message timestamp age, not sync freshness.\n",
			notes.LatestMessageAt.Format(time.RFC3339), *notes.LatestMessageAgeSeconds)
	}
	md.WriteString("\n### Anchored Activity\n\n")
	fmt.Fprintf(&md, "Intervals end at %s, not necessarily the current time. Start boundaries are inclusive.\n\n",
		notes.WindowAnchorAt.Format(time.RFC3339))
	for _, window := range notes.Windows {
		fmt.Fprintf(&md, "- Since %s: %d messages from %d active authors across %d active channels; %d messages contain attachments",
			window.Since.Format(time.RFC3339), window.Messages, window.ActiveAuthors,
			window.ActiveChannels, window.MessagesWithAttachments)
		if window.Messages > 0 {
			fmt.Fprintf(&md, " (%.1f%% of messages)", 100*float64(window.MessagesWithAttachments)/float64(window.Messages))
		}
		md.WriteString(".\n")
	}
	return []byte(md.String()), append(body, '\n'), nil
}

func UpdateFieldNotes(readme []byte, section string) ([]byte, error) {
	text := string(readme)
	start, end := strings.Index(text, FieldNotesStartMarker), strings.Index(text, FieldNotesEndMarker)
	replacement := FieldNotesStartMarker + "\n" + strings.TrimSpace(section) + "\n" + FieldNotesEndMarker
	if start == -1 && end == -1 {
		return []byte(strings.TrimRight(text, "\n") + "\n\n" + replacement + "\n"), nil
	}
	if start < 0 || end < start || strings.Count(text, FieldNotesStartMarker) != 1 ||
		strings.Count(text, FieldNotesEndMarker) != 1 {
		return nil, errors.New("invalid field notes README markers")
	}
	return []byte(text[:start] + replacement + text[end+len(FieldNotesEndMarker):]), nil
}

func checkFieldNotesPath(root *os.Root, name string, directory bool) error {
	info, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if (directory && !info.IsDir()) || (!directory && !info.Mode().IsRegular()) {
		return errors.New("field notes output must use regular files and directories")
	}
	return nil
}

// The workflow publishes only after every write succeeds. The pair and README
// share one report; a failed local write must never be treated as publishable.
func WriteReadmeWithFieldNotes(path, section string, activity ActivityReport) error {
	markdown, metadata, err := RenderFieldNotes(activity)
	if err != nil {
		return err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	dir, name := filepath.Split(abs)
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	if err := checkFieldNotesPath(root, name, false); err != nil {
		return err
	}
	if err := checkFieldNotesPath(root, "reports", true); err != nil {
		return err
	}
	for _, output := range []string{FieldNotesMarkdownPath, FieldNotesJSONPath} {
		if err := checkFieldNotesPath(root, output, false); err != nil {
			return err
		}
	}
	current, err := root.ReadFile(name)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Validate the notes markers before replacing the independently owned stats.
	updated, err := UpdateFieldNotes(current, string(markdown))
	if err != nil {
		return err
	}
	updated = UpdateReadme(updated, section)
	if strings.Count(string(updated), FieldNotesStartMarker) != 1 || strings.Count(string(updated), FieldNotesEndMarker) != 1 {
		return errors.New("field notes markers overlap the activity report")
	}
	if err := root.Mkdir("reports", 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	if err := root.WriteFile(FieldNotesMarkdownPath, markdown, 0o600); err != nil {
		return err
	}
	if err := root.WriteFile(FieldNotesJSONPath, metadata, 0o600); err != nil {
		return err
	}
	return root.WriteFile(name, updated, 0o600)
}

// These two exact paths are reserved outputs, never arbitrary report files.
func RemoveFieldNotes(repo string) error {
	root, err := os.OpenRoot(repo)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	if err := checkFieldNotesPath(root, "reports", true); err != nil {
		return err
	}
	for _, name := range []string{FieldNotesMarkdownPath, FieldNotesJSONPath} {
		if err := checkFieldNotesPath(root, name, false); err != nil {
			return err
		}
	}
	for _, name := range []string{FieldNotesMarkdownPath, FieldNotesJSONPath} {
		if err := root.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
