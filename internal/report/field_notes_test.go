package report

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func fieldNotesFixture() ActivityReport {
	now := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
	latest := now.Add(-48 * time.Hour)
	activity := ActivityReport{
		GeneratedAt: now, LatestMessageAt: latest,
		TotalMessages: 100, TotalChannels: 12, TotalMembers: 20,
		TopChannels: []RankedCount{{Name: "private-channel-sentinel", Count: 10}},
		TopAuthors:  []RankedCount{{Name: "private-author-sentinel", Count: 10}},
		BusiestDays: []RankedCount{{Name: "unused-ranking-sentinel", Count: 10}},
	}
	for _, hours := range []int{24, 168, 720} {
		activity.Windows = append(activity.Windows, WindowStats{
			Label: "unused-label-sentinel", Since: latest.Add(-time.Duration(hours) * time.Hour),
			Messages: 10, ActiveAuthors: 3, ActiveChannels: 2, Attachments: 4,
		})
	}
	return activity
}

func TestFieldNotesAggregatePair(t *testing.T) {
	activity := fieldNotesFixture()
	markdown, metadata, err := RenderFieldNotes(activity)
	require.NoError(t, err)
	var notes FieldNotes
	require.NoError(t, json.Unmarshal(metadata, &notes))
	require.Equal(t, 1, notes.Version)
	require.Equal(t, "deterministic", notes.Generator)
	require.Equal(t, "published-non-dm", notes.Scope)
	require.Equal(t, "unknown", notes.Coverage)
	require.Equal(t, "observed", notes.TimestampStatus)
	require.Equal(t, int64(172800), *notes.LatestMessageAgeSeconds)
	require.True(t, notes.WindowAnchorAt.Equal(activity.LatestMessageAt))
	require.Equal(t, FieldNotesTotals{100, 12, 20}, notes.Totals)
	require.Len(t, notes.Windows, 3)
	for i, window := range notes.Windows {
		require.True(t, window.Since.Equal(activity.Windows[i].Since))
		require.Equal(t, 4, window.MessagesWithAttachments)
		require.Contains(t, string(markdown), window.Since.Format(time.RFC3339))
	}
	require.Contains(t, string(markdown), "4 messages contain attachments (40.0% of messages)")
	require.Contains(t, string(markdown), "172800 seconds before generation")
	require.Contains(t, string(markdown), "not sync freshness")
	require.NotContains(t, string(markdown)+string(metadata), "sentinel")
	mdAgain, jsonAgain, err := RenderFieldNotes(activity)
	require.NoError(t, err)
	require.Equal(t, markdown, mdAgain)
	require.True(t, bytes.Equal(metadata, jsonAgain), "field notes JSON bytes must be deterministic")
}

func TestFieldNotesTimestampStates(t *testing.T) {
	for _, state := range []string{"empty", "unavailable", "future"} {
		t.Run(state, func(t *testing.T) {
			activity := fieldNotesFixture()
			switch state {
			case "empty":
				activity.TotalMessages = 0
				activity.LatestMessageAt = time.Time{}
				activity.Windows = []WindowStats{{Since: activity.GeneratedAt.Add(-24 * time.Hour)}}
			case "unavailable":
				activity.LatestMessageAt = time.Time{}
				activity.Windows[0].Since = activity.GeneratedAt.Add(-24 * time.Hour)
			case "future":
				activity.LatestMessageAt = activity.GeneratedAt.Add(24 * time.Hour)
			}
			markdown, metadata, err := RenderFieldNotes(activity)
			require.NoError(t, err)
			var notes FieldNotes
			require.NoError(t, json.Unmarshal(metadata, &notes))
			require.Equal(t, state, notes.TimestampStatus)
			require.Nil(t, notes.LatestMessageAgeSeconds)
			require.Equal(t, "unknown", notes.Coverage)
			require.NotContains(t, string(markdown), "NaN")
			require.NotContains(t, string(markdown), "Inf")
			if state != "future" {
				require.Nil(t, notes.LatestMessageAt)
				require.True(t, notes.WindowAnchorAt.Equal(activity.GeneratedAt))
			} else {
				require.True(t, notes.WindowAnchorAt.Equal(activity.LatestMessageAt))
				require.Contains(t, string(markdown), "later than report generation")
			}
		})
	}
}

func TestFieldNotesExistingBuildTimeSemantics(t *testing.T) {
	for _, timestamp := range []string{"2026-09-08T08:00:00Z", "2099-01-01T00:00:00Z", "not-a-time", ""} {
		t.Run(timestamp, func(t *testing.T) {
			s, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "fixture.db"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			if timestamp != "" {
				require.NoError(t, s.UpsertMessage(t.Context(), store.MessageRecord{
					ID: "fixture", GuildID: "guild", ChannelID: "channel", AuthorID: "author",
					CreatedAt: timestamp, Content: "content-sentinel", RawJSON: `{}`,
				}))
			}
			now := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
			activity, err := Build(t.Context(), s, Options{Now: now, Published: true})
			require.NoError(t, err)
			_, body, err := RenderFieldNotes(activity)
			require.NoError(t, err)
			var notes FieldNotes
			require.NoError(t, json.Unmarshal(body, &notes))
			anchor := activity.LatestMessageAt
			if anchor.IsZero() {
				anchor = now
			}
			require.True(t, notes.WindowAnchorAt.Equal(anchor))
			for i, hours := range []int{24, 168, 720} {
				require.True(t, notes.Windows[i].Since.Equal(anchor.Add(-time.Duration(hours)*time.Hour)))
			}
			require.NotContains(t, string(body), "content-sentinel")
			if timestamp == "not-a-time" {
				require.Equal(t, "unavailable", notes.TimestampStatus)
			}
		})
	}
}

func TestFieldNotesReadmeAndArtifacts(t *testing.T) {
	dir := t.TempDir()
	readme := filepath.Join(dir, "README.md")
	activity := fieldNotesFixture()
	markdown, metadata, err := RenderFieldNotes(activity)
	require.NoError(t, err)
	base := []byte("# Maintainer docs\n\n" + StartMarker + "\nold report\n" + EndMarker + "\n\nmanual notes\n")
	require.NoError(t, os.WriteFile(readme, base, 0o600))
	for range 2 {
		require.NoError(t, WriteReadmeWithFieldNotes(readme, "new statistics", activity))
		body, err := os.ReadFile(readme)
		require.NoError(t, err)
		require.Contains(t, string(body), "# Maintainer docs")
		require.Contains(t, string(body), "manual notes")
		require.Contains(t, string(body), strings.TrimSpace(string(markdown)))
		require.Equal(t, 1, strings.Count(string(body), FieldNotesStartMarker))
		for name, expected := range map[string][]byte{FieldNotesMarkdownPath: markdown, FieldNotesJSONPath: metadata} {
			actual, err := os.ReadFile(filepath.Join(dir, name))
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		}
		updated := UpdateReadme(body, "later scheduled publisher statistics")
		require.Contains(t, string(updated), strings.TrimSpace(string(markdown)))
		require.NoError(t, os.WriteFile(readme, updated, 0o600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "reports", "manual.md"), []byte("keep"), 0o600))
	require.NoError(t, RemoveFieldNotes(dir))
	require.NoError(t, RemoveFieldNotes(dir))
	require.FileExists(t, readme)
	require.FileExists(t, filepath.Join(dir, "reports", "manual.md"))
	require.NoFileExists(t, filepath.Join(dir, FieldNotesMarkdownPath))
	require.NoFileExists(t, filepath.Join(dir, FieldNotesJSONPath))
}

func TestFieldNotesRejectMalformedMarkersAndUnsafePaths(t *testing.T) {
	for _, text := range []string{
		FieldNotesStartMarker, FieldNotesEndMarker,
		FieldNotesEndMarker + FieldNotesStartMarker,
		FieldNotesStartMarker + FieldNotesStartMarker + FieldNotesEndMarker,
		StartMarker + FieldNotesStartMarker + FieldNotesEndMarker + EndMarker,
	} {
		t.Run(text, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "README.md")
			require.NoError(t, os.WriteFile(path, []byte(text), 0o600))
			require.Error(t, WriteReadmeWithFieldNotes(path, "statistics", fieldNotesFixture()))
			require.NoDirExists(t, filepath.Join(dir, "reports"))
			body, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, text, string(body))
		})
	}
	if runtime.GOOS == "windows" {
		t.Skip("symlink fixture requires Unix permissions")
	}
	for _, name := range []string{"reports", FieldNotesMarkdownPath, FieldNotesJSONPath, "README.md"} {
		t.Run(name, func(t *testing.T) {
			dir, outside := t.TempDir(), t.TempDir()
			if name != "reports" {
				require.NoError(t, os.Mkdir(filepath.Join(dir, "reports"), 0o700))
			}
			target := filepath.Join(outside, "sentinel")
			require.NoError(t, os.WriteFile(target, []byte("unchanged"), 0o600))
			if name == "reports" {
				target = outside
			}
			require.NoError(t, os.Symlink(target, filepath.Join(dir, name)))
			require.Error(t, WriteReadmeWithFieldNotes(filepath.Join(dir, "README.md"), "stats", fieldNotesFixture()))
			if name != "README.md" {
				require.Error(t, RemoveFieldNotes(dir))
			}
			body, err := os.ReadFile(filepath.Join(outside, "sentinel"))
			require.NoError(t, err)
			require.Equal(t, "unchanged", string(body))
		})
	}
}
