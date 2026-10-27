package store

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Exercise both public admission entrypoints, including legacy receipts that
// cannot establish semantic identity. Native SDK coverage lives in syncer.
func TestEmptySnapshotAdmissionBoundaries(t *testing.T) {
	const positive = `{"id":"100","channel_id":"c","author":{"id":"u"},"timestamp":"2026-10-01T00:00:00Z","content":"quartz"}`
	const empty = `{"id":"100","channel_id":"c","author":{"id":"u"},"timestamp":"2026-10-01T00:00:00Z","content":""}`
	for _, batch := range []bool{false, true} {
		for _, tc := range []struct {
			name, raw          string
			preserve, retained bool
		}{
			{"same-shape", empty, true, true},
			{"explicit-replacement", empty, false, false},
			{"malformed-receipt", "{", true, false},
			{"unidentifiable-receipt", "{}", true, false},
			{"different-identity", `{"id":"101","channel_id":"c","content":""}`, true, false},
		} {
			t.Run(tc.name+map[bool]string{false: "/single", true: "/batch"}[batch], func(t *testing.T) {
				s, err := Open(t.Context(), filepath.Join(t.TempDir(), "private-fixture.db"))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, s.Close()) })
				message := MessageRecord{ID: "100", GuildID: "g", ChannelID: "c", AuthorID: "u", Content: "quartz", NormalizedContent: "quartz", CreatedAt: "2026-10-01T00:00:00Z", RawJSON: positive}
				require.NoError(t, s.UpsertMessage(t.Context(), message))
				message.Content, message.NormalizedContent, message.RawJSON = "", "", tc.raw
				opts := WriteOptions{PreserveNewer: tc.preserve}
				if batch {
					err = s.UpsertMessages(t.Context(), []MessageMutation{{Record: message, Options: opts}})
				} else {
					err = s.UpsertMessageWithOptions(t.Context(), message, opts)
				}
				require.NoError(t, err)
				var content, raw string
				require.NoError(t, s.DB().QueryRowContext(t.Context(), "select content,raw_json from messages where id='100'").Scan(&content, &raw))
				if tc.retained {
					require.Equal(t, "quartz", content)
					require.Equal(t, positive, raw)
				} else {
					require.Empty(t, content)
					require.Equal(t, tc.raw, raw)
				}
			})
		}
	}
}
