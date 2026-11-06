package syncer

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newTestLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}))
}

func TestMessageSyncProgressFinishReportsSummaryCounts(t *testing.T) {
	t.Parallel()

	out := &lockedBuffer{}
	svc := New(&fakeClient{}, nil, newTestLogger(out))
	svc.messageSyncLogEvery = time.Hour
	svc.messageSyncWaitEvery = time.Hour

	progress := newMessageSyncProgress(svc, "g1", 3, SyncOptions{Full: true, Concurrency: 2})
	require.NotNil(t, progress)

	first := &discordgo.Channel{ID: "c1", Name: "missing"}
	second := &discordgo.Channel{ID: "c2", Name: "gone"}
	third := &discordgo.Channel{ID: "c3", Name: "ok"}

	progress.start(first)
	progress.recordSkip(first, errors.New(`HTTP 403 Forbidden, {"message": "Missing Access", "code": 50001}`))
	progress.start(second)
	progress.recordSkip(second, errors.New(`HTTP 404 Not Found, {"message": "Unknown Channel", "code": 10003}`))
	progress.start(third)
	progress.record(third, 42)
	progress.finish(nil)

	logs := out.String()
	require.Contains(t, logs, `msg="message sync finished"`)
	require.Contains(t, logs, `processed_channels=3`)
	require.Contains(t, logs, `percent=100.0`)
	require.Contains(t, logs, `completion=100.0%`)
	require.Contains(t, logs, `messages_written=42`)
	require.Contains(t, logs, `skipped_missing_access_channels=1`)
	require.Contains(t, logs, `skipped_unknown_channel_channels=1`)
	require.Contains(t, logs, `deferred_channels=0`)
}

func TestMessageSyncProgressReportsWaitingHeartbeat(t *testing.T) {
	t.Parallel()

	out := &lockedBuffer{}
	svc := New(&fakeClient{}, nil, newTestLogger(out))
	svc.messageSyncLogEvery = time.Hour
	svc.messageSyncWaitEvery = 10 * time.Millisecond

	progress := newMessageSyncProgress(svc, "g1", 1, SyncOptions{Full: true, Concurrency: 1})
	require.NotNil(t, progress)

	channel := &discordgo.Channel{ID: "c1", Name: "slowpoke"}
	progress.start(channel)

	require.Eventually(t, func() bool {
		return strings.Contains(out.String(), `msg="message sync waiting"`)
	}, time.Second, 10*time.Millisecond)
	progress.finish(nil)

	logs := out.String()
	require.Contains(t, logs, `oldest_active_channel_id=c1`)
	require.Contains(t, logs, `oldest_active_channel_name=slowpoke`)
	require.Contains(t, logs, `active_channels=1`)
	require.Contains(t, logs, `percent=0.0`)
	require.Contains(t, logs, `completion=0.0%`)
}
