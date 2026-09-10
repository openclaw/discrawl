package cli

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusJSONIncludesTailFreshnessWithoutChangingSyncTime(t *testing.T) {
	cfg, cfgPath := writeTestConfig(t, t.TempDir())
	s := seedCLIStore(t, cfg.DBPath)
	defer func() { _ = s.Close() }()
	_, err := s.DB().ExecContext(t.Context(), `delete from sync_state where scope = 'tail:last_event'`)
	require.NoError(t, err)
	require.NoError(t, s.SetSyncState(t.Context(), "sync:last_success", "completed"))
	_, err = s.DB().ExecContext(t.Context(), `update sync_state set updated_at='2026-09-10T09:00:00Z' where scope='sync:last_success'`)
	require.NoError(t, err)
	var before bytes.Buffer
	require.NoError(t, Run(t.Context(), []string{"--config", cfgPath, "status", "--json"}, &before, &bytes.Buffer{}))
	var withoutTail map[string]any
	require.NoError(t, json.Unmarshal(before.Bytes(), &withoutTail))
	require.NotContains(t, withoutTail, "last_tail_event_at")

	require.NoError(t, s.SetSyncState(t.Context(), "tail:last_event", "message-id"))
	_, err = s.DB().ExecContext(t.Context(), `update sync_state set updated_at='2026-09-10T10:00:00Z' where scope='tail:last_event'`)
	require.NoError(t, err)
	var after bytes.Buffer
	require.NoError(t, Run(t.Context(), []string{"--config", cfgPath, "status", "--json"}, &after, &bytes.Buffer{}))
	var withTail map[string]any
	require.NoError(t, json.Unmarshal(after.Bytes(), &withTail))
	require.Equal(t, "2026-09-10T10:00:00Z", withTail["last_tail_event_at"])
	require.Equal(t, "2026-09-10T09:00:00Z", withTail["last_sync_at"])
	require.Equal(t, withoutTail["schema_version"], withTail["schema_version"])
	require.Equal(t, withoutTail["counts"], withTail["counts"])
}
