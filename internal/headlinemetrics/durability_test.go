package headlinemetrics

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetricsWriterDurabilityOnReplacement(t *testing.T) {
	s := newMetrics(t)
	s.DB().SetMaxIdleConns(0)
	for range 2 {
		conn, err := s.DB().Conn(t.Context())
		require.NoError(t, err)
		var synchronous int
		require.NoError(t, conn.QueryRowContext(t.Context(), "PRAGMA synchronous").Scan(&synchronous))
		require.Equal(t, 2, synchronous)
		if runtime.GOOS == "darwin" {
			for _, pragma := range []string{"fullfsync", "checkpoint_fullfsync"} {
				var value int
				require.NoError(t, conn.QueryRowContext(t.Context(), "PRAGMA "+pragma).Scan(&value))
				require.Equal(t, 1, value, pragma)
			}
		}
		_, err = conn.ExecContext(t.Context(), "PRAGMA synchronous=OFF")
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}
}
