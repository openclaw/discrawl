package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTailStartupRepairIsExplicitAndIncompatibleWithReplayOnly(t *testing.T) {
	for _, args := range [][]string{{"tail"}, {"tail", "--repair-on-start", "--repair-every", "0"}, {"tail", "--repair-on-start", "--replay-failures-only"}} {
		_, path := writeTestConfig(t, t.TempDir())
		fake := &fakeSyncService{callTailReady: true}
		rt := tailTestRuntime(t.Context(), path, fake)
		err := rt.dispatch(args)
		if args[len(args)-1] == "--replay-failures-only" {
			require.ErrorContains(t, err, "cannot be combined")
			require.Zero(t, fake.tailCalls)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, len(args) > 1, fake.tailRepairOnStart)
		require.Equal(t, 1, fake.tailCalls)
	}
}
