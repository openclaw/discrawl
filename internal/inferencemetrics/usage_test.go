package inferencemetrics

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUsageScannerBoundedFragmentedMetadata(t *testing.T) {
	payload := `{"data":[{"embedding":[` + strings.Repeat(`0.123,`, 50000) + `0],"private":"` + strings.Repeat("never retain", 1000) + `"}],"usage":{"prompt_tokens":100,"prompt_tokens_details":{"cached_tokens":20}},"service_tier":"default"}`
	for _, chunk := range []int{1, 7, 4096} {
		var s usageScanner
		for i := 0; i < len(payload); i += chunk {
			s.feed([]byte(payload[i:min(i+chunk, len(payload))]))
		}
		require.True(t, s.done)
		require.NotNil(t, s.input)
		require.EqualValues(t, 100, *s.input)
		require.EqualValues(t, 20, *s.cached)
		require.Equal(t, "default", s.tier)
		require.NotContains(t, string(s.token[:]), "never retain")
	}
}

func TestUsageScannerOnlyExactOwnFields(t *testing.T) {
	for _, payload := range []string{
		`{"data":{"usage":{"prompt_tokens":100}}}`,
		`{"usage":{"prompt_tokens":100,"prompt_tokens":null}}`,
		`{"usage":{"prompt_tokens":100},"usage":null}`,
		`{"usage":{"prompt_tokens":100},"usage":{"total_tokens":100}}`,
		`{"usage":{"prompt_tokens":1e2}}`,
		`{"usage":{"prompt_tokens":[]}}`,
		`{"usage":{"prompt_tokens":{"prompt_tokens":100}}}`,
		`{"usage":[{"prompt_tokens":100}]}`,
	} {
		var s usageScanner
		s.feed([]byte(payload))
		require.Nil(t, s.input, payload)
	}
	var s usageScanner
	s.feed([]byte(`{"us\u0061ge":{"prompt_tokens":42},"data":{"usage":{"prompt_tokens":100}}}`))
	require.NotNil(t, s.input)
	require.EqualValues(t, 42, *s.input)
	s.feed([]byte(`{"usage":{"prompt_tokens":100}}`))
	require.EqualValues(t, 42, *s.input) // first decoded object only
}

func TestUsageScannerLimitsOnlyTelemetry(t *testing.T) {
	var s usageScanner
	s.feed([]byte(`{"usage":{"prompt_tokens":7},"deep":` + strings.Repeat("[", 65)))
	require.True(t, s.disabled)
	require.Nil(t, s.input)
	s = usageScanner{}
	s.feed([]byte(`{"usage":{"prompt_tokens":` + strings.Repeat("9", 1000) + `}}`))
	require.Nil(t, s.input)
}
