package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"

	"github.com/openclaw/discrawl/internal/remotedelivery"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func deliveryInputs(t *testing.T) (*store.Store, string, string, string, string) {
	t.Helper()
	if goruntime.GOOS != "darwin" && goruntime.GOOS != "linux" {
		t.Skip("native capture identity is supported on macOS and Linux")
	}
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source.sqlite")
	source, err := store.Open(t.Context(), sourcePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	require.NoError(t, source.UpsertGuild(t.Context(), store.GuildRecord{ID: "g1", Name: "fixture", RawJSON: "{}"}))
	require.NoError(t, source.UpsertChannel(t.Context(), store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", RawJSON: "{}"}))
	baseline, err := store.ObserveNativeBaseline(t.Context(), sourcePath)
	require.NoError(t, err)
	keys := []remotedelivery.SeedKey{
		{Table: "channels", Key: []store.NativeValue{{Kind: "text", Text: "c1"}}},
		{Table: "guilds", Key: []store.NativeValue{{Kind: "text", Text: "g1"}}},
	}
	hash := sha256.New()
	var stream bytes.Buffer
	for _, entry := range keys {
		key, err := remotedelivery.OwnershipKey(entry.Table, entry.Key)
		require.NoError(t, err)
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(key)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(key))
		require.NoError(t, json.NewEncoder(&stream).Encode(entry))
	}
	registration := deliveryRegistration{Format: "discrawl-delivery-registration/v1", Registration: remotedelivery.Registration{
		Origin: "https://archive.example.test", Archive: "fixture", BindingID: "binding", SeedManifestSHA256: strings.Repeat("a", 64), QualificationSHA256: strings.Repeat("b", 64),
		OwnedKeysSHA256: hex.EncodeToString(hash.Sum(nil)), OwnedKeysCount: int64(len(keys)), Scope: remotedelivery.Scope{GuildIDs: []string{"g1"}}, Baseline: baseline, ServerCursor: "seed:complete",
	}}
	body, err := json.Marshal(registration)
	require.NoError(t, err)
	registrationPath, ownershipPath := filepath.Join(root, "registration.json"), filepath.Join(root, "ownership.jsonl")
	require.NoError(t, os.WriteFile(registrationPath, body, 0o600))
	require.NoError(t, os.WriteFile(ownershipPath, stream.Bytes(), 0o600))
	return source, sourcePath, filepath.Join(root, "outbox.sqlite"), registrationPath, ownershipPath
}

func TestDeliveryUsesExplicitStoresAndPreservesPendingCapture(t *testing.T) {
	source, sourcePath, outboxPath, registration, ownership := deliveryInputs(t)
	configPath := filepath.Join(t.TempDir(), "absent-config.toml")
	run := func(args ...string) (deliveryResult, error) {
		t.Helper()
		var output bytes.Buffer
		err := Run(t.Context(), append([]string{"--config", configPath, "delivery"}, args...), &output, &bytes.Buffer{})
		var result deliveryResult
		if output.Len() > 0 {
			require.NoError(t, json.Unmarshal(output.Bytes(), &result))
		}
		require.NoFileExists(t, configPath)
		return result, err
	}
	init := []string{"init", "--outbox", outboxPath, "--registration", registration, "--ownership", ownership}
	result, err := run(init...)
	require.NoError(t, err)
	require.Equal(t, "initialized", result.Status)
	_, err = run(init...)
	require.Error(t, err)
	require.NotContains(t, err.Error(), outboxPath)
	result, err = run("capture", "--outbox", outboxPath, "--source-db", sourcePath)
	require.NoError(t, err)
	require.Equal(t, "idle", result.Status)
	require.NoError(t, source.UpsertMessages(t.Context(), []store.MessageMutation{{Record: store.MessageRecord{ID: "m1", GuildID: "g1", ChannelID: "c1", CreatedAt: "2026-11-14T00:00:00Z", Content: "private\x00text", RawJSON: "{}"}}}))
	_, err = run("capture", "--outbox", outboxPath, "--source-db", sourcePath, "--max-bytes", "1")
	require.Error(t, err)
	result, err = run("capture", "--outbox", outboxPath, "--source-db", sourcePath)
	require.NoError(t, err)
	require.Equal(t, "staged", result.Status)
	// A pending batch takes precedence over even an invalid source path.
	pending, err := run("capture", "--outbox", outboxPath, "--source-db", filepath.Join(t.TempDir(), "missing.sqlite"))
	require.Error(t, err)
	require.Equal(t, "pending", pending.Status)
	require.Equal(t, result.CaptureID, pending.CaptureID)
	t.Setenv("DISCRAWL_TEST_MISSING_TOKEN", "")
	for _, op := range []string{"drain", "cycle"} {
		args := []string{op, "--outbox", outboxPath, "--token-env", "DISCRAWL_TEST_MISSING_TOKEN"}
		if op == "cycle" {
			args = append(args, "--source-db", filepath.Join(t.TempDir(), "missing.sqlite"))
		}
		_, err = run(args...)
		require.Error(t, err)
	}
	outbox, err := remotedelivery.OpenOutbox(t.Context(), outboxPath)
	require.NoError(t, err)
	defer outbox.Close()
	state, err := outbox.State(t.Context())
	require.NoError(t, err)
	require.Equal(t, result.CaptureID, state.PendingID)
}

func TestDeliveryRejectsUnqualifiedInputsWithoutOverwriting(t *testing.T) {
	_, _, outboxPath, registration, ownership := deliveryInputs(t)
	body, err := os.ReadFile(ownership)
	require.NoError(t, err)
	// Extra ownership is rejected even if it is otherwise valid JSON.
	require.NoError(t, os.WriteFile(ownership, append(body, body...), 0o600))
	var output bytes.Buffer
	err = Run(t.Context(), []string{"delivery", "init", "--outbox", outboxPath, "--registration", registration, "--ownership", ownership}, &output, &bytes.Buffer{})
	require.Error(t, err)
	require.Empty(t, output.String())
	_, err = remotedelivery.OpenOutbox(t.Context(), outboxPath)
	require.Error(t, err, "failed initialization must not be treated as qualified")
	require.FileExists(t, outboxPath, "retain failed initialization for reconciliation")
	for _, args := range [][]string{
		{"capture", "--outbox", outboxPath, "--source-db", registration, "--timeout", "0s"},
		{"drain", "--outbox", outboxPath, "--token-env", "TOKEN", "--max-parts", "65"},
		{"drain", "--outbox", outboxPath, "--token-env", "TOKEN", "--source-db", registration},
		{"cycle", "--outbox", outboxPath, "--source-db", registration},
		{"cycle", "--outbox", outboxPath, "--token-env", "TOKEN"},
	} {
		err := Run(t.Context(), append([]string{"delivery"}, args...), &output, &bytes.Buffer{})
		require.Equal(t, 2, ExitCode(err))
	}
}

func TestDeliveryHelp(t *testing.T) {
	for _, args := range [][]string{{"delivery"}, {"help", "delivery"}, {"delivery", "--help"}, {"delivery", "capture", "--help"}, {"delivery", "cycle", "--help"}, {"help", "delivery", "cycle"}, {"delivery", "prepare-seed", "--help"}} {
		var output bytes.Buffer
		require.NoError(t, Run(t.Context(), args, &output, &bytes.Buffer{}))
		require.Contains(t, output.String(), "discrawl delivery capture")
	}
}

func TestDeliveryPreparesPrivateSeedWithoutCollectorConfig(t *testing.T) {
	_, source, _, _, _ := deliveryInputs(t)
	root := filepath.Join(t.TempDir(), "seed")
	config := filepath.Join(t.TempDir(), "absent-config.toml")
	var output bytes.Buffer
	err := Run(t.Context(), []string{"--config", config, "delivery", "prepare-seed", "--source-db", source, "--output", root, "--guilds", "g1"}, &output, &bytes.Buffer{})
	require.NoError(t, err)
	require.NoFileExists(t, config)
	var result deliveryResult
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.Equal(t, "prepared", result.Status)
	require.Equal(t, int64(2), result.OwnedKeysCount)
	receipt, err := os.ReadFile(filepath.Join(root, "preparation.json"))
	require.NoError(t, err)
	sum := sha256.Sum256(receipt)
	require.Equal(t, hex.EncodeToString(sum[:]), result.QualificationSHA256)
	require.NotContains(t, output.String(), source)
	require.NotContains(t, output.String(), root)
	output.Reset()
	err = Run(t.Context(), []string{"delivery", "prepare-seed", "--source-db", source, "--output", root, "--guilds", "g1"}, &output, &bytes.Buffer{})
	require.Error(t, err)
	require.Empty(t, output.String())
	require.NotContains(t, err.Error(), root)
}
