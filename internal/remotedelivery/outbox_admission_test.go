package remotedelivery

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestOutboxRejectsInvalidRegistrationBeforeCreatingFile(t *testing.T) {
	ctx, _, _, outbox, _, _ := outboxFixture(t)
	state, err := outbox.State(ctx)
	require.NoError(t, err)
	raw, err := json.Marshal(state.Registration)
	require.NoError(t, err)
	tests := map[string]func(*Registration){
		"credentials":          func(r *Registration) { r.Origin = "https://user:secret@example.test" },
		"path":                 func(r *Registration) { r.Origin += "/private" },
		"archive":              func(r *Registration) { r.Archive = "" },
		"digest":               func(r *Registration) { r.SeedManifestSHA256 = strings.Repeat("A", 64) },
		"scope":                func(r *Registration) { r.Scope.GuildIDs = []string{"@me"} },
		"source":               func(r *Registration) { r.Baseline.SourceID = "" },
		"sequence":             func(r *Registration) { r.Baseline.Cursor.Sequence = -1 },
		"missing generation":   func(r *Registration) { r.Baseline.Cursor.Generations = nil },
		"duplicate generation": func(r *Registration) { r.Baseline.Cursor.Generations[1] = r.Baseline.Cursor.Generations[0] },
		"invalid generation":   func(r *Registration) { r.Baseline.Cursor.Generations[0].GenerationID = "bad" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			var registration Registration
			require.NoError(t, json.Unmarshal(raw, &registration))
			mutate(&registration)
			path := filepath.Join(t.TempDir(), "outbox.sqlite")
			_, err := InitOutbox(ctx, path, registration, func(func(SeedKey) error) error { t.Fatal("invalid registration reached seed reader"); return nil })
			require.Error(t, err)
			_, err = os.Lstat(path)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
	for _, path := range []string{"relative.sqlite", "file:outbox.sqlite", "/invalid\x00path"} {
		_, err := InitOutbox(ctx, path, state.Registration, nil)
		require.Error(t, err)
		_, err = OpenOutbox(ctx, path)
		require.Error(t, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = InitOutbox(canceled, filepath.Join(t.TempDir(), "canceled.sqlite"), state.Registration, nil)
	require.ErrorIs(t, err, context.Canceled)
	_, err = InitOutbox(ctx, filepath.Join(t.TempDir(), "missing-seed.sqlite"), state.Registration, nil)
	require.ErrorIs(t, err, ErrOwnership)
}

func TestOutboxFailedSeedCannotBecomeUsable(t *testing.T) {
	ctx, _, _, outbox, _, _ := outboxFixture(t)
	state, err := outbox.State(ctx)
	require.NoError(t, err)
	rejected := errors.New("seed reader failed")
	tests := map[string]SeedKeys{
		"reader error": func(func(SeedKey) error) error { return rejected },
		"invalid key":  func(yield func(SeedKey) error) error { return yield(SeedKey{Table: "unknown"}) },
		"duplicate key": func(yield func(SeedKey) error) error {
			key := SeedKey{Table: "guilds", Key: []store.NativeValue{{Kind: "text", Text: "g1"}}}
			if err := yield(key); err != nil {
				return err
			}
			return yield(key)
		},
	}
	for name, seed := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "outbox.sqlite")
			_, err := InitOutbox(ctx, path, state.Registration, seed)
			require.Error(t, err)
			_, err = OpenOutbox(ctx, path)
			require.Error(t, err, "failed seed must not leave a usable binding")
		})
	}
}

func TestOutboxRejectsCorruptPersistedBinding(t *testing.T) {
	tests := map[string]func(*OutboxState){
		"registration JSON":    nil,
		"invalid registration": func(s *OutboxState) { s.Registration.Archive = "" },
		"invalid baseline":     func(s *OutboxState) { s.Baseline.Cursor.Generations = nil },
		"different source":     func(s *OutboxState) { s.Baseline.SourceID = "other-source" },
		"rewound baseline":     func(s *OutboxState) { s.Registration.Baseline.Cursor.Sequence++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, _, _, outbox, path, _ := outboxFixture(t)
			state, err := outbox.State(ctx)
			require.NoError(t, err)
			// The two decoded baselines have independent generation slices.
			if mutate != nil {
				mutate(&state)
			}
			registration, err := json.Marshal(state.Registration)
			require.NoError(t, err)
			baseline, err := json.Marshal(state.Baseline)
			require.NoError(t, err)
			if mutate == nil {
				registration = []byte("{")
			}
			_, err = outbox.base.DB().ExecContext(ctx, `update binding set registration=?,baseline=?`, registration, baseline)
			require.NoError(t, err)
			_, err = outbox.State(ctx)
			require.Error(t, err)
			_, err = OpenOutbox(ctx, path)
			require.Error(t, err)
		})
	}
}
