package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
	"modernc.org/sqlite"
)

func TestMainHelpAndVersion(t *testing.T) {
	if os.Getenv("DISCRAWL_MAIN_ERROR") == "1" {
		os.Args = []string{"discrawl", "bogus"}
		main()
		return
	}

	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })

	os.Args = []string{"discrawl", "help"}
	main()

	os.Args = []string{"discrawl", "--version"}
	main()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.CommandContext(t.Context(), exe, "-test.run=TestMainHelpAndVersion")
	cmd.Env = append(os.Environ(), "DISCRAWL_MAIN_ERROR=1")
	err = cmd.Run()
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		if exitErr.ExitCode() == 2 {
			return
		}
	}
	t.Fatalf("expected exit code 2, got %v", err)
}

func TestMainCancelsWatchOnShutdownSignal(t *testing.T) {
	if os.Getenv("DISCRAWL_MAIN_SIGNAL_CHILD") == "1" {
		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "config.toml")
		cfg := config.Default()
		cfg.DBPath = filepath.Join(dir, "discrawl.db")
		cfg.CacheDir = filepath.Join(dir, "cache")
		cfg.LogDir = filepath.Join(dir, "logs")
		cfg.Desktop.Path = filepath.Join(dir, "discord")
		cfg.Discord.TokenSource = "none"
		cfg.Share.AutoUpdate = false
		requireNoError(t, os.MkdirAll(cfg.Desktop.Path, 0o755))
		requireNoError(t, config.Write(cfgPath, cfg))

		oldArgs := os.Args
		t.Cleanup(func() { os.Args = oldArgs })
		os.Args = []string{"discrawl", "--config", cfgPath, "wiretap", "--dry-run", "--watch-every", "1s"}
		reader, writer, err := os.Pipe()
		requireNoError(t, err)
		stdout := os.Stdout
		os.Stdout = writer
		t.Cleanup(func() {
			os.Stdout = stdout
			_ = writer.Close()
			_ = reader.Close()
		})
		done := make(chan error, 1)
		go func() {
			scanner := bufio.NewScanner(io.TeeReader(reader, stdout))
			signaled := false
			for scanner.Scan() {
				if !signaled && scanner.Text() == "dry_run=true" {
					if err := sendShutdownSignal(); err != nil {
						done <- fmt.Errorf("send shutdown signal: %w", err)
						return
					}
					signaled = true
				}
			}
			if err := scanner.Err(); err != nil {
				done <- err
			} else if !signaled {
				done <- errors.New("watch did not complete its first dry run")
			} else {
				done <- nil
			}
		}()
		main()
		requireNoError(t, writer.Close())
		requireNoError(t, <-done)
		return
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := shutdownTestCommand(t, exe, "TestMainCancelsWatchOnShutdownSignal")
	cmd.Env = append(os.Environ(), "DISCRAWL_MAIN_SIGNAL_CHILD=1")
	output, err := cmd.CombinedOutput()
	if !bytes.Contains(output, []byte("\ndry_run=true\n")) {
		t.Fatalf("watch did not complete its first dry run: err=%v output=%s", err, output)
	}
	if isContextCanceledExit(err, output) {
		return
	}
	if err != nil {
		t.Fatalf("expected graceful shutdown cancellation, got %v", err)
	}
}

func TestMainCancelsWiretapImportOnShutdownWithoutCorruptingDB(t *testing.T) {
	if dir := os.Getenv("DISCRAWL_MAIN_IMPORT_SIGNAL_DIR"); dir != "" {
		runWiretapImportSignalChild(t, dir)
		return
	}

	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := shutdownTestCommand(t, exe, "TestMainCancelsWiretapImportOnShutdownWithoutCorruptingDB")
	cmd.Env = append(os.Environ(), "DISCRAWL_MAIN_IMPORT_SIGNAL_DIR="+dir)
	output, err := cmd.CombinedOutput()
	started, startErr := os.ReadFile(filepath.Join(dir, "import-started"))
	if startErr != nil || string(started) != "message insert\n" {
		t.Fatalf("import did not reach a message insert: err=%v output=%s", startErr, output)
	}
	if !isContextCanceledExit(err, output) {
		t.Fatalf("expected context-canceled exit from shutdown, got err=%v output=%s", err, output)
	}

	ctx := t.Context()
	s, err := store.OpenReadOnly(ctx, filepath.Join(dir, "discrawl.db"))
	if err != nil {
		t.Fatalf("open db after shutdown: %v output=%s", err, output)
	}
	defer func() { _ = s.Close() }()
	_, rows, err := s.ReadOnlyQuery(ctx, "pragma quick_check")
	if err != nil {
		t.Fatalf("quick_check after shutdown: %v output=%s", err, output)
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != "ok" {
		t.Fatalf("quick_check after shutdown = %#v output=%s", rows, output)
	}
	var messages int
	if err := s.DB().QueryRowContext(ctx, "SELECT count(*) FROM messages").Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if messages != 0 {
		t.Fatal("interrupted message insert committed", messages)
	}
}

func runWiretapImportSignalChild(t *testing.T, dir string) {
	t.Helper()

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "discrawl.db")
	cfg.CacheDir = filepath.Join(dir, "cache")
	cfg.LogDir = filepath.Join(dir, "logs")
	cfg.Desktop.Path = filepath.Join(dir, "discord")
	cfg.Discord.TokenSource = "none"
	cfg.Share.AutoUpdate = false
	cachePath := filepath.Join(cfg.Desktop.Path, "Local Storage", "leveldb")
	requireNoError(t, os.MkdirAll(cachePath, 0o755))
	requireNoError(t, config.Write(cfgPath, cfg))
	requireNoError(t, os.WriteFile(filepath.Join(cachePath, "000001.log"), []byte(`
{"id":"111111111111111111","type":1,"recipients":[{"id":"222222222222222222","username":"alice"}]}
{"id":"333333333333333333","channel_id":"111111111111111111","content":"signal import fixture","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222222","username":"alice"}}
`), 0o600))
	requireNoError(t, sqlite.RegisterScalarFunction("signal_import_fixture", 0, func(_ *sqlite.FunctionContext, _ []driver.Value) (driver.Value, error) {
		if err := os.WriteFile(filepath.Join(dir, "import-started"), []byte("message insert\n"), 0o600); err != nil {
			return nil, err
		}
		return nil, sendShutdownSignal()
	}))
	fixture, err := store.Open(t.Context(), cfg.DBPath)
	requireNoError(t, err)
	t.Cleanup(func() { _ = fixture.Close() })
	// Keep the first import write in flight until its real context is canceled.
	_, err = fixture.DB().ExecContext(t.Context(), `CREATE TRIGGER interrupt_import BEFORE INSERT ON messages BEGIN
SELECT signal_import_fixture();
SELECT count(*) FROM (WITH RECURSIVE waiting(n) AS (SELECT 1 UNION ALL SELECT 1 FROM waiting) SELECT n FROM waiting);
END`)
	requireNoError(t, err)
	requireNoError(t, fixture.Close())

	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })
	os.Args = []string{"discrawl", "--config", cfgPath, "wiretap", "--path", cfg.Desktop.Path}
	main()
}

func isContextCanceledExit(err error, output []byte) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == 1 && bytes.Contains(output, []byte("context canceled"))
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func shutdownTestCommand(t *testing.T, exe, testName string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, exe, "-test.run=^"+testName+"$")
	configureShutdownChild(cmd)
	return cmd
}
