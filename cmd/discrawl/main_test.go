package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/store"
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

func TestMainCancelsWatchOnSIGTERM(t *testing.T) {
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
		go func() {
			time.Sleep(50 * time.Millisecond)
			process, err := os.FindProcess(os.Getpid())
			if err == nil {
				_ = process.Signal(syscall.SIGTERM)
			}
		}()
		main()
		return
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.CommandContext(t.Context(), exe, "-test.run=TestMainCancelsWatchOnSIGTERM")
	cmd.Env = append(os.Environ(), "DISCRAWL_MAIN_SIGNAL_CHILD=1")
	output, err := cmd.CombinedOutput()
	if isContextCanceledExit(err, output) {
		return
	}
	if err != nil {
		t.Fatalf("expected graceful SIGTERM cancellation, got %v", err)
	}
}

func TestMainCancelsWiretapImportOnSIGTERMWithoutCorruptingDB(t *testing.T) {
	if dir := os.Getenv("DISCRAWL_MAIN_IMPORT_SIGNAL_DIR"); dir != "" {
		runWiretapImportSignalChild(t, dir)
		return
	}

	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.CommandContext(t.Context(), exe, "-test.run=TestMainCancelsWiretapImportOnSIGTERMWithoutCorruptingDB")
	cmd.Env = append(os.Environ(), "DISCRAWL_MAIN_IMPORT_SIGNAL_DIR="+dir)
	output, err := cmd.CombinedOutput()
	if !isContextCanceledExit(err, output) {
		t.Fatalf("expected context-canceled exit from SIGTERM, got err=%v output=%s", err, output)
	}

	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	if err != nil {
		t.Fatalf("open db after SIGTERM: %v output=%s", err, output)
	}
	defer func() { _ = s.Close() }()
	_, rows, err := s.ReadOnlyQuery(ctx, "pragma quick_check")
	if err != nil {
		t.Fatalf("quick_check after SIGTERM: %v output=%s", err, output)
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != "ok" {
		t.Fatalf("quick_check after SIGTERM = %#v output=%s", rows, output)
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
	writeLargeWiretapCache(t, filepath.Join(cachePath, "000001.log"), 50000)

	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })
	os.Args = []string{"discrawl", "--config", cfgPath, "wiretap", "--path", cfg.Desktop.Path}
	go func() {
		time.Sleep(15 * time.Millisecond)
		process, err := os.FindProcess(os.Getpid())
		if err == nil {
			_ = process.Signal(syscall.SIGTERM)
		}
	}()
	main()
}

func writeLargeWiretapCache(t *testing.T, path string, count int) {
	t.Helper()

	file, err := os.Create(path)
	requireNoError(t, err)
	defer func() { requireNoError(t, file.Close()) }()
	_, err = fmt.Fprintln(file, `{"id":"111111111111111117","guild_id":"999999999999999997","type":0,"name":"sigterm-import"}`)
	requireNoError(t, err)
	for i := range count {
		_, err = fmt.Fprintf(
			file,
			`{"id":"3333333333%09d","channel_id":"111111111111111117","content":"sigterm import message %d","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222228","username":"alice"}}`+"\n",
			i,
			i,
		)
		requireNoError(t, err)
	}
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
