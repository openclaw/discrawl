package main

import (
	"bufio"
	"bytes"
	"context"
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
	if !isContextCanceledExit(err, output) {
		t.Fatalf("expected context-canceled exit from shutdown, got err=%v output=%s", err, output)
	}

	ctx := t.Context()
	s, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
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
		if err := sendShutdownSignal(); err != nil {
			panic(fmt.Sprintf("send shutdown signal: %v", err))
		}
	}()
	main()
}

func writeLargeWiretapCache(t *testing.T, path string, count int) {
	t.Helper()

	file, err := os.Create(path)
	requireNoError(t, err)
	defer func() { requireNoError(t, file.Close()) }()
	writer := bufio.NewWriter(file)
	_, err = fmt.Fprintln(writer, `{"id":"111111111111111117","guild_id":"999999999999999997","type":0,"name":"sigterm-import"}`)
	requireNoError(t, err)
	for i := range count {
		_, err = fmt.Fprintf(
			writer,
			`{"id":"3333333333%09d","channel_id":"111111111111111117","content":"sigterm import message %d","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222228","username":"alice"}}`+"\n",
			i,
			i,
		)
		requireNoError(t, err)
	}
	requireNoError(t, writer.Flush())
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
