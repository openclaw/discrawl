// Package sqlitedurable configures persistent writers without changing readers.
package sqlitedurable

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	crawlstore "github.com/openclaw/crawlkit/store"
)

// Open retains Crawlkit's schema, journal, pool and busy-timeout behavior. The
// pinned modernc driver applies _synchronous after all _pragma values, including
// Crawlkit's NORMAL default, on every physical connection (also replacements).
func Open(ctx context.Context, opts crawlstore.Options) (*crawlstore.Store, error) {
	path, filename, err := writerURI(opts.Path)
	if err != nil {
		return nil, err
	}
	opts.Path = path
	s, err := crawlstore.Open(ctx, opts)
	if err != nil {
		return nil, err
	}
	// Match Crawlkit: tighten an existing file only after a successful open.
	if filename != "" && runtime.GOOS != "windows" {
		if err := os.Chmod(filename, 0o600); err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("chmod sqlite writer: %w", err)
		}
	}
	var synchronous int
	err = s.DB().QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous)
	if err == nil && synchronous != 2 {
		err = fmt.Errorf("sqlite writer synchronous=%d, require FULL", synchronous)
	}
	if err == nil && runtime.GOOS == "darwin" {
		for _, pragma := range []string{"fullfsync", "checkpoint_fullfsync"} {
			var value int
			if err = s.DB().QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&value); err != nil {
				break
			}
			if value != 1 {
				err = fmt.Errorf("sqlite writer %s=%d, require 1", pragma, value)
				break
			}
		}
	}
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("verify sqlite writer durability: %w", err)
	}
	return s, nil
}

func writerURI(path string) (string, string, error) {
	// Match the pinned Crawlkit Open contract, including its whitespace trim.
	path = strings.TrimSpace(path)
	if path == "" {
		return "", "", errors.New("sqlite path is required")
	}
	var u *url.URL
	var err error
	switch {
	case path == ":memory:":
		u = &url.URL{Scheme: "file", Opaque: "discrawl-" + rand.Text(), RawQuery: "mode=memory&cache=shared"}
	case strings.HasPrefix(path, "file:"):
		u, err = url.Parse(path)
		if err != nil {
			return "", "", fmt.Errorf("parse sqlite writer path: %w", err)
		}
	default:
		// filepath.Abs/Join clean .. before the filesystem follows symlinks,
		// which can select a different existing database. Preserve components.
		if runtime.GOOS == "windows" {
			// Windows also has drive-relative and current-drive-rooted paths;
			// use its full-path resolver instead of treating them as relative.
			path, err = filepath.Abs(path)
			if err != nil {
				return "", "", err
			}
		} else if !filepath.IsAbs(path) {
			cwd, err := os.Getwd()
			if err != nil {
				return "", "", err
			}
			path = cwd + string(filepath.Separator) + path
		}
		path = filepath.ToSlash(path)
		if runtime.GOOS == "windows" && !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		u = &url.URL{Scheme: "file", Path: path}
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", "", fmt.Errorf("parse sqlite writer options: %w", err)
	}
	if q.Get("mode") == "ro" || q.Get("immutable") == "1" {
		return "", "", errors.New("read-only sqlite URI passed to writer")
	}
	if u.Host != "" && u.Host != "localhost" {
		return "", "", errors.New("sqlite writer requires a local file")
	}
	filename := u.Path
	if u.Opaque != "" {
		filename, err = url.PathUnescape(u.Opaque)
		if err != nil {
			return "", "", err
		}
	}
	if q.Get("mode") != "memory" && filename != ":memory:" {
		if runtime.GOOS == "windows" && len(filename) > 2 && filename[0] == '/' && filename[2] == ':' {
			filename = filename[1:]
		}
		if filename == "" {
			return "", "", errors.New("sqlite file path is required")
		}
		filename = filepath.FromSlash(filename)
		if err := prepareFile(filename, q.Get("mode") == "rw"); err != nil {
			return "", "", err
		}
	}
	if q.Get("mode") == "memory" || filename == ":memory:" {
		filename = ""
	}
	q.Set("_synchronous", "FULL")
	q.Del("_sync")
	if runtime.GOOS == "darwin" {
		// Remove caller overrides before adding our flush policy. modernc sorts
		// _pragma entries, so appending alone would not establish precedence.
		pragmas := q["_pragma"][:0]
		for _, value := range q["_pragma"] {
			name := strings.ToLower(strings.TrimSpace(value))
			if i := strings.IndexAny(name, "(="); i >= 0 {
				name = strings.TrimSpace(name[:i])
			}
			if name != "fullfsync" && name != "checkpoint_fullfsync" {
				pragmas = append(pragmas, value)
			}
		}
		q["_pragma"] = append(pragmas, "fullfsync(1)", "checkpoint_fullfsync(1)")
	}
	u.RawQuery = q.Encode()
	return u.String(), filename, nil
}

// Crawlkit skips file preparation for file: URIs; retain private creation and
// tightening here before the URI can open its first writable connection.
func prepareFile(path string, existingOnly bool) error {
	if _, err := os.Stat(path); err != nil {
		if !errors.Is(err, os.ErrNotExist) || existingOnly {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if f != nil {
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}
