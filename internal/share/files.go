package share

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func regularMediaFile(root, path, label string) (os.FileInfo, error) {
	return regularFileInRoot(root, path, label, "media")
}

func regularFileInRoot(root, path, label, kind string) (os.FileInfo, error) {
	root = filepath.Clean(root)
	path = filepath.Clean(path)
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || rel == "" || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return nil, fmt.Errorf("%w: %s %s escapes %s root", errUnsafeMediaPath, kind, label, kind)
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("%w: %s root for %s is not a directory", errUnsafeMediaPath, kind, label)
	}
	current := root
	parts := strings.Split(rel, string(filepath.Separator))
	for i, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, fmt.Errorf("%w: invalid %s path %q", errUnsafeMediaPath, kind, label)
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if i == len(parts)-1 {
				return nil, fmt.Errorf("%w: %s %s is not a regular file", errUnsafeMediaPath, kind, label)
			}
			return nil, fmt.Errorf("%w: %s %s contains symlinked path component", errUnsafeMediaPath, kind, label)
		}
		if i < len(parts)-1 {
			if !info.IsDir() {
				return nil, fmt.Errorf("%w: %s %s parent is not a directory", errUnsafeMediaPath, kind, label)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%w: %s %s is not a regular file", errUnsafeMediaPath, kind, label)
		}
		return info, nil
	}
	return nil, fmt.Errorf("%w: invalid %s path %q", errUnsafeMediaPath, kind, label)
}

func copyFile(target, source string) error {
	src, err := os.Open(source) // #nosec G304 -- source is constrained by media path helpers.
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	return writeAtomicFile(target, func(tmp *os.File) error {
		_, err := io.Copy(tmp, src)
		return err
	})
}

func writeAtomicFile(target string, write func(*os.File) error) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".copy-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if err := write(tmp); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, target); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func copyWithLimit(dst io.Writer, src io.Reader, limit int64) error {
	if limit <= 0 {
		return errors.New("media decompression limit must be positive")
	}
	n, err := io.Copy(dst, io.LimitReader(src, limit+1))
	if err != nil {
		return err
	}
	if n > limit {
		return fmt.Errorf("media decompressed size exceeds %d bytes", limit)
	}
	return nil
}

type limitedReader struct {
	r     io.Reader
	limit int64
	n     int64
	label string
}

func (r *limitedReader) Read(p []byte) (int, error) {
	if r.limit <= 0 {
		return 0, fmt.Errorf("%s decompression limit must be positive", r.label)
	}
	remaining := r.limit + 1 - r.n
	if remaining <= 0 {
		return 0, fmt.Errorf("%s decompressed size exceeds %d bytes", r.label, r.limit)
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.r.Read(p)
	r.n += int64(n)
	if r.n > r.limit {
		return n, fmt.Errorf("%s decompressed size exceeds %d bytes", r.label, r.limit)
	}
	return n, err
}

func sameFileHash(path, hash string) bool {
	current, err := fileSHA256(path)
	return err == nil && current == hash
}
