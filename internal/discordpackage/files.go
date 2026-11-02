package discordpackage

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
)

func openPackage(filename string) (fs.FS, func() error, error) {
	info, err := os.Stat(filename)
	if err != nil {
		return nil, nil, err
	}
	if info.IsDir() {
		root, err := os.OpenRoot(filename)
		if err != nil {
			return nil, nil, err
		}
		return root.FS(), root.Close, nil
	}
	r, err := zip.OpenReader(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("open Discord package ZIP: %w", err)
	}
	var total uint64
	seen := make(map[string]bool)
	if len(r.File) > 100000 {
		_ = r.Close()
		return nil, nil, errors.New("too many package ZIP entries")
	}
	for _, f := range r.File {
		name := strings.TrimSuffix(f.Name, "/")
		total += f.UncompressedSize64
		if !fs.ValidPath(name) || strings.Contains(name, "\\") || f.Mode()&fs.ModeSymlink != 0 || seen[name] || f.UncompressedSize64 > maxFileBytes || total > 2<<30 {
			_ = r.Close()
			return nil, nil, errors.New("unsafe, duplicate or oversized package ZIP entry")
		}
		seen[name] = true
	}
	return r, r.Close, nil
}

func readFile(files fs.FS, name string) ([]byte, error) {
	info, err := fs.Stat(files, name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxFileBytes {
		return nil, errors.New("package file is not regular or exceeds 64 MiB")
	}
	f, err := files.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, maxFileBytes+1))
	if err == nil && len(data) > maxFileBytes {
		err = errors.New("package file exceeds 64 MiB")
	}
	return data, err
}

func childDirectory(files fs.FS, parent, name string) (string, error) {
	entries, err := fs.ReadDir(files, parent)
	if err != nil {
		return "", err
	}
	found := ""
	for _, e := range entries {
		if strings.EqualFold(e.Name(), name) {
			if !e.IsDir() || found != "" {
				return "", errors.New("ambiguous or non-directory package folder")
			}
			found = path.Join(parent, e.Name())
		}
	}
	if found == "" {
		return "", fs.ErrNotExist
	}
	return found, nil
}

func packageDirectories(files fs.FS) (string, string, error) {
	if dir, err := childDirectory(files, ".", "messages"); err == nil {
		return ".", dir, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", "", err
	}
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return "", "", err
	}
	base, messages := "", ""
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir, err := childDirectory(files, entry.Name(), "messages")
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return "", "", err
			}
			continue
		}
		if base != "" {
			return "", "", errors.New("multiple package roots")
		}
		base, messages = entry.Name(), dir
	}
	if base == "" {
		return "", "", errors.New("discord package messages directory not found")
	}
	return base, messages, nil
}
