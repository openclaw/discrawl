package share

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

// Generation names are private staging identities, not subscriber checkpoints.
// Only freshly exported tables pass through here; prior manifests stay intact.
func normalizePublicationTables(stage string, manifest *Manifest) error {
	if _, err := publicationPaths(*manifest); err != nil {
		return err
	}
	renames := map[string]string{}
	targets := map[string]string{}
	tables := map[string]bool{}
	for _, table := range manifest.Tables {
		if tables[table.Name] {
			return fmt.Errorf("duplicate publication table %q", table.Name)
		}
		tables[table.Name] = true
		names := append([]string(nil), table.Files...)
		if table.File != "" {
			names = append(names, table.File)
		}
		for _, file := range table.FileManifests {
			names = append(names, file.Path)
		}
		for _, name := range names {
			target := name
			if strings.HasPrefix(name, "tables/.generations/") {
				target = path.Join("tables", table.Name, path.Base(name))
			}
			if prior, ok := targets[target]; ok && prior != name {
				return fmt.Errorf("ambiguous publication shard %q", target)
			}
			targets[target] = name
			renames[name] = target
		}
	}
	// Validate every source and collision before moving even private stage files.
	names := make([]string, 0, len(renames))
	for name, target := range renames {
		if _, err := regularFileInRoot(stage, filepath.Join(stage, filepath.FromSlash(name)), name, "publication"); err != nil {
			return err
		}
		if name != target {
			if _, ok := manifest.Files[target]; ok {
				return fmt.Errorf("publication alias collision %q", target)
			}
			_, err := regularFileInRoot(stage, filepath.Join(stage, filepath.FromSlash(target)), target, "publication")
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("publication staging collision %q: %w", target, errors.Join(os.ErrExist, err))
			}
		}
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		target := renames[name]
		if name == target {
			continue
		}
		dest := filepath.Join(stage, filepath.FromSlash(target))
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return err
		}
		if err := os.Rename(filepath.Join(stage, filepath.FromSlash(name)), dest); err != nil {
			return err
		}
		if hash, ok := manifest.Files[name]; ok {
			manifest.Files[target] = hash
			delete(manifest.Files, name)
		}
	}
	for i := range manifest.Tables {
		table := &manifest.Tables[i]
		if table.File != "" {
			table.File = renames[table.File]
		}
		for j, name := range table.Files {
			table.Files[j] = renames[name]
		}
		for j := range table.FileManifests {
			file := &table.FileManifests[j]
			file.Path = renames[file.Path]
		}
	}
	return nil
}
