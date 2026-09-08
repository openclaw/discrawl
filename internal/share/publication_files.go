package share

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/openclaw/crawlkit/mirror"
	"github.com/openclaw/discrawl/internal/report"
)

var publicationShard = regexp.MustCompile(`^[0-9]{6,}\.jsonl\.gz$`)
var publicationGeneration = regexp.MustCompile(`^[0-9a-f]{32}$`)

func publicationPaths(manifest Manifest) (map[string]bool, error) {
	files := map[string]bool{ManifestName: true}
	add := func(name, prefix string, shard bool) error {
		if !safePublicationPath(name) || !strings.HasPrefix(name, prefix+"/") ||
			(shard && (path.Dir(name) != prefix || !publicationShard.MatchString(path.Base(name)))) {
			return fmt.Errorf("unowned publication path %q", name)
		}
		files[name] = true
		return nil
	}
	for _, table := range manifest.Tables {
		if !slices.Contains(SnapshotTables, table.Name) {
			return nil, fmt.Errorf("unowned publication table %q", table.Name)
		}
		names := append([]string(nil), table.Files...)
		if table.File != "" {
			names = append(names, table.File)
		}
		for _, file := range table.FileManifests {
			names = append(names, file.Path)
		}
		for _, name := range names {
			// Accept legacy layouts and exact manifest-owned generation shards.
			if name == "tables/"+table.Name+".jsonl" || name == "tables/"+table.Name+".jsonl.gz" {
				files[name] = true
				continue
			}
			prefix := "tables/" + table.Name
			parts := strings.Split(name, "/")
			if len(parts) == 5 && parts[0] == "tables" && parts[1] == ".generations" &&
				publicationGeneration.MatchString(parts[2]) && parts[3] == table.Name {
				prefix = strings.Join(parts[:4], "/")
			}
			if err := add(name, prefix, true); err != nil {
				return nil, err
			}
		}
	}
	for _, embedding := range manifest.Embeddings {
		prefix := path.Join("embeddings", safePathSegment(embedding.Provider),
			safePathSegment(embedding.Model), safePathSegment(embedding.InputVersion))
		for _, name := range embedding.Files {
			if err := add(name, prefix, true); err != nil {
				return nil, err
			}
		}
	}
	if manifest.Media != nil {
		for _, file := range manifest.Media.Files {
			if err := add(file.Path, "media", false); err != nil {
				return nil, err
			}
		}
	}
	return files, nil
}

func safePublicationPath(name string) bool {
	if name == "" || name == "." || path.IsAbs(name) || path.Clean(name) != name ||
		strings.ContainsAny(name, "\\\x00\r\n") || strings.Contains(name, ":") {
		return false
	}
	for _, part := range strings.Split(name, "/") {
		if part == ".." || strings.EqualFold(part, ".git") {
			return false
		}
	}
	return true
}

func previousPublicationPaths(ctx context.Context, repo string, includeRemovedReadme bool) (map[string]bool, error) {
	files := map[string]bool{}
	if _, err := regularFileInRoot(repo, filepath.Join(repo, ManifestName), ManifestName, "publication"); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	current, err := ReadManifest(repo)
	if err != nil && !errors.Is(err, ErrNoManifest) {
		return nil, err
	}
	if err == nil {
		files, err = publicationPaths(current)
		if err != nil {
			return nil, err
		}
	}
	// Include HEAD ownership so a previous --no-commit export cannot conceal
	// managed deletions from the next publication commit.
	head, err := publicationGit(ctx, repo, nil, "rev-parse", "--verify", "--quiet", "HEAD")
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 1 {
			return files, nil // New, unborn archive repository.
		}
		return nil, err
	}
	tree, err := publicationGit(ctx, repo, nil, "ls-tree", "--name-only", strings.TrimSpace(string(head)), "--", ManifestName)
	if err != nil || len(tree) == 0 {
		return files, err
	}
	body, err := publicationGit(ctx, repo, nil, "show", strings.TrimSpace(string(head))+":"+ManifestName)
	if err != nil {
		return nil, err
	}
	prior, err := parseManifest(body)
	if err != nil {
		return nil, err
	}
	owned, err := publicationPaths(prior)
	if err != nil {
		return nil, err
	}
	for name := range owned {
		files[name] = true
	}
	if includeRemovedReadme {
		_, err := os.Lstat(filepath.Join(repo, "README.md"))
		if errors.Is(err, os.ErrNotExist) {
			tree, err := publicationGit(ctx, repo, nil, "ls-tree", "--name-only", strings.TrimSpace(string(head)), "--", "README.md")
			if err != nil {
				return nil, err
			}
			if len(tree) > 0 {
				body, err := publicationGit(ctx, repo, nil, "show", strings.TrimSpace(string(head))+":README.md")
				if err != nil {
					return nil, err
				}
				if strings.Contains(string(body), report.StartMarker) && strings.Contains(string(body), report.EndMarker) {
					files["README.md"] = true
				}
			}
		} else if err != nil {
			return nil, err
		}
	}
	return files, nil
}

func installPublication(repo, stage string, previous map[string]bool, manifest Manifest) error {
	current, err := publicationPaths(manifest)
	if err != nil {
		return err
	}
	all := map[string]bool{}
	for name := range previous {
		all[name] = true
	}
	for name := range current {
		all[name] = true
	}
	for name := range all {
		if _, err := regularFileInRoot(repo, filepath.Join(repo, filepath.FromSlash(name)), name, "publication"); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	for _, name := range sortedPublicationPaths(current) {
		if name == ManifestName {
			continue
		}
		if err := copyFile(filepath.Join(repo, filepath.FromSlash(name)), filepath.Join(stage, filepath.FromSlash(name))); err != nil {
			return fmt.Errorf("install publication %s: %w", name, err)
		}
	}
	for name := range previous {
		if !current[name] {
			if err := os.Remove(filepath.Join(repo, filepath.FromSlash(name))); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove previous publication %s: %w", name, err)
			}
		}
	}
	return copyFile(filepath.Join(repo, ManifestName), filepath.Join(stage, ManifestName))
}

func commitPublication(ctx context.Context, opts Options, message string) (bool, error) {
	paths, err := previousPublicationPaths(ctx, opts.RepoPath, opts.Filter.Active())
	if err != nil {
		return false, err
	}
	if opts.ReadmePath != "" {
		if !safePublicationPath(opts.ReadmePath) || strings.TrimSpace(opts.ReadmePath) != opts.ReadmePath {
			return false, fmt.Errorf("invalid generated README path %q", opts.ReadmePath)
		}
		paths[opts.ReadmePath] = true
	}
	index, err := publicationGit(ctx, opts.RepoPath, nil, "ls-files", "-z")
	if err != nil {
		return false, err
	}
	indexed := map[string]bool{}
	for _, name := range strings.Split(string(index), "\x00") {
		indexed[name] = true
	}
	var selected []string
	for _, name := range sortedPublicationPaths(paths) {
		_, err := regularFileInRoot(opts.RepoPath, filepath.Join(opts.RepoPath, filepath.FromSlash(name)), name, "publication")
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		if err == nil || indexed[name] {
			selected = append(selected, ":(literal)"+name)
		}
	}
	deleted, err := publicationGit(ctx, opts.RepoPath, nil, "diff", "--cached", "--diff-filter=D", "--name-only", "--no-renames", "-z")
	if err != nil {
		return false, err
	}
	var restage []string
	for _, name := range strings.Split(string(deleted), "\x00") {
		if paths[name] && !indexed[name] {
			restage = append(restage, name)
			if !slices.Contains(selected, ":(literal)"+name) {
				selected = append(selected, ":(literal)"+name)
			}
		}
	}
	// CommitPaths stages its pathspecs itself. Reintroduce only owned deletions
	// into the index so Git add can find them; the helper then stages their removal.
	if len(restage) > 0 {
		if _, err := publicationGit(ctx, opts.RepoPath, restage, "restore", "--staged", "--source=HEAD",
			"--pathspec-from-file=-", "--pathspec-file-nul"); err != nil {
			return false, err
		}
	}
	return mirror.CommitPaths(ctx, mirrorOptions(opts), message, selected)
}

func sortedPublicationPaths(paths map[string]bool) []string {
	names := make([]string, 0, len(paths))
	for name := range paths {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func publicationGit(ctx context.Context, repo string, paths []string, args ...string) ([]byte, error) {
	// NUL-delimited literal paths preserve filename metacharacters.
	cmd := exec.CommandContext(ctx, "git", append([]string{"--literal-pathspecs", "-C", repo}, args...)...)
	if len(paths) > 0 {
		cmd.Stdin = strings.NewReader(strings.Join(paths, "\x00") + "\x00")
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("publication git %s: %w: %s", args[0], err, strings.TrimSpace(string(out)))
	}
	return out, nil
}
