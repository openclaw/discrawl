package share

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// An external snapshot owner can replace this checkout to bound its object
// store. Discrawl may import it under that owner's lock, but must not fetch or
// publish into it and recreate history outside that lifecycle.
func requireWritableRepo(ctx context.Context, opts Options) error {
	if strings.TrimSpace(opts.RepoPath) == "" {
		return nil
	}
	if _, err := os.Stat(filepath.Join(opts.RepoPath, ".git")); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect share checkout ownership: %w", err)
	}
	cmd := exec.CommandContext(ctx, "git", "config", "--local", "--default", "", "--get", "crawlkit.snapshot-cache-owner")
	cmd.Dir = opts.RepoPath
	body, err := cmd.Output()
	if err != nil {
		return errors.New("cannot verify share checkout ownership")
	}
	if strings.TrimSpace(string(body)) != "" {
		return errors.New("share checkout is managed by an external snapshot owner; refresh with that owner and run update --no-fetch under its lock")
	}
	return nil
}
