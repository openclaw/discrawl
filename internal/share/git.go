package share

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/openclaw/crawlkit/mirror"
)

func EnsureRepo(ctx context.Context, opts Options) error {
	if strings.TrimSpace(opts.RepoPath) == "" {
		return errors.New("share repo path is empty")
	}
	return mirror.EnsureRepo(ctx, mirrorOptions(opts))
}

func Pull(ctx context.Context, opts Options) error {
	if strings.TrimSpace(opts.Remote) == "" && strings.TrimSpace(opts.RepoPath) == "" {
		return nil
	}
	if strings.TrimSpace(opts.Remote) == "" {
		return EnsureRepo(ctx, opts)
	}
	if err := mirror.EnsureRemote(ctx, mirrorOptions(opts)); err != nil {
		return err
	}
	pullOpts := mirrorOptions(opts)
	pullOpts.Remote = ""
	return mirror.PullCurrent(ctx, pullOpts)
}

func Commit(ctx context.Context, opts Options, message string) (bool, error) {
	return commitPublication(ctx, opts, message)
}

func Push(ctx context.Context, opts Options) error {
	manifest, receipt, err := workingPublicationBinding(opts.RepoPath, opts.Producer)
	if err != nil {
		return err
	}
	if receipt != nil {
		return pushBoundPublication(ctx, opts, manifest, receipt)
	}
	if strings.TrimSpace(opts.Tag) == "" {
		err = mirror.Push(ctx, mirrorOptions(opts))
	} else {
		err = mirror.PushSnapshot(ctx, mirrorOptions(opts), opts.Tag)
	}
	if err != nil {
		branch := opts.Branch
		if strings.TrimSpace(branch) == "" {
			branch = "main"
		}
		return fmt.Errorf("git push -u origin %s: %w", branch, err)
	}
	return nil
}

func ValidateTag(ctx context.Context, opts Options) error {
	if strings.TrimSpace(opts.Tag) == "" {
		return nil
	}
	if strings.TrimSpace(opts.Remote) != "" {
		if err := mirror.EnsureRemote(ctx, mirrorOptions(opts)); err != nil {
			return err
		}
	} else if err := mirror.EnsureRepo(ctx, mirrorOptions(opts)); err != nil {
		return err
	}
	if err := mirror.ValidateTag(ctx, mirrorOptions(opts), opts.Tag); err != nil {
		return err
	}
	if err := mirror.SyncForWrite(ctx, mirrorOptions(opts)); err != nil {
		return err
	}
	return nil
}

func CreateImmutableTag(ctx context.Context, opts Options) (string, error) {
	return mirror.CreateImmutableTag(ctx, mirrorOptions(opts), opts.Tag)
}

func mirrorOptions(opts Options) mirror.Options {
	return mirror.Options{RepoPath: opts.RepoPath, Remote: opts.Remote, Branch: opts.Branch, DirMode: 0o750}
}
