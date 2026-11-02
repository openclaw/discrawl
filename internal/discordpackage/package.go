// Package discordpackage reads Discord account exports without credentials or
// network access. Account exports contain the account owner's sent messages.
package discordpackage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strconv"

	"github.com/openclaw/discrawl/internal/store"
)

const maxFileBytes = 64 << 20

type Options struct {
	Path      string
	AccountID string
	DryRun    bool
}

type Stats struct {
	AccountID       string `json:"account_id"`
	Coverage        string `json:"coverage"`
	HistoryComplete bool   `json:"history_complete"`
	Channels        int    `json:"channels"`
	SkippedChannels int    `json:"skipped_guild_channels"`
	Messages        int    `json:"messages"`
	Inserted        int    `json:"inserted"`
	Existing        int    `json:"existing"`
	AttachmentLinks int    `json:"messages_with_attachment_links"`
	DryRun          bool   `json:"dry_run"`
}

// Import validates each transcript before committing that channel. On failure,
// stats describe earlier committed channels; rerunning never replaces messages.
func Import(ctx context.Context, st *store.Store, opts Options) (stats Stats, err error) {
	stats.Coverage, stats.DryRun = "sent_only", opts.DryRun
	if !opts.DryRun && st == nil {
		return stats, errors.New("store is required")
	}
	files, closePackage, err := openPackage(opts.Path)
	if err != nil {
		return stats, err
	}
	defer func() { err = errors.Join(err, closePackage()) }()
	base, messagesDir, err := packageDirectories(files)
	if err != nil {
		return stats, err
	}
	stats.AccountID, err = accountID(files, base, opts.AccountID)
	if err != nil {
		return stats, err
	}
	entries, err := fs.ReadDir(files, messagesDir)
	if err != nil {
		return stats, err
	}
	if len(entries) > 100000 {
		return stats, errors.New("too many package channel entries")
	}
	seen := make(map[string]bool)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if !entry.IsDir() {
			if entry.Type()&fs.ModeSymlink != 0 {
				return stats, errors.New("symlink in package channels")
			}
			continue // channel-name index is not message evidence.
		}
		dir := path.Join(messagesDir, entry.Name())
		channel, skip, err := readChannel(files, dir, stats.AccountID)
		if err != nil {
			return stats, fmt.Errorf("read package channel metadata: %w", err)
		}
		if skip {
			stats.SkippedChannels++
			continue
		}
		if seen[channel.ID] {
			return stats, errors.New("duplicate package channel identity")
		}
		seen[channel.ID] = true
		data, err := readFile(files, path.Join(dir, "messages.json"))
		if err != nil {
			return stats, fmt.Errorf("read package messages.json (JSON transcript required): %w", err)
		}
		messages, attachments, err := readMessages(ctx, data, channel.ID, stats.AccountID)
		if err != nil {
			return stats, fmt.Errorf("validate package channel %s: %w", channel.ID, err)
		}
		inserted := 0
		if !opts.DryRun {
			inserted, err = st.ImportPersonalPackageChannel(ctx, channel, messages)
			if err != nil {
				return stats, err
			}
			stats.Existing += len(messages) - inserted
		}
		stats.Inserted += inserted
		stats.Messages += len(messages)
		stats.AttachmentLinks += attachments
		stats.Channels++
	}
	return stats, nil
}

func accountID(files fs.FS, base, supplied string) (string, error) {
	if supplied != "" {
		if _, err := snowflake(json.RawMessage(strconv.Quote(supplied))); err != nil {
			return "", errors.New("invalid --account-id")
		}
	}
	account, err := childDirectory(files, base, "account")
	if errors.Is(err, fs.ErrNotExist) && supplied != "" {
		return supplied, nil
	}
	if err != nil {
		return "", errors.New("account/user.json is required, or supply --account-id for a messages-only package")
	}
	data, err := readFile(files, path.Join(account, "user.json"))
	if err != nil {
		return "", err
	}
	object, err := objectFields(data)
	if err != nil {
		return "", errors.New("invalid package account JSON")
	}
	id, err := snowflake(object["id"])
	if err != nil {
		return "", errors.New("invalid package account ID")
	}
	if supplied != "" && id != supplied {
		return "", errors.New("--account-id conflicts with package account")
	}
	return id, nil
}
