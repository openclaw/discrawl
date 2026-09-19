//go:build ignore

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/openclaw/discrawl/internal/config"
	discordclient "github.com/openclaw/discrawl/internal/discord"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/openclaw/discrawl/internal/syncer"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Source reconciliation failed; no Git or Cloud publication was requested by this helper.")
		os.Exit(1)
	}
}

func run() error {
	compressed, err := base64.StdEncoding.DecodeString(os.Getenv("DISCRAWL_CLOUD_RECONCILE_PLAN"))
	if err != nil {
		return err
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return err
	}
	defer reader.Close()
	raw, err := io.ReadAll(io.LimitReader(reader, 1024*1024+1))
	if err != nil || len(raw) > 1024*1024 {
		return errors.New("invalid plan size")
	}
	var plan syncer.ReconcilePlan
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return err
	}
	if plan.Archive == "" || plan.Archive != os.Getenv("DISCRAWL_CLOUD_ARCHIVE") {
		return errors.New("archive mismatch")
	}
	for _, row := range plan.Members {
		if row.GuildID != os.Getenv("DISCRAWL_GUILD_ID") {
			return errors.New("member outside configured collector guild")
		}
	}
	for _, row := range plan.Messages {
		if row.GuildID != os.Getenv("DISCRAWL_GUILD_ID") {
			return errors.New("message outside configured collector guild")
		}
	}
	cfg, err := config.Load(os.Getenv("CONFIG"))
	if err != nil {
		return err
	}
	if cfg.DBPath != os.Getenv("DB") {
		return errors.New("runtime mismatch")
	}
	if _, err := os.Stat(cfg.DBPath); err != nil {
		return err
	}
	token, err := config.ResolveDiscordToken(cfg)
	if err != nil {
		return err
	}
	client, err := discordclient.New(token.Token)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	db, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer db.Close()
	stats, err := syncer.ReconcileMissing(ctx, db, client, plan)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(stats)
}
