package cli

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strings"

	"github.com/openclaw/crawlkit/embed"
	"github.com/openclaw/discrawl/internal/config"
)

func newEmbeddingProvider(cfg config.EmbeddingsConfig) (embed.Provider, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.APIKeySource)) {
	case "", "env":
		// Preserve Crawlkit's provider-specific optional/required env behavior.
		return embed.NewProvider(crawlkitEmbeddingConfig(cfg))
	case "keyring":
		key, err := config.ResolveEmbeddingKeyringAPIKey(cfg)
		if err != nil {
			return nil, err
		}
		return embed.NewProvider(crawlkitEmbeddingConfig(cfg), embed.WithAPIKey(key))
	default:
		return nil, errors.New("unsupported embedding api_key_source; use env or keyring")
	}
}

func checkEmbeddingProvider(ctx context.Context, cfg config.EmbeddingsConfig) embed.CheckResult {
	if cfg.APIKeySource == "" || cfg.APIKeySource == "env" {
		return embed.CheckProvider(ctx, crawlkitEmbeddingConfig(cfg))
	}
	result := embed.CheckResult{Provider: cfg.Provider, Model: cfg.Model, BaseURL: cfg.BaseURL, Status: "ok"}
	provider, err := newEmbeddingProvider(cfg)
	if err != nil {
		result.Status, result.Warning = "warning", err.Error()
		return result
	}
	// CheckProvider has no per-call credential option. Keep its local-only
	// probe boundary without exporting a keyring credential into the environment.
	probe := cfg.Provider == embed.ProviderOllama || cfg.Provider == embed.ProviderLlamaCpp
	if cfg.Provider == embed.ProviderOpenAICompatible {
		u, parseErr := url.Parse(cfg.BaseURL)
		if parseErr == nil {
			host := u.Hostname()
			probe = host == "localhost" || net.ParseIP(host).IsLoopback()
		}
	}
	if probe {
		probeCtx, cancel := context.WithTimeout(ctx, embed.DefaultProbeTimeout)
		defer cancel()
		if _, err := provider.Embed(probeCtx, []string{"discrawl probe"}); err != nil {
			result.Status, result.Warning = "warning", "embedding provider probe failed"
		} else {
			result.Probed = true
		}
	}
	return result
}
