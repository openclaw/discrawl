package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/zalando/go-keyring"
)

var discordTokenKeyringGet = keyring.Get

func ResolveDiscordToken(cfg Config) (TokenResolution, error) {
	if err := cfg.Normalize(); err != nil {
		return TokenResolution{}, err
	}
	switch cfg.Discord.TokenSource {
	case "none":
		return TokenResolution{}, errors.New("discord token disabled by config")
	case "env":
		envToken := NormalizeBotToken(os.Getenv(cfg.Discord.TokenEnv))
		if envToken != "" {
			return TokenResolution{Token: envToken, Source: "env", Path: cfg.Discord.TokenEnv}, nil
		}
		token, err := resolveDiscordTokenFromKeyring(cfg)
		if err == nil {
			return token, nil
		}
		return TokenResolution{}, fmt.Errorf(
			"discord token not found in environment variable %q or keyring item %q/%q: %w",
			cfg.Discord.TokenEnv,
			cfg.Discord.TokenKeyringService,
			cfg.Discord.TokenKeyringAccount,
			err,
		)
	case "keyring":
		return resolveDiscordTokenFromKeyring(cfg)
	case "file":
		raw, err := readProtectedCredential(cfg.Discord.TokenFile)
		if err != nil {
			return TokenResolution{}, err
		}
		token := NormalizeBotToken(raw)
		if token == "" {
			return TokenResolution{}, errors.New("discord credential file is empty")
		}
		return TokenResolution{Token: token, Source: "file", Path: cfg.Discord.TokenFile}, nil
	default:
		return TokenResolution{}, fmt.Errorf("unsupported discord token_source %q", cfg.Discord.TokenSource)
	}
}

func resolveDiscordTokenFromKeyring(cfg Config) (TokenResolution, error) {
	raw, err := discordTokenKeyringGet(cfg.Discord.TokenKeyringService, cfg.Discord.TokenKeyringAccount)
	if err != nil {
		return TokenResolution{}, err
	}
	token := NormalizeBotToken(raw)
	if token == "" {
		return TokenResolution{}, errors.New("keyring item is empty")
	}
	return TokenResolution{
		Token:  token,
		Source: "keyring",
		Path:   cfg.Discord.TokenKeyringService + "/" + cfg.Discord.TokenKeyringAccount,
	}, nil
}

func NormalizeBotToken(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "Bot ")
	return strings.TrimSpace(raw)
}
