package config

import (
	"errors"
	"strings"

	"github.com/zalando/go-keyring"
)

var embeddingKeyringGet = keyring.Get

// ResolveEmbeddingFileAPIKey never falls back to a keyring or environment.
func ResolveEmbeddingFileAPIKey(cfg EmbeddingsConfig) (string, error) {
	if strings.ToLower(strings.TrimSpace(cfg.APIKeySource)) != "file" {
		return "", errors.New("embedding file source is not selected")
	}
	raw, err := readProtectedCredential(cfg.APIKeyFile)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", errors.New("embedding credential file is empty")
	}
	return value, nil
}

// ResolveEmbeddingKeyringAPIKey reads only the explicitly selected keyring item.
// Do not normalize this as a Discord bot token or return keyring diagnostics:
// providers need the original API key, and OS errors can contain secret data.
func ResolveEmbeddingKeyringAPIKey(cfg EmbeddingsConfig) (string, error) {
	if strings.ToLower(strings.TrimSpace(cfg.APIKeySource)) != "keyring" {
		return "", errors.New("embedding keyring source is not selected")
	}
	service, account := strings.TrimSpace(cfg.APIKeyKeyringService), strings.TrimSpace(cfg.APIKeyKeyringAccount)
	if service == "" || account == "" {
		return "", errors.New("embedding keyring service and account must be configured")
	}
	value, err := embeddingKeyringGet(service, account)
	if err != nil {
		return "", errors.New("embedding keyring credential is unavailable")
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("embedding keyring credential is empty")
	}
	return value, nil
}
