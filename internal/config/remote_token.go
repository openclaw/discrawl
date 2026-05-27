package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/zalando/go-keyring"
)

const (
	DefaultRemoteTokenKeyringService = "crawl-remote"
	DefaultRemoteTokenKeyringAccount = "discrawl"
)

var (
	remoteTokenKeyringGet = keyring.Get
	remoteTokenKeyringSet = keyring.Set
)

func ResolveRemoteToken(cfg Config) (TokenResolution, error) {
	if err := cfg.Normalize(); err != nil {
		return TokenResolution{}, err
	}
	source := strings.TrimSpace(cfg.Remote.Auth.TokenSource)
	switch source {
	case "", "env":
		if token := strings.TrimSpace(os.Getenv(cfg.Remote.TokenEnv)); token != "" {
			return TokenResolution{Token: token, Source: "env", Path: cfg.Remote.TokenEnv}, nil
		}
		if cfg.Remote.Auth.KeyringService != "" || cfg.Remote.Auth.KeyringAccount != "" {
			return resolveRemoteTokenFromKeyring(cfg.Remote)
		}
		return TokenResolution{}, nil
	case "keyring":
		return resolveRemoteTokenFromKeyring(cfg.Remote)
	case "none":
		return TokenResolution{}, crawlremote.ErrMissingToken
	default:
		return TokenResolution{}, fmt.Errorf("unsupported remote token_source %q", source)
	}
}

func StoreRemoteToken(cfg Config, token string) (crawlremote.AuthConfig, error) {
	auth := normalizedRemoteAuth(cfg.Remote)
	token = strings.TrimSpace(token)
	if token == "" {
		return auth, errors.New("remote token is empty")
	}
	if err := remoteTokenKeyringSet(auth.KeyringService, auth.KeyringAccount, token); err != nil {
		return auth, err
	}
	auth.TokenSource = "keyring"
	return auth, nil
}

func resolveRemoteTokenFromKeyring(remote crawlremote.Config) (TokenResolution, error) {
	auth := normalizedRemoteAuth(remote)
	raw, err := remoteTokenKeyringGet(auth.KeyringService, auth.KeyringAccount)
	if err != nil {
		return TokenResolution{}, err
	}
	token := strings.TrimSpace(raw)
	if token == "" {
		return TokenResolution{}, errors.New("keyring item is empty")
	}
	return TokenResolution{
		Token:  token,
		Source: "keyring",
		Path:   auth.KeyringService + "/" + auth.KeyringAccount,
	}, nil
}

func normalizedRemoteAuth(remote crawlremote.Config) crawlremote.AuthConfig {
	remote.Normalize()
	auth := remote.Auth
	if strings.TrimSpace(auth.KeyringService) == "" {
		auth.KeyringService = DefaultRemoteTokenKeyringService
	}
	if strings.TrimSpace(auth.KeyringAccount) == "" {
		auth.KeyringAccount = defaultRemoteTokenAccount(remote)
	}
	return auth
}

func defaultRemoteTokenAccount(remote crawlremote.Config) string {
	if archive := strings.TrimSpace(remote.Archive); archive != "" {
		return "discrawl:" + archive
	}
	if endpoint := strings.TrimSpace(remote.Endpoint); endpoint != "" {
		if parsed, err := url.Parse(endpoint); err == nil && parsed.Host != "" {
			return "discrawl:" + parsed.Host
		}
	}
	return DefaultRemoteTokenKeyringAccount
}
