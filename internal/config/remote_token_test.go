package config

import (
	"errors"
	"strings"
	"testing"

	crawlremote "github.com/openclaw/crawlkit/remote"
)

func TestRemoteTokenResolutionWithKeyring(t *testing.T) {
	originalGet := remoteTokenKeyringGet
	originalSet := remoteTokenKeyringSet
	t.Cleanup(func() {
		remoteTokenKeyringGet = originalGet
		remoteTokenKeyringSet = originalSet
	})

	keyringValues := map[string]string{}
	remoteTokenKeyringGet = func(service, account string) (string, error) {
		value, ok := keyringValues[service+"\x00"+account]
		if !ok {
			return "", errors.New("missing keyring item")
		}
		return value, nil
	}
	remoteTokenKeyringSet = func(service, account, password string) error {
		keyringValues[service+"\x00"+account] = password
		return nil
	}

	cfg := Default()
	cfg.Remote.Endpoint = "https://worker.example.test"
	cfg.Remote.Archive = "discrawl/openclaw"
	cfg.Remote.TokenEnv = "DISCRAWL_TEST_REMOTE_TOKEN"
	t.Setenv("DISCRAWL_TEST_REMOTE_TOKEN", "env-session")

	resolved, err := ResolveRemoteToken(cfg)
	if err != nil {
		t.Fatalf("resolve env token: %v", err)
	}
	if resolved.Token != "env-session" || resolved.Source != "env" || resolved.Path != "DISCRAWL_TEST_REMOTE_TOKEN" {
		t.Fatalf("env token = %#v", resolved)
	}

	t.Setenv("DISCRAWL_TEST_REMOTE_TOKEN", "")
	auth, err := StoreRemoteToken(cfg, " keyring-session ")
	if err != nil {
		t.Fatalf("store token: %v", err)
	}
	if auth.TokenSource != "keyring" || auth.KeyringService != DefaultRemoteTokenKeyringService || auth.KeyringAccount != "discrawl:discrawl/openclaw" {
		t.Fatalf("auth defaults = %#v", auth)
	}
	cfg.Remote.Auth = auth
	resolved, err = ResolveRemoteToken(cfg)
	if err != nil {
		t.Fatalf("resolve keyring token: %v", err)
	}
	if resolved.Token != "keyring-session" || resolved.Source != "keyring" {
		t.Fatalf("keyring token = %#v", resolved)
	}

	cfg.Remote.Auth.TokenSource = "none"
	if _, err := ResolveRemoteToken(cfg); !errors.Is(err, crawlremote.ErrMissingToken) {
		t.Fatalf("none token source error = %v", err)
	}

	cfg.Remote.Auth.TokenSource = "bogus"
	if _, err := ResolveRemoteToken(cfg); err == nil || !strings.Contains(err.Error(), "unsupported remote token_source") {
		t.Fatalf("unsupported token source error = %v", err)
	}

	if _, err := StoreRemoteToken(cfg, " "); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("empty store error = %v", err)
	}
	cfg.Remote.Auth = crawlremote.AuthConfig{TokenSource: "keyring", KeyringService: "svc", KeyringAccount: "empty"}
	keyringValues["svc\x00empty"] = " "
	if _, err := ResolveRemoteToken(cfg); err == nil || !strings.Contains(err.Error(), "keyring item is empty") {
		t.Fatalf("empty keyring error = %v", err)
	}
}

func TestDefaultRemoteTokenAccountFallbacks(t *testing.T) {
	if got := defaultRemoteTokenAccount(crawlremote.Config{Archive: "discrawl/example"}); got != "discrawl:discrawl/example" {
		t.Fatalf("archive account = %q", got)
	}
	if got := defaultRemoteTokenAccount(crawlremote.Config{Endpoint: "https://crawl.example.test/base"}); got != "discrawl:crawl.example.test" {
		t.Fatalf("endpoint account = %q", got)
	}
	if got := defaultRemoteTokenAccount(crawlremote.Config{}); got != DefaultRemoteTokenKeyringAccount {
		t.Fatalf("default account = %q", got)
	}
}
