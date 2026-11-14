package store

import (
	"encoding/json"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

type extractedMemberProfile struct {
	Bio         string
	Pronouns    string
	Location    string
	Website     string
	XHandle     string
	GitHubLogin string
	URLs        []string
	SearchText  string
}

func memberProfileSearchText(raw string) string {
	return extractMemberProfile(raw).SearchText
}

func extractMemberProfile(raw string) extractedMemberProfile {
	if strings.TrimSpace(raw) == "" {
		return extractedMemberProfile{}
	}
	var payload any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return extractedMemberProfile{SearchText: raw}
	}
	acc := memberProfileAccumulator{
		fields: map[string][]string{},
		seen:   map[string]struct{}{},
		urls:   map[string]struct{}{},
	}
	acc.walk("", payload)
	urls := acc.urlList()
	return extractedMemberProfile{
		Bio:         acc.first("bio", "about", "about_me", "description"),
		Pronouns:    acc.first("pronouns"),
		Location:    acc.first("location"),
		Website:     firstNonEmptyString(acc.first("website", "site", "homepage", "blog", "url"), firstNonSocialURL(urls)),
		XHandle:     firstNonEmptyString(normalizeXHandle(acc.first("twitter", "twitter_username", "x", "x_handle", "x_username")), socialLoginFromURLs(urls, "x.com", "twitter.com")),
		GitHubLogin: firstNonEmptyString(normalizeGitHubLogin(acc.first("github", "github_username", "github_login")), socialLoginFromURLs(urls, "github.com")),
		URLs:        urls,
		SearchText:  strings.Join(acc.terms, " "),
	}
}

type memberProfileAccumulator struct {
	fields map[string][]string
	terms  []string
	seen   map[string]struct{}
	urls   map[string]struct{}
}

func (a *memberProfileAccumulator) walk(key string, value any) {
	switch v := value.(type) {
	case map[string]any:
		for _, k := range sortedMapKeys(v) {
			a.walk(k, v[k])
		}
	case []any:
		for _, item := range v {
			a.walk(key, item)
		}
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return
		}
		lowerKey := strings.ToLower(strings.TrimSpace(key))
		if shouldIgnoreProfileValue(lowerKey, text) {
			return
		}
		a.addTerm(text)
		if lowerKey != "" {
			a.fields[lowerKey] = appendUnique(a.fields[lowerKey], text)
		}
		if parsed, err := url.Parse(text); err == nil && parsed.Host != "" {
			a.urls[text] = struct{}{}
		}
	case float64:
		if strings.EqualFold(key, "accent_color") && v != 0 {
			a.addTerm(strconv.FormatInt(int64(v), 10))
		}
	case bool:
		if strings.EqualFold(key, "verified") && v {
			a.addTerm("verified")
		}
	}
}

func (a *memberProfileAccumulator) addTerm(value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	if _, ok := a.seen[value]; ok {
		return
	}
	a.seen[value] = struct{}{}
	a.terms = append(a.terms, value)
}

func (a *memberProfileAccumulator) first(keys ...string) string {
	for _, key := range keys {
		if values := a.fields[strings.ToLower(key)]; len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func (a *memberProfileAccumulator) urlList() []string {
	if len(a.urls) == 0 {
		return nil
	}
	return sortedMapKeys(a.urls)
}

func shouldIgnoreProfileValue(key, value string) bool {
	switch key {
	case "token", "email", "id", "guild_id", "avatar", "banner", "permissions":
		return true
	}
	if strings.HasSuffix(key, "_id") {
		return true
	}
	if len(value) > 128 && !strings.Contains(value, " ") && !strings.Contains(value, "/") {
		return true
	}
	return false
}

func appendUnique(items []string, value string) []string {
	if slices.Contains(items, value) {
		return items
	}
	return append(items, value)
}

func firstNonEmptyString(items ...string) string {
	for _, item := range items {
		if strings.TrimSpace(item) != "" {
			return item
		}
	}
	return ""
}

func firstNonSocialURL(urls []string) string {
	for _, raw := range urls {
		host := normalizedHost(raw)
		if host == "" || host == "x.com" || host == "twitter.com" || host == "github.com" {
			continue
		}
		return raw
	}
	return ""
}

func socialLoginFromURLs(urls []string, hosts ...string) string {
	allowed := map[string]struct{}{}
	for _, host := range hosts {
		allowed[strings.ToLower(host)] = struct{}{}
	}
	for _, raw := range urls {
		parsed, err := url.Parse(raw)
		if err != nil {
			continue
		}
		host := strings.ToLower(strings.TrimPrefix(parsed.Host, "www."))
		if _, ok := allowed[host]; !ok {
			continue
		}
		parts := strings.FieldsFunc(parsed.Path, func(r rune) bool { return r == '/' || r == '@' })
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				return part
			}
		}
	}
	return ""
}

func normalizeXHandle(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "@")
	if strings.Contains(value, "://") {
		return socialLoginFromURLs([]string{value}, "x.com", "twitter.com")
	}
	return value
}

func normalizeGitHubLogin(value string) string {
	value = strings.TrimSpace(value)
	if strings.Contains(value, "://") {
		return socialLoginFromURLs([]string{value}, "github.com")
	}
	value = strings.TrimPrefix(value, "@")
	return value
}

func normalizedHost(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(parsed.Host, "www."))
}
