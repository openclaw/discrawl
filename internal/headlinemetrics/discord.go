package headlinemetrics

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"time"
)

var inviteCode = regexp.MustCompile(`^[A-Za-z0-9_-]{1,100}$`)

func CollectDiscord(ctx context.Context, c Config, ts string) ([]Row, error) {
	client := &http.Client{
		Timeout:       30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return collectDiscord(ctx, c, ts, client)
}

func collectDiscord(ctx context.Context, c Config, ts string, client *http.Client) ([]Row, error) {
	rows := make([]Row, 0, len(c.Targets)*2)
	failed := false
	for _, t := range c.Targets {
		members, online := inviteCounts(ctx, client, t.Target)
		if members == nil || online == nil {
			failed = true
		}
		rows = append(rows, Counter(t, "members", members, ts, "discord_invite_approximate"), Counter(t, "online", online, ts, "discord_invite_approximate"))
	}
	if failed {
		return rows, errors.New("discord invite counts unavailable")
	}
	return rows, nil
}

func inviteCounts(ctx context.Context, client *http.Client, code string) (*float64, *float64) {
	if !inviteCode.MatchString(code) {
		return nil, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://discord.com/api/v10/invites/"+url.PathEscape(code)+"?with_counts=true", nil)
	if err != nil {
		return nil, nil
	}
	req.Header.Set("Accept", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return nil, nil
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		// A scheduler may try again later, including after 429; never convert
		// permission, expiry, or transient HTTP errors into zero members.
		return nil, nil
	}
	const maxBody = 2 * 1024 * 1024
	body, err := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err != nil || len(body) > maxBody {
		return nil, nil
	}
	var data struct {
		Guild *struct {
			ID string `json:"id"`
		} `json:"guild"`
		Members *int64 `json:"approximate_member_count"`
		Online  *int64 `json:"approximate_presence_count"`
	}
	if json.Unmarshal(body, &data) != nil || data.Guild == nil || data.Guild.ID == "" {
		return nil, nil
	}
	return approximateCount(data.Members), approximateCount(data.Online)
}

func approximateCount(n *int64) *float64 {
	if n == nil || *n < 0 || *n > 1<<53 {
		return nil
	}
	value := float64(*n)
	return &value
}
