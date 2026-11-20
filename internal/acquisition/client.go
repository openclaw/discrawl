// Package acquisition collects message creation windows using API-owned state.
// It never opens a local archive or persists an outbox.
package acquisition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
)

const (
	profile   = "discord-acquisition/v1"
	pageLimit = 4 << 20
)

type Run struct {
	ID               string    `json:"id"`
	ExpectedRevision int64     `json:"expectedRevision"`
	GuildID          string    `json:"guildId"`
	ChannelID        string    `json:"channelId"`
	Since            time.Time `json:"since"`
	Horizon          time.Time `json:"horizon"`
}
type State struct {
	Profile           string     `json:"profile"`
	Revision          int64      `json:"revision"`
	Sequence          int64      `json:"sequence"`
	CompletedSequence int64      `json:"completedSequence"`
	CompletedThrough  *time.Time `json:"completedThrough"`
	ActiveRun         *Run       `json:"activeRun,omitempty"`
	Before            string     `json:"before"`
}
type Page struct {
	RunID            string          `json:"runId"`
	ID               string          `json:"id"`
	ExpectedRevision int64           `json:"expectedRevision"`
	Before           string          `json:"before"`
	NextBefore       string          `json:"nextBefore"`
	Terminal         string          `json:"terminal"`
	Changes          []remote.Change `json:"changes"`
}
type Receipt struct {
	RunID    string `json:"runId"`
	PageID   string `json:"pageId"`
	SHA256   string `json:"sha256"`
	Revision int64  `json:"revision"`
	Sequence int64  `json:"sequence"`
	Final    bool   `json:"final"`
	Replayed bool   `json:"replayed"`
}
type Client struct {
	origin, source, token string
	http                  *http.Client
}
type APIError struct{ Status int }

func (e *APIError) Error() string { return fmt.Sprintf("acquisition API returned HTTP %d", e.Status) }

func NewClient(origin, source, token string) (*Client, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
		return nil, errors.New("acquisition origin must be an HTTPS origin")
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || ip == nil || !ip.IsLoopback() {
			return nil, errors.New("acquisition HTTP requires a loopback address")
		}
	}
	if source == "" || len(source) > 128 || strings.IndexFunc(source, func(r rune) bool {
		return (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' && r != '-'
	}) >= 0 || token == "" || strings.ContainsAny(token, " \t\r\n") {
		return nil, errors.New("explicit source and publisher token are required")
	}
	return &Client{strings.TrimRight(origin, "/"), source, token, &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (c *Client) request(ctx context.Context, method, path string, input, output any, limit int) ([]byte, error) {
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil || len(body) > pageLimit {
			return nil, errors.New("acquisition request is too large")
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.origin+"/v1/apps/discrawl/archives/"+c.source+"/acquisition/"+path, bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("invalid acquisition request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, errors.New("acquisition API transport failed; reconcile server state")
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return nil, &APIError{res.StatusCode}
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, int64(limit)+1))
	if err != nil || len(raw) > limit {
		return nil, errors.New("acquisition response incomplete or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(output) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid acquisition response")
	}
	return body, nil
}

func (c *Client) State(ctx context.Context, channel string) (State, error) {
	var s State
	_, err := c.request(ctx, http.MethodGet, "state?channel="+url.QueryEscape(channel), nil, &s, 128<<10)
	if err == nil && (s.Profile != profile || s.Revision < 0 || s.Sequence < s.CompletedSequence || s.CompletedSequence < 1) {
		err = errors.New("invalid acquisition state")
	}
	return s, err
}

func (c *Client) Begin(ctx context.Context, r Run) error {
	var result struct {
		RunID  string `json:"runId"`
		Status string `json:"status"`
	}
	_, err := c.request(ctx, http.MethodPost, "runs", r, &result, 4096)
	if err == nil && (result.RunID != r.ID || result.Status != "accepted") {
		return errors.New("invalid acquisition run receipt")
	}
	return err
}

func (c *Client) Messages(ctx context.Context, r Run, s State, ids []string) ([]remote.Change, error) {
	request := struct {
		RunID            string   `json:"runId"`
		ExpectedRevision int64    `json:"expectedRevision"`
		IDs              []string `json:"ids"`
	}{r.ID, s.Revision, ids}
	var result struct {
		Revision int64           `json:"revision"`
		Sequence int64           `json:"sequence"`
		Messages []remote.Change `json:"messages"`
	}
	_, err := c.request(ctx, http.MethodPost, "messages", request, &result, pageLimit)
	if err != nil {
		return nil, err
	}
	if result.Revision != s.Revision || result.Sequence != s.Sequence || result.Messages == nil {
		return nil, errors.New("retained message revision differs")
	}
	selected := map[string]bool{}
	for _, id := range ids {
		selected[id] = true
	}
	for _, row := range result.Messages {
		if row.Table != "messages" || row.Operation != "upsert" || len(row.Key) != 1 || row.Key[0].Kind != "text" || row.Key[0].Value == nil || !selected[*row.Key[0].Value] || len(row.Row) != 17 {
			return nil, errors.New("invalid retained message selection")
		}
		delete(selected, *row.Key[0].Value)
	}
	return result.Messages, nil
}

func (c *Client) Commit(ctx context.Context, s State, p Page) (Receipt, error) {
	var receipt Receipt
	body, err := c.request(ctx, http.MethodPost, "pages", p, &receipt, 4096)
	if err != nil {
		return receipt, err
	}
	hash := sha256.Sum256(body)
	if receipt.RunID != p.RunID || receipt.PageID != p.ID || receipt.SHA256 != hex.EncodeToString(hash[:]) || receipt.Revision != s.Revision+1 || receipt.Sequence != s.Sequence+1 || receipt.Final != (p.Terminal != "") {
		return Receipt{}, errors.New("invalid acquisition page receipt; reconcile server state")
	}
	return receipt, nil
}
