package acquisition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"
	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/openclaw/discrawl/internal/syncer"
)

type Provider interface {
	Channel(context.Context, string) (*discordgo.Channel, error)
	CanReadHistory(context.Context, *discordgo.Channel) (bool, error)
	ChannelMessages(context.Context, string, int, string, string) ([]*discordgo.Message, error)
}
type Options struct {
	Guild, Channel string
	Since, Horizon time.Time
	MaxPages       int
}
type Result struct {
	Status   string    `json:"status"`
	RunID    string    `json:"run_id,omitempty"`
	Pages    int       `json:"pages"`
	Messages int       `json:"messages"`
	Through  time.Time `json:"through,omitempty"`
}

func identifier(label string) string {
	hash := sha256.Sum256([]byte(label))
	return hex.EncodeToString(hash[:])
}
func snowflake(id string) (uint64, error) {
	n, e := strconv.ParseUint(id, 10, 64)
	if e != nil || n == 0 || strconv.FormatUint(n, 10) != id {
		return 0, errors.New("invalid Discord snowflake")
	}
	return n, nil
}
func validTime(t time.Time) bool {
	return !t.IsZero() && t.Location() == time.UTC && t.Nanosecond()%1000000 == 0
}

func Collect(ctx context.Context, api *Client, provider Provider, opts Options) (Result, error) {
	var result Result
	if api == nil || provider == nil || opts.MaxPages < 1 || opts.MaxPages > 10000 {
		return result, errors.New("bounded acquisition options are required")
	}
	if _, err := snowflake(opts.Guild); err != nil {
		return result, err
	}
	if _, err := snowflake(opts.Channel); err != nil {
		return result, err
	}
	state, err := api.State(ctx, opts.Channel)
	if err != nil {
		return result, err
	}
	var run Run
	if state.ActiveRun != nil {
		run = *state.ActiveRun
		if run.GuildID != opts.Guild || run.ChannelID != opts.Channel || !opts.Since.IsZero() && !opts.Since.Equal(run.Since) || !opts.Horizon.IsZero() && !opts.Horizon.Equal(run.Horizon) {
			return result, errors.New("another acquisition window is active; resume its exact channel and bounds")
		}
	} else {
		since := opts.Since
		if state.CompletedThrough != nil {
			if !since.IsZero() && !since.Equal(*state.CompletedThrough) {
				return result, errors.New("since differs from the channel checkpoint")
			}
			since = *state.CompletedThrough
		}
		horizon := opts.Horizon
		if horizon.IsZero() {
			horizon = time.Now().UTC().Truncate(time.Millisecond)
		}
		if !validTime(since) || !validTime(horizon) || !since.Before(horizon) || horizon.After(time.Now()) {
			return result, errors.New("first acquisition needs explicit UTC millisecond since before horizon")
		}
		run = Run{ExpectedRevision: state.Revision, GuildID: opts.Guild, ChannelID: opts.Channel, Since: since, Horizon: horizon}
		encoded, _ := json.Marshal(struct{ Source, Guild, Channel, Since, Horizon string }{api.source, opts.Guild, opts.Channel, since.Format(time.RFC3339Nano), horizon.Format(time.RFC3339Nano)})
		run.ID = identifier(string(encoded))
	}
	result.RunID = run.ID
	channel, err := provider.Channel(ctx, opts.Channel)
	if err != nil {
		return result, errors.New("Discord channel metadata unavailable")
	}
	if channel == nil || channel.ID != opts.Channel || channel.GuildID != opts.Guild {
		return result, errors.New("Discord channel scope differs")
	}
	allowed, err := provider.CanReadHistory(ctx, channel)
	if err != nil || !allowed {
		return result, errors.New("Discord history permission unavailable")
	}
	if state.ActiveRun == nil {
		if err = api.Begin(ctx, run); err != nil {
			state, readErr := api.State(ctx, opts.Channel)
			if readErr != nil || state.ActiveRun == nil || state.ActiveRun.ID != run.ID {
				return result, err
			}
		}
		state, err = api.State(ctx, opts.Channel)
		if err != nil {
			return result, err
		}
	}
	for result.Pages < opts.MaxPages {
		if state.ActiveRun == nil {
			if state.CompletedThrough != nil && !state.CompletedThrough.Before(run.Horizon) {
				result.Status = "complete"
				result.Through = run.Horizon
				return result, nil
			}
			return result, errors.New("acquisition run disappeared")
		}
		if state.ActiveRun.ID != run.ID {
			return result, errors.New("active acquisition changed")
		}
		page, err := buildPage(ctx, api, provider, run, state)
		if err != nil {
			return result, err
		}
		if page.Terminal != "" {
			allowed, e := provider.CanReadHistory(ctx, channel)
			if e != nil || !allowed {
				return result, errors.New("Discord history permission changed before completion")
			}
		}
		_, err = api.Commit(ctx, state, page)
		if err != nil {
			var apiErr *APIError
			if errors.As(err, &apiErr) && apiErr.Status != 409 && apiErr.Status != 503 {
				return result, err
			}
			observed, readErr := api.State(ctx, opts.Channel)
			if readErr != nil || observed.Revision <= state.Revision {
				return result, err
			}
			state = observed
		} else {
			result.Messages += len(page.Changes)
			state, err = api.State(ctx, opts.Channel)
			if err != nil {
				return result, err
			}
		}
		result.Pages++
	}
	if state.ActiveRun == nil && state.CompletedThrough != nil && !state.CompletedThrough.Before(run.Horizon) {
		result.Status = "complete"
		result.Through = run.Horizon
	} else {
		result.Status = "bounded"
	}
	return result, nil
}

func buildPage(ctx context.Context, api *Client, provider Provider, run Run, state State) (Page, error) {
	p := Page{RunID: run.ID, ID: identifier(run.ID + ":" + strconv.FormatInt(state.Revision, 10)), ExpectedRevision: state.Revision, Before: state.Before, Changes: []remote.Change{}}
	before, err := snowflake(state.Before)
	if err != nil {
		return p, err
	}
	messages, err := provider.ChannelMessages(ctx, run.ChannelID, 100, state.Before, "")
	if err != nil {
		return p, errors.New("Discord message page unavailable")
	}
	if len(messages) > 100 {
		return p, errors.New("Discord page exceeds requested bound")
	}
	if len(messages) == 0 {
		p.Terminal = "empty"
		return p, nil
	}
	ids := []string{}
	selected := []*discordgo.Message{}
	for _, m := range messages {
		if m == nil || m.ChannelID != run.ChannelID || m.GuildID != "" && m.GuildID != run.GuildID {
			return p, errors.New("Discord page contains a different channel")
		}
		id, e := snowflake(m.ID)
		if e != nil || id >= before {
			return p, errors.New("Discord page does not descend from its cursor")
		}
		before = id
		created := time.UnixMilli(int64(id>>22) + 1420070400000).UTC()
		if m.Timestamp.UnixMilli() != created.UnixMilli() || !created.Before(run.Horizon) {
			return p, errors.New("Discord message timestamp differs from its creation window")
		}
		if created.Before(run.Since) {
			if p.Terminal == "" {
				p.Terminal = "since_reached"
				p.NextBefore = m.ID
			}
			continue
		}
		if p.Terminal != "" {
			return p, errors.New("Discord message page crosses the window out of order")
		}
		ids = append(ids, m.ID)
		selected = append(selected, m)
		p.NextBefore = m.ID
	}
	retained := map[string]remote.Change{}
	if len(ids) > 0 {
		rows, e := api.Messages(ctx, run, state, ids)
		if e != nil {
			return p, e
		}
		for _, r := range rows {
			retained[*r.Key[0].Value] = r
		}
	}
	observed := time.Now().UTC().Format(time.RFC3339Nano)
	for _, message := range selected {
		record, e := syncer.NormalizeAcquiredMessage(ctx, message, run.GuildID)
		if e != nil {
			return p, errors.New("Discord message normalization failed")
		}
		if old, ok := retained[message.ID]; ok {
			previous, e := retainedRecord(old)
			if e != nil {
				return p, e
			}
			if e = store.CheckAcquiredMessageRevision(record, previous); e != nil {
				return p, e
			}
			p.Changes = append(p.Changes, old)
		} else {
			p.Changes = append(p.Changes, messageChange(record, observed))
		}
	}
	return p, nil
}

func retainedRecord(row remote.Change) (store.MessageRecord, error) {
	var r store.MessageRecord
	if len(row.Row) != 17 {
		return r, errors.New("retained message columns differ")
	}
	value := func(i int) (string, error) {
		v := row.Row[i]
		if v.Kind == "null" && v.Value == nil {
			return "", nil
		}
		if v.Value == nil || !utf8.ValidString(*v.Value) {
			return "", errors.New("invalid retained message scalar")
		}
		return *v.Value, nil
	}
	fields := []*string{&r.ID, &r.GuildID, &r.ChannelID, nil, &r.CreatedAt, &r.EditedAt, &r.DeletedAt, &r.Content}
	indices := []int{0, 1, 2, 3, 5, 6, 7, 8}
	for j, dest := range fields {
		if dest == nil {
			continue
		}
		v, e := value(indices[j])
		if e != nil {
			return r, e
		}
		*dest = v
	}
	return r, nil
}
func messageChange(r store.MessageRecord, observed string) remote.Change {
	text := func(s string) remote.ChangeValue { return remote.ChangeValue{Kind: "text", Value: &s} }
	optional := func(s string) remote.ChangeValue {
		if s == "" {
			return remote.ChangeValue{Kind: "null"}
		}
		return text(s)
	}
	integer := func(n int) remote.ChangeValue {
		v := strconv.Itoa(n)
		return remote.ChangeValue{Kind: "integer", Value: &v}
	}
	flag := func(b bool) remote.ChangeValue {
		if b {
			return integer(1)
		}
		return integer(0)
	}
	return remote.Change{Table: "messages", Operation: "upsert", Key: []remote.ChangeValue{text(r.ID)}, Row: []remote.ChangeValue{text(r.ID), text(r.GuildID), text(r.ChannelID), optional(r.AuthorID), integer(r.MessageType), text(r.CreatedAt), optional(r.EditedAt), optional(r.DeletedAt), text(r.Content), text(r.NormalizedContent), optional(r.ReplyToMessageID), flag(r.Pinned), flag(r.HasAttachments), text(r.RawJSON), text(observed), text(r.TextPartsJSON), integer(r.TextVersion)}}
}
