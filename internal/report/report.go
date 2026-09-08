package report

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

const (
	StartMarker = "<!-- discrawl-report:start -->"
	EndMarker   = "<!-- discrawl-report:end -->"
)

type Options struct {
	Now time.Time
	// Published excludes local-only DMs, not private guild content.
	Published bool
}

type ActivityReport struct {
	GeneratedAt     time.Time
	LatestMessageAt time.Time
	TotalMessages   int
	TotalChannels   int
	TotalMembers    int
	Windows         []WindowStats
	TopChannels     []RankedCount
	TopAuthors      []RankedCount
	BusiestDays     []RankedCount
}

type WindowStats struct {
	Label          string
	Since          time.Time
	Messages       int
	ActiveAuthors  int
	ActiveChannels int
	Attachments    int
}

type RankedCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func Build(ctx context.Context, s *store.Store, opts Options) (ActivityReport, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	report := ActivityReport{GeneratedAt: now.UTC()}
	if err := scanTotals(ctx, s.DB(), &report, opts.Published); err != nil {
		return ActivityReport{}, err
	}
	anchor := report.LatestMessageAt
	if anchor.IsZero() {
		anchor = now
	}
	windows := []struct {
		label string
		dur   time.Duration
	}{
		{"24 hours", 24 * time.Hour},
		{"7 days", 7 * 24 * time.Hour},
		{"30 days", 30 * 24 * time.Hour},
	}
	for _, window := range windows {
		stats, err := scanWindow(ctx, s.DB(), window.label, anchor.Add(-window.dur), opts.Published)
		if err != nil {
			return ActivityReport{}, err
		}
		report.Windows = append(report.Windows, stats)
	}
	weekSince := anchor.Add(-7 * 24 * time.Hour)
	monthSince := anchor.Add(-30 * 24 * time.Hour)
	var err error
	report.TopChannels, err = topChannels(ctx, s.DB(), weekSince, 8, opts.Published)
	if err != nil {
		return ActivityReport{}, err
	}
	report.TopAuthors, err = topAuthors(ctx, s.DB(), weekSince, 8, opts.Published)
	if err != nil {
		return ActivityReport{}, err
	}
	report.BusiestDays, err = busiestDays(ctx, s.DB(), monthSince, 7, opts.Published)
	if err != nil {
		return ActivityReport{}, err
	}
	return report, nil
}

func scanTotals(ctx context.Context, db *sql.DB, report *ActivityReport, published bool) error {
	var latest sql.NullString
	if err := db.QueryRowContext(ctx, `
		select
			(select count(*) from messages where (? = 0 or guild_id <> ?)),
			(select count(*) from channels where (? = 0 or guild_id <> ?)),
			(select count(*) from members where (? = 0 or guild_id <> ?)),
			(select created_at from messages where (? = 0 or guild_id <> ?)
			 order by julianday(created_at) desc, id desc limit 1)
	`, published, store.DirectMessageGuildID, published, store.DirectMessageGuildID,
		published, store.DirectMessageGuildID, published, store.DirectMessageGuildID,
	).Scan(&report.TotalMessages, &report.TotalChannels, &report.TotalMembers, &latest); err != nil {
		return fmt.Errorf("scan report totals: %w", err)
	}
	report.LatestMessageAt = parseTime(latest.String)
	return nil
}

func scanWindow(ctx context.Context, db *sql.DB, label string, since time.Time, published bool) (WindowStats, error) {
	stats := WindowStats{Label: label, Since: since.UTC()}
	if err := db.QueryRowContext(ctx, `
		select
			count(*),
			count(distinct nullif(author_id, '')),
			count(distinct nullif(channel_id, '')),
			coalesce(sum(case when has_attachments then 1 else 0 end), 0)
		from messages
		where julianday(created_at) >= julianday(?)
		  and (? = 0 or guild_id <> ?)
	`, reportTimeArg(since), published, store.DirectMessageGuildID).Scan(&stats.Messages, &stats.ActiveAuthors, &stats.ActiveChannels, &stats.Attachments); err != nil {
		return WindowStats{}, fmt.Errorf("scan %s stats: %w", label, err)
	}
	return stats, nil
}

func topChannels(ctx context.Context, db *sql.DB, since time.Time, limit int, published bool) ([]RankedCount, error) {
	return ranked(ctx, db, `
		select coalesce(nullif(c.name, ''), m.channel_id) as name, count(*) as total
		from messages m
		left join channels c on c.id = m.channel_id
		where julianday(m.created_at) >= julianday(?)
		  and (? = 0 or m.guild_id <> ?)
		group by m.channel_id, coalesce(nullif(c.name, ''), m.channel_id)
		order by total desc, name asc
		limit ?
	`, reportTimeArg(since), published, store.DirectMessageGuildID, limit)
}

func topAuthors(ctx context.Context, db *sql.DB, since time.Time, limit int, published bool) ([]RankedCount, error) {
	return ranked(ctx, db, `
		select
			coalesce(
				nullif(mem.display_name, ''),
				nullif(mem.nick, ''),
				nullif(mem.global_name, ''),
				nullif(mem.username, ''),
				nullif(json_extract(m.raw_json, '$.author.global_name'), ''),
				nullif(json_extract(m.raw_json, '$.author.username'), ''),
				nullif(m.author_id, ''),
				'unknown'
			) as name,
			count(*) as total
		from messages m
		left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id
		where julianday(m.created_at) >= julianday(?)
		  and (? = 0 or m.guild_id <> ?)
		group by m.author_id, name
		order by total desc, name asc
		limit ?
	`, reportTimeArg(since), published, store.DirectMessageGuildID, limit)
}

func busiestDays(ctx context.Context, db *sql.DB, since time.Time, limit int, published bool) ([]RankedCount, error) {
	return ranked(ctx, db, `
		select date(created_at) as name, count(*) as total
		from messages
		where julianday(created_at) >= julianday(?)
		  and (? = 0 or guild_id <> ?)
		group by date(created_at)
		order by total desc, name desc
		limit ?
	`, reportTimeArg(since), published, store.DirectMessageGuildID, limit)
}

func ranked(ctx context.Context, db *sql.DB, query string, args ...any) ([]RankedCount, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []RankedCount
	for rows.Next() {
		var row RankedCount
		if err := rows.Scan(&row.Name, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func RenderMarkdown(report ActivityReport) (string, error) {
	var body bytes.Buffer
	if err := reportTemplate.Execute(&body, report); err != nil {
		return "", err
	}
	return strings.TrimSpace(body.String()) + "\n", nil
}

func UpdateReadme(readme []byte, section string) []byte {
	section = strings.TrimSpace(section)
	text := string(readme)
	start := strings.Index(text, StartMarker)
	end := strings.Index(text, EndMarker)
	if start >= 0 && end >= start {
		end += len(EndMarker)
		replacement := StartMarker + "\n" + section + "\n" + EndMarker
		return []byte(text[:start] + replacement + text[end:])
	}
	replacement := StartMarker + "\n" + section + "\n" + EndMarker
	text = strings.TrimRight(text, "\n")
	if text == "" {
		return []byte(replacement + "\n")
	}
	return []byte(text + "\n\n" + replacement + "\n")
}

func WriteReadme(path string, section string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	dir, name := filepath.Split(absPath)
	if name == "" {
		return fmt.Errorf("readme path %q does not name a file", path)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	current, err := root.ReadFile(name)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	updated := UpdateReadme(current, section)
	return root.WriteFile(name, updated, 0o600)
}

func MarkdownTable(rows []RankedCount, nameTitle string) string {
	if len(rows) == 0 {
		return "_No activity._"
	}
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "| %s | Messages |\n| --- | ---: |\n", nameTitle)
	for _, row := range rows {
		_, _ = fmt.Fprintf(&b, "| %s | %s |\n", escapeMD(row.Name), formatInt(row.Count))
	}
	return strings.TrimRight(b.String(), "\n")
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "n/a"
	}
	return t.UTC().Format("2006-01-02 15:04 UTC")
}

func formatInt(v int) string {
	return strconv.FormatInt(int64(v), 10)
}

func escapeMD(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	s = html.EscapeString(s)
	s = strings.ReplaceAll(s, "|", "\\|")
	return s
}

func clipWhitespace(s string, limit int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= limit {
		return s
	}
	return strings.TrimSpace(s[:limit]) + "..."
}

func parseTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func reportTimeArg(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z07:00")
}

var reportTemplate = template.Must(template.New("report").Funcs(template.FuncMap{
	"formatTime":  formatTime,
	"formatInt":   formatInt,
	"rankedTable": MarkdownTable,
	"escapeMD":    escapeMD,
}).Parse(`## Discord Activity Report

Last updated: {{ formatTime .GeneratedAt }}
Latest archived message: {{ formatTime .LatestMessageAt }}

Archive size: {{ formatInt .TotalMessages }} messages, {{ formatInt .TotalChannels }} channels, {{ formatInt .TotalMembers }} members.

### Activity

| Window | Messages | Active people | Active channels | Attachments |
| --- | ---: | ---: | ---: | ---: |
{{- range .Windows }}
| Last {{ .Label }} | {{ formatInt .Messages }} | {{ formatInt .ActiveAuthors }} | {{ formatInt .ActiveChannels }} | {{ formatInt .Attachments }} |
{{- end }}

### Hot Channels This Week

{{ rankedTable .TopChannels "Channel" }}

### Top Posters This Week

{{ rankedTable .TopAuthors "Person" }}

### Busiest Days This Month

{{ rankedTable .BusiestDays "Day" }}
`))
