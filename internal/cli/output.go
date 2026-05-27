package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/openclaw/discrawl/internal/discorddesktop"
	"github.com/openclaw/discrawl/internal/media"
	"github.com/openclaw/discrawl/internal/report"
	"github.com/openclaw/discrawl/internal/store"
	"github.com/openclaw/discrawl/internal/syncer"
)

func (r *runtime) print(value any) error {
	if r.json {
		enc := json.NewEncoder(r.stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(value)
	}
	if r.plain {
		if err := printPlain(r.stdout, value); err == nil {
			return nil
		}
	}
	if err := printHuman(r.stdout, value); err == nil {
		return nil
	}
	enc := json.NewEncoder(r.stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func printPlain(w io.Writer, value any) error {
	switch v := value.(type) {
	case []store.SearchResult:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", row.GuildID, row.ChannelID, row.AuthorID, row.Content)
		}
		return nil
	case []store.MemberRow:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", row.GuildID, row.UserID, row.Username)
		}
		return nil
	case store.MemberProfile:
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", v.Member.GuildID, v.Member.UserID, v.Member.Username)
		return nil
	case []store.ChannelRow:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", row.GuildID, row.ID, row.Kind, row.Name)
		}
		return nil
	case []store.MessageRow:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", formatTime(row.CreatedAt), row.GuildID, row.ChannelID, row.AuthorID, row.MessageID, row.Content)
		}
		return nil
	case []store.DirectMessageConversationRow:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\n", row.ChannelID, row.Name, row.MessageCount, row.AuthorCount, formatTime(row.FirstMessageAt), formatTime(row.LastMessageAt))
		}
		return nil
	case []store.MentionRow:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", formatTime(row.CreatedAt), row.GuildID, row.ChannelID, row.AuthorID, row.TargetType, row.TargetID, row.Content)
		}
		return nil
	case report.Digest:
		for _, row := range v.Channels {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%d\n", row.ChannelID, row.ChannelName, row.Kind, row.GuildID, row.Messages, row.Replies, row.ActiveAuthors)
		}
		return nil
	case report.Quiet:
		for _, row := range v.Channels {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\n", row.ChannelID, row.ChannelName, row.Kind, row.GuildID, row.LastMessage, row.DaysSilent)
		}
		return nil
	case report.Trends:
		for _, row := range v.Rows {
			for _, week := range row.Weekly {
				_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\n", row.GuildID, row.ChannelID, row.ChannelName, row.Kind, formatTime(week.WeekStart), week.Messages)
			}
		}
		return nil
	default:
		return errors.New("no plain printer")
	}
}

func printUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, `discrawl archives Discord guild data into local SQLite.

Usage:
  discrawl [global flags] <command> [args]

Commands:
  metadata
  check-update
  version
  init
  sync
  tail
  tap
  cache-import
  wiretap
  search
  tui
  messages
  digest
  analytics
  dms
  mentions
  attachments
  embed
  sql
  members
  channels
  status
  remote
  whoami
  report
  doctor
  cloud
  subscribe-cloud
`)
}

func printCommandUsage(w io.Writer, args []string) error {
	if len(args) != 1 {
		return usageErr(errors.New("usage: discrawl help <command>"))
	}
	text, ok := commandUsage[args[0]]
	if !ok {
		return usageErr(fmt.Errorf("unknown help topic %q", args[0]))
	}
	_, _ = fmt.Fprint(w, text)
	return nil
}

var commandUsage = map[string]string{
	"check-update": `Usage:
  discrawl check-update [--json] [--force]

Checks GitHub Releases for a newer discrawl build.
`,
	"search": `Usage:
  discrawl search [flags] <query>

Flags:
  --mode fts|semantic|hybrid  Search mode.
  --channel ID_OR_NAME        Filter by channel id or name.
  --author ID_OR_NAME         Filter by author id or name.
  --limit N                   Maximum results. Default: 20.
  --include-empty             Include empty/attachment-only messages.
  --dm                        Search local desktop DM cache.
  --guild ID                  Restrict to one guild id.
  --guilds ID,ID              Restrict to guild ids.
`,
	"attachments": `Usage:
  discrawl attachments [flags]
  discrawl attachments fetch [flags]

Flags:
  --channel ID_OR_NAME        Filter by channel id or name.
  --author ID_OR_NAME         Filter by author id or name.
  --message ID                Filter by message id.
  --filename TEXT             Filter by filename.
  --type TEXT                 Filter by content type.
  --hours N                   Attachments from the last N hours.
  --days N                    Attachments from the last N days.
  --since RFC3339             Attachments at or after timestamp.
  --before RFC3339            Attachments before timestamp.
  --limit N                   Maximum rows. Default: 200.
  --all                       Return all matching rows.
  --missing                   Only attachments without cached media.
  --dm                        Read local desktop DM cache.
  --guild ID                  Restrict to one guild id.
  --guilds ID,ID              Restrict to guild ids.

Fetch flags:
  --force                     Re-download already cached attachments.
  --max-bytes N               Per-attachment download cap.
`,
	"messages": `Usage:
  discrawl messages [flags]

Flags:
  --channel ID_OR_NAME        Filter by channel id or name.
  --author ID_OR_NAME         Filter by author id or name.
  --hours N                   Messages from the last N hours.
  --days N                    Messages from the last N days.
  --since RFC3339             Messages at or after timestamp.
  --before RFC3339            Messages before timestamp.
  --limit N                   Maximum rows. Default: 200.
  --last N                    Most recent N rows.
  --all                       Return all matching rows.
  --sync                      Refresh channel before reading.
  --include-empty             Include empty/attachment-only messages.
  --dm                        Read local desktop DM cache.
  --guild ID                  Restrict to one guild id.
  --guilds ID,ID              Restrict to guild ids.
`,
	"sql": `Usage:
  discrawl sql [--unsafe --confirm] <query>
  discrawl sql [--unsafe --confirm] -

Flags:
  --unsafe                    Allow write queries.
  --confirm                   Required with --unsafe.

Read-only SQL is allowed by default. Use "-" or no query to read SQL from stdin.
`,
	"remote": `Usage:
  discrawl remote status
  discrawl remote archives
  discrawl remote login --endpoint URL
  discrawl remote login --endpoint URL --github-token-env GITHUB_TOKEN
  discrawl remote whoami

Reads the configured Cloudflare-backed remote archive without opening the local SQLite database.
`,
	"cloud": `Usage:
  discrawl cloud publish --remote URL --archive ARCHIVE [--token-env ENV]

Publishes the local non-DM SQLite archive into a Cloudflare-backed remote archive.
`,
	"subscribe-cloud": `Usage:
  discrawl subscribe-cloud --endpoint URL --archive ARCHIVE [--token-env ENV]

Writes a read-only cloud archive config. This does not create or import a local SQLite database.
`,
}

func printRows(w io.Writer, cols []string, rows [][]string) error {
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, strings.Join(cols, "\t"))
	for _, row := range rows {
		_, _ = fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	return tw.Flush()
}

func printHuman(w io.Writer, value any) error {
	switch v := value.(type) {
	case syncRunStats:
		if _, err := fmt.Fprintf(w, "source=%s\n", v.Source); err != nil {
			return err
		}
		if v.Discord != nil {
			if _, err := fmt.Fprintf(w, "discord_guilds=%d\ndiscord_channels=%d\ndiscord_threads=%d\ndiscord_members=%d\ndiscord_messages=%d\n",
				v.Discord.Guilds, v.Discord.Channels, v.Discord.Threads, v.Discord.Members, v.Discord.Messages); err != nil {
				return err
			}
		}
		if v.Wiretap != nil {
			if _, err := fmt.Fprintf(w, "wiretap_visited=%d\nwiretap_files=%d\nwiretap_unchanged=%d\nwiretap_fast_skipped=%d\nwiretap_messages=%d\nwiretap_dm_messages=%d\nwiretap_dm_channels=%d\nwiretap_guild_messages=%d\nwiretap_skipped_messages=%d\nwiretap_skipped_channels=%d\nwiretap_checkpoints=%d\n",
				v.Wiretap.FilesVisited, v.Wiretap.FilesScanned, v.Wiretap.FilesUnchanged, v.Wiretap.CacheFilesFastSkipped, v.Wiretap.Messages, v.Wiretap.DMMessages, v.Wiretap.DMChannels, v.Wiretap.GuildMessages, v.Wiretap.SkippedMessages, v.Wiretap.SkippedChannels, v.Wiretap.Checkpoints); err != nil {
				return err
			}
		}
		if v.Media != nil {
			if _, err := fmt.Fprintf(w, "media_attachments=%d\nmedia_fetched=%d\nmedia_reused=%d\nmedia_skipped=%d\nmedia_failed=%d\nmedia_bytes=%d\n",
				v.Media.Attachments, v.Media.Fetched, v.Media.Reused, v.Media.Skipped, v.Media.Failed, v.Media.Bytes); err != nil {
				return err
			}
		}
		return nil
	case syncer.SyncStats:
		_, err := fmt.Fprintf(w, "guilds=%d channels=%d threads=%d members=%d messages=%d\n", v.Guilds, v.Channels, v.Threads, v.Members, v.Messages)
		return err
	case discorddesktop.Stats:
		_, err := fmt.Fprintf(w, "path=%s\nvisited=%d\nfiles=%d\nskipped=%d\nunchanged=%d\nfast_skipped=%d\nobjects=%d\nguilds=%d\nchannels=%d\nmessages=%d\ndm_messages=%d\ndm_channels=%d\nguild_messages=%d\nskipped_messages=%d\nskipped_channels=%d\ncheckpoints=%d\nfull_cache=%t\ndry_run=%t\n",
			v.Path, v.FilesVisited, v.FilesScanned, v.FilesSkipped, v.FilesUnchanged, v.CacheFilesFastSkipped, v.JSONObjects, v.Guilds, v.Channels, v.Messages, v.DMMessages, v.DMChannels, v.GuildMessages, v.SkippedMessages, v.SkippedChannels, v.Checkpoints, v.FullCache, v.DryRun)
		return err
	case store.Status:
		_, err := fmt.Fprintf(w, "db=%s\nguilds=%d\nchannels=%d\nthreads=%d\nmessages=%d\nmembers=%d\nembedding_backlog=%d\nlast_sync=%s\nlast_tail_event=%s\n",
			v.DBPath, v.GuildCount, v.ChannelCount, v.ThreadCount, v.MessageCount, v.MemberCount, v.EmbeddingBacklog,
			formatTime(v.LastSyncAt), formatTime(v.LastTailEventAt))
		return err
	case store.EmbeddingDrainStats:
		_, err := fmt.Fprintf(w, "processed=%d\nsucceeded=%d\nfailed=%d\nskipped=%d\nremaining_backlog=%d\nprovider=%s\nmodel=%s\ninput_version=%s\n",
			v.Processed, v.Succeeded, v.Failed, v.Skipped, v.RemainingBacklog, v.Provider, v.Model, v.InputVersion)
		if err != nil {
			return err
		}
		if v.Requeued > 0 {
			if _, err := fmt.Fprintf(w, "requeued=%d\n", v.Requeued); err != nil {
				return err
			}
		}
		if v.RateLimited {
			_, err = fmt.Fprintln(w, "rate_limited=true")
		}
		return err
	case []store.SearchResult:
		for _, row := range v {
			if _, err := fmt.Fprintf(w, "[%s/%s] %s %s\n%s\n\n", row.GuildID, row.ChannelName, row.AuthorName, formatTime(row.CreatedAt), row.Content); err != nil {
				return err
			}
		}
		return nil
	case []store.MessageRow:
		for _, row := range v {
			if _, err := fmt.Fprintf(w, "[%s/%s] %s %s\n%s\n\n", row.GuildID, row.ChannelName, row.AuthorName, formatTime(row.CreatedAt), row.Content); err != nil {
				return err
			}
		}
		return nil
	case []store.AttachmentRow:
		for _, row := range v {
			if _, err := fmt.Fprintf(w, "[%s/%s] %s %s\n%s type=%s size=%d status=%s media=%s url=%s\n\n",
				row.GuildID, row.ChannelName, row.AuthorName, formatTime(row.CreatedAt), row.Filename,
				row.ContentType, row.Size, firstNonEmpty(row.FetchStatus, "metadata"), row.MediaPath, firstNonEmpty(row.URL, row.ProxyURL)); err != nil {
				return err
			}
		}
		return nil
	case media.FetchStats:
		_, err := fmt.Fprintf(w, "attachments=%d\nfetched=%d\nreused=%d\nskipped=%d\nfailed=%d\nbytes=%d\n",
			v.Attachments, v.Fetched, v.Reused, v.Skipped, v.Failed, v.Bytes)
		return err
	case []store.DirectMessageConversationRow:
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "CHANNEL\tNAME\tMESSAGES\tAUTHORS\tFIRST\tLAST")
		for _, row := range v {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\t%s\n",
				row.ChannelID,
				row.Name,
				row.MessageCount,
				row.AuthorCount,
				formatTime(row.FirstMessageAt),
				formatTime(row.LastMessageAt),
			)
		}
		return tw.Flush()
	case []store.MentionRow:
		for _, row := range v {
			if _, err := fmt.Fprintf(w, "[%s/%s] %s -> %s:%s %s\n%s\n\n", row.GuildID, row.ChannelName, row.AuthorName, row.TargetType, firstNonEmpty(row.TargetName, row.TargetID), formatTime(row.CreatedAt), row.Content); err != nil {
				return err
			}
		}
		return nil
	case []store.MemberRow:
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "GUILD\tUSER\tNAME\tDISPLAY\tPROFILE")
		for _, row := range v {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
				row.GuildID,
				row.UserID,
				row.Username,
				firstNonEmpty(row.DisplayName, row.Nick, row.GlobalName),
				memberProfileSummary(row),
			)
		}
		return tw.Flush()
	case store.MemberProfile:
		if _, err := fmt.Fprintf(w, "guild=%s\nuser=%s\nusername=%s\ndisplay=%s\njoined=%s\nbot=%t\n",
			v.Member.GuildID,
			v.Member.UserID,
			v.Member.Username,
			firstNonEmpty(v.Member.DisplayName, v.Member.Nick, v.Member.GlobalName),
			formatTime(v.Member.JoinedAt),
			v.Member.Bot,
		); err != nil {
			return err
		}
		if v.Member.XHandle != "" {
			if _, err := fmt.Fprintf(w, "x=%s\n", v.Member.XHandle); err != nil {
				return err
			}
		}
		if v.Member.GitHubLogin != "" {
			if _, err := fmt.Fprintf(w, "github=%s\n", v.Member.GitHubLogin); err != nil {
				return err
			}
		}
		if v.Member.Website != "" {
			if _, err := fmt.Fprintf(w, "website=%s\n", v.Member.Website); err != nil {
				return err
			}
		}
		if v.Member.Pronouns != "" {
			if _, err := fmt.Fprintf(w, "pronouns=%s\n", v.Member.Pronouns); err != nil {
				return err
			}
		}
		if v.Member.Location != "" {
			if _, err := fmt.Fprintf(w, "location=%s\n", v.Member.Location); err != nil {
				return err
			}
		}
		if v.Member.Bio != "" {
			if _, err := fmt.Fprintf(w, "bio=%s\n", v.Member.Bio); err != nil {
				return err
			}
		}
		if len(v.Member.URLs) > 0 {
			if _, err := fmt.Fprintf(w, "urls=%s\n", strings.Join(v.Member.URLs, ", ")); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "message_count=%d\nfirst_message=%s\nlast_message=%s\n",
			v.MessageCount,
			formatTime(v.FirstMessageAt),
			formatTime(v.LastMessageAt),
		); err != nil {
			return err
		}
		if len(v.RecentMessages) == 0 {
			return nil
		}
		if _, err := fmt.Fprintln(w, "\nRecent messages:"); err != nil {
			return err
		}
		for _, row := range v.RecentMessages {
			if _, err := fmt.Fprintf(w, "[%s] %s\n%s\n\n", row.ChannelName, formatTime(row.CreatedAt), row.Content); err != nil {
				return err
			}
		}
		return nil
	case []store.ChannelRow:
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "GUILD\tCHANNEL\tKIND\tNAME")
		for _, row := range v {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", row.GuildID, row.ID, row.Kind, row.Name)
		}
		return tw.Flush()
	case report.Digest:
		for _, channel := range v.Channels {
			if _, err := fmt.Fprintf(w, "%s (%s)\n", channel.ChannelName, firstNonEmpty(channel.Kind, "unknown")); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "  messages=%d replies=%d authors=%d\n", channel.Messages, channel.Replies, channel.ActiveAuthors); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "  top posters  %s\n", formatRankedCounts(channel.TopPosters)); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "  top mentions %s\n\n", formatRankedCounts(channel.TopMentions)); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "Window: %s to %s (%s)\n", formatTime(v.Since), formatTime(v.Until), v.WindowLabel); err != nil {
			return err
		}
		_, err := fmt.Fprintf(w, "Totals: messages=%d replies=%d channels=%d authors=%d\n", v.Totals.Messages, v.Totals.Replies, v.Totals.Channels, v.Totals.ActiveAuthors)
		return err
	case report.Quiet:
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "CHANNEL\tKIND\tLAST MESSAGE\tDAYS SILENT")
		for _, row := range v.Channels {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
				row.ChannelName,
				firstNonEmpty(row.Kind, "unknown"),
				firstNonEmpty(row.LastMessage, "never"),
				formatDaysSilent(row.DaysSilent),
			)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "\nWindow: %s to %s (%s)\n", formatTime(v.Since), formatTime(v.Until), formatWindowDuration(v.Until.Sub(v.Since))); err != nil {
			return err
		}
		_, err := fmt.Fprintf(w, "Totals: channels=%d\n", v.Totals.Channels)
		return err
	case report.Trends:
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		header := []string{"CHANNEL", "KIND", "TOTAL"}
		weekStarts := make([]time.Time, 0, v.Weeks)
		if len(v.Rows) > 0 {
			for _, week := range v.Rows[0].Weekly {
				weekStarts = append(weekStarts, week.WeekStart)
			}
		} else {
			for i := range v.Weeks {
				weekStarts = append(weekStarts, v.Since.AddDate(0, 0, 7*i))
			}
		}
		for _, start := range weekStarts {
			header = append(header, start.Format(time.DateOnly))
		}
		_, _ = fmt.Fprintln(tw, strings.Join(header, "\t"))
		for _, row := range v.Rows {
			cols := []string{row.ChannelName, firstNonEmpty(row.Kind, "unknown"), strconv.Itoa(trendsRowTotal(row.Weekly))}
			for _, week := range row.Weekly {
				cols = append(cols, strconv.Itoa(week.Messages))
			}
			_, _ = fmt.Fprintln(tw, strings.Join(cols, "\t"))
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		_, err := fmt.Fprintf(w, "\nWindow: %s to %s (%d weeks)\n", formatTime(v.Since), formatTime(v.Until), v.Weeks)
		return err
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, err := fmt.Fprintf(w, "%s=%v\n", key, v[key]); err != nil {
				return err
			}
		}
		return nil
	default:
		return errors.New("no human printer")
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func memberProfileSummary(row store.MemberRow) string {
	parts := []string{}
	if row.XHandle != "" {
		parts = append(parts, "x:"+row.XHandle)
	}
	if row.GitHubLogin != "" {
		parts = append(parts, "gh:"+row.GitHubLogin)
	}
	if row.Website != "" {
		parts = append(parts, row.Website)
	}
	if row.Bio != "" {
		parts = append(parts, trimForTable(row.Bio))
	}
	return strings.Join(parts, " | ")
}

func trimForTable(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 40 {
		return value
	}
	return value[:37] + "..."
}

func formatRankedCounts(rows []report.RankedCount) string {
	if len(rows) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		parts = append(parts, fmt.Sprintf("%s (%d)", firstNonEmpty(row.Name, "unknown"), row.Count))
	}
	return strings.Join(parts, ", ")
}

func formatDaysSilent(days int) string {
	if days < 0 {
		return "-"
	}
	return strconv.Itoa(days)
}

func formatWindowDuration(d time.Duration) string {
	if d <= 0 {
		return "0"
	}
	if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
	}
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(d/time.Hour))
	}
	return d.String()
}

func trendsRowTotal(weekly []report.WeeklyCount) int {
	total := 0
	for _, row := range weekly {
		total += row.Messages
	}
	return total
}
