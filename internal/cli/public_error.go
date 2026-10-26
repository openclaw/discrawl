package cli

import (
	"context"
	"encoding/json"
	"errors"
	"net"

	"github.com/bwmarrin/discordgo"
	"modernc.org/sqlite"
)

// This record contains no Error() strings: provider bodies, URLs, identifiers
// and filesystem paths are private even after ordinary credential redaction.
type publicFailure struct {
	Schema      int    `json:"schema"`
	Class       string `json:"class"`
	Exit        int    `json:"exit"`
	HTTPStatus  int    `json:"http_status,omitempty"`
	DiscordCode int    `json:"discord_code,omitempty"`
	SQLiteCode  int    `json:"sqlite_code,omitempty"`
}

type publicError struct{ err error }

func (e *publicError) Unwrap() error { return e.err }

func (e *publicError) Error() string {
	encoded, err := json.Marshal(describePublicFailure(e.err))
	if err != nil {
		return "discrawl_error={}" // A failed encoding remains unknown to the supervisor.
	}
	return "discrawl_error=" + string(encoded)
}

func describePublicFailure(err error) publicFailure {
	report := publicFailure{Schema: 1, Class: "unknown", Exit: ExitCode(err)}
	var rest *discordgo.RESTError
	var rate *discordgo.RateLimitError
	var database *sqlite.Error
	var network net.Error
	switch {
	case errors.Is(err, context.Canceled):
		report.Class = "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		report.Class = "deadline"
	case errors.As(err, &rest) && rest != nil:
		report.Class = "http"
		if rest.Response != nil && rest.Response.StatusCode >= 100 && rest.Response.StatusCode <= 599 {
			report.HTTPStatus = rest.Response.StatusCode
		}
		if rest.Message != nil && rest.Message.Code > 0 && rest.Message.Code <= 1<<31-1 {
			report.DiscordCode = rest.Message.Code
		}
	case errors.As(err, &rate):
		report.Class = "rate_limit"
	case errors.As(err, &database) && database != nil:
		report.Class = "sqlite"
		if code := database.Code(); code > 0 && code <= 1<<31-1 {
			report.SQLiteCode = code
		}
	case errors.As(err, &network):
		report.Class = "network"
		if network.Timeout() {
			report.Class = "network_timeout"
		}
	default:
		switch report.Exit {
		case 2:
			report.Class = "usage"
		case 3:
			report.Class = "config"
		case 4:
			report.Class = "auth"
		case 5:
			report.Class = "database"
		}
	}
	return report
}
