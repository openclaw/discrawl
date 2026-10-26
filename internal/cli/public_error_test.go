package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

type unprintableError struct{}

func (unprintableError) Error() string { panic("private error must not be formatted") }

func TestPublicFailureClassification(t *testing.T) {
	private := unprintableError{}
	rest := &discordgo.RESTError{
		Response:     &http.Response{StatusCode: http.StatusForbidden},
		ResponseBody: []byte("private response"), Message: &discordgo.APIErrorMessage{Code: 50001, Message: "private channel"},
	}
	cases := []struct {
		name string
		err  error
		want publicFailure
	}{
		{"unknown", private, publicFailure{Schema: 1, Class: "unknown", Exit: 1}},
		{"usage", usageErr(private), publicFailure{Schema: 1, Class: "usage", Exit: 2}},
		{"config", configErr(private), publicFailure{Schema: 1, Class: "config", Exit: 3}},
		{"auth", authErr(private), publicFailure{Schema: 1, Class: "auth", Exit: 4}},
		{"database", dbErr(private), publicFailure{Schema: 1, Class: "database", Exit: 5}},
		{"cancelled", errors.Join(private, context.Canceled), publicFailure{Schema: 1, Class: "cancelled", Exit: 1}},
		{"deadline", fmt.Errorf("private path: %w", context.DeadlineExceeded), publicFailure{Schema: 1, Class: "deadline", Exit: 1}},
		{"http", errors.Join(private, authErr(rest)), publicFailure{Schema: 1, Class: "http", Exit: 4, HTTPStatus: http.StatusForbidden, DiscordCode: 50001}},
		{"rate_limit", &discordgo.RateLimitError{}, publicFailure{Schema: 1, Class: "rate_limit", Exit: 1}},
		{"network", &net.DNSError{Err: "private endpoint", Name: "private.invalid"}, publicFailure{Schema: 1, Class: "network", Exit: 1}},
		{"network_timeout", &net.DNSError{Err: "private endpoint", IsTimeout: true}, publicFailure{Schema: 1, Class: "network_timeout", Exit: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := &publicError{err: tc.err}
			var got publicFailure
			require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(err.Error(), "discrawl_error=")), &got))
			require.Equal(t, tc.want, got)
			require.Equal(t, ExitCode(tc.err), ExitCode(err))
			require.ErrorIs(t, err, tc.err)
			require.NotContains(t, err.Error(), "private")
		})
	}
	var typed *discordgo.RESTError
	require.ErrorAs(t, &publicError{err: authErr(rest)}, &typed)
	require.Same(t, rest, typed)
}

func TestPublicFailureSQLiteCode(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.ExecContext(t.Context(), "SELECT private_column FROM private_table")
	require.Error(t, err)
	report := describePublicFailure(dbErr(err))
	require.Equal(t, publicFailure{Schema: 1, Class: "sqlite", Exit: 5, SQLiteCode: 1}, report)
}

func TestPublicErrorsOptIn(t *testing.T) {
	t.Setenv("DISCRAWL_NO_AUTO_UPDATE", "1")
	for _, command := range [][]string{{"private-command"}, {"--private-flag"}} {
		err := Run(context.Background(), command, &bytes.Buffer{}, &bytes.Buffer{})
		require.ErrorContains(t, err, "private-")
		err = Run(context.Background(), append([]string{"--public-errors"}, command...), &bytes.Buffer{}, &bytes.Buffer{})
		require.EqualError(t, err, `discrawl_error={"schema":1,"class":"usage","exit":2}`)
		require.Equal(t, 2, ExitCode(err))
	}
	for _, args := range [][]string{{"--public-errors=false", "--private-flag"}, {"private-command", "--public-errors"}} {
		err := Run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{})
		require.ErrorContains(t, err, "private-")
		require.NotContains(t, err.Error(), "discrawl_error=")
	}
	var help bytes.Buffer
	require.NoError(t, Run(context.Background(), []string{"--help"}, &help, &bytes.Buffer{}))
	require.Contains(t, help.String(), "--public-errors")
	require.Contains(t, strings.Join(strings.Fields(help.String()), " "), "other output remains private")
}
