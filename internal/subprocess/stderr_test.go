package subprocess

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedactSecrets(t *testing.T) {
	tests := []struct {
		name string
		env  string
		line string
		want string
	}{
		{name: "token redacted", env: "GITHUB_TOKEN=s3cr3tvalue", line: "auth s3cr3tvalue", want: "auth [REDACTED]"},
		{name: "auth redacted", env: "MY_AUTH=abcdefgh", line: "x abcdefgh", want: "x [REDACTED]"},
		{name: "credential redacted", env: "DB_CREDENTIAL=hunter22", line: "x hunter22", want: "x [REDACTED]"},
		{name: "non sensitive kept", env: "PATH=/usr/bin:/bin", line: "x /usr/bin:/bin", want: "x /usr/bin:/bin"},
		{name: "short value kept", env: "API_KEY=abc", line: "x abc", want: "x abc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, RedactSecrets(tt.line, []string{tt.env}))
		})
	}
}

func TestStripANSI(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "codex log line", in: "\x1b[2m2026-10-11T02:46:13Z\x1b[0m \x1b[31mERROR\x1b[0m failed", want: "2026-10-11T02:46:13Z ERROR failed"},
		{name: "plain text kept", in: "x /usr/bin:/bin", want: "x /usr/bin:/bin"},
		{name: "empty", in: "", want: ""},
		{name: "multi param", in: "\x1b[1;31mred\x1b[0m", want: "red"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, StripANSI(tt.in))
		})
	}
}

func TestStderrDebugLogIsBoundedAndRedacted(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	DrainStderr(strings.NewReader("error: SECRET_TOKEN_VALUE\n"+strings.Repeat("x", 5000)+"\n"), logger, "test", []string{"ACCESS_TOKEN=SECRET_TOKEN_VALUE"})
	got := logs.String()
	assert.Contains(t, got, "error: [REDACTED]")
	assert.Contains(t, got, `"truncated":true`)
	assert.NotContains(t, got, "SECRET_TOKEN_VALUE")
	assert.NotContains(t, got, strings.Repeat("x", 5000))
}
