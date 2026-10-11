package subprocess

import (
	"bufio"
	"errors"
	"io"
	"log/slog"
	"regexp"
	"strings"
)

var sensitiveKeyParts = []string{"TOKEN", "KEY", "SECRET", "PASS", "AUTH", "CREDENTIAL"}

// Provider CLIs colour their stderr; escape codes make structured logs unreadable.
var ansiEscape = regexp.MustCompile("\x1b\\[[0-9;?]*[ -/]*[@-~]")

// DrainStderr logs bounded, redacted stderr lines; name prefixes the log messages.
func DrainStderr(r io.Reader, logger *slog.Logger, name string, env []string) {
	const maxLines = 100
	const maxLineBytes = 4096
	reader := bufio.NewReader(r)
	lines := 0
	for {
		var line []byte
		truncated := false
		for {
			part, prefix, err := reader.ReadLine()
			if err != nil {
				if err != io.EOF {
					logger.Debug(name+" stderr read failed", "error_kind", FailureKind(err))
				}
				return
			}
			if lines < maxLines {
				remaining := maxLineBytes - len(line)
				if len(part) > remaining {
					line = append(line, part[:remaining]...)
					truncated = true
				} else {
					line = append(line, part...)
				}
			}
			if !prefix {
				break
			}
		}
		if lines == maxLines {
			logger.Debug(name+" stderr limit reached", "lines", maxLines)
		}
		if lines < maxLines {
			logger.Debug(name+" stderr", "line", RedactSecrets(StripANSI(string(line)), env), "truncated", truncated)
		}
		lines++
	}
}

// FailureKind classifies err without leaking its message into logs.
func FailureKind(err error) string {
	switch {
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, io.EOF):
		return "eof"
	default:
		return "other"
	}
}

func StripANSI(s string) string {
	return ansiEscape.ReplaceAllString(s, "")
}

func RedactSecrets(line string, env []string) string {
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || len(value) < 4 {
			continue
		}
		key = strings.ToUpper(key)
		for _, part := range sensitiveKeyParts {
			if strings.Contains(key, part) {
				line = strings.ReplaceAll(line, value, "[REDACTED]")
				break
			}
		}
	}
	return line
}
