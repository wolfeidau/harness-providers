//go:build integration

package codex

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	hp "github.com/wolfeidau/harness-providers"
)

func TestCodexReviewE2E(t *testing.T) {
	if os.Getenv("HARNESS_LIVE_TEST") != "1" {
		t.Skip("set HARNESS_LIVE_TEST=1 to make model calls")
	}
	_, err := exec.LookPath("codex")
	require.NoError(t, err, "codex binary unavailable")
	home := os.Getenv("HARNESS_TEST_CODEX_HOME")
	if home == "" {
		require.NotEmpty(t, os.Getenv("CODEX_ACCESS_TOKEN"), "set HARNESS_TEST_CODEX_HOME or CODEX_ACCESS_TOKEN; default Codex home is never used")
		home = filepath.Join(t.TempDir(), "codex-home")
		require.NoError(t, os.Mkdir(home, 0700))
	}
	require.NoError(t, checkLiveModel(t, home, "gpt-6-luna", "low"))
	workspace := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(workspace, "access.go"), []byte("package access\n\nfunc CanDelete(actor, owner string) bool {\n return actor != owner\n}\n"), 0600))
	markerBytes := make([]byte, 8)
	_, err = rand.Read(markerBytes)
	require.NoError(t, err)
	marker := hex.EncodeToString(markerBytes)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := New(Config{Home: home, Logger: logger})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	s, err := p.Open(ctx, hp.OpenRequest{WorkingDirectory: workspace})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close Codex session: %v", err)
		}
	})
	input := hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "The read-only sandbox permits reading files. Use the command tool to run `cat access.go` in the current workspace, then review it for authorization bugs. Do not edit files. Keep your answer short. Remember this marker for my next message: " + marker}}, Model: "gpt-6-luna", Effort: "low", Policy: hp.ExecutionPolicy{Approval: hp.ApprovalNever, Sandbox: hp.SandboxReadOnly}}
	turn, err := s.StartTurn(ctx, input)
	require.NoError(t, err)
	require.NotEmpty(t, turn.ID())
	review, err := readLiveTurn(t, turn)
	require.NoError(t, err)
	lower := strings.ToLower(review)
	require.Contains(t, lower, "candelete", "review: %s", review)
	comparison := false
	for _, phrase := range []string{"!=", "==", "not equal", "inequal", "inverted", "reversed", "opposite"} {
		if strings.Contains(lower, phrase) {
			comparison = true
			break
		}
	}
	require.True(t, comparison, "review omitted inverted comparison: %s", review)
	cursor := s.Cursor()
	require.NoError(t, s.Close())
	b, err := json.Marshal(cursor)
	require.NoError(t, err)
	var restored hp.Cursor
	require.NoError(t, json.Unmarshal(b, &restored))
	s2, err := p.Open(ctx, hp.OpenRequest{WorkingDirectory: workspace, Resume: &restored})
	require.NoError(t, err)
	defer func() {
		if err := s2.Close(); err != nil {
			t.Errorf("close resumed Codex session: %v", err)
		}
	}()
	require.Equal(t, string(restored.Data), string(s2.Cursor().Data), "resume changed native thread identity")
	turn2, err := s2.StartTurn(ctx, hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "What was the exact marker in my previous message? Reply with only the marker."}}, Model: "gpt-6-luna", Effort: "low", Policy: hp.ExecutionPolicy{Approval: hp.ApprovalNever, Sandbox: hp.SandboxReadOnly}})
	require.NoError(t, err)
	answer, err := readLiveTurn(t, turn2)
	require.NoError(t, err)
	require.Contains(t, answer, marker, "resume lost previous message marker")
}

func checkLiveModel(t *testing.T, home, model, effort string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	c, err := newClient("codex", nil, environment(home, nil), t.TempDir(), New(Config{}).config.Logger, func(frame) {})
	if err != nil {
		return err
	}
	defer func() { _ = c.close() }()
	if _, err = c.request(ctx, "initialize", map[string]any{"clientInfo": map[string]any{"name": "harness-providers-test", "version": "0.1.0"}, "capabilities": map[string]any{"experimentalApi": true}}); err != nil {
		return err
	}
	if err = c.notify("initialized", map[string]any{}); err != nil {
		return err
	}
	cursor := ""
	for {
		params := map[string]any{"includeHidden": true}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err := c.request(ctx, "model/list", params)
		if err != nil {
			return err
		}
		var page struct {
			Data []struct {
				Model   string `json:"model"`
				Efforts []struct {
					Effort string `json:"reasoningEffort"`
				} `json:"supportedReasoningEfforts"`
			} `json:"data"`
			NextCursor string `json:"nextCursor"`
		}
		if err = json.Unmarshal(raw, &page); err != nil {
			return err
		}
		for _, m := range page.Data {
			if m.Model == model {
				for _, e := range m.Efforts {
					if e.Effort == effort {
						return nil
					}
				}
				return fmt.Errorf("%s is available but %s effort is not", model, effort)
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return fmt.Errorf("%s unavailable from app-server model/list", model)
}

func readLiveTurn(t *testing.T, turn hp.Turn) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	var out strings.Builder
	for {
		e, err := turn.Next(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				stopLiveTurn(turn)
				return "", fmt.Errorf("turn %s exceeded 75 seconds", turn.ID())
			}
			if errors.Is(err, io.EOF) {
				return "", fmt.Errorf("turn %s ended without terminal event", turn.ID())
			}
			return "", err
		}
		if e.Kind == hp.EventRequest {
			stopLiveTurn(turn)
			return "", fmt.Errorf("unexpected interactive request: %s", e.Request.Kind)
		}
		if (e.Kind == hp.EventItemStarted || e.Kind == hp.EventItemFinished) && e.Item != nil {
			t.Logf("codex item: kind=%s status=%s label=%q", e.Item.Kind, e.Item.Status, e.Item.Label)
		}
		if e.Kind == hp.EventTextDelta {
			if out.Len()+len(e.Text) > 8192 {
				stopLiveTurn(turn)
				return "", fmt.Errorf("turn output exceeded 8 KiB")
			}
			out.WriteString(e.Text)
		}
		if e.Kind == hp.EventTurnFinished {
			if e.Outcome == nil || e.Outcome.Status != hp.OutcomeCompleted {
				return "", fmt.Errorf("turn %s ended: %+v", turn.ID(), e.Outcome)
			}
			if out.Len() == 0 {
				return "", fmt.Errorf("turn %s returned no text", turn.ID())
			}
			return out.String(), nil
		}
	}
}

func stopLiveTurn(turn hp.Turn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = turn.Interrupt(ctx)
	for {
		e, err := turn.Next(ctx)
		if err != nil || e.Kind == hp.EventTurnFinished {
			return
		}
	}
}
