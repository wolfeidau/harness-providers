//go:build integration

package claude

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

const (
	liveModel  = "claude-haiku-5-5"
	liveEffort = "low"
)

func liveProvider(t *testing.T) *Provider {
	t.Helper()
	if os.Getenv("HARNESS_LIVE_TEST") != "1" {
		t.Skip("set HARNESS_LIVE_TEST=1 to make model calls")
	}
	_, err := exec.LookPath("claude")
	require.NoError(t, err, "claude binary unavailable")
	dir := os.Getenv("HARNESS_TEST_CLAUDE_CONFIG_DIR")
	if dir == "" {
		if os.Getenv("ANTHROPIC_API_KEY") == "" {
			require.NotEmpty(t, os.Getenv("CLAUDE_CODE_OAUTH_TOKEN"), "set HARNESS_TEST_CLAUDE_CONFIG_DIR, ANTHROPIC_API_KEY or CLAUDE_CODE_OAUTH_TOKEN; default Claude config dir is never used")
		}
		dir = filepath.Join(t.TempDir(), "claude-config")
		require.NoError(t, os.Mkdir(dir, 0700))
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return New(Config{ConfigDir: dir, Logger: logger})
}

func TestClaudeReviewE2E(t *testing.T) {
	p := liveProvider(t)
	workspace := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(workspace, "access.go"), []byte("package access\n\nfunc CanDelete(actor, owner string) bool {\n return actor != owner\n}\n"), 0600))
	markerBytes := make([]byte, 8)
	_, err := rand.Read(markerBytes)
	require.NoError(t, err)
	marker := hex.EncodeToString(markerBytes)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	s, err := p.Open(ctx, hp.OpenRequest{WorkingDirectory: workspace})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close Claude session: %v", err)
		}
	})
	input := hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "Use the Read tool on access.go in the current workspace, then review it for authorization bugs, naming the affected function. Do not edit files. Keep your answer short. Remember this marker for my next message: " + marker}}, Model: liveModel, Effort: liveEffort, Policy: hp.ExecutionPolicy{Approval: hp.ApprovalNever, Sandbox: hp.SandboxReadOnly}}
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
			t.Errorf("close resumed Claude session: %v", err)
		}
	}()
	require.Equal(t, string(restored.Data), string(s2.Cursor().Data), "resume changed native thread identity")
	turn2, err := s2.StartTurn(ctx, hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "What was the exact marker in my previous message? Reply with only the marker."}}, Model: liveModel, Effort: liveEffort, Policy: hp.ExecutionPolicy{Approval: hp.ApprovalNever, Sandbox: hp.SandboxReadOnly}})
	require.NoError(t, err)
	answer, err := readLiveTurn(t, turn2)
	require.NoError(t, err)
	require.Contains(t, answer, marker, "resume lost previous message marker")
}

func TestClaudeApprovalE2E(t *testing.T) {
	p := liveProvider(t)
	workspace := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	s, err := p.Open(ctx, hp.OpenRequest{WorkingDirectory: workspace})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close Claude session: %v", err)
		}
	})
	prompt := "Use the Bash tool to run exactly `touch approved.txt` in the current directory. If it is denied, stop and do not try any other way."
	turn, err := s.StartTurn(ctx, hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: prompt}}, Model: liveModel, Effort: liveEffort, Policy: hp.ExecutionPolicy{Approval: hp.ApprovalOnRequest, Sandbox: hp.SandboxDefault}})
	require.NoError(t, err)
	var finished *hp.Outcome
	declined := false
	for finished == nil {
		e, err := turn.Next(ctx)
		require.NoError(t, err)
		switch {
		case e.Kind == hp.EventRequest && e.Request.Kind == hp.RequestApproval:
			require.False(t, declined, "unexpected second approval request: %+v", e.Request.Approval)
			require.NotEmpty(t, e.Request.ItemID)
			a := e.Request.Approval
			require.NotNil(t, a)
			t.Logf("approval: %+v", *a)
			require.Equal(t, hp.ActionCommand, a.Action)
			require.Contains(t, a.Command, "touch approved.txt")
			require.NotEmpty(t, a.Cwd)
			require.NoError(t, turn.Respond(ctx, e.Request.ID, hp.Response{OptionID: "decline"}))
			declined = true
		case e.Kind == hp.EventRequest:
			stopLiveTurn(turn)
			t.Fatalf("unexpected request kind: %s", e.Request.Kind)
		case e.Kind == hp.EventTurnFinished:
			finished = e.Outcome
		}
	}
	require.True(t, declined, "turn finished without an approval request: %+v", finished)
	require.NotNil(t, finished)
	_, err = os.Stat(filepath.Join(workspace, "approved.txt"))
	require.ErrorIs(t, err, os.ErrNotExist, "declined command created the file")
}

func readLiveTurn(t *testing.T, turn hp.Turn) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 75*time.Second)
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
			t.Logf("claude item: kind=%s status=%s label=%q", e.Item.Kind, e.Item.Status, e.Item.Label)
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
