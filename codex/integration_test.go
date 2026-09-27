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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hp "github.com/wolfeidau/harness-providers"
)

func TestCodexReviewE2E(t *testing.T) {
	if os.Getenv("HARNESS_LIVE_TEST") != "1" {
		t.Skip("set HARNESS_LIVE_TEST=1 to make model calls")
	}
	if _, err := exec.LookPath("codex"); err != nil {
		t.Fatal("codex binary unavailable:", err)
	}
	home := os.Getenv("HARNESS_TEST_CODEX_HOME")
	if home == "" {
		if os.Getenv("CODEX_ACCESS_TOKEN") == "" {
			t.Fatal("set HARNESS_TEST_CODEX_HOME or CODEX_ACCESS_TOKEN; default Codex home is never used")
		}
		home = filepath.Join(t.TempDir(), "codex-home")
		if err := os.Mkdir(home, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := checkLiveModel(t, home, "gpt-6-luna", "low"); err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "access.go"), []byte("package access\n\nfunc CanDelete(actor, owner string) bool {\n return actor != owner\n}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	markerBytes := make([]byte, 8)
	if _, err := rand.Read(markerBytes); err != nil {
		t.Fatal(err)
	}
	marker := hex.EncodeToString(markerBytes)
	p := New(Config{Home: home})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	s, err := p.Open(ctx, hp.OpenRequest{WorkingDirectory: workspace})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close Codex session: %v", err)
		}
	})
	input := hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "Review access.go for authorization bugs. Do not edit files. Keep your answer short. Remember this marker for my next message: " + marker}}, Model: "gpt-6-luna", Effort: "low", Policy: hp.ExecutionPolicy{Approval: hp.ApprovalNever, Sandbox: hp.SandboxReadOnly}}
	turn, err := s.StartTurn(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if turn.ID() == "" {
		t.Fatal("empty turn ID")
	}
	review, err := readLiveTurn(turn)
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(review)
	if !strings.Contains(lower, "candelete") {
		t.Fatalf("review omitted CanDelete: %s", review)
	}
	comparison := false
	for _, phrase := range []string{"!=", "not equal", "inequal", "inverted", "reversed", "opposite"} {
		if strings.Contains(lower, phrase) {
			comparison = true
			break
		}
	}
	if !comparison {
		t.Fatalf("review omitted inverted comparison: %s", review)
	}
	if !strings.Contains(lower, "unauthoriz") && !strings.Contains(lower, "non-owner") && !strings.Contains(lower, "non owner") {
		t.Fatalf("review omitted unauthorized deletion: %s", review)
	}
	cursor := s.Cursor()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(cursor)
	if err != nil {
		t.Fatal(err)
	}
	var restored hp.Cursor
	if err = json.Unmarshal(b, &restored); err != nil {
		t.Fatal(err)
	}
	s2, err := p.Open(ctx, hp.OpenRequest{WorkingDirectory: workspace, Resume: &restored})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s2.Close(); err != nil {
			t.Errorf("close resumed Codex session: %v", err)
		}
	}()
	if string(s2.Cursor().Data) != string(restored.Data) {
		t.Fatal("resume changed native thread identity")
	}
	turn2, err := s2.StartTurn(ctx, hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "What was the exact marker in my previous message? Reply with only the marker."}}, Model: "gpt-6-luna", Effort: "low", Policy: hp.ExecutionPolicy{Approval: hp.ApprovalNever, Sandbox: hp.SandboxReadOnly}})
	if err != nil {
		t.Fatal(err)
	}
	answer, err := readLiveTurn(turn2)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(answer, marker) {
		t.Fatalf("resume lost previous message marker: %q", answer)
	}
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

func readLiveTurn(turn hp.Turn) (string, error) {
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
