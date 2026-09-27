package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	hp "github.com/wolfeidau/harness-providers"
)

func TestFakePeer(t *testing.T) {
	if os.Getenv("HARNESS_FAKE_PEER") == "" {
		return
	}
	s := bufio.NewScanner(os.Stdin)
	w := bufio.NewWriter(os.Stdout)
	scenario := os.Getenv("HARNESS_FAKE_SCENARIO")
	for s.Scan() {
		var f frame
		if json.Unmarshal(s.Bytes(), &f) != nil {
			os.Exit(2)
		}
		write := func(v any) {
			b, err := json.Marshal(v)
			if err != nil {
				os.Exit(2)
			}
			if _, err := fmt.Fprintln(w, string(b)); err != nil {
				os.Exit(2)
			}
			if err := w.Flush(); err != nil {
				os.Exit(2)
			}
		}
		switch f.Method {
		case "initialize":
			write(map[string]any{"id": f.ID, "result": map[string]any{}})
		case "thread/start":
			write(map[string]any{"id": f.ID, "result": map[string]any{"thread": map[string]any{"id": "thread-1"}}})
		case "thread/resume":
			if scenario == "missing" {
				write(map[string]any{"id": f.ID, "error": map[string]any{"code": -32000, "message": "thread not found"}})
			} else {
				write(map[string]any{"id": f.ID, "result": map[string]any{"thread": map[string]any{"id": "thread-1"}}})
			}
		case "turn/start":
			if scenario == "timeout" {
				continue
			}
			if scenario == "early_terminal" {
				write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "completed"}}})
			}
			if scenario == "early" {
				write(map[string]any{"method": "turn/started", "params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1"}}})
			}
			write(map[string]any{"id": f.ID, "result": map[string]any{"turn": map[string]any{"id": "turn-1"}}})
			if scenario == "exit" {
				os.Exit(3)
			}
			if scenario == "approval" {
				write(map[string]any{"id": 77, "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-1"}})
			}
			if scenario == "unknown_request" {
				write(map[string]any{"id": 78, "method": "item/unknown/requestApproval", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1"}})
			}
			if scenario == "snapshot" || scenario == "delta_snapshot" {
				if scenario == "delta_snapshot" {
					write(map[string]any{"method": "item/agentMessage/delta", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "message-1", "delta": "hello"}})
				}
				write(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "item": map[string]any{"id": "message-1", "type": "agentMessage", "text": "hello"}}})
				write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "completed"}}})
			}
			if scenario == "normal" || scenario == "early" {
				write(map[string]any{"method": "item/agentMessage/delta", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "delta": "hello"}})
				write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "completed"}}})
			}
		case "turn/interrupt":
			write(map[string]any{"id": f.ID, "result": map[string]any{}})
			write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "interrupted"}}})
		default:
			if len(f.ID) > 0 && f.Method == "" {
				if scenario == "unknown_request" && (f.Error == nil || f.Error.Code != -32601) {
					os.Exit(4)
				}
				write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "completed"}}})
			}
		}
	}
	os.Exit(0)
}

func fakeProvider(t *testing.T, scenario string) *Provider {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "fake-codex")
	content := "#!/bin/sh\nexec '" + strings.ReplaceAll(bin, "'", "'\\''") + "' -test.run=^TestFakePeer$\n"
	if err = os.WriteFile(script, []byte(content), 0700); err != nil {
		t.Fatal(err)
	}
	return New(Config{Binary: script, Env: []string{"HARNESS_FAKE_PEER=1", "HARNESS_FAKE_SCENARIO=" + scenario}})
}
func contextForTest(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func openFake(t *testing.T, scenario string) hp.Session {
	t.Helper()
	s, err := fakeProvider(t, scenario).Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close fake session: %v", err)
		}
	})
	return s
}
func startFake(t *testing.T, s hp.Session) hp.Turn {
	t.Helper()
	turn, err := s.StartTurn(contextForTest(t), hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}
	if turn.ID() != "turn-1" {
		t.Fatalf("id=%q", turn.ID())
	}
	return turn
}
func next(t *testing.T, turn hp.Turn) hp.Event {
	t.Helper()
	e, err := turn.Next(contextForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestTurnEventsAndEarlyNotification(t *testing.T) {
	for _, scenario := range []string{"normal", "early"} {
		t.Run(scenario, func(t *testing.T) {
			s := openFake(t, scenario)
			if s.Cursor().Provider != "codex" {
				t.Fatal(s.Cursor())
			}
			turn := startFake(t, s)
			var seen []hp.EventKind
			for {
				e := next(t, turn)
				seen = append(seen, e.Kind)
				if e.Kind == hp.EventTurnFinished {
					if e.Outcome.Status != hp.OutcomeCompleted {
						t.Fatal(e)
					}
					break
				}
			}
			if _, err := turn.Next(contextForTest(t)); err != io.EOF {
				t.Fatalf("want EOF: %v", err)
			}
			if scenario == "early" && seen[0] != hp.EventTurnStarted {
				t.Fatalf("early event lost: %v", seen)
			}
		})
	}
}
func TestApprovalAndInterrupt(t *testing.T) {
	s := openFake(t, "approval")
	turn := startFake(t, s)
	e := next(t, turn)
	if e.Kind != hp.EventRequest {
		t.Fatal(e)
	}
	if err := turn.Respond(contextForTest(t), e.Request.ID, hp.Response{OptionID: "decline"}); err != nil {
		t.Fatal(err)
	}
	if err := turn.Respond(contextForTest(t), e.Request.ID, hp.Response{OptionID: "decline"}); !errors.Is(err, hp.ErrRequestNotFound) {
		t.Fatal(err)
	}
	if e = next(t, turn); e.Kind != hp.EventTurnFinished {
		t.Fatal(e)
	}
	s2 := openFake(t, "pending")
	turn2 := startFake(t, s2)
	if _, err := s2.StartTurn(contextForTest(t), hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "again"}}}); !errors.Is(err, hp.ErrBusy) {
		t.Fatal(err)
	}
	if err := turn2.Interrupt(contextForTest(t)); err != nil {
		t.Fatal(err)
	}
	if e = next(t, turn2); e.Outcome.Status != hp.OutcomeInterrupted {
		t.Fatal(e)
	}
}
func TestExitAndResumeErrors(t *testing.T) {
	s := openFake(t, "exit")
	turn := startFake(t, s)
	if _, err := turn.Next(contextForTest(t)); err == nil || err == io.EOF {
		t.Fatalf("expected transport error: %v", err)
	}
	p := fakeProvider(t, "missing")
	_, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir(), Resume: &hp.Cursor{Provider: "codex", Version: 1, Data: json.RawMessage(`{"threadId":"thread-1"}`)}})
	if !errors.Is(err, hp.ErrSessionNotFound) {
		t.Fatal(err)
	}
	_, err = p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir(), Resume: &hp.Cursor{Provider: "claude", Version: 1, Data: json.RawMessage(`{}`)}})
	if !errors.Is(err, hp.ErrInvalidCursor) {
		t.Fatal(err)
	}
}

func TestUncertainStartClosesSession(t *testing.T) {
	s := openFake(t, "timeout")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	in := hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "hello"}}}
	_, err := s.StartTurn(ctx, in)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout: %v", err)
	}
	_, err = s.StartTurn(contextForTest(t), in)
	if !errors.Is(err, hp.ErrClosed) {
		t.Fatalf("uncertain session still accepts turns: %v", err)
	}
}

func TestEnvironmentHome(t *testing.T) {
	t.Setenv("CODEX_HOME", "/ambient")
	if got := environment("", nil); !contains(got, "CODEX_HOME=/ambient") {
		t.Fatal("ambient home was dropped")
	}
	if got := environment("/explicit", nil); !contains(got, "CODEX_HOME=/explicit") || contains(got, "CODEX_HOME=/ambient") {
		t.Fatalf("explicit home was not authoritative: %v", got)
	}
}

func TestEarlyTerminalIsDeliveredBeforeEOF(t *testing.T) {
	for i := range 30 {
		s := openFake(t, "early_terminal")
		turn := startFake(t, s)
		e := next(t, turn)
		if e.Kind != hp.EventTurnFinished || e.Outcome.Status != hp.OutcomeCompleted {
			t.Fatalf("iteration %d: %+v", i, e)
		}
		if _, err := turn.Next(contextForTest(t)); err != io.EOF {
			t.Fatalf("iteration %d: want EOF, got %v", i, err)
		}
		_ = s.Close()
	}
}
func TestUnknownRequestGetsProtocolError(t *testing.T) {
	s := openFake(t, "unknown_request")
	turn := startFake(t, s)
	if e := next(t, turn); e.Kind != hp.EventTurnFinished {
		t.Fatalf("server request hung: %+v", e)
	}
}
func TestAgentMessageSnapshotFallback(t *testing.T) {
	for _, scenario := range []string{"snapshot", "delta_snapshot"} {
		t.Run(scenario, func(t *testing.T) {
			turn := startFake(t, openFake(t, scenario))
			count := 0
			text := ""
			for {
				e := next(t, turn)
				if e.Kind == hp.EventTextDelta {
					count++
					text += e.Text
				}
				if e.Kind == hp.EventTurnFinished {
					break
				}
			}
			if count != 1 || text != "hello" {
				t.Fatalf("text events=%d text=%q", count, text)
			}
		})
	}
}
func contains(v []string, w string) bool {
	for _, x := range v {
		if x == w {
			return true
		}
	}
	return false
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestDebugLogsLifecycleWithoutContent(t *testing.T) {
	var logs lockedBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := fakeProvider(t, "normal")
	p.config.Logger = logger
	secret := "PROMPT_SECRET_6a93"
	envSecret := "ENV_SECRET_81d2"
	p.config.Env = append(p.config.Env, "TEST_CREDENTIAL="+envSecret)
	workspace := t.TempDir()
	s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: workspace})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.StartTurn(contextForTest(t), hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: secret}}})
	if err != nil {
		t.Fatal(err)
	}
	for {
		if e := next(t, turn); e.Kind == hp.EventTurnFinished {
			break
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	got := logs.String()
	for _, want := range []string{"codex app-server spawned", "codex initialized", "codex session opened", "codex turn admitted", "codex turn finished", "codex session closing", `"thread_id":"thread-1"`, `"turn_id":"turn-1"`} {
		if !strings.Contains(got, want) {
			t.Errorf("missing log %q", want)
		}
	}
	for _, forbidden := range []string{secret, envSecret, workspace, "hello", "HARNESS_FAKE_PEER"} {
		if strings.Contains(got, forbidden) {
			t.Errorf("sensitive content in logs: %q", forbidden)
		}
	}
}

func TestDebugLogsApprovalAndInterrupt(t *testing.T) {
	var logs lockedBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := fakeProvider(t, "approval")
	p.config.Logger = logger
	s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	turn := startFake(t, s)
	e := next(t, turn)
	if e.Kind != hp.EventRequest {
		t.Fatal(e)
	}
	if err := turn.Respond(contextForTest(t), e.Request.ID, hp.Response{OptionID: "decline"}); err != nil {
		t.Fatal(err)
	}
	if e := next(t, turn); e.Kind != hp.EventTurnFinished {
		t.Fatal(e)
	}
	_ = s.Close()

	p = fakeProvider(t, "pending")
	p.config.Logger = logger
	s, err = p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	turn = startFake(t, s)
	if err := turn.Interrupt(contextForTest(t)); err != nil {
		t.Fatal(err)
	}
	if e := next(t, turn); e.Kind != hp.EventTurnFinished {
		t.Fatal(e)
	}
	_ = s.Close()

	got := logs.String()
	for _, want := range []string{"codex server request", "codex server request answered", "codex interrupt requested", "codex interrupt acknowledged"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing log %q", want)
		}
	}
}
