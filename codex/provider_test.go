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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		write := func(v any) { writeFakeFrame(w, v) }
		switch f.Method {
		case "initialize":
			spawnFakeGrandchild(scenario)
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
			fakeTurnStart(write, f, scenario)
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
	fakeExit(scenario)
}

func fakeExit(scenario string) {
	if scenario == "grandchild" {
		select {} // ignore stdin EOF so only the kill can stop us
	}
	os.Exit(0)
}

func spawnFakeGrandchild(scenario string) {
	if scenario != "grandchild" {
		return
	}
	gc := exec.Command("sleep", "60")
	gc.Stdout, gc.Stderr = os.Stdout, os.Stderr
	if err := gc.Start(); err != nil {
		os.Exit(2)
	}
	pid := []byte(strconv.Itoa(gc.Process.Pid))
	if err := os.WriteFile(os.Getenv("HARNESS_FAKE_PIDFILE"), pid, 0600); err != nil {
		os.Exit(2)
	}
}

func writeFakeFrame(w *bufio.Writer, v any) {
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

func fakeTurnStart(write func(any), f frame, scenario string) {
	if scenario == "timeout" {
		return
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
	if scenario == "structured" {
		write(map[string]any{"id": 80, "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "cmd-1", "command": "rm -rf build", "cwd": "/work", "reason": "cleanup"}})
		write(map[string]any{"method": "item/started", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "item": map[string]any{"id": "patch-1", "type": "fileChange", "status": "inProgress", "changes": []any{map[string]any{"path": "/work/a.go", "kind": "update", "diff": ""}, map[string]any{"path": "/work/b.go", "kind": "add", "diff": ""}}}}})
		write(map[string]any{"method": "item/updated", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "item": map[string]any{"id": "patch-1", "type": "fileChange", "status": "inProgress", "changes": []any{}}}})
		write(map[string]any{"id": 81, "method": "item/fileChange/requestApproval", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "patch-1", "grantRoot": "/work", "reason": "needs write"}})
		write(map[string]any{"id": 82, "method": "item/tool/requestUserInput", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "tool-1", "questions": []any{map[string]any{"id": "q1", "header": "Pick", "question": "Which?", "options": []any{map[string]any{"label": "A", "description": "first"}}}}}})
		write(map[string]any{"id": 83, "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "cmd-2", "kind": "writeStdin", "networkApprovalContext": map[string]any{"host": "example.com", "protocol": "https"}}})
		write(map[string]any{"id": 84, "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "cmd-3", "kind": "future"}})
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
	if scenario == "command_failure" {
		write(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "item": map[string]any{"id": "command-1", "type": "commandExecution", "status": "failed", "command": "cat access.go", "exitCode": 1, "aggregatedOutput": "permission denied: SECRET_TOKEN_VALUE"}}})
		write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "completed"}}})
	}
	if scenario == "normal" || scenario == "early" {
		write(map[string]any{"method": "item/agentMessage/delta", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "delta": "hello"}})
		write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "completed"}}})
	}
}

func fakeProvider(t *testing.T, scenario string, extraEnv ...string) *Provider {
	t.Helper()
	bin, err := os.Executable()
	require.NoError(t, err)
	script := filepath.Join(t.TempDir(), "fake-codex")
	content := "#!/bin/sh\nexec '" + strings.ReplaceAll(bin, "'", "'\\''") + "' -test.run=^TestFakePeer$\n"
	require.NoError(t, os.WriteFile(script, []byte(content), 0700))
	return New(Config{Binary: script, Env: append([]string{"HARNESS_FAKE_PEER=1", "HARNESS_FAKE_SCENARIO=" + scenario}, extraEnv...)})
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
	require.NoError(t, err)
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
	require.NoError(t, err)
	assert.Equal(t, "turn-1", turn.ID())
	return turn
}
func next(t *testing.T, turn hp.Turn) hp.Event {
	t.Helper()
	e, err := turn.Next(contextForTest(t))
	require.NoError(t, err)
	return e
}

func TestTurnEventsAndEarlyNotification(t *testing.T) {
	for _, scenario := range []string{"normal", "early"} {
		t.Run(scenario, func(t *testing.T) {
			s := openFake(t, scenario)
			assert.Equal(t, "codex", s.Cursor().Provider)
			turn := startFake(t, s)
			var seen []hp.EventKind
			for {
				e := next(t, turn)
				seen = append(seen, e.Kind)
				if e.Kind == hp.EventTurnFinished {
					require.NotNil(t, e.Outcome)
					assert.Equal(t, hp.OutcomeCompleted, e.Outcome.Status)
					break
				}
			}
			_, err := turn.Next(contextForTest(t))
			require.ErrorIs(t, err, io.EOF)
			if scenario == "early" {
				require.NotEmpty(t, seen)
				assert.Equal(t, hp.EventTurnStarted, seen[0])
			}
		})
	}
}
func TestApprovalAndInterrupt(t *testing.T) {
	s := openFake(t, "approval")
	turn := startFake(t, s)
	e := next(t, turn)
	require.Equal(t, hp.EventRequest, e.Kind)
	require.NotNil(t, e.Request)
	require.NoError(t, turn.Respond(contextForTest(t), e.Request.ID, hp.Response{OptionID: "decline"}))
	require.ErrorIs(t, turn.Respond(contextForTest(t), e.Request.ID, hp.Response{OptionID: "decline"}), hp.ErrRequestNotFound)
	e = next(t, turn)
	assert.Equal(t, hp.EventTurnFinished, e.Kind)
	s2 := openFake(t, "pending")
	turn2 := startFake(t, s2)
	_, err := s2.StartTurn(contextForTest(t), hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "again"}}})
	require.ErrorIs(t, err, hp.ErrBusy)
	require.NoError(t, turn2.Interrupt(contextForTest(t)))
	e = next(t, turn2)
	require.NotNil(t, e.Outcome)
	assert.Equal(t, hp.OutcomeInterrupted, e.Outcome.Status)
}
func TestStructuredRequests(t *testing.T) {
	turn := startFake(t, openFake(t, "structured"))
	var cmd, stdin, patch, input *hp.Request
	for cmd == nil || stdin == nil || patch == nil || input == nil {
		e := next(t, turn)
		switch {
		case e.Kind != hp.EventRequest:
		case e.Request.Approval != nil && e.Request.Approval.Action == hp.ActionCommand:
			cmd = e.Request
		case e.Request.Approval != nil && e.Request.Approval.Action == hp.ActionWriteStdin:
			stdin = e.Request
		case e.Request.Approval != nil:
			patch = e.Request
		default:
			input = e.Request
		}
	}
	assert.Equal(t, "cmd-1", cmd.ItemID)
	assert.Equal(t, &hp.Approval{Action: hp.ActionCommand, Command: "rm -rf build", Cwd: "/work", Reason: "cleanup"}, cmd.Approval)
	assert.Equal(t, &hp.Approval{Action: hp.ActionWriteStdin, Host: "example.com", Protocol: "https"}, stdin.Approval)
	assert.Equal(t, "patch-1", patch.ItemID)
	assert.Equal(t, &hp.Approval{Action: hp.ActionFileChange, Paths: []string{"/work/a.go", "/work/b.go"}, WriteRoot: "/work", Reason: "needs write"}, patch.Approval)
	require.Len(t, input.Questions, 1)
	assert.Equal(t, "Pick", input.Questions[0].Header)
	assert.Equal(t, "first", input.Questions[0].Options[0].Description)
	// The unknown approval kind is rejected, which makes the fake peer end the turn.
	for e := next(t, turn); e.Kind != hp.EventTurnFinished; e = next(t, turn) {
		assert.NotEqual(t, hp.EventRequest, e.Kind)
	}
}
func TestExitAndResumeErrors(t *testing.T) {
	s := openFake(t, "exit")
	turn := startFake(t, s)
	_, err := turn.Next(contextForTest(t))
	require.Error(t, err)
	require.NotErrorIs(t, err, io.EOF)
	require.ErrorContains(t, err, "exit status 3")
	p := fakeProvider(t, "missing")
	_, err = p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir(), Resume: &hp.Cursor{Provider: "codex", Version: 1, Data: json.RawMessage(`{"threadId":"thread-1"}`)}})
	require.ErrorIs(t, err, hp.ErrSessionNotFound)
	_, err = p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir(), Resume: &hp.Cursor{Provider: "claude", Version: 1, Data: json.RawMessage(`{}`)}})
	require.ErrorIs(t, err, hp.ErrInvalidCursor)
}

func TestUncertainStartClosesSession(t *testing.T) {
	s := openFake(t, "timeout")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	in := hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "hello"}}}
	_, err := s.StartTurn(ctx, in)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = s.StartTurn(contextForTest(t), in)
	require.ErrorIs(t, err, hp.ErrClosed)
}

func TestEnvironmentHome(t *testing.T) {
	t.Setenv("CODEX_HOME", "/ambient")
	assert.Contains(t, environment("", nil), "CODEX_HOME=/ambient")
	explicit := environment("/explicit", nil)
	assert.Contains(t, explicit, "CODEX_HOME=/explicit")
	assert.NotContains(t, explicit, "CODEX_HOME=/ambient")
}

func TestEarlyTerminalIsDeliveredBeforeEOF(t *testing.T) {
	for i := range 30 {
		s := openFake(t, "early_terminal")
		turn := startFake(t, s)
		e := next(t, turn)
		require.Equal(t, hp.EventTurnFinished, e.Kind, "iteration %d", i)
		require.NotNil(t, e.Outcome, "iteration %d", i)
		assert.Equal(t, hp.OutcomeCompleted, e.Outcome.Status, "iteration %d", i)
		_, err := turn.Next(contextForTest(t))
		require.ErrorIs(t, err, io.EOF, "iteration %d", i)
		_ = s.Close()
	}
}
func TestUnknownRequestGetsProtocolError(t *testing.T) {
	s := openFake(t, "unknown_request")
	turn := startFake(t, s)
	assert.Equal(t, hp.EventTurnFinished, next(t, turn).Kind)
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
			assert.Equal(t, 1, count)
			assert.Equal(t, "hello", text)
		})
	}
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
	require.NoError(t, err)
	turn, err := s.StartTurn(contextForTest(t), hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: secret}}})
	require.NoError(t, err)
	for {
		if e := next(t, turn); e.Kind == hp.EventTurnFinished {
			break
		}
	}
	require.NoError(t, s.Close())
	got := logs.String()
	for _, want := range []string{"codex app-server spawned", "codex initialized", "codex session opened", "codex turn admitted", "codex turn finished", "codex session closing", `"thread_id":"thread-1"`, `"turn_id":"turn-1"`} {
		assert.Contains(t, got, want)
	}
	for _, forbidden := range []string{secret, envSecret, workspace, "hello", "HARNESS_FAKE_PEER"} {
		assert.NotContains(t, got, forbidden)
	}
}

func TestStderrDebugLogIsBoundedAndRedacted(t *testing.T) {
	var logs lockedBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	drainStderr(strings.NewReader("error: SECRET_TOKEN_VALUE\n"+strings.Repeat("x", 5000)+"\n"), logger, []string{"ACCESS_TOKEN=SECRET_TOKEN_VALUE"})
	got := logs.String()
	assert.Contains(t, got, "error: [REDACTED]")
	assert.Contains(t, got, `"truncated":true`)
	assert.NotContains(t, got, "SECRET_TOKEN_VALUE")
	assert.NotContains(t, got, strings.Repeat("x", 5000))
}

func TestDebugLogsFailedCommandOutcome(t *testing.T) {
	var logs lockedBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := fakeProvider(t, "command_failure")
	p.config.Logger = logger
	p.config.Env = append(p.config.Env, "ACCESS_TOKEN=SECRET_TOKEN_VALUE")
	s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir()})
	require.NoError(t, err)
	turn := startFake(t, s)
	for {
		if e := next(t, turn); e.Kind == hp.EventTurnFinished {
			break
		}
	}
	require.NoError(t, s.Close())
	got := logs.String()
	for _, want := range []string{`"item_type":"commandExecution"`, `"status":"failed"`, `"exit_code":1`, "permission denied: [REDACTED]"} {
		assert.Contains(t, got, want)
	}
	assert.NotContains(t, got, "SECRET_TOKEN_VALUE")
}

func TestDebugLogsApprovalAndInterrupt(t *testing.T) {
	var logs lockedBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := fakeProvider(t, "approval")
	p.config.Logger = logger
	s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir()})
	require.NoError(t, err)
	turn := startFake(t, s)
	e := next(t, turn)
	require.Equal(t, hp.EventRequest, e.Kind)
	require.NotNil(t, e.Request)
	require.NoError(t, turn.Respond(contextForTest(t), e.Request.ID, hp.Response{OptionID: "decline"}))
	assert.Equal(t, hp.EventTurnFinished, next(t, turn).Kind)
	_ = s.Close()

	p = fakeProvider(t, "pending")
	p.config.Logger = logger
	s, err = p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir()})
	require.NoError(t, err)
	turn = startFake(t, s)
	require.NoError(t, turn.Interrupt(contextForTest(t)))
	assert.Equal(t, hp.EventTurnFinished, next(t, turn).Kind)
	_ = s.Close()

	got := logs.String()
	for _, want := range []string{"codex server request", "codex server request answered", "codex interrupt requested", "codex interrupt acknowledged"} {
		assert.Contains(t, got, want)
	}
}

func TestItemStatusNormalization(t *testing.T) {
	for raw, want := range map[string]hp.ItemStatus{
		"inProgress":  hp.ItemRunning,
		"completed":   hp.ItemCompleted,
		"failed":      hp.ItemFailed,
		"declined":    hp.ItemDeclined,
		"interrupted": hp.ItemInterrupted,
		"queued":      hp.ItemStatus("queued"),
	} {
		assert.Equal(t, want, itemStatus(raw), raw)
	}
}

func TestOpenRejectsFileWorkspace(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	_, err := New(Config{}).Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: file})
	require.Error(t, err)
	assert.True(t, errors.Is(err, syscall.ENOTDIR))
}

func TestCloseKillsProcessGroupAndDoesNotHang(t *testing.T) {
	prev := closeGrace
	closeGrace = 100 * time.Millisecond
	t.Cleanup(func() { closeGrace = prev })
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	s, err := fakeProvider(t, "grandchild", "HARNESS_FAKE_PIDFILE="+pidFile).Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir()})
	require.NoError(t, err)
	raw, err := os.ReadFile(pidFile) //nolint:gosec // Test-controlled path.
	require.NoError(t, err)
	pid, err := strconv.Atoi(string(raw))
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(pid, 0))

	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
	require.Eventually(t, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) }, 2*time.Second, 10*time.Millisecond)
}

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
			require.Equal(t, tt.want, redactSecrets(tt.line, []string{tt.env}))
		})
	}
}
