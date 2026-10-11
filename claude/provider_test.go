package claude

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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

const fakeMessageID = "msg_fake_1"

type peerFrame struct {
	Type      string `json:"type"`
	UUID      string `json:"uuid"`
	RequestID string `json:"request_id"`
	Request   *struct {
		Subtype string `json:"subtype"`
	} `json:"request"`
	Response *struct {
		RequestID string `json:"request_id"`
		Response  struct {
			Behavior string `json:"behavior"`
		} `json:"response"`
	} `json:"response"`
}

type peerState struct {
	w           *bufio.Writer
	scenario    string
	sid         string
	turn        string
	outstanding int
	pendingIDs  []string
}

func (p *peerState) write(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		os.Exit(2)
	}
	if _, err := fmt.Fprintln(p.w, string(b)); err != nil {
		os.Exit(2)
	}
	if err := p.w.Flush(); err != nil {
		os.Exit(2)
	}
}

func appendLine(path, line string) {
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // Test-controlled path.
	if err != nil {
		os.Exit(2)
	}
	defer f.Close() //nolint:errcheck // Test helper.
	_, _ = fmt.Fprintln(f, line)
}

func argValue(args []string, name string) string {
	if i := slices.Index(args, name); i >= 0 && i+1 < len(args) {
		return args[i+1]
	}
	return ""
}

func TestFakePeer(t *testing.T) {
	if os.Getenv("HARNESS_FAKE_PEER") == "" {
		return
	}
	args := flag.Args()
	argv, _ := json.Marshal(args)
	appendLine(os.Getenv("HARNESS_FAKE_ARGVLOG"), string(argv))
	p := &peerState{w: bufio.NewWriter(os.Stdout), scenario: os.Getenv("HARNESS_FAKE_SCENARIO"), sid: argValue(args, "--session-id")}
	if p.sid == "" {
		p.sid = argValue(args, "--resume")
	}
	s := bufio.NewScanner(os.Stdin)
	s.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for s.Scan() {
		appendLine(os.Getenv("HARNESS_FAKE_RECVLOG"), s.Text())
		var f peerFrame
		if json.Unmarshal(s.Bytes(), &f) != nil {
			os.Exit(2)
		}
		switch {
		case f.Type == "control_request" && f.Request.Subtype == "initialize":
			p.initialize(f)
		case f.Type == "control_request" && f.Request.Subtype == "interrupt":
			p.interrupt(f)
		case f.Type == "user":
			p.userMessage(f)
		case f.Type == "control_response":
			p.answered(f)
		}
	}
	if p.scenario == "grandchild" {
		select {} // ignore stdin EOF so only the kill can stop us
	}
	os.Exit(0)
}

func (p *peerState) initialize(f peerFrame) {
	if p.scenario == "missing" {
		msg := "No conversation found with session ID: " + p.sid
		p.write(map[string]any{"type": "result", "subtype": "error_during_execution", "is_error": true, "errors": []string{msg}, "session_id": p.sid})
		fmt.Fprintln(os.Stderr, msg)
		os.Exit(1)
	}
	if p.scenario == "grandchild" {
		gc := exec.Command("sleep", "60")
		gc.Stdout, gc.Stderr = os.Stdout, os.Stderr
		if err := gc.Start(); err != nil {
			os.Exit(2)
		}
		if err := os.WriteFile(os.Getenv("HARNESS_FAKE_PIDFILE"), []byte(strconv.Itoa(gc.Process.Pid)), 0o600); err != nil {
			os.Exit(2)
		}
	}
	p.write(map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": f.RequestID, "response": map[string]any{"commands": []any{}}}})
}

func (p *peerState) lifecycle(state string) {
	p.write(map[string]any{"type": "command_lifecycle", "command_uuid": p.turn, "state": state, "session_id": p.sid})
}

func (p *peerState) toolRequest(id, tool string, input map[string]any, toolUseID string) {
	p.write(map[string]any{"type": "assistant", "message": map[string]any{"id": fakeMessageID, "content": []any{map[string]any{"type": "tool_use", "id": toolUseID, "name": tool, "input": input}}}})
	p.write(map[string]any{"type": "control_request", "request_id": id, "request": map[string]any{"subtype": "can_use_tool", "tool_name": tool, "input": input, "tool_use_id": toolUseID, "description": "peer description"}})
	p.pendingIDs = append(p.pendingIDs, id)
	p.outstanding++
}

func (p *peerState) text(delta bool, snapshot bool) {
	if delta {
		p.write(map[string]any{"type": "stream_event", "event": map[string]any{"type": "content_block_delta", "delta": map[string]any{"type": "text_delta", "text": "pong"}}, "api_message_id": fakeMessageID})
	}
	if snapshot {
		p.write(map[string]any{"type": "assistant", "message": map[string]any{"id": fakeMessageID, "content": []any{map[string]any{"type": "text", "text": "pong"}}}})
	}
}

func (p *peerState) result(extra map[string]any) {
	r := map[string]any{"type": "result", "subtype": "success", "is_error": false, "result": "pong", "terminal_reason": "completed", "user_message_uuid": p.turn, "session_id": p.sid,
		"usage": map[string]any{"input_tokens": 2, "cache_creation_input_tokens": 10, "cache_read_input_tokens": 5, "output_tokens": 4}}
	for k, v := range extra {
		r[k] = v
	}
	p.write(r)
	p.lifecycle("completed")
}

func (p *peerState) userMessage(f peerFrame) {
	p.turn = f.UUID
	switch p.scenario {
	case "no_admission":
		return
	case "echo":
		p.write(map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": "echo-1", "response": map[string]any{"behavior": "allow"}}})
	case "exit":
		p.lifecycle("queued")
		os.Exit(3)
	}
	p.lifecycle("queued")
	p.lifecycle("started")
	initSID := p.sid
	if p.scenario == "init_mismatch" {
		initSID = "some-other-session"
	}
	p.write(map[string]any{"type": "system", "subtype": "init", "session_id": initSID})
	switch p.scenario {
	case "snapshot":
		p.text(false, true)
		p.result(nil)
	case "delta_snapshot":
		p.text(true, true)
		p.result(nil)
	case "error":
		p.result(map[string]any{"is_error": true, "result": "Invalid API key", "errors": []string{"[ede_diagnostic] noise", "boom"}})
	case "approval", "interrupt":
		p.toolRequest("perm-1", "Bash", map[string]any{"command": "touch approved.txt", "description": "Create file"}, "toolu_1")
	case "question":
		p.toolRequest("perm-1", "AskUserQuestion", map[string]any{"questions": []any{map[string]any{"question": "Which colour?", "header": "Colour", "options": []any{map[string]any{"label": "Red", "description": "r"}, map[string]any{"label": "Blue", "description": "b"}}, "multiSelect": true}}}, "toolu_1")
	case "tools":
		p.toolRequest("perm-1", "Edit", map[string]any{"file_path": "/work/a.go"}, "toolu_1")
		p.toolRequest("perm-2", "NotebookEdit", map[string]any{"notebook_path": "/work/n.ipynb"}, "toolu_2")
		p.toolRequest("perm-3", "WebFetch", map[string]any{"url": strings.Repeat("u", 5000)}, "toolu_3")
	case "unknown_control":
		p.write(map[string]any{"type": "control_request", "request_id": "m1", "request": map[string]any{"subtype": "mystery"}})
		p.outstanding = 1
		p.pendingIDs = []string{"m1"}
	default:
		p.text(true, true)
		p.result(nil)
	}
}

func (p *peerState) answered(f peerFrame) {
	if f.Response == nil || !slices.Contains(p.pendingIDs, f.Response.RequestID) {
		return
	}
	p.pendingIDs = slices.DeleteFunc(p.pendingIDs, func(id string) bool { return id == f.Response.RequestID })
	if !strings.HasPrefix(f.Response.RequestID, "m") {
		block := map[string]any{"type": "tool_result", "tool_use_id": "toolu_" + strings.TrimPrefix(f.Response.RequestID, "perm-")}
		msg := map[string]any{"type": "user", "message": map[string]any{"content": []any{block}}}
		if f.Response.Response.Behavior == "deny" {
			block["is_error"] = true
			msg["tool_result_meta"] = []any{map[string]any{"id": block["tool_use_id"], "permission_decision": map[string]any{"decision": "reject"}}}
		}
		p.write(msg)
	}
	p.outstanding--
	if p.outstanding == 0 {
		p.text(true, false)
		p.result(nil)
	}
}

func (p *peerState) interrupt(f peerFrame) {
	for _, id := range p.pendingIDs {
		p.write(map[string]any{"type": "control_cancel_request", "request_id": id})
	}
	p.pendingIDs = nil
	p.write(map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": f.RequestID, "response": map[string]any{"still_queued": []any{}}}})
	if p.scenario == "interrupt" {
		block := map[string]any{"type": "tool_result", "tool_use_id": "toolu_1", "is_error": true}
		p.write(map[string]any{"type": "user", "message": map[string]any{"content": []any{block}}})
		p.result(map[string]any{"terminal_reason": "aborted_tools", "stop_reason": "tool_use"})
	}
}

type peerLogs struct{ argv, recv string }

func (l peerLogs) argvs(t *testing.T) [][]string {
	t.Helper()
	var out [][]string
	for _, line := range readLines(t, l.argv) {
		var a []string
		require.NoError(t, json.Unmarshal([]byte(line), &a))
		out = append(out, a)
	}
	return out
}

// received polls because the peer logs asynchronously to the adapter's calls.
func (l peerLogs) received(t *testing.T, match func(map[string]any) bool) map[string]any {
	t.Helper()
	var found map[string]any
	require.Eventually(t, func() bool {
		for _, line := range readLines(t, l.recv) {
			var m map[string]any
			require.NoError(t, json.Unmarshal([]byte(line), &m))
			if match(m) {
				found = m
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond)
	return found
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // Test-controlled path.
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	trimmed := strings.TrimSpace(string(b))
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "\n")
}

func fakeProvider(t *testing.T, scenario string, extraEnv ...string) (*Provider, peerLogs) {
	t.Helper()
	bin, err := os.Executable()
	require.NoError(t, err)
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-claude")
	content := "#!/bin/sh\nexec '" + strings.ReplaceAll(bin, "'", "'\\''") + "' -test.run=^TestFakePeer$ -- \"$@\"\n"
	require.NoError(t, os.WriteFile(script, []byte(content), 0o700)) //nolint:gosec // Test script must be executable.
	logs := peerLogs{argv: filepath.Join(dir, "argv.log"), recv: filepath.Join(dir, "recv.log")}
	// The race runtime sleeps 1s at exit by default, which slows every graceful close.
	env := append([]string{"HARNESS_FAKE_PEER=1", "HARNESS_FAKE_SCENARIO=" + scenario, "GORACE=atexit_sleep_ms=0", "HARNESS_FAKE_ARGVLOG=" + logs.argv, "HARNESS_FAKE_RECVLOG=" + logs.recv}, extraEnv...)
	return New(Config{Binary: script, Env: env}), logs
}

func contextForTest(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func openFake(t *testing.T, scenario string) (hp.Session, peerLogs) {
	t.Helper()
	p, logs := fakeProvider(t, scenario)
	s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close fake session: %v", err)
		}
	})
	return s, logs
}

func textInput(text string) hp.TurnInput {
	return hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: text}}}
}

func startFake(t *testing.T, s hp.Session) hp.Turn {
	t.Helper()
	turn, err := s.StartTurn(contextForTest(t), textInput("hello"))
	require.NoError(t, err)
	assert.NotEmpty(t, turn.ID())
	return turn
}

func next(t *testing.T, turn hp.Turn) hp.Event {
	t.Helper()
	e, err := turn.Next(contextForTest(t))
	require.NoError(t, err)
	return e
}

func collect(t *testing.T, turn hp.Turn) []hp.Event {
	t.Helper()
	var events []hp.Event
	for {
		e := next(t, turn)
		events = append(events, e)
		if e.Kind == hp.EventTurnFinished {
			return events
		}
	}
}

func kinds(events []hp.Event) []hp.EventKind {
	out := make([]hp.EventKind, len(events))
	for i, e := range events {
		out[i] = e.Kind
	}
	return out
}

func TestTextTurnEventsAndEOF(t *testing.T) {
	s, logs := openFake(t, "normal")
	cursor := s.Cursor()
	assert.Equal(t, "claude", cursor.Provider)
	var cd cursorData
	require.NoError(t, json.Unmarshal(cursor.Data, &cd))
	require.NotEmpty(t, cd.SessionID)
	assert.Equal(t, [][]string{launchArgs(launchKey{}, cd.SessionID, false, nil)}, logs.argvs(t))

	turn := startFake(t, s)
	events := collect(t, turn)
	assert.Equal(t, []hp.EventKind{hp.EventTurnStarted, hp.EventTextDelta, hp.EventUsage, hp.EventTurnFinished}, kinds(events))
	assert.Equal(t, hp.Event{Kind: hp.EventTextDelta, TurnID: turn.ID(), ItemID: fakeMessageID, Text: "pong"}, events[1])
	assert.Equal(t, &hp.Usage{InputTokens: 17, OutputTokens: 4}, events[2].Usage)
	assert.Equal(t, hp.OutcomeCompleted, events[3].Outcome.Status)
	_, err := turn.Next(contextForTest(t))
	require.ErrorIs(t, err, io.EOF)

	user := logs.received(t, func(m map[string]any) bool { return m["type"] == "user" })
	assert.Equal(t, turn.ID(), user["uuid"])
	assert.Equal(t, "", user["session_id"])
	assert.Nil(t, user["parent_tool_use_id"])
	content := user["message"].(map[string]any)["content"].([]any)
	assert.Equal(t, map[string]any{"type": "text", "text": "hello"}, content[0])
}

func TestSnapshotFallback(t *testing.T) {
	for _, scenario := range []string{"snapshot", "delta_snapshot"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := openFake(t, scenario)
			text := ""
			count := 0
			for _, e := range collect(t, startFake(t, s)) {
				if e.Kind == hp.EventTextDelta {
					count++
					text += e.Text
				}
			}
			assert.Equal(t, 1, count)
			assert.Equal(t, "pong", text)
		})
	}
}

func TestApprovalRequestAndRespond(t *testing.T) {
	tests := []struct {
		name   string
		option string
		want   map[string]any
		status hp.ItemStatus
	}{
		{name: "accept", option: "accept", want: map[string]any{"behavior": "allow", "updatedInput": map[string]any{"command": "touch approved.txt", "description": "Create file"}}, status: hp.ItemCompleted},
		{name: "decline", option: "decline", want: map[string]any{"behavior": "deny", "message": "User declined tool execution."}, status: hp.ItemDeclined},
		{name: "cancel", option: "cancel", want: map[string]any{"behavior": "deny", "message": "User canceled tool execution.", "interrupt": true}, status: hp.ItemDeclined},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, logs := fakeProvider(t, "approval")
			workspace := t.TempDir()
			s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: workspace})
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			turn := startFake(t, s)
			started := next(t, turn)
			require.Equal(t, hp.EventTurnStarted, started.Kind)
			item := next(t, turn)
			assert.Equal(t, hp.Event{Kind: hp.EventItemStarted, TurnID: turn.ID(), ItemID: "toolu_1", Item: &hp.Item{Kind: "command", Label: "touch approved.txt", Status: hp.ItemRunning}}, item)
			e := next(t, turn)
			require.Equal(t, hp.EventRequest, e.Kind)
			assert.Equal(t, "perm-1", e.Request.ID)
			assert.Equal(t, "toolu_1", e.Request.ItemID)
			assert.Equal(t, hp.RequestApproval, e.Request.Kind)
			assert.Equal(t, &hp.Approval{Action: hp.ActionCommand, Command: "touch approved.txt", Cwd: workspace, Reason: "Create file"}, e.Request.Approval)
			assert.Equal(t, []string{"accept", "decline", "cancel"}, []string{e.Request.Options[0].ID, e.Request.Options[1].ID, e.Request.Options[2].ID})

			require.ErrorIs(t, turn.Respond(contextForTest(t), e.Request.ID, hp.Response{OptionID: "bogus"}), hp.ErrUnsupported)
			require.NoError(t, turn.Respond(contextForTest(t), e.Request.ID, hp.Response{OptionID: tt.option}))
			require.ErrorIs(t, turn.Respond(contextForTest(t), e.Request.ID, hp.Response{OptionID: tt.option}), hp.ErrRequestNotFound)

			sent := logs.received(t, func(m map[string]any) bool { return m["type"] == "control_response" })
			resp := sent["response"].(map[string]any)
			assert.Equal(t, "success", resp["subtype"])
			assert.Equal(t, "perm-1", resp["request_id"])
			assert.Equal(t, tt.want, resp["response"])

			finished := next(t, turn)
			assert.Equal(t, hp.EventItemFinished, finished.Kind)
			assert.Equal(t, tt.status, finished.Item.Status)
			assert.Equal(t, "command", finished.Item.Kind)
			assert.Equal(t, []hp.EventKind{hp.EventTextDelta, hp.EventUsage, hp.EventTurnFinished}, kinds(collect(t, turn)))
		})
	}
}

func TestInterruptWithPendingRequest(t *testing.T) {
	s, logs := openFake(t, "interrupt")
	turn := startFake(t, s)
	var req *hp.Request
	for req == nil {
		if e := next(t, turn); e.Kind == hp.EventRequest {
			req = e.Request
		}
	}
	_, err := s.StartTurn(contextForTest(t), textInput("again"))
	require.ErrorIs(t, err, hp.ErrBusy)
	require.NoError(t, turn.Interrupt(contextForTest(t)))
	logs.received(t, func(m map[string]any) bool {
		r, ok := m["request"].(map[string]any)
		return ok && r["subtype"] == "interrupt"
	})
	require.ErrorIs(t, turn.Respond(contextForTest(t), req.ID, hp.Response{OptionID: "accept"}), hp.ErrRequestNotFound)
	var last hp.Event
	for _, e := range collect(t, turn) {
		last = e
	}
	assert.Equal(t, hp.OutcomeInterrupted, last.Outcome.Status)
	require.NoError(t, turn.Interrupt(contextForTest(t)))
}

func TestAskUserQuestion(t *testing.T) {
	s, logs := openFake(t, "question")
	turn := startFake(t, s)
	var req *hp.Request
	for req == nil {
		if e := next(t, turn); e.Kind == hp.EventRequest {
			req = e.Request
		}
	}
	assert.Equal(t, hp.RequestUserInput, req.Kind)
	assert.Nil(t, req.Approval)
	require.Len(t, req.Questions, 1)
	q := req.Questions[0]
	assert.Equal(t, "Which colour?", q.ID)
	assert.Equal(t, "Colour", q.Header)
	assert.Equal(t, "Which colour?", q.Prompt)
	assert.True(t, q.Multiple)
	assert.Equal(t, []hp.Option{{ID: "Red", Label: "Red", Description: "r"}, {ID: "Blue", Label: "Blue", Description: "b"}}, q.Options)

	require.NoError(t, turn.Respond(contextForTest(t), req.ID, hp.Response{Answers: map[string][]string{"Which colour?": {"Red", "Blue"}}}))
	sent := logs.received(t, func(m map[string]any) bool { return m["type"] == "control_response" })
	payload := sent["response"].(map[string]any)["response"].(map[string]any)
	assert.Equal(t, "allow", payload["behavior"])
	updated := payload["updatedInput"].(map[string]any)
	assert.Equal(t, map[string]any{"Which colour?": "Red, Blue"}, updated["answers"])
	assert.Contains(t, updated, "questions")
	events := collect(t, turn)
	assert.Equal(t, hp.EventTurnFinished, events[len(events)-1].Kind)
}

func TestFileChangeAndToolRequests(t *testing.T) {
	s, _ := openFake(t, "tools")
	turn := startFake(t, s)
	var reqs []*hp.Request
	for len(reqs) < 3 {
		if e := next(t, turn); e.Kind == hp.EventRequest {
			reqs = append(reqs, e.Request)
		}
	}
	assert.Equal(t, &hp.Approval{Action: hp.ActionFileChange, Paths: []string{"/work/a.go"}, Reason: "peer description"}, reqs[0].Approval)
	assert.Equal(t, []string{"/work/n.ipynb"}, reqs[1].Approval.Paths)
	tool := reqs[2].Approval
	assert.Equal(t, hp.ActionTool, tool.Action)
	assert.Equal(t, "WebFetch", tool.Tool)
	assert.Len(t, tool.Command, toolInputLimit)
	assert.True(t, strings.HasPrefix(tool.Command, `{"url":"uuu`))
}

func TestResultErrorWithSuccessSubtypeFails(t *testing.T) {
	s, _ := openFake(t, "error")
	events := collect(t, startFake(t, s))
	out := events[len(events)-1].Outcome
	assert.Equal(t, hp.OutcomeFailed, out.Status)
	assert.Equal(t, "boom", out.Error)
}

func TestFailureDetail(t *testing.T) {
	assert.Equal(t, "a; b", failureDetail(frame{Errors: []string{"[ede_diagnostic] x", "a", "b"}, Result: "r"}))
	assert.Equal(t, "r", failureDetail(frame{Errors: []string{"[ede_diagnostic] x"}, Result: "r"}))
	assert.Equal(t, "claude reported an error", failureDetail(frame{}))
}

func TestMissingSessionOnResume(t *testing.T) {
	p, _ := fakeProvider(t, "missing")
	_, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir(), Resume: &hp.Cursor{Provider: "claude", Version: 1, Data: json.RawMessage(`{"sessionId":"s-1"}`)}})
	require.ErrorIs(t, err, hp.ErrSessionNotFound)
}

func TestInvalidCursor(t *testing.T) {
	p, _ := fakeProvider(t, "normal")
	for name, c := range map[string]hp.Cursor{
		"provider": {Provider: "codex", Version: 1, Data: json.RawMessage(`{"sessionId":"s"}`)},
		"version":  {Provider: "claude", Version: 2, Data: json.RawMessage(`{"sessionId":"s"}`)},
		"data":     {Provider: "claude", Version: 1, Data: json.RawMessage(`{}`)},
		"garbage":  {Provider: "claude", Version: 1, Data: json.RawMessage(`nope`)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir(), Resume: &c})
			require.ErrorIs(t, err, hp.ErrInvalidCursor)
		})
	}
}

func TestResumeUsesResumeFlag(t *testing.T) {
	p, logs := fakeProvider(t, "normal")
	s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir(), Resume: &hp.Cursor{Provider: "claude", Version: 1, Data: json.RawMessage(`{"sessionId":"s-1"}`)}})
	require.NoError(t, err)
	defer s.Close() //nolint:errcheck // Test cleanup.
	assert.Equal(t, [][]string{launchArgs(launchKey{}, "s-1", true, nil)}, logs.argvs(t))
	assert.Equal(t, hp.EventTurnFinished, collect(t, startFake(t, s))[3].Kind)
}

func TestInitSessionMismatchFailsTurnAndClosesSession(t *testing.T) {
	s, _ := openFake(t, "init_mismatch")
	turn := startFake(t, s)
	var err error
	for err == nil {
		_, err = turn.Next(contextForTest(t))
	}
	require.ErrorContains(t, err, "identity mismatch")
	_, err = s.StartTurn(contextForTest(t), textInput("again"))
	require.ErrorIs(t, err, hp.ErrClosed)
}

func TestUncertainAdmissionClosesSession(t *testing.T) {
	s, _ := openFake(t, "no_admission")
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := s.StartTurn(ctx, textInput("hello"))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = s.StartTurn(contextForTest(t), textInput("hello"))
	require.ErrorIs(t, err, hp.ErrClosed)
}

func TestChildExitFailsTurn(t *testing.T) {
	s, _ := openFake(t, "exit")
	turn := startFake(t, s)
	var err error
	for err == nil {
		_, err = turn.Next(contextForTest(t))
	}
	require.NotErrorIs(t, err, io.EOF)
	require.ErrorContains(t, err, "exit status 3")
}

func TestEchoedAndUnknownControlFrames(t *testing.T) {
	s, _ := openFake(t, "echo")
	assert.Equal(t, hp.EventTurnFinished, collect(t, startFake(t, s))[3].Kind)

	s, logs := openFake(t, "unknown_control")
	assert.Equal(t, hp.EventTurnFinished, collect(t, startFake(t, s))[3].Kind)
	sent := logs.received(t, func(m map[string]any) bool { return m["type"] == "control_response" })
	resp := sent["response"].(map[string]any)
	assert.Equal(t, "error", resp["subtype"])
	assert.Equal(t, "m1", resp["request_id"])
}

func TestRespawnOnSettingsChange(t *testing.T) {
	p, logs := fakeProvider(t, "normal")
	s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir()})
	require.NoError(t, err)
	defer s.Close() //nolint:errcheck // Test cleanup.
	id := s.Cursor()
	var cd cursorData
	require.NoError(t, json.Unmarshal(id.Data, &cd))

	run := func(model string) {
		in := textInput("hello")
		in.Model = model
		turn, err := s.StartTurn(contextForTest(t), in)
		require.NoError(t, err)
		assert.Equal(t, hp.EventTurnFinished, collect(t, turn)[3].Kind)
	}
	run("")
	require.Len(t, logs.argvs(t), 1)
	run("model-a")
	run("model-a")
	run("model-b")
	argvs := logs.argvs(t)
	require.Len(t, argvs, 3)
	assert.Equal(t, launchArgs(launchKey{model: "model-a"}, cd.SessionID, true, nil), argvs[1])
	assert.Equal(t, launchArgs(launchKey{model: "model-b"}, cd.SessionID, true, nil), argvs[2])
	assert.Equal(t, id, s.Cursor())
}

func TestRespawnBeforeFirstTurnReusesSessionID(t *testing.T) {
	p, logs := fakeProvider(t, "normal")
	s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir()})
	require.NoError(t, err)
	defer s.Close() //nolint:errcheck // Test cleanup.
	in := textInput("hello")
	in.Effort = "low"
	turn, err := s.StartTurn(contextForTest(t), in)
	require.NoError(t, err)
	collect(t, turn)
	argvs := logs.argvs(t)
	require.Len(t, argvs, 2)
	assert.Contains(t, argvs[1], "--session-id")
	assert.NotContains(t, argvs[1], "--resume")
	assert.Equal(t, "low", argValue(argvs[1], "--effort"))
}

func TestLaunchArgs(t *testing.T) {
	const sid = "s-1"
	base := []string{"-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--include-partial-messages", "--replay-user-messages", "--permission-prompt-tool", "stdio", "--session-id", sid}
	tests := []struct {
		name   string
		key    launchKey
		resume bool
		extra  []string
		want   []string
	}{
		{name: "default", want: []string{"--permission-mode", "default"}},
		{name: "model effort", key: launchKey{model: "m", effort: "max"}, want: []string{"--model", "m", "--effort", "max", "--permission-mode", "default"}},
		{name: "default never", key: launchKey{policy: hp.ExecutionPolicy{Approval: hp.ApprovalNever}}, want: []string{"--permission-mode", "default", "--permission-prompts", "none"}},
		{name: "read only", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxReadOnly}}, want: []string{"--tools", "Read,Glob,Grep", "--strict-mcp-config", "--permission-mode", "dontAsk"}},
		{name: "read only never", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxReadOnly, Approval: hp.ApprovalNever}}, want: []string{"--tools", "Read,Glob,Grep", "--strict-mcp-config", "--permission-mode", "dontAsk"}},
		{name: "workspace write", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxWorkspaceWrite, Approval: hp.ApprovalOnRequest}}, want: []string{"--permission-mode", "acceptEdits", "--settings", sandboxSettings}},
		{name: "workspace write never", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxWorkspaceWrite, Approval: hp.ApprovalNever}}, want: []string{"--permission-mode", "acceptEdits", "--settings", sandboxSettings, "--permission-prompts", "none"}},
		{name: "unrestricted", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxUnrestricted}}, want: []string{"--permission-mode", "default"}},
		{name: "unrestricted never", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxUnrestricted, Approval: hp.ApprovalNever}}, want: []string{"--permission-mode", "bypassPermissions"}},
		{name: "resume and extra", resume: true, extra: []string{"--foo"}, want: []string{"--permission-mode", "default", "--foo"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := slices.Clone(base)
			if tt.resume {
				want[len(want)-2] = "--resume"
			}
			assert.Equal(t, append(want, tt.want...), launchArgs(tt.key, sid, tt.resume, tt.extra))
		})
	}
}

func TestSandboxSettingsAreValidJSON(t *testing.T) {
	var v map[string]any
	require.NoError(t, json.Unmarshal([]byte(sandboxSettings), &v))
}

func TestValidateInput(t *testing.T) {
	dir := t.TempDir()
	png := filepath.Join(dir, "a.png")
	require.NoError(t, os.WriteFile(png, []byte("png-bytes"), 0o600))
	bmp := filepath.Join(dir, "a.bmp")
	require.NoError(t, os.WriteFile(bmp, nil, 0o600))
	bad := []hp.TurnInput{
		{},
		{Parts: []hp.InputPart{{Kind: hp.InputText}}},
		{Parts: []hp.InputPart{{Kind: "audio"}}},
		{Parts: []hp.InputPart{{Kind: hp.InputLocalImage, Path: "relative.png"}}},
		{Parts: []hp.InputPart{{Kind: hp.InputLocalImage, Path: bmp}}},
		{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "x"}}, Effort: "none"},
		{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "x"}}, Policy: hp.ExecutionPolicy{Approval: "sometimes"}},
		{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "x"}}, Policy: hp.ExecutionPolicy{Sandbox: "moat"}},
	}
	for i, in := range bad {
		_, err := validateInput(in)
		require.ErrorIs(t, err, hp.ErrUnsupported, "case %d", i)
	}
	parts, err := validateInput(hp.TurnInput{Parts: []hp.InputPart{{Kind: hp.InputText, Text: "x"}, {Kind: hp.InputLocalImage, Path: png}}, Effort: "xhigh"})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": "cG5nLWJ5dGVz"}}, parts[1])
}

func TestEnvironmentConfigDir(t *testing.T) {
	t.Setenv("CLAUDE_CONFIG_DIR", "/ambient")
	assert.Contains(t, environment("", nil), "CLAUDE_CONFIG_DIR=/ambient")
	explicit := environment("/explicit", nil)
	assert.Contains(t, explicit, "CLAUDE_CONFIG_DIR=/explicit")
	assert.NotContains(t, explicit, "CLAUDE_CONFIG_DIR=/ambient")
}

func TestOpenRejectsFileWorkspace(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	_, err := New(Config{}).Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: file})
	require.Error(t, err)
	assert.True(t, errors.Is(err, syscall.ENOTDIR))
	assert.Equal(t, "claude", New(Config{}).Name())
}

func TestTruncateKeepsRunes(t *testing.T) {
	assert.Equal(t, "ab", truncate("abc", 2))
	assert.Equal(t, "a", truncate("aé", 2))
	assert.Equal(t, "abc", truncate("abc", 3))
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
	p, _ := fakeProvider(t, "normal")
	p.config.Logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	secret := "PROMPT_SECRET_6a93"
	envSecret := "ENV_SECRET_81d2"
	p.config.Env = append(p.config.Env, "TEST_CREDENTIAL="+envSecret)
	workspace := t.TempDir()
	s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: workspace})
	require.NoError(t, err)
	turn, err := s.StartTurn(contextForTest(t), textInput(secret))
	require.NoError(t, err)
	collect(t, turn)
	require.NoError(t, s.Close())
	got := logs.String()
	for _, want := range []string{"claude spawned", "claude initialized", "claude session opened", "claude turn admitted", "claude turn finished", "claude session closing", `"turn_id":"` + turn.ID() + `"`} {
		assert.Contains(t, got, want)
	}
	for _, forbidden := range []string{secret, envSecret, workspace, "pong", "HARNESS_FAKE_PEER"} {
		assert.NotContains(t, got, forbidden)
	}
}

func TestCloseKillsProcessGroupAndDoesNotHang(t *testing.T) {
	prev := closeGrace
	closeGrace = 100 * time.Millisecond
	t.Cleanup(func() { closeGrace = prev })
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	p, _ := fakeProvider(t, "grandchild", "HARNESS_FAKE_PIDFILE="+pidFile)
	s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir()})
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
