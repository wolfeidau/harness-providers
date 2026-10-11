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
	"regexp"
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
	fresh       bool // launched with --session-id, i.e. not a respawn
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
	p := &peerState{w: bufio.NewWriter(os.Stdout), scenario: os.Getenv("HARNESS_FAKE_SCENARIO"), sid: argValue(args, "--session-id"), fresh: slices.Contains(args, "--session-id")}
	if p.sid == "" {
		p.sid = argValue(args, "--resume")
	}
	s := bufio.NewScanner(os.Stdin)
	s.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	if p.scenario == "replay" {
		p.replay(s, os.Getenv("HARNESS_FAKE_FIXTURE"))
	}
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
	case "exit_first":
		if p.fresh {
			p.lifecycle("queued")
			os.Exit(3)
		}
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
	case "question", "duplicate_question":
		p.question()
	case "tools":
		p.toolRequest("perm-1", "Edit", map[string]any{"file_path": "/work/a.go"}, "toolu_1")
		p.toolRequest("perm-2", "NotebookEdit", map[string]any{"notebook_path": "/work/n.ipynb"}, "toolu_2")
		p.toolRequest("perm-3", "WebFetch", map[string]any{"url": strings.Repeat("u", 5000)}, "toolu_3")
	case "subagent", "result_pending", "foreign_result", "junk":
		p.extra()
	case "unknown_control":
		p.write(map[string]any{"type": "control_request", "request_id": "m1", "request": map[string]any{"subtype": "mystery"}})
		p.outstanding = 1
		p.pendingIDs = []string{"m1"}
	default:
		p.text(true, true)
		p.result(nil)
	}
}

func (p *peerState) question() {
	q := map[string]any{"question": "Which colour?", "header": "Colour"}
	questions := []any{q, q}
	if p.scenario == "question" {
		questions = []any{map[string]any{"question": "Which colour?", "header": "Colour", "options": []any{map[string]any{"label": "Red", "description": "r"}, map[string]any{"label": "Blue", "description": "b"}}, "multiSelect": true}}
	}
	p.toolRequest("perm-1", "AskUserQuestion", map[string]any{"questions": questions}, "toolu_1")
}

// extra plays scenarios that exercise frame filtering rather than the normal turn flow.
func (p *peerState) extra() {
	switch p.scenario {
	case "subagent":
		sub := func(m map[string]any) {
			m["parent_tool_use_id"] = "toolu_parent"
			p.write(m)
		}
		sub(map[string]any{"type": "stream_event", "event": map[string]any{"type": "content_block_delta", "delta": map[string]any{"type": "text_delta", "text": "SUB"}}, "api_message_id": "msg_sub"})
		sub(map[string]any{"type": "assistant", "message": map[string]any{"id": "msg_sub", "content": []any{map[string]any{"type": "text", "text": "SUB"}, map[string]any{"type": "tool_use", "id": "toolu_sub", "name": "Bash", "input": map[string]any{"command": "ls"}}}}})
		sub(map[string]any{"type": "user", "message": map[string]any{"content": []any{map[string]any{"type": "tool_result", "tool_use_id": "toolu_sub"}}}})
		p.write(map[string]any{"type": "control_request", "request_id": "perm-1", "parent_tool_use_id": "toolu_parent", "request": map[string]any{"subtype": "can_use_tool", "tool_name": "Bash", "input": map[string]any{"command": "ls"}, "tool_use_id": "toolu_sub"}})
		p.pendingIDs = append(p.pendingIDs, "perm-1")
		p.outstanding++
	case "result_pending":
		p.toolRequest("perm-1", "Bash", map[string]any{"command": "ls"}, "toolu_1")
		p.result(nil)
	case "foreign_result":
		p.result(map[string]any{"user_message_uuid": "other-turn"})
		p.text(true, true)
		p.result(nil)
	case "junk":
		p.emit("not json {", "", nil)
		p.write(map[string]any{"type": "assistant", "errors": []any{map[string]any{"x": 1}}})
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

var fixtureSessionID = regexp.MustCompile(`"session_id":"([0-9a-f-]{36})"`)

func (p *peerState) die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "replay: "+format+"\n", args...)
	os.Exit(2)
}

// expect blocks for the adapter's next frame and requires it to match want's type and subtype.
func (p *peerState) expect(s *bufio.Scanner, want peerFrame) peerFrame {
	if !s.Scan() {
		p.die("stdin closed while waiting for %s", want.Type)
	}
	appendLine(os.Getenv("HARNESS_FAKE_RECVLOG"), s.Text())
	var got peerFrame
	if json.Unmarshal(s.Bytes(), &got) != nil {
		p.die("undecodable frame %q", s.Text())
	}
	subtype := func(f peerFrame) string {
		if f.Request == nil {
			return ""
		}
		return f.Request.Subtype
	}
	if got.Type != want.Type || subtype(got) != subtype(want) {
		p.die("got %s/%s, want %s/%s", got.Type, subtype(got), want.Type, subtype(want))
	}
	return got
}

// replay plays a captured fixture back: `>>` lines wait for the adapter's matching frame, except a
// control_response after an interrupt (the adapter drops pending requests); `<<` lines are emitted
// with the capture's session, user and request IDs rewritten to the adapter's.
func (p *peerState) replay(s *bufio.Scanner, path string) {
	raw, err := os.ReadFile(path) //nolint:gosec // Test-controlled path.
	if err != nil {
		p.die("%v", err)
	}
	fixtureSID := ""
	if m := fixtureSessionID.FindSubmatch(raw); m != nil {
		fixtureSID = string(m[1])
	}
	rewrite := map[string]string{}
	interrupted := false
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		switch {
		case strings.HasPrefix(line, ">> "):
			var want peerFrame
			if json.Unmarshal([]byte(line[3:]), &want) != nil {
				p.die("bad fixture line %q", line)
			}
			if want.Type == "control_response" && interrupted {
				continue
			}
			got := p.expect(s, want)
			switch want.Type {
			case "user":
				rewrite[want.UUID] = got.UUID
			case "control_request":
				rewrite[want.RequestID] = got.RequestID
				interrupted = interrupted || want.Request.Subtype == "interrupt"
			}
		case strings.HasPrefix(line, "<< "):
			p.emit(line[3:], fixtureSID, rewrite)
		case strings.HasPrefix(line, "## exit="):
			p.exit(s, line, fixtureSID)
		}
	}
	p.die("fixture has no exit line")
}

func (p *peerState) emit(out, fixtureSID string, rewrite map[string]string) {
	if fixtureSID != "" {
		out = strings.ReplaceAll(out, fixtureSID, p.sid)
	}
	for from, to := range rewrite {
		out = strings.ReplaceAll(out, `"`+from+`"`, `"`+to+`"`)
	}
	if _, err := fmt.Fprintln(p.w, out); err != nil {
		os.Exit(2)
	}
	if err := p.w.Flush(); err != nil {
		os.Exit(2)
	}
}

func (p *peerState) exit(s *bufio.Scanner, line, fixtureSID string) {
	code, stderr, _ := strings.Cut(strings.TrimPrefix(line, "## exit="), " stderr=")
	n, err := strconv.Atoi(code)
	if err != nil {
		p.die("bad exit line %q", line)
	}
	if stderr == "" {
		for s.Scan() { // a clean capture ends at stdin EOF, which is how Close signals shutdown
			appendLine(os.Getenv("HARNESS_FAKE_RECVLOG"), s.Text())
		}
	}
	fmt.Fprintln(os.Stderr, strings.ReplaceAll(stderr, fixtureSID, p.sid))
	os.Exit(n)
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
	assert.Empty(t, logs.argvs(t), "Open must not spawn a new session")

	turn := startFake(t, s)
	assert.Equal(t, [][]string{launchArgs(launchKey{}, cd.SessionID, false, nil)}, logs.argvs(t))
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

func TestSubagentFramesAreIgnoredButRequestsSurface(t *testing.T) {
	s, _ := openFake(t, "subagent")
	turn := startFake(t, s)
	req := nextRequest(t, turn)
	assert.Equal(t, "perm-1", req.ID)
	require.NoError(t, turn.Respond(contextForTest(t), req.ID, hp.Response{OptionID: "accept"}))
	events := collect(t, turn)
	assert.Equal(t, "pong", joinText(events))
	for _, e := range events {
		assert.NotEqual(t, hp.EventItemStarted, e.Kind)
	}
	for _, it := range finishedItems(events) {
		assert.NotEqual(t, "toolu_sub", it.Label)
	}
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
	assert.Equal(t, []hp.Option{{ID: "decline", Label: "Decline"}, {ID: "cancel", Label: "Cancel"}}, req.Options)
	require.Len(t, req.Questions, 1)
	q := req.Questions[0]
	assert.Equal(t, "q0", q.ID)
	assert.Equal(t, "Colour", q.Header)
	assert.Equal(t, "Which colour?", q.Prompt)
	assert.True(t, q.Multiple)
	assert.Equal(t, []hp.Option{{ID: "Red", Label: "Red", Description: "r"}, {ID: "Blue", Label: "Blue", Description: "b"}}, q.Options)

	require.ErrorIs(t, turn.Respond(contextForTest(t), req.ID, hp.Response{OptionID: "accept"}), hp.ErrUnsupported)
	require.NoError(t, turn.Respond(contextForTest(t), req.ID, hp.Response{Answers: map[string][]string{"q0": {"Red", "Blue"}}}))
	sent := logs.received(t, func(m map[string]any) bool { return m["type"] == "control_response" })
	payload := sent["response"].(map[string]any)["response"].(map[string]any)
	assert.Equal(t, "allow", payload["behavior"])
	updated := payload["updatedInput"].(map[string]any)
	assert.Equal(t, map[string]any{"Which colour?": "Red, Blue"}, updated["answers"])
	assert.Contains(t, updated, "questions")
	events := collect(t, turn)
	assert.Equal(t, hp.EventTurnFinished, events[len(events)-1].Kind)
}

func TestAskUserQuestionDecline(t *testing.T) {
	s, logs := openFake(t, "question")
	turn := startFake(t, s)
	req := nextRequest(t, turn)
	require.NoError(t, turn.Respond(contextForTest(t), req.ID, hp.Response{OptionID: "decline"}))
	sent := logs.received(t, func(m map[string]any) bool { return m["type"] == "control_response" })
	payload := sent["response"].(map[string]any)["response"].(map[string]any)
	assert.Equal(t, "deny", payload["behavior"])
	assert.NotContains(t, payload, "interrupt")
	require.ErrorIs(t, turn.Respond(contextForTest(t), req.ID, hp.Response{OptionID: "decline"}), hp.ErrRequestNotFound)
	collect(t, turn)
}

func TestAskUserQuestionDuplicateText(t *testing.T) {
	s, logs := openFake(t, "duplicate_question")
	turn := startFake(t, s)
	req := nextRequest(t, turn)
	require.Len(t, req.Questions, 2)
	assert.Equal(t, "q0", req.Questions[0].ID)
	assert.Equal(t, "q1", req.Questions[1].ID)
	require.NoError(t, turn.Respond(contextForTest(t), req.ID, hp.Response{Answers: map[string][]string{"q0": {"Red"}, "q1": {"Blue"}}}))
	sent := logs.received(t, func(m map[string]any) bool { return m["type"] == "control_response" })
	payload := sent["response"].(map[string]any)["response"].(map[string]any)
	assert.Equal(t, map[string]any{"Which colour?": "Red, Blue"}, payload["updatedInput"].(map[string]any)["answers"])
	collect(t, turn)
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
	assert.Equal(t, `{"url":"`+strings.Repeat("u", 5000)+`"}`, tool.Command)
}

func TestResultErrorWithSuccessSubtypeFails(t *testing.T) {
	s, _ := openFake(t, "error")
	events := collect(t, startFake(t, s))
	out := events[len(events)-1].Outcome
	assert.Equal(t, hp.OutcomeFailed, out.Status)
	assert.Equal(t, "boom", out.Error)
}

func TestResultClearsPendingRequests(t *testing.T) {
	s, _ := openFake(t, "result_pending")
	turn := startFake(t, s)
	req := nextRequest(t, turn)
	collect(t, turn)
	require.ErrorIs(t, turn.Respond(contextForTest(t), req.ID, hp.Response{OptionID: "accept"}), hp.ErrRequestNotFound)
}

func TestForeignResultIsIgnored(t *testing.T) {
	s, _ := openFake(t, "foreign_result")
	events := collect(t, startFake(t, s))
	assert.Equal(t, []hp.EventKind{hp.EventTurnStarted, hp.EventTextDelta, hp.EventUsage, hp.EventTurnFinished}, kinds(events))
}

func TestUndecodableFramesAreSkipped(t *testing.T) {
	s, _ := openFake(t, "junk")
	events := collect(t, startFake(t, s))
	assert.Equal(t, "pong", joinText(events))
	assert.Equal(t, hp.OutcomeCompleted, events[len(events)-1].Outcome.Status)
}

func TestCloseSurfacesErrClosed(t *testing.T) {
	s, _ := openFake(t, "approval")
	turn := startFake(t, s)
	req := nextRequest(t, turn)
	require.NoError(t, s.Close())
	var err error
	for err == nil {
		_, err = turn.Next(contextForTest(t))
	}
	require.ErrorIs(t, err, hp.ErrClosed)
	require.ErrorIs(t, turn.Respond(contextForTest(t), req.ID, hp.Response{OptionID: "accept"}), hp.ErrClosed)
	require.ErrorIs(t, turn.Interrupt(contextForTest(t)), hp.ErrClosed)
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
	require.NotErrorIs(t, err, hp.ErrClosed)
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
	assert.Equal(t, launchArgs(launchKey{}, cd.SessionID, false, nil), logs.argvs(t)[0])
	run("model-a")
	run("model-a")
	run("model-b")
	argvs := logs.argvs(t)
	require.Len(t, argvs, 3)
	assert.Equal(t, launchArgs(launchKey{model: "model-a"}, cd.SessionID, true, nil), argvs[1])
	assert.Equal(t, launchArgs(launchKey{model: "model-b"}, cd.SessionID, true, nil), argvs[2])
	assert.Equal(t, id, s.Cursor())
}

func TestFirstTurnSpawnsOnceWithSessionID(t *testing.T) {
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
	require.Len(t, argvs, 1)
	assert.Contains(t, argvs[0], "--session-id")
	assert.NotContains(t, argvs[0], "--resume")
	assert.Equal(t, "low", argValue(argvs[0], "--effort"))
}

func TestOpenRejectsUnresolvableBinary(t *testing.T) {
	_, err := New(Config{Binary: filepath.Join(t.TempDir(), "missing-claude")}).Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir()})
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestSessionRecoversAfterChildExit(t *testing.T) {
	s, logs := openFake(t, "exit_first")
	turn := startFake(t, s)
	var err error
	for err == nil {
		_, err = turn.Next(contextForTest(t))
	}
	require.ErrorContains(t, err, "exit status 3")
	second := startFake(t, s)
	assert.Equal(t, hp.EventTurnFinished, collect(t, second)[3].Kind)
	argvs := logs.argvs(t)
	require.Len(t, argvs, 2)
	assert.Contains(t, argvs[0], "--session-id")
	assert.Contains(t, argvs[1], "--resume")
	assert.NotContains(t, argvs[1], "--session-id")
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
		{name: "default"},
		{name: "model effort", key: launchKey{model: "m", effort: "max"}, want: []string{"--model", "m", "--effort", "max"}},
		{name: "default never", key: launchKey{policy: hp.ExecutionPolicy{Approval: hp.ApprovalNever}}, want: []string{"--permission-prompts", "none"}},
		{name: "read only", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxReadOnly}}, want: []string{"--tools", "Read,Glob,Grep", "--strict-mcp-config", "--setting-sources", "user", "--permission-mode", "dontAsk"}},
		{name: "read only never", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxReadOnly, Approval: hp.ApprovalNever}}, want: []string{"--tools", "Read,Glob,Grep", "--strict-mcp-config", "--setting-sources", "user", "--permission-mode", "dontAsk"}},
		{name: "workspace write", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxWorkspaceWrite, Approval: hp.ApprovalOnRequest}}, want: []string{"--permission-mode", "acceptEdits", "--settings", sandboxSettings}},
		{name: "workspace write never", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxWorkspaceWrite, Approval: hp.ApprovalNever}}, want: []string{"--permission-mode", "acceptEdits", "--settings", sandboxSettings, "--permission-prompts", "none"}},
		{name: "unrestricted", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxUnrestricted}}},
		{name: "unrestricted on request", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxUnrestricted, Approval: hp.ApprovalOnRequest}}},
		{name: "unrestricted never", key: launchKey{policy: hp.ExecutionPolicy{Sandbox: hp.SandboxUnrestricted, Approval: hp.ApprovalNever}}, want: []string{"--permission-mode", "bypassPermissions"}},
		{name: "resume and extra", resume: true, extra: []string{"--foo"}, want: []string{"--foo"}},
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
	assert.Contains(t, explicit, "CLAUDE_BASH_MAINTAIN_PROJECT_WORKING_DIR=1")
}

func TestOpenRejectsFileWorkspace(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	_, err := New(Config{}).Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: file})
	require.Error(t, err)
	assert.True(t, errors.Is(err, syscall.ENOTDIR))
	assert.Equal(t, "claude", New(Config{}).Name())
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
	s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir(), Resume: &hp.Cursor{Provider: "claude", Version: 1, Data: json.RawMessage(`{"sessionId":"s-1"}`)}})
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

func replayFixture(t *testing.T, name string, resume *hp.Cursor) (hp.Session, peerLogs, error) {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("testdata", name+".ndjson"))
	require.NoError(t, err)
	p, logs := fakeProvider(t, "replay", "HARNESS_FAKE_FIXTURE="+path)
	s, err := p.Open(contextForTest(t), hp.OpenRequest{WorkingDirectory: t.TempDir(), Resume: resume})
	if err != nil {
		return nil, logs, err
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close replay session: %v", err)
		}
	})
	return s, logs, nil
}

func replayTurn(t *testing.T, name string, sandbox hp.SandboxMode) (hp.Turn, peerLogs) {
	t.Helper()
	s, logs, err := replayFixture(t, name, nil)
	require.NoError(t, err)
	in := textInput("hello")
	in.Policy.Sandbox = sandbox
	turn, err := s.StartTurn(contextForTest(t), in)
	require.NoError(t, err)
	return turn, logs
}

func nextRequest(t *testing.T, turn hp.Turn) *hp.Request {
	t.Helper()
	for {
		if e := next(t, turn); e.Kind == hp.EventRequest {
			return e.Request
		}
	}
}

func joinText(events []hp.Event) string {
	var b strings.Builder
	for _, e := range events {
		if e.Kind == hp.EventTextDelta {
			b.WriteString(e.Text)
		}
	}
	return b.String()
}

func finishedItems(events []hp.Event) []*hp.Item {
	var out []*hp.Item
	for _, e := range events {
		if e.Kind == hp.EventItemFinished {
			out = append(out, e.Item)
		}
	}
	return out
}

func TestReplayFixtures(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		turn, _ := replayTurn(t, "simple", hp.SandboxDefault)
		events := collect(t, turn)
		assert.Equal(t, hp.EventTurnStarted, events[0].Kind)
		assert.Equal(t, "pong", joinText(events))
		var usage *hp.Usage
		for _, e := range events {
			if e.Kind == hp.EventUsage {
				usage = e.Usage
			}
		}
		require.NotNil(t, usage)
		assert.NotZero(t, usage.InputTokens)
		assert.NotZero(t, usage.OutputTokens)
		assert.Equal(t, hp.OutcomeCompleted, events[len(events)-1].Outcome.Status)
		_, err := turn.Next(contextForTest(t))
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("deny", func(t *testing.T) {
		turn, _ := replayTurn(t, "deny", hp.SandboxDefault)
		req := nextRequest(t, turn)
		require.NotNil(t, req.Approval)
		assert.Equal(t, hp.ActionCommand, req.Approval.Action)
		assert.Equal(t, "touch approved.txt", req.Approval.Command)
		assert.NotEmpty(t, req.ItemID)
		require.NoError(t, turn.Respond(contextForTest(t), req.ID, hp.Response{OptionID: "decline"}))
		events := collect(t, turn)
		items := finishedItems(events)
		require.Len(t, items, 1)
		assert.Equal(t, hp.ItemDeclined, items[0].Status)
		assert.Equal(t, hp.OutcomeCompleted, events[len(events)-1].Outcome.Status)
	})

	t.Run("interrupt", func(t *testing.T) {
		turn, _ := replayTurn(t, "interrupt", hp.SandboxDefault)
		req := nextRequest(t, turn)
		require.NoError(t, turn.Interrupt(contextForTest(t)))
		events := collect(t, turn)
		require.ErrorIs(t, turn.Respond(contextForTest(t), req.ID, hp.Response{OptionID: "accept"}), hp.ErrRequestNotFound)
		assert.Equal(t, hp.OutcomeInterrupted, events[len(events)-1].Outcome.Status)
	})

	t.Run("ask_user_question", func(t *testing.T) {
		turn, logs := replayTurn(t, "ask_user_question", hp.SandboxDefault)
		req := nextRequest(t, turn)
		assert.Equal(t, hp.RequestUserInput, req.Kind)
		require.Len(t, req.Questions, 1)
		q := req.Questions[0]
		assert.Equal(t, "q0", q.ID)
		labels := make([]string, len(q.Options))
		for i, o := range q.Options {
			labels[i] = o.Label
		}
		assert.Equal(t, []string{"Red", "Blue"}, labels)
		require.NoError(t, turn.Respond(contextForTest(t), req.ID, hp.Response{Answers: map[string][]string{q.ID: {"Blue"}}}))
		sent := logs.received(t, func(m map[string]any) bool { return m["type"] == "control_response" })
		payload := sent["response"].(map[string]any)["response"].(map[string]any)
		answers := payload["updatedInput"].(map[string]any)["answers"].(map[string]any)
		assert.Equal(t, "Blue", answers["Which colour do you prefer?"])
		events := collect(t, turn)
		assert.Contains(t, joinText(events), "Blue")
		assert.Equal(t, hp.OutcomeCompleted, events[len(events)-1].Outcome.Status)
	})

	t.Run("read_only", func(t *testing.T) {
		turn, _ := replayTurn(t, "read_only", hp.SandboxReadOnly)
		events := collect(t, turn)
		for _, e := range events {
			assert.NotEqual(t, hp.EventRequest, e.Kind)
		}
		items := finishedItems(events)
		require.Len(t, items, 1)
		assert.Equal(t, hp.ItemFailed, items[0].Status)
		assert.Equal(t, hp.OutcomeCompleted, events[len(events)-1].Outcome.Status)
	})

	t.Run("workspace_write", func(t *testing.T) {
		turn, _ := replayTurn(t, "workspace_write", hp.SandboxWorkspaceWrite)
		events := collect(t, turn)
		items := finishedItems(events)
		require.Len(t, items, 2)
		assert.Equal(t, hp.ItemCompleted, items[0].Status)
		assert.Equal(t, hp.ItemFailed, items[1].Status)
		var labels []string
		for _, e := range events {
			if e.Kind == hp.EventItemStarted {
				labels = append(labels, e.Item.Label)
			}
		}
		assert.Equal(t, []string{"touch inside.txt", "touch ../outside.txt"}, labels)
		assert.Equal(t, hp.OutcomeCompleted, events[len(events)-1].Outcome.Status)
	})

	t.Run("missing_session", func(t *testing.T) {
		cursor := &hp.Cursor{Provider: "claude", Version: 1, Data: json.RawMessage(`{"sessionId":"s-1"}`)}
		_, _, err := replayFixture(t, "missing_session", cursor)
		require.ErrorIs(t, err, hp.ErrSessionNotFound)
	})
}
