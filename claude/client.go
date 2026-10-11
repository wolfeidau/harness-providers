package claude

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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wolfeidau/harness-providers/internal/subprocess"
)

const (
	frameControlRequest  = "control_request"
	frameControlResponse = "control_response"
	frameControlCancel   = "control_cancel_request"
	frameResult          = "result"
	fieldRequestID       = "request_id"
	fieldSubtype         = "subtype"
	fieldResponse        = "response"
	subtypeSuccess       = "success"
	subtypeCanUseTool    = "can_use_tool"
	maxFrameBytes        = 8 * 1024 * 1024
	initialFrameBytes    = 64 * 1024
	stderrTailBytes      = 4096
	// Bounds close if a process outside the group still holds the pipes.
	killWait = 2 * time.Second
)

// Time for Claude to stop its own commands after stdin closes; a var so tests can shorten it.
var closeGrace = 3 * time.Second

type controlRequest struct {
	Subtype     string          `json:"subtype"`
	ToolName    string          `json:"tool_name"`
	Input       json.RawMessage `json:"input"`
	Description string          `json:"description"`
	ToolUseID   string          `json:"tool_use_id"`
}

type controlResponse struct {
	Subtype   string `json:"subtype"`
	RequestID string `json:"request_id"`
	Error     string `json:"error"`
}

type streamEvent struct {
	Type  string `json:"type"`
	Delta struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"delta"`
}

type contentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	IsError   bool            `json:"is_error"`
}

type message struct {
	ID      string          `json:"id"`
	Content json.RawMessage `json:"content"`
}

// blocks tolerates string content, which the CLI uses for some user messages.
func (m *message) blocks() []contentBlock {
	if m == nil {
		return nil
	}
	var blocks []contentBlock
	if json.Unmarshal(m.Content, &blocks) != nil {
		return nil
	}
	return blocks
}

type toolResultMeta struct {
	ID                 string `json:"id"`
	PermissionDecision *struct {
		Decision string `json:"decision"`
	} `json:"permission_decision"`
}

type usage struct {
	InputTokens         int64 `json:"input_tokens"`
	CacheCreationTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadTokens     int64 `json:"cache_read_input_tokens"`
	OutputTokens        int64 `json:"output_tokens"`
}

type frame struct {
	Type            string           `json:"type"`
	Subtype         string           `json:"subtype"`
	RequestID       string           `json:"request_id"`
	Request         *controlRequest  `json:"request"`
	Response        *controlResponse `json:"response"`
	State           string           `json:"state"`
	CommandUUID     string           `json:"command_uuid"`
	SessionID       string           `json:"session_id"`
	APIMessageID    string           `json:"api_message_id"`
	UserMessageUUID string           `json:"user_message_uuid"`
	Event           *streamEvent     `json:"event"`
	Message         *message         `json:"message"`
	ToolResultMeta  []toolResultMeta `json:"tool_result_meta"`
	IsError         bool             `json:"is_error"`
	Errors          []string         `json:"errors"`
	Result          string           `json:"result"`
	TerminalReason  string           `json:"terminal_reason"`
	Usage           *usage           `json:"usage"`
}

type reply struct {
	err error
}

type client struct {
	cmd        *exec.Cmd
	logger     *slog.Logger
	stdin      io.WriteCloser
	writeMu    sync.Mutex
	mu         sync.Mutex
	nextID     int64
	waiting    map[string]chan reply
	onMessage  func(frame)
	done       chan struct{}
	waitDone   chan struct{}
	err        error
	closing    bool
	closeOnce  sync.Once
	resultErrs []string
	stderrTail tailWriter
}

// tailWriter keeps the first bytes written; it never errors so it cannot stall stderr draining.
type tailWriter struct{ buf bytes.Buffer }

func (w *tailWriter) Write(p []byte) (int, error) {
	if room := stderrTailBytes - w.buf.Len(); room > 0 {
		w.buf.Write(p[:min(room, len(p))])
	}
	return len(p), nil
}

func newClient(binary string, args, env []string, dir string, logger *slog.Logger, onMessage func(frame)) (*client, error) {
	logger.Debug("claude spawn")
	//nolint:gosec // The caller explicitly configures the Claude executable and its arguments.
	cmd := exec.Command(binary, args...)
	cmd.Dir = dir
	cmd.Env = env
	subprocess.SetProcessGroup(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("claude stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("claude stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("claude stderr pipe: %w", err)
	}
	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("claude start: %w", err)
	}
	logger.Debug("claude spawned", "pid", cmd.Process.Pid)
	c := &client{cmd: cmd, logger: logger, stdin: stdin, waiting: make(map[string]chan reply), onMessage: onMessage, done: make(chan struct{}), waitDone: make(chan struct{})}
	stderrDone := make(chan struct{})
	go func() {
		subprocess.DrainStderr(io.TeeReader(stderr, &c.stderrTail), logger, "claude", env)
		close(stderrDone)
	}()
	readDone := make(chan struct{})
	go func() { c.read(stdout); close(readDone) }()
	go func() {
		<-readDone
		<-stderrDone
		err := cmd.Wait()
		c.mu.Lock()
		closing := c.closing
		c.mu.Unlock()
		switch {
		case closing:
			logger.Debug("claude stopped")
		case err != nil:
			logger.Warn("claude exited", "outcome", "error")
		default:
			logger.Debug("claude exited", "outcome", "success")
		}
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		c.fail(fmt.Errorf("claude exited: %w", err))
		close(c.waitDone)
	}()
	return c, nil
}

func (c *client) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	if !c.closing {
		c.logger.Warn("claude failed", "error_kind", subprocess.FailureKind(err))
	}
	c.err = err
	for id, ch := range c.waiting {
		ch <- reply{err: err}
		delete(c.waiting, id)
	}
	close(c.done)
}

func (c *client) read(r io.Reader) {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, initialFrameBytes), maxFrameBytes)
	for s.Scan() {
		var f frame
		if err := json.Unmarshal(s.Bytes(), &f); err != nil {
			c.fail(fmt.Errorf("decode claude frame: %w", err))
			return
		}
		c.handleFrame(f)
	}
	if err := s.Err(); err != nil {
		c.fail(fmt.Errorf("read claude: %w", err))
	}
}

func (c *client) handleFrame(f frame) {
	switch f.Type {
	case frameControlResponse:
		c.handleResponse(f)
	case frameControlRequest:
		if f.Request == nil || f.Request.Subtype != subtypeCanUseTool {
			subtype := ""
			if f.Request != nil {
				subtype = f.Request.Subtype
			}
			c.logger.Warn("claude unsupported control request", "subtype", subtype)
			if err := c.reject(f.RequestID, "unsupported control request: "+subtype); err != nil {
				c.fail(err)
			}
			return
		}
		c.onMessage(f)
	case frameResult:
		c.mu.Lock()
		c.resultErrs = f.Errors
		c.mu.Unlock()
		c.onMessage(f)
	default:
		c.onMessage(f)
	}
}

// Responses with unknown IDs are the CLI echoing our own control_response frames.
func (c *client) handleResponse(f frame) {
	if f.Response == nil {
		return
	}
	c.mu.Lock()
	ch := c.waiting[f.Response.RequestID]
	delete(c.waiting, f.Response.RequestID)
	c.mu.Unlock()
	if ch == nil {
		return
	}
	if f.Response.Subtype != subtypeSuccess {
		c.logger.Debug("claude response", "request_id", f.Response.RequestID, "outcome", "error")
		ch <- reply{err: fmt.Errorf("claude control error: %s", f.Response.Error)}
		return
	}
	c.logger.Debug("claude response", "request_id", f.Response.RequestID, "outcome", "success")
	ch <- reply{}
}

func (c *client) write(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode claude frame: %w", err)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err = c.stdin.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("write claude frame: %w", err)
	}
	return nil
}

func (c *client) request(ctx context.Context, subtype string) error {
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return err
	}
	c.nextID++
	id := "req_" + strconv.FormatInt(c.nextID, 10)
	ch := make(chan reply, 1)
	c.waiting[id] = ch
	c.mu.Unlock()
	c.logger.DebugContext(ctx, "claude request", "subtype", subtype, "request_id", id)
	if err := c.write(map[string]any{typeField: frameControlRequest, fieldRequestID: id, "request": map[string]any{fieldSubtype: subtype}}); err != nil {
		c.forget(id)
		return fmt.Errorf("%s write: %w", subtype, err)
	}
	select {
	case r := <-ch:
		if r.err != nil {
			return fmt.Errorf("%s: %w", subtype, r.err)
		}
		return nil
	case <-ctx.Done():
		c.logger.DebugContext(ctx, "claude request canceled", "subtype", subtype, "request_id", id)
		c.forget(id)
		return fmt.Errorf("%s: %w", subtype, ctx.Err())
	}
}

func (c *client) forget(id string) {
	c.mu.Lock()
	delete(c.waiting, id)
	c.mu.Unlock()
}

func (c *client) respond(id string, payload any) error {
	return c.write(map[string]any{typeField: frameControlResponse, fieldResponse: map[string]any{fieldSubtype: subtypeSuccess, fieldRequestID: id, fieldResponse: payload}})
}

func (c *client) reject(id, msg string) error {
	return c.write(map[string]any{typeField: frameControlResponse, fieldResponse: map[string]any{fieldSubtype: "error", fieldRequestID: id, "error": msg}})
}

func (c *client) close() error {
	c.closeOnce.Do(func() {
		c.logger.Debug("claude closing")
		c.mu.Lock()
		c.closing = true
		c.mu.Unlock()
		c.fail(errors.New("claude closed"))
		_ = c.stdin.Close()
		select {
		case <-c.waitDone:
		case <-time.After(closeGrace):
		}
		// Commands Claude spawned may outlive it; killing an empty group is harmless.
		subprocess.KillProcessGroup(c.cmd.Process)
		select {
		case <-c.waitDone:
		case <-time.After(killWait):
		}
	})
	return nil
}

func (c *client) failure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// sessionMissing waits for the exiting process, whose result frame or stderr names the missing session.
func (c *client) sessionMissing() bool {
	select {
	case <-c.waitDone:
	case <-time.After(killWait):
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	const marker = "No conversation found with session ID"
	return strings.Contains(strings.Join(c.resultErrs, "\n"), marker) || strings.Contains(c.stderrTail.buf.String(), marker)
}

func environment(configDir string, extra []string) []string {
	env := make([]string, 0, len(os.Environ())+len(extra)+1)
	for _, v := range os.Environ() {
		if configDir == "" || !strings.HasPrefix(v, "CLAUDE_CONFIG_DIR=") {
			env = append(env, v)
		}
	}
	env = append(env, extra...)
	if configDir != "" {
		env = append(env, "CLAUDE_CONFIG_DIR="+configDir)
	}
	return env
}
