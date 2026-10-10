package codex

import (
	"bufio"
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
)

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

const (
	jsonRPCField             = "jsonrpc"
	jsonRPCVersion           = "2.0"
	requestUserInputMethod   = "item/tool/requestUserInput"
	commandApprovalMethod    = "item/commandExecution/requestApproval"
	fileChangeApprovalMethod = "item/fileChange/requestApproval"
)

func (e *rpcError) Error() string { return fmt.Sprintf("rpc %d: %s", e.Code, e.Message) }

type frame struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}
type reply struct {
	result json.RawMessage
	err    error
}
type client struct {
	cmd       *exec.Cmd
	logger    *slog.Logger
	stdin     io.WriteCloser
	writeMu   sync.Mutex
	mu        sync.Mutex
	nextID    int64
	waiting   map[string]chan reply
	onMessage func(frame)
	done      chan struct{}
	waitDone  chan struct{}
	err       error
	closing   bool
	closeOnce sync.Once
}

func newClient(binary string, args, env []string, dir string, logger *slog.Logger, onMessage func(frame)) (*client, error) {
	logger.Debug("codex app-server spawn")
	//nolint:gosec // The caller explicitly configures the Codex executable and its arguments.
	cmd := exec.Command(binary, append([]string{"app-server"}, args...)...)
	cmd.Dir = dir
	cmd.Env = env
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		logger.Error("codex app-server spawn failed", "error_kind", "start")
		return nil, err
	}
	logger.Debug("codex app-server spawned", "pid", cmd.Process.Pid)
	c := &client{cmd: cmd, logger: logger, stdin: stdin, waiting: make(map[string]chan reply), onMessage: onMessage, done: make(chan struct{}), waitDone: make(chan struct{})}
	stderrDone := make(chan struct{})
	go func() {
		drainStderr(stderr, logger, env)
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
			logger.Debug("codex app-server stopped")
		case err != nil:
			logger.Warn("codex app-server exited", "outcome", "error")
		default:
			logger.Debug("codex app-server exited", "outcome", "success")
		}
		c.fail(fmt.Errorf("codex app-server exited: %w", err))
		close(c.waitDone)
	}()
	return c, nil
}

func drainStderr(r io.Reader, logger *slog.Logger, env []string) {
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
					logger.Debug("codex app-server stderr read failed", "error_kind", failureKind(err))
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
			logger.Debug("codex app-server stderr limit reached", "lines", maxLines)
		}
		if lines < maxLines {
			logger.Debug("codex app-server stderr", "line", redactStderr(string(line), env), "truncated", truncated)
		}
		lines++
	}
}

func redactStderr(line string, env []string) string {
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || len(value) < 4 {
			continue
		}
		key = strings.ToUpper(key)
		if strings.Contains(key, "TOKEN") || strings.Contains(key, "KEY") || strings.Contains(key, "SECRET") || strings.Contains(key, "PASSWORD") {
			line = strings.ReplaceAll(line, value, "[REDACTED]")
		}
	}
	return line
}
func (c *client) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	if !c.closing {
		c.logger.Warn("codex app-server failed", "error_kind", failureKind(err))
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
	s.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for s.Scan() {
		var f frame
		if err := json.Unmarshal(s.Bytes(), &f); err != nil {
			c.logger.Warn("codex protocol decode failed")
			c.fail(fmt.Errorf("decode app-server frame: %w", err))
			return
		}
		if err := c.handleFrame(f); err != nil {
			c.fail(err)
			return
		}
	}
	c.mu.Lock()
	closing := c.closing
	c.mu.Unlock()
	if err := s.Err(); err != nil {
		if !closing {
			c.logger.Warn("codex app-server stream read failed", "error_kind", "read")
		}
		c.fail(fmt.Errorf("read app-server: %w", err))
	} else {
		if !closing {
			c.logger.Warn("codex app-server stream ended")
		}
		c.fail(fmt.Errorf("codex app-server stream ended unexpectedly: %w", io.ErrUnexpectedEOF))
	}
}
func (c *client) handleFrame(f frame) error {
	if len(f.ID) > 0 && f.Method == "" {
		c.handleResponse(f)
		return nil
	}
	if f.Method == "" {
		return nil
	}
	if len(f.ID) > 0 && !supportedServerRequest(f.Method) {
		c.logger.Warn("codex unsupported server request", "method", f.Method)
		return c.reject(f.ID, "unsupported server request: "+f.Method)
	}
	c.onMessage(f)
	return nil
}
func (c *client) handleResponse(f frame) {
	id := string(f.ID)
	c.mu.Lock()
	ch := c.waiting[id]
	delete(c.waiting, id)
	c.mu.Unlock()
	if ch == nil {
		return
	}
	if f.Error != nil {
		c.logger.Debug("codex response", "request_id", id, "outcome", "rpc_error", "rpc_code", f.Error.Code)
		ch <- reply{err: f.Error}
		return
	}
	c.logger.Debug("codex response", "request_id", id, "outcome", "success")
	ch <- reply{result: f.Result}
}
func supportedServerRequest(method string) bool {
	switch method {
	case commandApprovalMethod, fileChangeApprovalMethod, requestUserInputMethod:
		return true
	default:
		return false
	}
}
func (c *client) write(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.stdin.Write(append(b, '\n'))
	return err
}
func (c *client) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	c.nextID++
	numID := c.nextID
	id := strconv.FormatInt(numID, 10)
	ch := make(chan reply, 1)
	c.waiting[id] = ch
	c.mu.Unlock()
	c.logger.Debug("codex request", "method", method, "request_id", id)
	if err := c.write(map[string]any{jsonRPCField: jsonRPCVersion, "id": numID, "method": method, "params": params}); err != nil {
		c.logger.Warn("codex request write failed", "method", method, "request_id", id)
		c.mu.Lock()
		delete(c.waiting, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("%s write: %w", method, err)
	}
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, fmt.Errorf("%s: %w", method, r.err)
		}
		return r.result, nil
	case <-ctx.Done():
		c.logger.Debug("codex request canceled", "method", method, "request_id", id)
		c.mu.Lock()
		delete(c.waiting, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("%s: %w", method, ctx.Err())
	}
}
func (c *client) notify(method string, params any) error {
	return c.write(map[string]any{jsonRPCField: jsonRPCVersion, "method": method, "params": params})
}
func (c *client) respond(id json.RawMessage, result any) error {
	return c.write(map[string]any{jsonRPCField: jsonRPCVersion, "id": id, "result": result})
}
func (c *client) reject(id json.RawMessage, message string) error {
	return c.write(map[string]any{jsonRPCField: jsonRPCVersion, "id": id, "error": map[string]any{"code": -32601, "message": message}})
}
func (c *client) close() error {
	c.closeOnce.Do(func() {
		c.logger.Debug("codex app-server closing")
		c.mu.Lock()
		c.closing = true
		c.mu.Unlock()
		c.fail(errors.New("codex app-server closed"))
		_ = c.stdin.Close()
		if c.cmd.Process != nil {
			_ = c.cmd.Process.Kill()
		}
		<-c.waitDone
	})
	return nil
}
func failureKind(err error) string {
	switch {
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, io.EOF):
		return "eof"
	default:
		return "other"
	}
}
func (c *client) failure() error { c.mu.Lock(); defer c.mu.Unlock(); return c.err }
func environment(home string, extra []string) []string {
	env := make([]string, 0, len(os.Environ())+len(extra)+1)
	for _, v := range os.Environ() {
		if home == "" || !strings.HasPrefix(v, "CODEX_HOME=") {
			env = append(env, v)
		}
	}
	env = append(env, extra...)
	if home != "" {
		env = append(env, "CODEX_HOME="+home)
	}
	return env
}
