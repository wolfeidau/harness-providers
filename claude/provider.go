package claude

import (
	"context"
	"encoding/base64"
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
	"uuid"

	hp "github.com/wolfeidau/harness-providers"
)

type Config struct {
	Binary    string
	Args      []string
	ConfigDir string
	Env       []string
	Logger    *slog.Logger
}

type Provider struct{ config Config }

const (
	providerName    = "claude"
	cursorVersion   = 1
	eventBufferSize = 1024
	typeField       = "type"
	roleUser        = "user"
	fieldText       = "text"
	fieldMessage    = "message"
	fieldBehavior   = "behavior"
	toolBash        = "Bash"
	toolAskUser     = "AskUserQuestion"
	optAccept       = "accept"
	optDecline      = "decline"
	optCancel       = "cancel"
	sandboxSettings = `{"sandbox":{"enabled":true,"autoAllowBashIfSandboxed":true,"allowUnsandboxedCommands":false}}`
)

func New(config Config) *Provider {
	if config.Binary == "" {
		config.Binary = providerName
	}
	if config.Logger == nil {
		config.Logger = slog.New(slog.DiscardHandler)
	}
	return &Provider{config: config}
}

func (*Provider) Name() string { return providerName }

type cursorData struct {
	SessionID string `json:"sessionId"`
}

func decodeCursor(c *hp.Cursor) (string, error) {
	if c.Provider != providerName || c.Version != cursorVersion {
		return "", hp.ErrInvalidCursor
	}
	var d cursorData
	if json.Unmarshal(c.Data, &d) != nil || d.SessionID == "" {
		return "", hp.ErrInvalidCursor
	}
	return d.SessionID, nil
}

// launchKey holds the settings that are fixed for the life of a claude process.
type launchKey struct {
	model  string
	effort string
	policy hp.ExecutionPolicy
}

func launchArgs(key launchKey, sessionID string, resume bool, extra []string) []string {
	args := []string{"-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--include-partial-messages", "--replay-user-messages", "--permission-prompt-tool", "stdio"}
	if resume {
		args = append(args, "--resume", sessionID)
	} else {
		args = append(args, "--session-id", sessionID)
	}
	if key.model != "" {
		args = append(args, "--model", key.model)
	}
	if key.effort != "" {
		args = append(args, "--effort", key.effort)
	}
	never := key.policy.Approval == hp.ApprovalNever
	switch key.policy.Sandbox {
	case hp.SandboxReadOnly:
		// Project and local settings can define hooks that would run outside the read-only tool set.
		args = append(args, "--tools", "Read,Glob,Grep", "--strict-mcp-config", "--setting-sources", "user", "--permission-mode", "dontAsk")
	case hp.SandboxWorkspaceWrite:
		args = append(args, "--permission-mode", "acceptEdits", "--settings", sandboxSettings)
		if never {
			args = append(args, "--permission-prompts", "none")
		}
	case hp.SandboxUnrestricted:
		if never {
			args = append(args, "--permission-mode", "bypassPermissions")
		}
	default:
		if never {
			args = append(args, "--permission-prompts", "none")
		}
	}
	return append(args, extra...)
}

type session struct {
	cfg       Config
	env       []string
	cwd       string
	sessionID string
	logger    *slog.Logger
	mu        sync.Mutex
	client    *client
	gen       int // identifies the current child so frames from a replaced one are dropped
	key       launchKey
	started   bool // whether a turn has run, so the native session exists on disk
	active    *turn
	closed    bool
}

func (p *Provider) Open(ctx context.Context, in hp.OpenRequest) (hp.Session, error) {
	info, err := os.Stat(in.WorkingDirectory)
	if err != nil {
		return nil, fmt.Errorf("claude open workspace: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("claude open workspace %q: %w", in.WorkingDirectory, syscall.ENOTDIR)
	}
	cwd, err := filepath.Abs(in.WorkingDirectory)
	if err != nil {
		return nil, fmt.Errorf("claude resolve workspace: %w", err)
	}
	s := &session{cfg: p.config, cwd: cwd, logger: p.config.Logger, env: environment(p.config.ConfigDir, p.config.Env)}
	if in.Resume != nil {
		if s.sessionID, err = decodeCursor(in.Resume); err != nil {
			return nil, err
		}
		s.started = true
	} else {
		s.sessionID = uuid.NewV4().String()
	}
	s.logger.DebugContext(ctx, "claude session opening", "resume", s.started)
	if s.started {
		// Resume spawns now so a missing session is reported by Open.
		c, err := s.spawn(ctx, s.key)
		if err != nil {
			return nil, err
		}
		s.client = c
	} else if _, err = exec.LookPath(s.cfg.Binary); err != nil {
		return nil, fmt.Errorf("claude binary: %w", err)
	}
	s.logger.DebugContext(ctx, "claude session opened", "session_id", s.sessionID, "resume", s.started)
	return s, nil
}

// spawn starts a child and completes initialize; callers install the result in s.client.
func (s *session) spawn(ctx context.Context, key launchKey) (*client, error) {
	s.mu.Lock()
	s.gen++
	gen := s.gen
	resume := s.started
	s.mu.Unlock()
	c, err := newClient(s.cfg.Binary, launchArgs(key, s.sessionID, resume, s.cfg.Args), s.env, s.cwd, s.logger, func(f frame) { s.receive(gen, f) })
	if err != nil {
		return nil, err
	}
	if err = c.request(ctx, "initialize"); err != nil {
		missing := resume && c.sessionMissing()
		_ = c.close()
		if missing {
			return nil, fmt.Errorf("claude resume: %w", hp.ErrSessionNotFound)
		}
		return nil, fmt.Errorf("claude initialize: %w", err)
	}
	s.logger.DebugContext(ctx, "claude initialized")
	return c, nil
}

func (s *session) Cursor() hp.Cursor {
	b, _ := json.Marshal(cursorData{SessionID: s.sessionID})
	return hp.Cursor{Provider: providerName, Version: cursorVersion, Data: b}
}

func (s *session) Close() error {
	s.mu.Lock()
	s.closed = true
	c := s.client
	s.mu.Unlock()
	s.logger.Debug("claude session closing", "session_id", s.sessionID)
	if c == nil {
		return nil
	}
	return c.close()
}

var imageTypes = map[string]string{".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif", ".webp": "image/webp"}

func validateInput(in hp.TurnInput) ([]map[string]any, error) {
	if len(in.Parts) == 0 {
		return nil, hp.ErrUnsupported
	}
	parts := make([]map[string]any, 0, len(in.Parts))
	for _, p := range in.Parts {
		switch p.Kind {
		case hp.InputText:
			if p.Text == "" {
				continue
			}
			parts = append(parts, map[string]any{typeField: "text", fieldText: p.Text})
		case hp.InputLocalImage:
			block, err := imageBlock(p.Path)
			if err != nil {
				return nil, err
			}
			parts = append(parts, block)
		default:
			return nil, hp.ErrUnsupported
		}
	}
	if len(parts) == 0 {
		return nil, hp.ErrUnsupported
	}
	switch in.Effort {
	case "", "low", "medium", "high", "xhigh", "max":
	default:
		return nil, hp.ErrUnsupported
	}
	switch in.Policy.Approval {
	case hp.ApprovalDefault, hp.ApprovalOnRequest, hp.ApprovalNever:
	default:
		return nil, hp.ErrUnsupported
	}
	switch in.Policy.Sandbox {
	case hp.SandboxDefault, hp.SandboxReadOnly, hp.SandboxWorkspaceWrite, hp.SandboxUnrestricted:
	default:
		return nil, hp.ErrUnsupported
	}
	return parts, nil
}

func imageBlock(path string) (map[string]any, error) {
	if !filepath.IsAbs(path) {
		return nil, hp.ErrUnsupported
	}
	mediaType, ok := imageTypes[strings.ToLower(filepath.Ext(path))]
	if !ok {
		return nil, hp.ErrUnsupported
	}
	data, err := os.ReadFile(path) //nolint:gosec // The caller supplies the image path.
	if err != nil {
		return nil, fmt.Errorf("claude local image: %w", err)
	}
	return map[string]any{typeField: "image", "source": map[string]any{typeField: "base64", "media_type": mediaType, "data": base64.StdEncoding.EncodeToString(data)}}, nil
}

func (s *session) StartTurn(ctx context.Context, in hp.TurnInput) (hp.Turn, error) {
	parts, err := validateInput(in)
	if err != nil {
		return nil, err
	}
	key := launchKey{model: in.Model, effort: in.Effort, policy: in.Policy}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, hp.ErrClosed
	}
	if s.busy() {
		s.mu.Unlock()
		return nil, hp.ErrBusy
	}
	t := &turn{s: s, id: uuid.NewV4().String(), admitted: make(chan struct{}), events: make(chan hp.Event, eventBufferSize), pending: make(map[string]pendingRequest), seenText: make(map[string]bool), items: make(map[string]string)}
	s.active = t
	old := s.client
	respawn := old == nil || old.failed() || key != s.key
	if respawn {
		s.client = nil // a nil client keeps the turn busy while the child starts
	} else {
		t.client = old
	}
	s.mu.Unlock()
	if respawn {
		if err = s.launch(ctx, t, key, old); err != nil {
			return nil, err
		}
	}
	s.logger.DebugContext(ctx, "claude turn admission requested", "session_id", s.sessionID, "turn_id", t.id)
	userMsg := map[string]any{typeField: roleUser, fieldMessage: map[string]any{"role": roleUser, "content": parts}, "parent_tool_use_id": nil, "session_id": "", "uuid": t.id}
	if err = t.client.write(userMsg); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("claude user message (acceptance uncertain; session closed): %w", err)
	}
	select {
	case <-t.admitted:
	case <-ctx.Done():
		_ = s.Close()
		return nil, fmt.Errorf("claude turn admission (acceptance uncertain; session closed): %w", ctx.Err())
	case <-t.client.done:
		select {
		case <-t.admitted:
		default:
			_ = s.Close()
			return nil, fmt.Errorf("claude turn admission (acceptance uncertain; session closed): %w", t.client.failure())
		}
	}
	s.mu.Lock()
	s.started = true
	s.mu.Unlock()
	s.logger.DebugContext(ctx, "claude turn admitted", "session_id", s.sessionID, "turn_id", t.id)
	return t, nil
}

// A turn whose child has failed can no longer finish, so it does not block the next one. Callers must hold s.mu.
func (s *session) busy() bool {
	a := s.active
	return a != nil && (a.client == nil || !a.client.failed())
}

// launch replaces the child for t; callers have already detached old from the session.
func (s *session) launch(ctx context.Context, t *turn, key launchKey, old *client) error {
	s.logger.DebugContext(ctx, "claude spawning for turn", "session_id", s.sessionID)
	if old != nil {
		_ = old.close()
	}
	c, err := s.spawn(ctx, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err != nil:
		s.closed = true
		return fmt.Errorf("claude spawn (session closed): %w", err)
	case s.closed:
		_ = c.close()
		return hp.ErrClosed
	}
	s.client, s.key, t.client = c, key, c
	return nil
}

func (s *session) receive(gen int, f frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if gen != s.gen {
		return
	}
	t := s.active
	if s.closed || t == nil {
		if f.Type == frameControlRequest && s.client != nil {
			_ = s.client.reject(f.RequestID, "no active Claude turn")
		}
		return
	}
	if t.isAdmitted {
		s.dispatch(t, f)
		return
	}
	if f.Type == "command_lifecycle" && f.State == "queued" && f.CommandUUID == t.id {
		t.isAdmitted = true
		close(t.admitted)
		s.enqueue(t, hp.Event{Kind: hp.EventTurnStarted, TurnID: t.id})
		return
	}
	// Anything else before admission is a straggler from the previous turn.
	if f.Type == frameControlRequest && s.client != nil {
		_ = s.client.reject(f.RequestID, "no admitted Claude turn")
	}
}

// Callers must hold s.mu.
func (s *session) checkInit(t *turn, f frame) {
	if f.Subtype != "init" || f.SessionID == s.sessionID {
		return
	}
	s.logger.Warn("claude session identity mismatch", "session_id", s.sessionID)
	s.closed = true
	t.client.fail(errors.New("claude session identity mismatch"))
	// close waits for the reader that is running this callback.
	go func() { _ = t.client.close() }()
}

// Callers must hold s.mu.
func (s *session) dispatch(t *turn, f frame) {
	if f.fromSubagent() && (f.Type == "stream_event" || f.Type == "assistant" || f.Type == "user") {
		return
	}
	switch f.Type {
	case "stream_event":
		if f.Event != nil && f.Event.Type == "content_block_delta" && f.Event.Delta.Type == "text_delta" {
			t.seenText[f.APIMessageID] = true
			s.enqueue(t, hp.Event{Kind: hp.EventTextDelta, TurnID: t.id, ItemID: f.APIMessageID, Text: f.Event.Delta.Text})
		}
	case "assistant":
		s.assistantEvents(t, f)
	case "user":
		s.toolResultEvents(t, f)
	case "system":
		s.checkInit(t, f)
	case frameControlRequest:
		s.requestEvent(t, f)
	case frameControlCancel:
		delete(t.pending, f.RequestID)
	case frameResult:
		s.resultEvents(t, f)
	}
}

func (s *session) assistantEvents(t *turn, f frame) {
	if f.Message == nil {
		return
	}
	for _, b := range f.Message.blocks() {
		switch b.Type {
		case "text":
			if b.Text != "" && !t.seenText[f.Message.ID] {
				s.enqueue(t, hp.Event{Kind: hp.EventTextDelta, TurnID: t.id, ItemID: f.Message.ID, Text: b.Text})
			}
		case "tool_use":
			if _, dup := t.items[b.ID]; dup {
				continue
			}
			kind := toolKind(b.Name)
			t.items[b.ID] = kind
			label := b.Name
			if b.Name == toolBash {
				var in struct {
					Command string `json:"command"`
				}
				_ = json.Unmarshal(b.Input, &in)
				label = in.Command
			}
			s.enqueue(t, hp.Event{Kind: hp.EventItemStarted, TurnID: t.id, ItemID: b.ID, Item: &hp.Item{Kind: kind, Label: label, Status: hp.ItemRunning}})
		}
	}
}

func toolKind(name string) string {
	switch name {
	case toolBash:
		return "command"
	case "Edit", "MultiEdit", "Write", "NotebookEdit":
		return "file_change"
	default:
		return "tool"
	}
}

func (s *session) toolResultEvents(t *turn, f frame) {
	for _, b := range f.Message.blocks() {
		if b.Type != "tool_result" {
			continue
		}
		status := hp.ItemCompleted
		if b.IsError {
			status = hp.ItemFailed
		}
		for _, m := range f.ToolResultMeta {
			if m.ID == b.ToolUseID && m.PermissionDecision != nil && m.PermissionDecision.Decision == "reject" {
				status = hp.ItemDeclined
			}
		}
		s.enqueue(t, hp.Event{Kind: hp.EventItemFinished, TurnID: t.id, ItemID: b.ToolUseID, Item: &hp.Item{Kind: t.items[b.ToolUseID], Status: status}})
	}
}

func (s *session) resultEvents(t *turn, f frame) {
	if f.UserMessageUUID != "" && f.UserMessageUUID != t.id {
		return
	}
	if u := f.Usage; u != nil {
		s.enqueue(t, hp.Event{Kind: hp.EventUsage, TurnID: t.id, Usage: &hp.Usage{InputTokens: u.InputTokens + u.CacheCreationTokens + u.CacheReadTokens, OutputTokens: u.OutputTokens}})
	}
	out := &hp.Outcome{Status: hp.OutcomeCompleted}
	switch {
	case f.TerminalReason == "aborted_tools" || f.TerminalReason == "aborted_streaming":
		out.Status = hp.OutcomeInterrupted
	case f.IsError:
		out.Status = hp.OutcomeFailed
		out.Error = failureDetail(f)
	}
	s.enqueue(t, hp.Event{Kind: hp.EventTurnFinished, TurnID: t.id, Outcome: out})
	clear(t.pending)
	s.active = nil
	s.logger.Debug("claude turn finished", "session_id", s.sessionID, "turn_id", t.id, "outcome", string(out.Status))
}

func failureDetail(f frame) string {
	var details []string
	for _, e := range f.Errors {
		if !strings.HasPrefix(e, "[ede_diagnostic]") {
			details = append(details, e)
		}
	}
	if len(details) > 0 {
		return strings.Join(details, "; ")
	}
	if f.Result != "" {
		return f.Result
	}
	return "claude reported an error"
}

type pendingRequest struct {
	tool      string
	input     json.RawMessage
	questions []string // question text by index; Claude keys answers by text
}

func (s *session) requestEvent(t *turn, f frame) {
	req := f.Request
	r := &hp.Request{ID: f.RequestID, Kind: hp.RequestApproval, ItemID: req.ToolUseID, Options: []hp.Option{{ID: optAccept, Label: "Accept"}, {ID: optDecline, Label: "Decline"}, {ID: optCancel, Label: "Cancel"}}}
	var in struct {
		Command     string `json:"command"`
		Description string `json:"description"`
		FilePath    string `json:"file_path"`
		NotebookPth string `json:"notebook_path"`
		Questions   []struct {
			Question string `json:"question"`
			Header   string `json:"header"`
			Options  []struct {
				Label       string `json:"label"`
				Description string `json:"description"`
			} `json:"options"`
			MultiSelect bool `json:"multiSelect"`
		} `json:"questions"`
	}
	_ = json.Unmarshal(req.Input, &in)
	pending := pendingRequest{tool: req.ToolName, input: req.Input}
	switch {
	case req.ToolName == toolAskUser:
		r.Kind, r.Options = hp.RequestUserInput, []hp.Option{{ID: optDecline, Label: "Decline"}, {ID: optCancel, Label: "Cancel"}}
		for i, q := range in.Questions {
			pending.questions = append(pending.questions, q.Question)
			question := hp.Question{ID: questionID(i), Header: q.Header, Prompt: q.Question, AllowFreeText: true, Multiple: q.MultiSelect}
			for _, o := range q.Options {
				question.Options = append(question.Options, hp.Option{ID: o.Label, Label: o.Label, Description: o.Description})
			}
			r.Questions = append(r.Questions, question)
		}
	case req.ToolName == toolBash:
		r.Approval = &hp.Approval{Action: hp.ActionCommand, Command: in.Command, Cwd: s.cwd, Reason: in.Description}
	case toolKind(req.ToolName) == "file_change":
		path := in.FilePath
		if path == "" {
			path = in.NotebookPth
		}
		r.Approval = &hp.Approval{Action: hp.ActionFileChange, Reason: req.Description}
		if path != "" {
			r.Approval.Paths = []string{path}
		}
	default:
		r.Approval = &hp.Approval{Action: hp.ActionTool, Tool: req.ToolName, Command: string(req.Input), Reason: req.Description}
	}
	t.pending[f.RequestID] = pending
	s.logger.Debug("claude tool permission request", "session_id", s.sessionID, "turn_id", t.id, "request_id", f.RequestID)
	s.enqueue(t, hp.Event{Kind: hp.EventRequest, TurnID: t.id, Request: r})
}

func (s *session) enqueue(t *turn, e hp.Event) {
	select {
	case t.events <- e:
	default:
		s.logger.Warn("claude event buffer full", "session_id", s.sessionID, "turn_id", t.id)
		t.client.fail(fmt.Errorf("claude event buffer full"))
	}
}

type turn struct {
	s          *session
	id         string
	client     *client
	admitted   chan struct{}
	isAdmitted bool // protected by session.mu
	events     chan hp.Event
	pending    map[string]pendingRequest // protected by session.mu
	seenText   map[string]bool           // protected by session.mu
	items      map[string]string         // tool_use id -> item kind; protected by session.mu
	readDone   bool
}

func (t *turn) ID() string { return t.id }

func (t *turn) Next(ctx context.Context) (hp.Event, error) {
	if t.readDone {
		return hp.Event{}, io.EOF
	}
	select {
	case e := <-t.events:
		return t.delivered(e)
	default:
	}
	select {
	case e := <-t.events:
		return t.delivered(e)
	case <-ctx.Done():
		return hp.Event{}, ctx.Err()
	case <-t.client.done:
		select {
		case e := <-t.events:
			return t.delivered(e)
		default:
		}
		return hp.Event{}, t.client.failure()
	}
}

func (t *turn) delivered(e hp.Event) (hp.Event, error) {
	if e.Kind == hp.EventTurnFinished {
		t.readDone = true
	}
	return e, nil
}

func (t *turn) Respond(ctx context.Context, id string, response hp.Response) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := t.takePending(id, response)
	if err != nil {
		return err
	}
	if err := t.client.respond(id, result); err != nil {
		return fmt.Errorf("claude respond to request %s: %w", id, err)
	}
	t.s.logger.DebugContext(ctx, "claude tool permission answered", "session_id", t.s.sessionID, "turn_id", t.id, "request_id", id)
	return nil
}

// takePending leaves the request pending when the response is unsupported.
func (t *turn) takePending(id string, response hp.Response) (map[string]any, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	if t.s.closed {
		return nil, hp.ErrClosed
	}
	p, ok := t.pending[id]
	if !ok {
		return nil, hp.ErrRequestNotFound
	}
	var result map[string]any
	switch {
	case response.OptionID == optDecline:
		result = map[string]any{fieldBehavior: "deny", "message": "User declined tool execution."}
	case response.OptionID == optCancel:
		result = map[string]any{fieldBehavior: "deny", "message": "User canceled tool execution.", "interrupt": true}
	case p.tool == toolAskUser && response.OptionID == "":
		input, err := p.answered(response.Answers)
		if err != nil {
			return nil, err
		}
		result = map[string]any{fieldBehavior: "allow", "updatedInput": input}
	case p.tool != toolAskUser && response.OptionID == optAccept:
		result = map[string]any{fieldBehavior: "allow", "updatedInput": p.input}
	default:
		return nil, hp.ErrUnsupported
	}
	delete(t.pending, id)
	return result, nil
}

func questionID(i int) string { return "q" + strconv.Itoa(i) }

func (p pendingRequest) answered(response map[string][]string) (map[string]any, error) {
	input := map[string]any{}
	if err := json.Unmarshal(p.input, &input); err != nil {
		return nil, fmt.Errorf("claude decode question input: %w", err)
	}
	answers := map[string]string{}
	used := 0
	for i, text := range p.questions {
		v, ok := response[questionID(i)]
		if ok {
			used++
		}
		if len(v) > 0 {
			if prev, ok := answers[text]; ok {
				v = append([]string{prev}, v...)
			}
			answers[text] = strings.Join(v, ", ")
		}
	}
	if used != len(response) {
		return nil, fmt.Errorf("claude answers name unknown question IDs: %w", hp.ErrUnsupported)
	}
	input["answers"] = answers
	return input, nil
}

func (t *turn) Interrupt(ctx context.Context) error {
	t.s.mu.Lock()
	active, closed := t.s.active == t, t.s.closed
	t.s.mu.Unlock()
	if closed {
		return hp.ErrClosed
	}
	if !active {
		t.s.logger.DebugContext(ctx, "claude interrupt skipped", "turn_id", t.id, "reason", "inactive")
		return nil
	}
	t.s.logger.DebugContext(ctx, "claude interrupt requested", "session_id", t.s.sessionID, "turn_id", t.id)
	if err := t.client.request(ctx, "interrupt"); err != nil {
		return fmt.Errorf("claude interrupt: %w", err)
	}
	t.s.logger.DebugContext(ctx, "claude interrupt acknowledged", "session_id", t.s.sessionID, "turn_id", t.id)
	return nil
}
