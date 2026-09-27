package codex

import (
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

	hp "github.com/wolfeidau/harness-providers"
)

type Config struct {
	Binary string
	Args   []string
	Home   string
	Env    []string
	Logger *slog.Logger
}
type Provider struct{ config Config }

const (
	providerName        = "codex"
	itemTypeField       = "type"
	itemCompletedMethod = "item/completed"
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
	ThreadID string `json:"threadId"`
}

func decodeCursor(c *hp.Cursor) (string, error) {
	if c.Provider != providerName || c.Version != 1 {
		return "", hp.ErrInvalidCursor
	}
	var d cursorData
	if json.Unmarshal(c.Data, &d) != nil || d.ThreadID == "" {
		return "", hp.ErrInvalidCursor
	}
	return d.ThreadID, nil
}

type session struct {
	client   *client
	logger   *slog.Logger
	mu       sync.Mutex
	threadID string
	active   *turn
	early    []frame
	closed   bool
}

func (p *Provider) Open(ctx context.Context, in hp.OpenRequest) (hp.Session, error) {
	info, err := os.Stat(in.WorkingDirectory)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("codex open workspace: %w", os.ErrNotExist)
	}
	cwd, err := filepath.Abs(in.WorkingDirectory)
	if err != nil {
		return nil, err
	}
	var requested string
	if in.Resume != nil {
		requested, err = decodeCursor(in.Resume)
		if err != nil {
			return nil, err
		}
	}
	p.config.Logger.Debug("codex session opening", "resume", requested != "")
	s := &session{logger: p.config.Logger}
	c, err := newClient(p.config.Binary, p.config.Args, environment(p.config.Home, p.config.Env), cwd, p.config.Logger, s.receive)
	if err != nil {
		return nil, fmt.Errorf("codex start: %w", err)
	}
	s.client = c
	if _, err = c.request(ctx, "initialize", map[string]any{"clientInfo": map[string]any{"name": "harness-providers", "version": "0.1.0"}, "capabilities": map[string]any{"experimentalApi": true}}); err != nil {
		p.config.Logger.Warn("codex initialize failed", "error_kind", failureKind(err))
		_ = c.close()
		return nil, fmt.Errorf("codex initialize: %w", err)
	}
	p.config.Logger.Debug("codex initialized")
	if err = c.notify("initialized", map[string]any{}); err != nil {
		_ = c.close()
		return nil, err
	}
	threadID, err := s.openThread(ctx, cwd, requested)
	if err != nil {
		_ = c.close()
		return nil, err
	}
	s.mu.Lock()
	s.threadID = threadID
	s.early = nil
	s.mu.Unlock()
	p.config.Logger.Debug("codex session opened", "thread_id", threadID, "resume", requested != "")
	return s, nil
}
func (s *session) openThread(ctx context.Context, cwd, requested string) (string, error) {
	method := "thread/start"
	params := map[string]any{"cwd": cwd}
	if requested != "" {
		method = "thread/resume"
		params["threadId"] = requested
		params["excludeTurns"] = true
	}
	raw, err := s.client.request(ctx, method, params)
	if err != nil {
		s.logger.Warn("codex session open failed", "method", method, "error_kind", failureKind(err))
		if requested != "" && missingSession(err) {
			return "", fmt.Errorf("codex resume: %w", hp.ErrSessionNotFound)
		}
		return "", fmt.Errorf("codex %s: %w", method, err)
	}
	var result struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if err = json.Unmarshal(raw, &result); err != nil || result.Thread.ID == "" {
		s.logger.Warn("codex invalid thread response", "method", method)
		return "", fmt.Errorf("codex %s: invalid thread response", method)
	}
	if requested != "" && result.Thread.ID != requested {
		s.logger.Warn("codex resume identity changed")
		return "", fmt.Errorf("codex resume changed identity: %w", hp.ErrSessionNotFound)
	}
	return result.Thread.ID, nil
}
func missingSession(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "thread") && (strings.Contains(msg, "not found") || strings.Contains(msg, "does not exist") || strings.Contains(msg, "no such"))
}
func (s *session) Cursor() hp.Cursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _ := json.Marshal(cursorData{ThreadID: s.threadID})
	return hp.Cursor{Provider: providerName, Version: 1, Data: b}
}
func (s *session) Close() error {
	s.mu.Lock()
	s.closed = true
	threadID := s.threadID
	s.mu.Unlock()
	s.logger.Debug("codex session closing", "thread_id", threadID)
	return s.client.close()
}
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
			parts = append(parts, map[string]any{itemTypeField: "text", "text": p.Text})
		case hp.InputLocalImage:
			if !filepath.IsAbs(p.Path) {
				return nil, hp.ErrUnsupported
			}
			if _, err := os.Stat(p.Path); err != nil {
				return nil, err
			}
			parts = append(parts, map[string]any{itemTypeField: "localImage", "path": p.Path})
		default:
			return nil, hp.ErrUnsupported
		}
	}
	if len(parts) == 0 {
		return nil, hp.ErrUnsupported
	}
	if in.Effort != "" {
		switch in.Effort {
		case "none", "minimal", "low", "medium", "high", "xhigh":
		default:
			return nil, hp.ErrUnsupported
		}
	}
	if in.Policy.Approval != hp.ApprovalDefault && in.Policy.Approval != hp.ApprovalOnRequest && in.Policy.Approval != hp.ApprovalNever {
		return nil, hp.ErrUnsupported
	}
	switch in.Policy.Sandbox {
	case hp.SandboxDefault, hp.SandboxReadOnly, hp.SandboxWorkspaceWrite, hp.SandboxUnrestricted:
	default:
		return nil, hp.ErrUnsupported
	}
	return parts, nil
}
func (s *session) StartTurn(ctx context.Context, in hp.TurnInput) (hp.Turn, error) {
	parts, err := validateInput(in)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, hp.ErrClosed
	}
	if s.active != nil {
		s.mu.Unlock()
		return nil, hp.ErrBusy
	}
	t := &turn{s: s, events: make(chan hp.Event, 256), pending: make(map[string]pendingRequest), seenText: make(map[string]bool)}
	s.active = t
	s.early = nil
	threadID := s.threadID
	s.mu.Unlock()
	s.logger.Debug("codex turn admission requested", "thread_id", threadID)
	params := makeTurnParams(threadID, parts, in)
	raw, err := s.client.request(ctx, "turn/start", params)
	if err != nil {
		if rejected, ok := errors.AsType[*rpcError](err); ok {
			s.logger.Debug("codex turn admission rejected", "thread_id", threadID, "rpc_code", rejected.Code)
			s.mu.Lock()
			if s.active == t {
				s.active = nil
			}
			s.mu.Unlock()
			return nil, fmt.Errorf("codex turn/start rejected: %w", err)
		}
		s.logger.Warn("codex turn admission uncertain", "thread_id", threadID, "error_kind", failureKind(err))
		_ = s.Close()
		return nil, fmt.Errorf("codex turn/start (acceptance uncertain; session closed): %w", err)
	}
	var result struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	if err = json.Unmarshal(raw, &result); err != nil || result.Turn.ID == "" {
		s.logger.Warn("codex invalid turn response", "thread_id", threadID)
		_ = s.Close()
		return nil, fmt.Errorf("codex turn/start: invalid turn response; session closed")
	}
	s.mu.Lock()
	t.id = result.Turn.ID
	early := s.early
	s.early = nil
	for _, f := range early {
		s.dispatch(t, f)
	}
	s.mu.Unlock()
	s.logger.Debug("codex turn admitted", "thread_id", threadID, "turn_id", t.id)
	return t, nil
}
func makeTurnParams(threadID string, parts []map[string]any, in hp.TurnInput) map[string]any {
	params := map[string]any{"threadId": threadID, "input": parts}
	if in.Model != "" {
		params["model"] = in.Model
	}
	if in.Effort != "" {
		params["effort"] = in.Effort
	}
	if in.Policy.Approval != hp.ApprovalDefault {
		if in.Policy.Approval == hp.ApprovalNever {
			params["approvalPolicy"] = "never"
		} else {
			params["approvalPolicy"] = "on-request"
		}
	}
	if in.Policy.Sandbox != hp.SandboxDefault {
		switch in.Policy.Sandbox {
		case hp.SandboxReadOnly:
			params["sandboxPolicy"] = map[string]any{itemTypeField: "readOnly", "networkAccess": false}
		case hp.SandboxWorkspaceWrite:
			params["sandboxPolicy"] = map[string]any{itemTypeField: "workspaceWrite", "writableRoots": []string{}, "networkAccess": false, "excludeTmpdirEnvVar": false, "excludeSlashTmp": false}
		case hp.SandboxUnrestricted:
			params["sandboxPolicy"] = map[string]any{itemTypeField: "dangerFullAccess"}
		}
	}
	return params
}
func (s *session) receive(f frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.threadID == "" {
		if len(f.ID) > 0 {
			_ = s.client.reject(f.ID, "no active Codex turn")
		}
		return
	}
	if s.active == nil {
		if len(f.ID) > 0 {
			_ = s.client.reject(f.ID, "no active Codex turn")
		}
		return
	}
	if s.active.id == "" {
		if len(s.early) < 256 {
			s.early = append(s.early, f)
		} else {
			s.logger.Warn("codex early event buffer full", "thread_id", s.threadID)
			s.client.fail(fmt.Errorf("codex early event buffer full"))
		}
		return
	}
	s.dispatch(s.active, f)
}

type eventParams struct {
	ThreadID   string `json:"threadId"`
	TurnID     string `json:"turnId"`
	ItemID     string `json:"itemId"`
	Delta      string `json:"delta"`
	TokenUsage struct {
		Last struct {
			InputTokens  int64 `json:"inputTokens"`
			OutputTokens int64 `json:"outputTokens"`
		} `json:"last"`
	} `json:"tokenUsage"`
	Item struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Status   string `json:"status"`
		Command  string `json:"command"`
		Text     string `json:"text"`
		Output   string `json:"aggregatedOutput"`
		ExitCode *int   `json:"exitCode"`
	} `json:"item"`
	Turn struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Error  struct {
			Message string `json:"message"`
		} `json:"error"`
	} `json:"turn"`
	Questions []struct {
		ID       string `json:"id"`
		Header   string `json:"header"`
		Question string `json:"question"`
		Options  []struct {
			Label       string `json:"label"`
			Description string `json:"description"`
		} `json:"options"`
	} `json:"questions"`
}

func (s *session) dispatch(t *turn, f frame) {
	var p eventParams
	if json.Unmarshal(f.Params, &p) != nil || (p.ThreadID != "" && p.ThreadID != s.threadID) {
		return
	}
	id := p.TurnID
	if id == "" {
		id = p.Turn.ID
	}
	if id != "" && id != t.id {
		return
	}
	e := hp.Event{TurnID: t.id}
	switch f.Method {
	case "turn/started":
		e.Kind = hp.EventTurnStarted
	case "item/agentMessage/delta":
		e.Kind = hp.EventTextDelta
		e.Text = p.Delta
		if p.ItemID == "" {
			t.unattributedText = true
		} else {
			t.seenText[p.ItemID] = true
		}
	case "thread/tokenUsage/updated":
		e.Kind = hp.EventUsage
		e.Usage = &hp.Usage{InputTokens: p.TokenUsage.Last.InputTokens, OutputTokens: p.TokenUsage.Last.OutputTokens}
	case "item/started", "item/updated", itemCompletedMethod:
		e = s.itemEvent(t, p, f.Method)
	case "turn/completed":
		e = s.turnFinishedEvent(t, p)
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval", requestUserInputMethod:
		if len(f.ID) == 0 {
			return
		}
		e = s.requestEvent(t, p, f)
	default:
		return
	}
	s.enqueue(t, e)
}
func (s *session) itemEvent(t *turn, p eventParams, method string) hp.Event {
	if method == itemCompletedMethod {
		s.logCompletedItem(t, p)
		if p.Item.Type == "agentMessage" && p.Item.Text != "" && !t.seenText[p.Item.ID] && !t.unattributedText {
			s.enqueue(t, hp.Event{Kind: hp.EventTextDelta, TurnID: t.id, ItemID: p.Item.ID, Text: p.Item.Text})
		}
	}
	e := hp.Event{TurnID: t.id, ItemID: p.Item.ID, Item: &hp.Item{Kind: p.Item.Type, Label: p.Item.Command, Status: p.Item.Status}}
	switch method {
	case "item/started":
		e.Kind = hp.EventItemStarted
	case "item/updated":
		e.Kind = hp.EventItemUpdated
	case itemCompletedMethod:
		e.Kind = hp.EventItemFinished
	}
	return e
}
func (s *session) logCompletedItem(t *turn, p eventParams) {
	args := []any{"thread_id", s.threadID, "turn_id", t.id, "item_id", p.Item.ID, "item_type", p.Item.Type, "status", p.Item.Status}
	if p.Item.Type == "commandExecution" && p.Item.ExitCode != nil {
		args = append(args, "exit_code", *p.Item.ExitCode)
	}
	s.logger.Debug("codex item completed", args...)
	if p.Item.Type != "commandExecution" || p.Item.Status != "failed" || p.Item.Output == "" {
		return
	}
	output := p.Item.Output
	if len(output) > 2048 {
		output = output[:2048]
	}
	s.logger.Debug("codex command failed", "thread_id", s.threadID, "turn_id", t.id, "item_id", p.Item.ID, "output", redactStderr(output, s.client.cmd.Env))
}
func (s *session) turnFinishedEvent(t *turn, p eventParams) hp.Event {
	e := hp.Event{Kind: hp.EventTurnFinished, TurnID: t.id, Outcome: &hp.Outcome{Status: hp.OutcomeStatus(p.Turn.Status), Error: p.Turn.Error.Message}}
	if e.Outcome.Status != "completed" && e.Outcome.Status != "interrupted" && e.Outcome.Status != "failed" {
		e.Outcome.Status = hp.OutcomeFailed
	}
	s.active = nil
	s.logger.Debug("codex turn finished", "thread_id", s.threadID, "turn_id", t.id, "outcome", string(e.Outcome.Status))
	return e
}
func (s *session) requestEvent(t *turn, p eventParams, f frame) hp.Event {
	rid := string(f.ID)
	kind := hp.RequestApproval
	if f.Method == requestUserInputMethod {
		kind = hp.RequestUserInput
	}
	r := &hp.Request{ID: rid, Kind: kind, Title: f.Method, Options: []hp.Option{{ID: "accept", Label: "Accept"}, {ID: "decline", Label: "Decline"}, {ID: "cancel", Label: "Cancel"}}}
	if kind == hp.RequestUserInput {
		r.Options = nil
		for _, q := range p.Questions {
			question := hp.Question{ID: q.ID, Prompt: q.Question, AllowFreeText: true}
			for _, o := range q.Options {
				question.Options = append(question.Options, hp.Option{ID: o.Label, Label: o.Label})
			}
			r.Questions = append(r.Questions, question)
		}
	}
	t.pending[rid] = pendingRequest{id: f.ID, method: f.Method}
	s.logger.Debug("codex server request", "thread_id", s.threadID, "turn_id", t.id, "request_id", rid, "method", f.Method)
	return hp.Event{Kind: hp.EventRequest, TurnID: t.id, Request: r}
}
func (s *session) enqueue(t *turn, e hp.Event) {
	select {
	case t.events <- e:
	default:
		s.logger.Warn("codex event buffer full", "thread_id", s.threadID, "turn_id", t.id)
		s.client.fail(fmt.Errorf("codex event buffer full"))
	}
}

type pendingRequest struct {
	id     json.RawMessage
	method string
}
type turn struct {
	s                *session
	id               string
	events           chan hp.Event
	pending          map[string]pendingRequest // protected by session.mu
	seenText         map[string]bool
	unattributedText bool
	readDone         bool
}

func (t *turn) ID() string { return t.id }
func (t *turn) Next(ctx context.Context) (hp.Event, error) {
	if t.readDone {
		return hp.Event{}, io.EOF
	}
	select {
	case e := <-t.events:
		if e.Kind == hp.EventTurnFinished {
			t.readDone = true
		}
		return e, nil
	default:
	}
	select {
	case e := <-t.events:
		if e.Kind == hp.EventTurnFinished {
			t.readDone = true
		}
		return e, nil
	case <-ctx.Done():
		return hp.Event{}, ctx.Err()
	case <-t.s.client.done:
		select {
		case e := <-t.events:
			if e.Kind == hp.EventTurnFinished {
				t.readDone = true
			}
			return e, nil
		default:
		}
		return hp.Event{}, t.s.client.failure()
	}
}
func (t *turn) Respond(ctx context.Context, id string, response hp.Response) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.s.mu.Lock()
	p, ok := t.pending[id]
	t.s.mu.Unlock()
	if !ok {
		return hp.ErrRequestNotFound
	}
	var result any
	if p.method == "item/tool/requestUserInput" {
		answers := map[string]any{}
		for k, v := range response.Answers {
			if len(v) > 0 {
				answers[k] = map[string]any{"answers": v}
			}
		}
		result = map[string]any{"answers": answers}
	} else {
		switch response.OptionID {
		case "accept", "acceptForSession", "decline", "cancel":
		default:
			return hp.ErrUnsupported
		}
		result = map[string]any{"decision": response.OptionID}
	}
	t.s.mu.Lock()
	if _, exists := t.pending[id]; !exists {
		t.s.mu.Unlock()
		return hp.ErrRequestNotFound
	}
	delete(t.pending, id)
	t.s.mu.Unlock()
	err := t.s.client.respond(p.id, result)
	if err != nil {
		t.s.logger.Warn("codex server request response failed", "thread_id", t.s.threadID, "turn_id", t.id, "request_id", id)
	} else {
		t.s.logger.Debug("codex server request answered", "thread_id", t.s.threadID, "turn_id", t.id, "request_id", id, "method", p.method)
	}
	return err
}
func (t *turn) Interrupt(ctx context.Context) error {
	t.s.mu.Lock()
	active := t.s.active == t
	threadID := t.s.threadID
	t.s.mu.Unlock()
	if !active {
		t.s.logger.Debug("codex interrupt skipped", "turn_id", t.id, "reason", "inactive")
		return nil
	}
	t.s.logger.Debug("codex interrupt requested", "thread_id", threadID, "turn_id", t.id)
	_, err := t.s.client.request(ctx, "turn/interrupt", map[string]any{"threadId": threadID, "turnId": t.id})
	if err != nil {
		t.s.logger.Warn("codex interrupt failed", "thread_id", threadID, "turn_id", t.id, "error_kind", failureKind(err))
	} else {
		t.s.logger.Debug("codex interrupt acknowledged", "thread_id", threadID, "turn_id", t.id)
	}
	return err
}
