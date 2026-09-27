package harnessproviders

import (
	"context"
	"encoding/json"
	"errors"
)

var (
	ErrInvalidCursor   = errors.New("invalid cursor")
	ErrSessionNotFound = errors.New("session not found")
	ErrUnsupported     = errors.New("unsupported")
	ErrBusy            = errors.New("session busy")
	ErrClosed          = errors.New("session closed")
	ErrRequestNotFound = errors.New("request not found")
)

type Provider interface {
	Name() string
	Open(context.Context, OpenRequest) (Session, error)
}
type OpenRequest struct {
	WorkingDirectory string
	Resume           *Cursor
}
type Cursor struct {
	Provider string          `json:"provider"`
	Version  int             `json:"version"`
	Data     json.RawMessage `json:"data"`
}
type Session interface {
	Cursor() Cursor
	StartTurn(context.Context, TurnInput) (Turn, error)
	Close() error
}
type TurnInput struct {
	Parts  []InputPart
	Model  string
	Effort string
	Policy ExecutionPolicy
}
type InputPart struct {
	Kind InputKind
	Text string
	Path string
}
type InputKind string

const (
	InputText       InputKind = "text"
	InputLocalImage InputKind = "local_image"
)

type ExecutionPolicy struct {
	Approval ApprovalMode
	Sandbox  SandboxMode
}
type ApprovalMode string

const (
	ApprovalDefault   ApprovalMode = ""
	ApprovalOnRequest ApprovalMode = "on_request"
	ApprovalNever     ApprovalMode = "never"
)

type SandboxMode string

const (
	SandboxDefault        SandboxMode = ""
	SandboxReadOnly       SandboxMode = "read_only"
	SandboxWorkspaceWrite SandboxMode = "workspace_write"
	SandboxUnrestricted   SandboxMode = "unrestricted"
)

type Turn interface {
	ID() string
	Next(context.Context) (Event, error)
	Respond(context.Context, string, Response) error
	Interrupt(context.Context) error
}
type EventKind string

const (
	EventTurnStarted  EventKind = "turn_started"
	EventTextDelta    EventKind = "text_delta"
	EventItemStarted  EventKind = "item_started"
	EventItemUpdated  EventKind = "item_updated"
	EventItemFinished EventKind = "item_finished"
	EventRequest      EventKind = "request"
	EventUsage        EventKind = "usage"
	EventTurnFinished EventKind = "turn_finished"
)

type Event struct {
	Kind    EventKind
	TurnID  string
	ItemID  string
	Text    string
	Item    *Item
	Request *Request
	Usage   *Usage
	Outcome *Outcome
}
type Item struct{ Kind, Label, Status string }
type Usage struct{ InputTokens, OutputTokens int64 }
type Outcome struct {
	Status OutcomeStatus
	Error  string
}
type OutcomeStatus string

const (
	OutcomeCompleted   OutcomeStatus = "completed"
	OutcomeInterrupted OutcomeStatus = "interrupted"
	OutcomeFailed      OutcomeStatus = "failed"
)

type Request struct {
	ID          string
	Kind        RequestKind
	Title       string
	Description string
	Options     []Option
	Questions   []Question
}
type RequestKind string

const (
	RequestApproval  RequestKind = "approval"
	RequestUserInput RequestKind = "user_input"
)

type Option struct{ ID, Label string }
type Question struct {
	ID            string
	Prompt        string
	Options       []Option
	AllowFreeText bool
	Multiple      bool
}
type Response struct {
	OptionID string
	Answers  map[string][]string
}
