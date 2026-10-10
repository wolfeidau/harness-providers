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
	Open(ctx context.Context, in OpenRequest) (Session, error)
}

type OpenRequest struct {
	WorkingDirectory string
	Resume           *Cursor // nil creates a new native conversation
}

// Cursor is durable, opaque provider-owned resume data. It contains no secrets.
type Cursor struct {
	Provider string          `json:"provider"`
	Version  int             `json:"version"`
	Data     json.RawMessage `json:"data"`
}

type Session interface {
	Cursor() Cursor
	StartTurn(ctx context.Context, in TurnInput) (Turn, error)
	Close() error
}

type TurnInput struct {
	Parts  []InputPart
	Model  string // empty uses the provider default
	Effort string // empty uses the provider default; unsupported values fail
	Policy ExecutionPolicy
}

type InputPart struct {
	Kind InputKind
	Text string
	Path string // absolute path for local_image
}

type InputKind string

const (
	InputText       InputKind = "text"
	InputLocalImage InputKind = "local_image"
)

// Zero values mean provider defaults; adapters must reject unsupported non-zero settings rather than weaken them.
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

// Next yields one terminal event, then io.EOF; transport failures return an error.
type Turn interface {
	ID() string
	Next(ctx context.Context) (Event, error)
	Respond(ctx context.Context, requestID string, response Response) error
	Interrupt(ctx context.Context) error
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

// Fields are populated per Kind; consumers must tolerate unknown kinds.
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

type Item struct {
	Kind   string // descriptive only (e.g. command, file_change); must not drive authorization
	Label  string
	Status ItemStatus
}

type ItemStatus string

const (
	ItemRunning     ItemStatus = "running"
	ItemCompleted   ItemStatus = "completed"
	ItemFailed      ItemStatus = "failed"
	ItemDeclined    ItemStatus = "declined"
	ItemInterrupted ItemStatus = "interrupted"
)

type Usage struct {
	InputTokens  int64
	OutputTokens int64
}

type Outcome struct {
	Status OutcomeStatus
	Error  string // human-readable detail when failed
}

type OutcomeStatus string

const (
	OutcomeCompleted   OutcomeStatus = "completed"
	OutcomeInterrupted OutcomeStatus = "interrupted"
	OutcomeFailed      OutcomeStatus = "failed"
)

type Request struct {
	ID        string
	Kind      RequestKind
	ItemID    string    // the item this request concerns, if any
	Approval  *Approval // set when Kind is RequestApproval
	Options   []Option
	Questions []Question
}

type Approval struct {
	Action    ActionKind
	Command   string // exact command to run; never redacted or truncated so the approver sees what executes
	Cwd       string
	Paths     []string // files the change touches, when known
	WriteRoot string   // broader write access requested, if any
	Reason    string
}

type ActionKind string

const (
	ActionCommand    ActionKind = "command"
	ActionFileChange ActionKind = "file_change"
)

type RequestKind string

const (
	RequestApproval  RequestKind = "approval"
	RequestUserInput RequestKind = "user_input"
)

type Option struct {
	ID          string
	Label       string
	Description string
}

type Question struct {
	ID            string
	Header        string
	Prompt        string
	Options       []Option
	AllowFreeText bool
	Multiple      bool
}

type Response struct {
	OptionID string              // approval choice, if applicable
	Answers  map[string][]string // question ID -> answers
}
