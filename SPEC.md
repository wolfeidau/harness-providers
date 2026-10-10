# Harness providers: Go interface specification

## Purpose and scope

This module provides a Go boundary for interactive coding harnesses. The first implementation targets Codex through `codex app-server`. A caller owns conversation records, durable event delivery, worker placement, and workspace lifecycle; a provider owns its native process or connection, protocol, and event translation. The same boundary should later accommodate Claude's SDK, ACP providers, and OpenCode's HTTP server without making their transports part of the public API.

Version 1 covers starting or resuming a native conversation, running one turn at a time, streaming its events, answering requests, interrupting, and closing the runtime. It does not provide a server, database, scheduler, worktree manager, or exact mid-turn recovery.

## Design basis

| Basis | Evidence | Decision for this module |
|---|---|---|
| Codex protocol | [OpenAI app-server documentation](https://developers.openai.com/codex/app-server/) specifies `initialize`/`initialized`, `thread/start`/`thread/resume`, `turn/start`, `turn/interrupt`, notifications, and server-initiated requests. | Implement Codex behind the provider interface; keep JSONL, RPC IDs, and native request payloads private. |
| Codex resume | [OpenAI's generated `ThreadResumeParams`](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/schema/typescript/v2/ThreadResumeParams.ts) prefers a saved thread ID for disk-backed resume. | Return an opaque, versioned cursor as soon as `Open` succeeds. A resume request must preserve identity or return an error. |
| Existing integration | [T3 Codex runtime](https://github.com/pingdotgg/t3code/blob/6530de0339d2ca49957d0039133c49e3a08557f7/apps/server/src/provider/Layers/CodexSessionRuntime.ts) launches app-server, opens a thread, starts turns, and translates notifications; [its adapter contract](https://github.com/pingdotgg/t3code/blob/6530de0339d2ca49957d0039133c49e3a08557f7/apps/server/src/provider/Services/ProviderAdapter.ts) separates provider behavior from orchestration. | Retain the session/turn distinction, but keep orchestration persistence and client delivery outside this library. These links are implementation examples, not this module's API contract. |
| Other transports | [T3's ACP runtime](https://github.com/pingdotgg/t3code/blob/6530de0339d2ca49957d0039133c49e3a08557f7/apps/server/src/provider/acp/AcpSessionRuntime.ts) and [OpenCode adapter](https://github.com/pingdotgg/t3code/blob/6530de0339d2ca49957d0039133c49e3a08557f7/apps/server/src/provider/Layers/OpenCodeAdapter.ts) use different resume and streaming mechanisms. | Do not expose a common child-process or JSON-RPC interface. Each adapter maps its native operations to the same session and turn contract. |
| Go conventions | [Go's `context` package](https://pkg.go.dev/context) defines request cancellation; [Effective Go](https://go.dev/doc/effective_go#interfaces_and_types) favors small behavioral interfaces. | Use `context.Context` for each blocking call and keep the required interfaces small. Context cancellation of an event read is distinct from interrupting native work. |

The following contract is a proposal for this repository. Its names and failure rules are project decisions, not claims about native harness guarantees.

## Public contract

The intended package is the module root (`package harnessproviders`). Types below are the initial API shape; the first implementation should compile against it before adding optional operations.

```go
package harnessproviders

import (
	"context"
	"encoding/json"
)

// Provider is configured for one native harness installation and identity.
// Its implementation owns executable, home, credentials, and process settings.
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

// Session is one live connection to a native conversation.
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
	Kind InputKind // text or local_image in v1
	Text string
	Path string // absolute path for local_image
}

type InputKind string

const (
	InputText       InputKind = "text"
	InputLocalImage InputKind = "local_image"
)

// Zero values mean provider defaults. An adapter must reject unsupported
// nonzero settings rather than silently weaken a requested policy.
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

// A Turn exists after the native harness admits turn/start (or its equivalent).
// Next yields one terminal event, then io.EOF. A transport failure returns an
// error instead; it must not be reported as a successful terminal event.
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

// Fields are populated according to Kind. Native detail stays in adapters;
// consumers must tolerate event kinds added in later versions.
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
	Kind   string // e.g. command, file_change, tool_call, reasoning
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
	Answers  map[string][]string // question ID -> selected/free-text answers
}
```

The string enumerations above are closed for inputs in v1. Implementations should define named constants for their allowed values and validate them before making a native call. Event consumers must handle unknown future `EventKind` values so adding a normalized event does not break older clients. `Item.Kind` is descriptive and must not drive authorization; approval decisions use `Request.Options` and `Respond`, and come only from the chosen `Option`, never from `Approval` display fields. Approvals are single-action only in v1: options are accept, decline and cancel, with no session-wide grants or policy amendments.

### Contract semantics

1. **Provider identity.** `Name` is stable (initially `codex`). `Open` rejects a cursor with another provider name or unsupported version. A provider instance's executable, home, credentials, and environment are set when it is constructed; they are not serialized into `Cursor`.
2. **Create and resume.** `Open` with no cursor creates a native conversation. `Open` with a cursor resumes that exact conversation. If native state is absent, return a distinguishable `ErrSessionNotFound`; never create a new conversation under the old cursor. The caller decides whether to start fresh. `Cursor()` returns a safe copy and can be called after `Open`, after `StartTurn`, and after a terminal event. Its provider-native payload may change when a future harness forks or rekeys a session.
3. **Workspace.** `WorkingDirectory` must exist before `Open` and remains the session's workspace. The caller maintains its files and provider home across processes. A changed workspace requires a new `Open`; an adapter may reject resume if its native harness cannot preserve conversation identity there.
4. **Turn ownership.** At most one turn runs per session. A second `StartTurn` returns `ErrBusy`. An empty prompt is permitted only if the native harness explicitly supports continuation without user input; otherwise return `ErrUnsupported`. `StartTurn` returns after native admission, not after generation completes. Its context controls admission; it does not own the turn lifetime after return.
5. **Events.** `Next` returns events in observed order for that turn. The adapter correlates early native notifications that arrive before the start response. `EventTurnFinished` occurs exactly once when the harness reports a terminal outcome, followed by `io.EOF`. Event delivery is live and in memory; the caller must persist any events it needs to replay. A process exit or protocol error unblocks `Next` with an error and leaves the outcome uncertain unless a terminal event was already observed.
6. **Interactive requests.** `EventRequest` supplies a stable request ID and the choices/questions needed to answer it. The caller may persist or forward the request, then call `Respond` on the same live turn. A response to a stale ID returns `ErrRequestNotFound`. Pending requests are not resumable after a process dies. The adapter must not wait for a response on the same goroutine needed to read subsequent protocol messages.
7. **Cancellation.** Canceling `Next(ctx)` stops that wait only. `Interrupt` asks the harness to stop the active turn; it is idempotent while the turn is active, but completion is confirmed only by a terminal event or a transport failure. `Close` is idempotent, terminates the owned connection/process, releases pending requests, and unblocks readers. Closing during a turn is an interruption, not a durable suspension.
8. **Errors and retries.** Expose sentinel errors compatible with `errors.Is`: `ErrInvalidCursor`, `ErrSessionNotFound`, `ErrUnsupported`, `ErrBusy`, `ErrClosed`, and `ErrRequestNotFound`. Wrap protocol/process errors with operation and provider context. A timeout after `StartTurn` was sent may mean the native turn was accepted: the library must not automatically retry a non-idempotent start. A caller must reconcile uncertain work before submitting another prompt.
9. **Concurrency.** One goroutine may call `Next` while another calls `Respond`, `Interrupt`, or `Close`. Concurrent `Next` calls and concurrent `StartTurn` calls are invalid; the latter returns `ErrBusy`. `Cursor` is safe to read concurrently. Adapters must drain native stdout/stderr and bound or backpressure event buffers; a slow caller must not deadlock protocol responses or leak an unbounded queue.

## Codex implementation sequence

1. **Protocol client:** Start `codex app-server` with configured binary, arguments, environment, effective `CODEX_HOME`, and workspace. Implement JSONL request/response correlation, notification dispatch, server requests, stderr draining, and process-exit propagation. Complete `initialize`/`initialized` before thread calls.
2. **Session:** `Open` uses `thread/start` when `Resume == nil` and `thread/resume` otherwise. Store only the returned Codex thread ID in a version 1 `codex` cursor. Do not copy credentials or native transcript contents into the cursor. Verify resume preserves the requested ID. Close the child when `Session.Close` is called.
3. **Turn:** Map text/local images, model, effort, and execution policy to `turn/start`. Return the Codex turn ID. Normalize text deltas, item lifecycle, usage, approval/user-input requests, and `turn/completed` into events. Correlate native request IDs for `Respond`; use `turn/interrupt` for `Interrupt`.
4. **Verification:** Use deterministic protocol tests for framing, approvals, interruption, early notifications, child exit, and missing sessions. Add the opt-in live review test below for actual model execution and resume across process boundaries.

The first implementation may keep one app-server child per active session. The caller may close an idle session and later reopen it from the cursor. No API here promises resuming an in-flight turn after that close.

## Integration testing plan

This section defines a test to implement with the Codex adapter; it is not runnable while this repository has only the spec. Keep deterministic protocol tests in the default `go test ./...` path. Put the live test behind an `integration` build tag and `HARNESS_LIVE_TEST=1` so ordinary local and pull-request runs make no model calls.

### Live review scenario

1. **Preflight:** Require an installed `codex` binary and dedicated test authentication. CI can provide `CODEX_ACCESS_TOKEN` to a temporary `CODEX_HOME`; OpenAI documents this for trusted app-server automation, but token creation currently requires a Business or Enterprise workspace. A local developer can supply a separately signed-in `HARNESS_TEST_CODEX_HOME`. Never use the developer's default Codex home implicitly. Give the test a fresh temporary workspace. [Authentication](https://learn.chatgpt.com/docs/enterprise/access-tokens), [Codex home](https://learn.chatgpt.com/docs/config-file/config-advanced#config-and-state-locations).
2. **Check the model:** Query app-server `model/list` through a Codex-specific test helper before creating a session, including hidden entries and following result pages. Require `gpt-6-luna` with `low` in `supportedReasoningEfforts`; fail with a clear availability message if absent. Do not silently select another model or effort. The public model catalog lists Luna and low, while app-server availability depends on the account and client. [Model catalog](https://developers.openai.com/api/docs/models), [app-server model discovery](https://learn.chatgpt.com/docs/app-server).
3. **Make a tiny fixture:** Write `access.go` in the temporary workspace with a `CanDelete(actor, owner string) bool` function returning `actor != owner`. The deliberate defect is that a non-owner is allowed and the owner is denied. Use a read-only sandbox and no approval prompts; the test should fail if any request to mutate the workspace appears.
4. **Run the review:** Call the public `Provider.Open` and `Session.StartTurn` interfaces with `Model: "gpt-6-luna"`, `Effort: "low"`, and a short prompt asking for a review of `access.go` without edits. Include a random marker in the user message, solely for the continuation check. Consume `Turn.Next` through `EventTurnFinished`; assert a nonempty Codex turn ID, at least one text event, a `completed` outcome, and a review that identifies `CanDelete`, the inverted comparison, and unauthorized deletion. Match these facts with a small set of tolerant checks, not exact prose or line numbers. Record the cursor after completion. This is a normal review prompt through `turn/start`; native `review/start` is a separate optional operation outside the v1 interface. [Codex turn and review methods](https://learn.chatgpt.com/docs/app-server).
5. **Prove process-boundary resume:** Close the session, serialize/deserialize the cursor, and call `Open` again with the same workspace and Codex home. Assert the native Codex thread ID is unchanged. Start one short follow-up turn asking for the marker from the earlier user message; require that exact marker in the answer and a completed outcome. Close the second session. This uses two model turns per run and exercises the durable cursor and native history, not only transport reconnect.
6. **Bound and diagnose:** Use a 75-second deadline per turn and a 3-minute test timeout. Cap captured output at 8 KiB, interrupt a turn that exceeds its deadline or output allowance, and use a fresh short context to await its terminal event. Log only bounded event summaries, the selected model/effort, and sanitized failure detail; never log credentials or raw auth-bearing protocol frames. A deadline limits runtime, though it is not a hard token-spend limit.

Once implemented, the intended command is:

```sh
HARNESS_LIVE_TEST=1 go test -tags=integration ./... -run '^TestCodexReviewE2E$' -count=1 -timeout=3m
```

When the integration tag is present without `HARNESS_LIVE_TEST=1`, the live test skips. Once opted in, missing auth, binary, or model support is a failure, not a green skip. Run it on demand or on a trusted scheduled CI runner; keep deterministic tests as the pull-request gate. [OpenAI's model guidance](https://developers.openai.com/api/docs/guides/model-selection) recommends Luna at low effort for focused, cost-conscious work; this test's suitability still needs measurement from its own runs.

## Extension rules

- Add a provider implementation only when it can meet the create/resume and terminal-event rules. If a harness lacks a feature such as local images, model switching, or a policy value, return `ErrUnsupported` for that input.
- Provider-specific configuration belongs in its constructor or provider package. Provider-specific protocol fields stay there. Add a normalized event field only when multiple consumers need its meaning.
- Optional operations such as history read, rollback, compaction, or model discovery should use separate small interfaces when implemented. They are not required of every provider.
- Do not add an orchestration database or HTTP API to this package. A higher layer persists `Cursor` and events, maps its logical thread ID to the native cursor, and routes requests to the worker that owns the live `Turn`.
