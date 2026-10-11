# Harness providers: specification

## Purpose and scope

This module is a Go boundary for interactive coding harnesses. A caller owns conversation records, durable event delivery, worker placement, and workspace lifecycle; a provider owns its native process or connection, protocol, and event translation. Transports never appear in the public API.

The contract covers starting or resuming a native conversation, running one turn at a time, streaming its events, answering requests, interrupting, and closing the runtime. It does not provide a server, database, scheduler, worktree manager, or recovery of an in-flight turn.

| Provider | Package | Status |
|---|---|---|
| Codex (`codex app-server`) | `codex` | Implemented |
| Claude Code (`claude -p` stream-json) | `claude` | Implemented |

## Public contract

The contract lives in [`provider.go`](provider.go) (`package harnessproviders`), which is the source of truth for type names and fields. In outline: a `Provider` opens a `Session` from an `OpenRequest` (workspace plus optional `Cursor`); a `Session` starts at most one `Turn` at a time from a `TurnInput` (parts, model, effort, `ExecutionPolicy`); a `Turn` yields `Event`s and accepts `Respond` and `Interrupt`.

String enumerations are closed for inputs; adapters validate them before any native call and return `ErrUnsupported` for values they cannot honour rather than weakening a requested policy. Consumers must tolerate unknown future `EventKind` values. `Item.Kind` is descriptive and never drives authorization. Approval decisions come only from the chosen `Option` (accept, decline, cancel); the `Approval` display fields never decide anything. A `user_input` request is answered with an empty `OptionID` and `Answers` keyed by `Question.ID`; an unknown question ID or no non-empty answer returns `ErrUnsupported` and leaves the request pending. Its `Options` offer refusing instead of answering: cancel always (it also interrupts the turn), decline where the harness supports it. Approvals are single-action: no session-wide grants or policy amendments.

`Approval.Action` is one of `command`, `file_change`, `write_stdin`, or `tool`. `tool` covers any other harness tool (web fetch, MCP, subagents); `Approval.Tool` names it and `Command` carries its full input.

### Contract semantics

1. **Provider identity.** `Name` is stable. `Open` rejects a cursor for another provider or an unsupported version with `ErrInvalidCursor`. Executable, home, credentials, and environment are set at construction and never serialized into `Cursor`.
2. **Create and resume.** `Open` without a cursor creates a native conversation; with a cursor it resumes that exact conversation or returns `ErrSessionNotFound`. It never creates a new conversation under an old cursor. `Cursor()` returns a safe copy at any point after `Open`.
3. **Workspace.** `WorkingDirectory` must exist before `Open` and remains the session's workspace. The caller keeps workspace and provider home across processes.
4. **Turn ownership.** At most one turn runs per session; a second `StartTurn` returns `ErrBusy`. Empty input returns `ErrUnsupported`. `StartTurn` returns after native admission; its context controls admission only, not the turn lifetime.
5. **Events.** `Next` returns events in observed order. Native notifications that arrive before admission completes are buffered and correlated. `EventTurnFinished` occurs exactly once, followed by `io.EOF`. Delivery is live and in memory; the caller persists what it needs to replay. A process exit or protocol error unblocks `Next` with an error and leaves the outcome uncertain unless a terminal event was already observed.
6. **Interactive requests.** `EventRequest` carries a stable request ID and the options or questions needed to answer it. `Respond` on a stale ID returns `ErrRequestNotFound`. Pending requests do not survive process loss. Adapters never wait for a response on the goroutine reading the protocol.
7. **Cancellation.** Cancelling `Next(ctx)` stops that wait only. `Interrupt` asks the harness to stop the turn; it is idempotent, and completion is confirmed only by a terminal event or transport failure. `Close` is idempotent, terminates the owned process, releases pending requests, and unblocks readers. Closing during a turn is an interruption, not a suspension. After `Close`, `Next`, `Respond`, `Interrupt`, and `StartTurn` return errors matching `ErrClosed`; a process crash is reported as its own error, not `ErrClosed`.
8. **Errors and retries.** Sentinels (`errors.Is`): `ErrInvalidCursor`, `ErrSessionNotFound`, `ErrUnsupported`, `ErrBusy`, `ErrClosed`, `ErrRequestNotFound`. Native errors are wrapped with operation context. A transport failure ends the turn without leaving the session busy: Codex then returns `ErrClosed` from `StartTurn`, and Claude respawns with `--resume`. A start whose admission is uncertain is never retried; the adapter closes the session and the caller reconciles before submitting another prompt.
9. **Concurrency.** One goroutine may call `Next` while others call `Respond`, `Interrupt`, or `Close`. Concurrent `Next` calls are invalid. `Cursor` is safe concurrently. Adapters drain native stdout/stderr and bound event buffers; overflow fails the transport rather than growing without limit or deadlocking.

## Codex provider (implemented)

Built on the [app-server protocol](https://developers.openai.com/codex/app-server/): JSON-RPC over JSONL stdio. The [T3 Codex runtime](https://github.com/pingdotgg/t3code/blob/6530de0339d2ca49957d0039133c49e3a08557f7/apps/server/src/provider/Layers/CodexSessionRuntime.ts) informed the session/turn split.

- **Configuration.** `codex.Config{Binary, Args, Home, Env, Logger}`. `Home` sets `CODEX_HOME`; otherwise the child inherits the ambient environment.
- **Process.** One `codex app-server` child per session, in its own process group. `Open` completes `initialize`/`initialized` before any thread call. `Close` closes stdin, waits a short grace period, then kills the process group so commands Codex spawned do not outlive it.
- **Cursor.** Version 1 holds only `{threadId}`. `Open` uses `thread/start` or `thread/resume` and rejects a resume that returns a different thread ID. Codex has no error code for missing threads, so `ErrSessionNotFound` is detected by message text.
- **Turns.** `turn/start` maps text, absolute local image paths, model, effort (`none`…`xhigh`), and policy. A JSON-RPC rejection leaves the session usable; any other failure means admission is uncertain and closes the session. Notifications that arrive before the start response are buffered (bounded) and replayed once the turn ID is known.
- **Events.** Agent message deltas, item lifecycle (`inProgress`/`completed`/`failed`/`declined`/`interrupted`), last-turn token usage, and `turn/completed`. When an agent message produced no deltas, its completed snapshot is emitted as one text event. A full event buffer fails the transport.
- **Policy.**

  | Contract | Codex |
  |---|---|
  | `ApprovalOnRequest` / `ApprovalNever` | `approvalPolicy: on-request` / `never` |
  | `SandboxReadOnly` | `readOnly`, no network |
  | `SandboxWorkspaceWrite` | `workspaceWrite`, no extra roots, no network |
  | `SandboxUnrestricted` | `dangerFullAccess` |

- **Requests.** Command approvals (`command`, `writeStdin`, with network host/protocol when present), file-change approvals (paths remembered from item events, plus any grant root), and `requestUserInput` questions. Approval options are accept, decline, cancel. User input offers only cancel, because the protocol has no decline: it answers with empty `answers` and then sends `turn/interrupt`. Unknown approval kinds and unknown server requests get a protocol error rather than a silent accept.
- **Diagnostics.** Stderr is drained continuously; at debug level it is logged bounded, ANSI-stripped, and with values of secret-looking environment variables redacted. Failed command output is logged the same way.

### Verification

Deterministic tests use a fake app-server (the test binary re-executed as a peer) and cover framing, early notifications, approvals, interruption, structured requests, child exit, missing sessions, uncertain starts, process-group kill, and log redaction. They run in `go test ./...`.

Live tests sit behind the `integration` tag and `HARNESS_LIVE_TEST=1`, use `gpt-6-luna` at `low` effort, and require `HARNESS_TEST_CODEX_HOME` or `CODEX_ACCESS_TOKEN`; the default Codex home is never used. Once opted in, missing binary, auth, or model is a failure.

- `TestCodexReviewE2E`: read-only review of a fixture with an inverted authorization check, then close, round-trip the cursor through JSON, resume in a new process, and recall a marker from the first turn.
- `TestCodexApprovalE2E`: an escalated `touch` under `on_request` is declined and the file must not exist.

```sh
HARNESS_TEST_CODEX_HOME=/path/to/test-codex-home HARNESS_LIVE_TEST=1 \
  go test -tags=integration ./codex/... -run 'E2E$' -count=1 -timeout=3m
```

## Claude Code provider (implemented)

Drives the `claude` CLI directly over its stream-json stdio protocol, the same protocol the Claude Agent SDK uses internally. The [T3 Claude adapter](https://github.com/pingdotgg/t3code) (`ClaudeAdapterV2.ts`) is the reference integration. Frame shapes below were captured from `claude` 2.1.296; sanitized captures live in `claude/testdata/` (`>>` lines are sent, `<<` lines received).

- **Configuration.** `claude.Config{Binary, Args, ConfigDir, Env, Logger}`. `ConfigDir` sets `CLAUDE_CONFIG_DIR`; otherwise the child inherits the ambient environment. The child always gets `CLAUDE_BASH_MAINTAIN_PROJECT_WORKING_DIR=1` so each Bash call starts in the workspace; a `cd` does not carry into the next call.
- **Process.** One child per session, in its own process group, started in the workspace as `claude -p --input-format stream-json --output-format stream-json --verbose --include-partial-messages --replay-user-messages --permission-prompt-tool stdio` plus session, model, effort, and policy flags. Without `--permission-prompt-tool stdio` the CLI denies prompts itself instead of sending `can_use_tool`. A `control_request` with subtype `initialize` precedes the first user message. A new session spawns its child on the first turn, not in `Open`. Launch flags are fixed for the life of a process, so a turn whose model, effort, or policy differs from the running process, or whose previous child has died, closes it and respawns with `--resume`. Close and process-group kill follow Codex.
- **Control frames.** Adapter-initiated requests use adapter-generated `request_id`s and are matched to `control_response`. The CLI echoes the adapter's own `control_response` frames back on stdout; any response whose ID is not outstanding is ignored. Unknown control request subtypes get an error response. Lines that are not valid frames are logged at debug level and skipped rather than failing the transport, because the protocol is unpublished and changes between CLI versions.
- **Cursor.** Version 1 holds only `{sessionId}`. A new session generates a UUID and launches with `--session-id`; resume and any respawn after the first turn use `--resume`. `system/init` arrives with the first turn of each process, so identity is checked there: a `session_id` other than the requested one (the CLI copies a session that is already running) fails the turn and closes the session. A missing session makes the CLI exit before answering `initialize`, with a `result` whose `errors` contain `No conversation found with session ID`; `Open` returns `ErrSessionNotFound`. A session closed before its first turn has no native state, so its cursor also returns `ErrSessionNotFound`. A `--session-id` already on disk fails with `already in use`, reported as a wrapped open error.
- **Turns.** The adapter generates a fresh UUID per turn, used as the turn ID and the user message `uuid`; the CLI silently completes a reused UUID without running it. Admission is the `command_lifecycle` frame with `state: queued` and `command_uuid` equal to the turn ID; frames before it belong to the previous turn and are dropped; a write failure or no admission before the context ends closes the session. `result.user_message_uuid` correlates the terminal frame. Text parts become text blocks; local images become base64 image blocks (png, jpeg, gif, webp; other types are `ErrUnsupported`). Effort accepts `low`, `medium`, `high`, `xhigh`, `max`.
- **Events.**

  | Claude frame | Event |
  |---|---|
  | `command_lifecycle` `queued` | `turn_started` |
  | `stream_event` `content_block_delta` with `text_delta` | `text_delta`, `ItemID` = `api_message_id` |
  | `assistant` `tool_use` block | `item_started`; kind `command` (Bash), `file_change` (Edit, MultiEdit, Write, NotebookEdit), else `tool` |
  | `user` `tool_result` block | `item_finished`: `declined` when `tool_result_meta.permission_decision.decision` is `reject`, `failed` on `is_error`, else `completed` |
  | `result` | `usage` (input = input + cache creation + cache read tokens), then `turn_finished` |

  Terminal status: `terminal_reason` `aborted_tools` or `aborted_streaming` → interrupted; `is_error` → failed, even when `subtype` is `success` (an authentication failure reports `success` with `is_error: true`); otherwise completed. Failure detail comes from `errors`, skipping `[ede_diagnostic]` entries, else `result`. An assistant message with no streamed deltas is emitted from its snapshot, as with Codex. Other frames (`system` status, `rate_limit_event`, thinking) are ignored, as are `stream_event`, `assistant`, and `user` frames with a non-null `parent_tool_use_id`, which belong to subagents; subagent `can_use_tool` requests still surface.
- **Requests.** A `control_request` with subtype `can_use_tool` (`tool_name`, `input`, `tool_use_id`) becomes `EventRequest` with `ItemID` = `tool_use_id`:
  - Bash → `command` with `input.command`, workspace as cwd (accurate because Bash is pinned to the workspace), and `input.description` as reason.
  - Edit, MultiEdit, Write, NotebookEdit → `file_change` with `file_path` or `notebook_path`.
  - `AskUserQuestion` → `user_input` with options decline and cancel; `Question.ID` is the index (`q0`, `q1`, …) so repeated question text stays distinct, and `Multiple` follows `multiSelect`. A response with no `OptionID` returns `allow` with `updatedInput` = the original input plus `answers: {questionText: "a, b"}`; answers to repeated text are joined. Decline and cancel deny as for approvals.
  - Any other tool → `tool` with the tool name and its full input as JSON; it is never truncated, so the approver sees everything that runs.

  Accept returns `{behavior: allow, updatedInput: <original input>}`; decline returns `{behavior: deny, message}`; cancel adds `interrupt: true`. `permission_suggestions` are never applied (single-action approvals). A `control_cancel_request` (sent when a turn is interrupted with a request pending) drops it, so a later `Respond` returns `ErrRequestNotFound`; requests still pending when the turn finishes are dropped the same way.
- **Interrupt.** `control_request` with subtype `interrupt`; the CLI acknowledges it and the outcome still arrives as `result`.
- **Policy.** Claude has no per-turn sandbox, so policy maps to launch flags:

  | Sandbox \ Approval | default / `on_request` | `never` |
  |---|---|---|
  | default | no mode flag (CLI and user settings decide), prompts to host | `--permission-prompts none` |
  | `read_only` | `--tools Read,Glob,Grep --strict-mcp-config --setting-sources user`, mode `dontAsk` | same |
  | `workspace_write` | mode `acceptEdits`, `--settings` Bash sandbox, prompts to host | same, `--permission-prompts none` |
  | `unrestricted` | no mode flag, prompts to host | mode `bypassPermissions` |

  `--tools` removes Write, Edit, and Bash from the session entirely; `--strict-mcp-config` is required because account-level claude.ai connectors otherwise remain available; `--setting-sources user` stops the workspace's own `.claude/settings*.json` from loading, because hooks defined there run commands regardless of tool restrictions (verified: a workspace `UserPromptSubmit` hook ran under the other read-only flags). Other modes load project settings, so only use them on trusted workspaces. `--permission-prompts none` denies prompts even with the stdio prompt tool, and `bypassPermissions` needs no extra opt-in flag (both verified against 2.1.296). The Bash sandbox settings are `{"sandbox":{"enabled":true,"autoAllowBashIfSandboxed":true,"allowUnsandboxedCommands":false}}`; captured runs confirm writes inside the workspace succeed and writes to its parent fail with `Operation not permitted`. Like Codex `workspaceWrite`, the system temp directory stays writable.

### Verification

Deterministic tests mirror the Codex suite with a fake `claude` peer replaying the `claude/testdata` frames, plus the policy-to-flags table and respawn on settings change. Live tests use the same tag and opt-in, `claude-haiku-5-5` at `low` effort, and credentials from `HARNESS_TEST_CLAUDE_CONFIG_DIR` or `ANTHROPIC_API_KEY`/`CLAUDE_CODE_OAUTH_TOKEN` with a temporary config directory; the default `~/.claude` is never used. `TestClaudeReviewE2E` and `TestClaudeApprovalE2E` match the Codex scenarios; the review resume also exercises a policy respawn with `--resume`.

```sh
HARNESS_TEST_CLAUDE_CONFIG_DIR=/path/to/test-claude-config HARNESS_LIVE_TEST=1 \
  go test -tags=integration ./claude/... -run 'E2E$' -count=1 -timeout=3m
```

## Extension rules

- Add a provider only when it meets the create/resume and terminal-event rules. Return `ErrUnsupported` for inputs a harness cannot honour.
- Provider configuration and protocol fields stay in the provider package. Add a normalized event field only when multiple consumers need it.
- Optional operations (history, rollback, compaction, model discovery) belong in separate small interfaces, not in every provider.
- No orchestration database or HTTP API in this module. A higher layer persists `Cursor` and events, maps logical threads to cursors, and routes requests to the worker that owns the live `Turn`.
