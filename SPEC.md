# Harness providers: specification

## Purpose and scope

This module is a Go boundary for interactive coding harnesses. A caller owns conversation records, durable event delivery, worker placement, and workspace lifecycle; a provider owns its native process or connection, protocol, and event translation. Transports never appear in the public API.

The contract covers starting or resuming a native conversation, running one turn at a time, streaming its events, answering requests, interrupting, and closing the runtime. It does not provide a server, database, scheduler, worktree manager, or recovery of an in-flight turn.

| Provider | Package | Status |
|---|---|---|
| Codex (`codex app-server`) | `codex` | Implemented |
| Claude Code (`claude -p` stream-json) | `claude` | Planned |

## Public contract

The contract lives in [`provider.go`](provider.go) (`package harnessproviders`), which is the source of truth for type names and fields. In outline: a `Provider` opens a `Session` from an `OpenRequest` (workspace plus optional `Cursor`); a `Session` starts at most one `Turn` at a time from a `TurnInput` (parts, model, effort, `ExecutionPolicy`); a `Turn` yields `Event`s and accepts `Respond` and `Interrupt`.

String enumerations are closed for inputs; adapters validate them before any native call and return `ErrUnsupported` for values they cannot honour rather than weakening a requested policy. Consumers must tolerate unknown future `EventKind` values. `Item.Kind` is descriptive and never drives authorization. Approval decisions come only from the chosen `Option` (accept, decline, cancel); the `Approval` display fields never decide anything. Approvals are single-action: no session-wide grants or policy amendments.

`Approval.Action` is one of `command`, `file_change`, `write_stdin`, or `tool`. `tool` covers any other harness tool (web fetch, MCP, subagents); `Approval.Tool` names it and `Command` carries a bounded summary of its input.

### Contract semantics

1. **Provider identity.** `Name` is stable. `Open` rejects a cursor for another provider or an unsupported version with `ErrInvalidCursor`. Executable, home, credentials, and environment are set at construction and never serialized into `Cursor`.
2. **Create and resume.** `Open` without a cursor creates a native conversation; with a cursor it resumes that exact conversation or returns `ErrSessionNotFound`. It never creates a new conversation under an old cursor. `Cursor()` returns a safe copy at any point after `Open`.
3. **Workspace.** `WorkingDirectory` must exist before `Open` and remains the session's workspace. The caller keeps workspace and provider home across processes.
4. **Turn ownership.** At most one turn runs per session; a second `StartTurn` returns `ErrBusy`. Empty input returns `ErrUnsupported`. `StartTurn` returns after native admission; its context controls admission only, not the turn lifetime.
5. **Events.** `Next` returns events in observed order. Native notifications that arrive before admission completes are buffered and correlated. `EventTurnFinished` occurs exactly once, followed by `io.EOF`. Delivery is live and in memory; the caller persists what it needs to replay. A process exit or protocol error unblocks `Next` with an error and leaves the outcome uncertain unless a terminal event was already observed.
6. **Interactive requests.** `EventRequest` carries a stable request ID and the options or questions needed to answer it. `Respond` on a stale ID returns `ErrRequestNotFound`. Pending requests do not survive process loss. Adapters never wait for a response on the goroutine reading the protocol.
7. **Cancellation.** Cancelling `Next(ctx)` stops that wait only. `Interrupt` asks the harness to stop the turn; it is idempotent, and completion is confirmed only by a terminal event or transport failure. `Close` is idempotent, terminates the owned process, releases pending requests, and unblocks readers. Closing during a turn is an interruption, not a suspension.
8. **Errors and retries.** Sentinels (`errors.Is`): `ErrInvalidCursor`, `ErrSessionNotFound`, `ErrUnsupported`, `ErrBusy`, `ErrClosed`, `ErrRequestNotFound`. Native errors are wrapped with operation context. A start whose admission is uncertain is never retried; the adapter closes the session and the caller reconciles before submitting another prompt.
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

- **Requests.** Command approvals (`command`, `writeStdin`, with network host/protocol when present), file-change approvals (paths remembered from item events, plus any grant root), and `requestUserInput` questions. Options are accept, decline, cancel. Unknown approval kinds and unknown server requests get a protocol error rather than a silent accept.
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

## Claude Code provider (planned)

Drives the `claude` CLI directly over its stream-json stdio protocol, the same protocol the Claude Agent SDK uses internally. The [T3 Claude adapter](https://github.com/pingdotgg/t3code) (`ClaudeAdapterV2.ts`) is the reference integration. Frame shapes below must be confirmed against a captured session from the installed CLI before implementation; captured frames become test fixtures.

- **Configuration.** `claude.Config{Binary, Args, ConfigDir, Env, Logger}`. `ConfigDir` sets `CLAUDE_CONFIG_DIR`; otherwise the child inherits the ambient environment.
- **Process.** One child per session, in its own process group, started in the workspace as `claude -p --input-format stream-json --output-format stream-json --verbose --include-partial-messages --replay-user-messages` plus session, model, effort, and policy flags. A `control_request` with subtype `initialize` precedes the first user message. Launch flags are fixed for the life of a process, so a turn whose model, effort, or policy differs from the running process closes it and respawns with `--resume`. Close and process-group kill follow Codex.
- **Cursor.** Version 1 holds only `{sessionId}`. A new session generates a UUID and launches with `--session-id`; resume and any respawn after the first turn use `--resume`. The `session_id` in `system/init` must equal the requested ID; a mismatch (the CLI copies a session that is already running) or the CLI's missing-conversation error returns `ErrSessionNotFound`. A cursor whose session never ran a turn has no native state and also returns `ErrSessionNotFound`.
- **Turns.** The adapter generates the turn ID and sends it as the user message `uuid`. Admission is the replayed user message carrying that UUID; a write failure or no echo before the context ends closes the session. Text parts become text blocks; local images become base64 image blocks (png, jpeg, gif, webp; other types are `ErrUnsupported`). Effort accepts `low`, `medium`, `high`, `xhigh`, `max`.
- **Events.**

  | Claude frame | Event |
  |---|---|
  | replayed user message | `turn_started` |
  | `stream_event` text delta | `text_delta`, `ItemID` = message ID |
  | `assistant` `tool_use` block | `item_started`; kind `command` (Bash), `file_change` (Edit, MultiEdit, Write, NotebookEdit), else `tool` |
  | `user` `tool_result` block | `item_finished`; `completed`, `failed` on `is_error`, `declined` when the adapter denied it |
  | `result` | `usage`, then `turn_finished`: `success` → completed; after an interrupt → interrupted; otherwise failed with the result errors |

  An assistant message with no streamed deltas is emitted from its snapshot, as with Codex.
- **Requests.** A `control_request` with subtype `can_use_tool` becomes `EventRequest`:
  - Bash → `command` with the command, workspace as cwd, and the tool's description as reason.
  - Edit, MultiEdit, Write, NotebookEdit → `file_change` with the target path.
  - `AskUserQuestion` → `user_input`; answers return as allow with `updatedInput` carrying the original questions and an answers map keyed by question text.
  - Any other tool → `tool` with the tool name and its input as JSON, truncated at 2 KiB.

  Accept returns `allow` with the original input; decline returns `deny`; cancel returns `deny` with `interrupt: true`. A `control_cancel_request` drops the pending request so a later `Respond` returns `ErrRequestNotFound`. Unknown control subtypes get an error response.
- **Interrupt.** `control_request` with subtype `interrupt`; the outcome still arrives as `result`.
- **Policy.** Claude has no per-turn sandbox, so policy maps to tool restriction and permission mode:

  | Sandbox \ Approval | default / `on_request` | `never` |
  |---|---|---|
  | default | mode `default`, prompts to host | mode `default`, `--permission-prompts none` |
  | `read_only` | `--tools Read,Glob,Grep`, mode `dontAsk` | same |
  | `workspace_write` | mode `acceptEdits`, Bash sandbox enabled via `--settings`, prompts to host | same, `--permission-prompts none` |
  | `unrestricted` | mode `default`, prompts to host | mode `bypassPermissions` |

  If the CLI's sandbox settings cannot confine writes to the workspace, `workspace_write` returns `ErrUnsupported`.

### Verification

Deterministic tests mirror the Codex suite with a fake `claude` peer replaying captured frames, plus the policy-to-flags table and respawn on settings change. Live tests use the same tag and opt-in, `claude-haiku-5-5` at `low` effort, and credentials from `HARNESS_TEST_CLAUDE_CONFIG_DIR` or `ANTHROPIC_API_KEY`/`CLAUDE_CODE_OAUTH_TOKEN` with a temporary config directory; the default `~/.claude` is never used. `TestClaudeReviewE2E` and `TestClaudeApprovalE2E` match the Codex scenarios.

## Extension rules

- Add a provider only when it meets the create/resume and terminal-event rules. Return `ErrUnsupported` for inputs a harness cannot honour.
- Provider configuration and protocol fields stay in the provider package. Add a normalized event field only when multiple consumers need it.
- Optional operations (history, rollback, compaction, model discovery) belong in separate small interfaces, not in every provider.
- No orchestration database or HTTP API in this module. A higher layer persists `Cursor` and events, maps logical threads to cursors, and routes requests to the worker that owns the live `Turn`.
