# Harness providers

`harness-providers` is a Go interface for running interactive coding harnesses. The current implementation supports Codex through `codex app-server` over stdio. The caller owns workspace files, conversation persistence, and event delivery; the provider translates native sessions, turns, and events into the shared API. See [SPEC.md](SPEC.md) for the contract and design rationale.

## Requirements

- Go 1.26.5 or newer (the version in `go.mod`).
- A `codex` CLI on `PATH`, or its path in `codex.Config.Binary`.
- Codex authentication available to the child process.
- An existing workspace directory.

## Use Codex

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	hp "github.com/wolfeidau/harness-providers"
	"github.com/wolfeidau/harness-providers/codex"
)

func main() {
	ctx := context.Background()
	provider := codex.New(codex.Config{Home: "/path/to/codex-home"})
	session, err := provider.Open(ctx, hp.OpenRequest{WorkingDirectory: "/path/to/workspace"})
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Persist this cursor to reopen the same Codex thread in a later process.
	cursor := session.Cursor()
	_ = cursor

	turn, err := session.StartTurn(ctx, hp.TurnInput{
		Parts: []hp.InputPart{{Kind: hp.InputText, Text: "Review this workspace."}},
	})
	if err != nil {
		panic(err)
	}
	for {
		event, err := turn.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			panic(err)
		}
		if event.Kind == hp.EventTextDelta {
			fmt.Print(event.Text)
		}
		if event.Kind == hp.EventTurnFinished {
			if event.Outcome == nil || event.Outcome.Status != hp.OutcomeCompleted {
				panic("Codex turn did not complete")
			}
		}
	}
}
```

Pass the saved cursor as `OpenRequest.Resume` to resume; keep the same Codex home and workspace available. The cursor contains a Codex thread ID, not credentials or a copy of the conversation. `Close` stops the child process and leaves the home and workspace in place.

`Config.Home` sets `CODEX_HOME` for the Codex child. If omitted, the child inherits the ambient environment, including any `CODEX_HOME`. `Config.Env` adds child environment entries; `Config.Logger` accepts a `*slog.Logger` for diagnostic logs and defaults to a discard logger. The library does not sign in, create workspaces, or copy files. Local image input requires an existing absolute path accessible to the Codex process.

At debug level, the logger includes bounded Codex stderr and failed command output. These diagnostics can contain workspace data, so use a log destination with appropriate access controls.

## Tests

Deterministic protocol tests require no model access:

```sh
go test ./...
```

The opt-in end-to-end test makes two real Codex turns: a read-only review using `gpt-6-luna` at low effort, then a resume check across a new app-server process. It creates a temporary workspace. Give it a separately signed-in Codex home, or a `CODEX_ACCESS_TOKEN` in an environment where that token is supported. It never selects the default Codex home implicitly.

```sh
HARNESS_TEST_CODEX_HOME=/path/to/test-codex-home HARNESS_LIVE_TEST=1 \
  go test -tags=integration ./... -run '^TestCodexReviewE2E$' -count=1 -timeout=3m
```

If using `CODEX_ACCESS_TOKEN`, set it in the environment and omit `HARNESS_TEST_CODEX_HOME`; the test creates a temporary Codex home. Once opted in, missing CLI, authentication, or model support fails the test. The live test uses model calls and may incur cost.

## Scope

Only Codex is implemented. Other harnesses can implement the same `Provider` interface, with their own configuration and transports. This module does not provide a server, database, scheduler, worktree manager, container launcher, or recovery of an in-flight turn after process loss. Event delivery is live; callers must store events they need to replay.

## License

Apache License 2.0. See [LICENSE](LICENSE).
