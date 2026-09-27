## Code conventions

### Errors

Return errors up the stack; log only at the top level (the command `Run` function or the HTTP handler boundary). Never log an error and then return it — it will be logged twice.

Always wrap with context using `%w`:

```go
return fmt.Errorf("failed to parse base URL: %w", err)
```

Do not add error handling for impossible cases. Trust the compiler and internal invariants.

### Logging

Use `slog` throughout. Pass the logger via `Globals.Logger` rather than calling `slog.Default()` in business logic. Reserve `slog.Default()` for package-level handlers (middleware, telemetry) where dependency injection is impractical.

Use `InfoContext` / `ErrorContext` inside HTTP handlers so trace IDs propagate automatically.

```go
logger.InfoContext(r.Context(), "access", slog.String("method", r.Method), ...)
```

### Contexts

Pass `context.Context` as the first argument to every function that does I/O, makes external calls, or should respect cancellation. Do not store contexts in structs. Use the context for both cancellation and OTel trace propagation.

### Package names

Lowercase, single word, descriptive of the domain — not the layer. Examples: `oidc`, `middleware`, `telemetry`, `tokens`, `commands`. Avoid generic names like `util`, `helpers`, `common`.

### Comments

Only comment the **why**, never the **what**. No docstrings for obvious functions. A one-line comment is the maximum.
