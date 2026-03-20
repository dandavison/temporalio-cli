# Review: Decouple CLI user messages from structured logging

## Is this the right move?

Yes. The prior design conflated two distinct concerns:

1. **User-facing messages** (errors, deprecation warnings, diagnostics) — these should always
   appear, in human-readable form, regardless of log level.
2. **Structured log output** — consumed by the SDK client, dev server, and potentially
   machine-parsed by ops tooling.

The original code routed user-facing messages through `slog`, which meant:
- Errors appeared as `level=ERROR msg="..."` structured log lines — ugly and confusing for CLI users.
- Setting `--log-level=never` (or any level that suppressed INFO/WARN) silenced deprecation
  warnings and diagnostics that users should always see.
- The default `Fail` callback had a nil-check branch for `Logger` because logging wasn't yet
  initialized when the callback was first constructed — a symptom of the conflation.

This is a well-established pattern: CLIs like `kubectl`, `gh`, and `docker` all emit user-facing
messages as plain text to stderr and reserve structured logging for internal/library use. The change
aligns `temporal` with that convention.

Changing the default `--log-level` from `info` to `never` is the correct corollary: once the CLI's
own messages bypass the logger, the logger serves only the SDK client and dev server. Most CLI users
don't want structured log output by default.

## Prior art

The author appears aware of the established convention. The commit messages explicitly note that the
structured logger "now exists solely for the SDK client and dev server, which conventionally use
it." The `--verbose` flag for CLI-level diagnostics follows the pattern used by `git`, `rsync`, and
many other CLI tools.

The `populateFlagsFromEnv` refactoring — eliminating the deferred-callback pattern — is the kind of
simplification that naturally falls out once logging is no longer needed before the logger is
initialized. This is a clean improvement.

## Process

### Repro / problem demonstration

The commit history shows the author iterated: an initial "remove remaining logging" commit was
reverted, then the work was restructured into well-separated commits. The test
`TestErrorReporting_IndependentOfLogLevel` serves as the repro — it demonstrates the original bug
(errors appearing as structured log lines on stderr) and verifies the fix across multiple log
levels.

### Tests

Two tests were added:

- `TestErrorReporting_IndependentOfLogLevel`: Verifies that command errors appear as plain text
  (`Error: ...`) on stderr with no `level=ERROR` structured output, across all log levels. Uses a
  connection-refused scenario to trigger a real error. Good.

- `TestEnv_DeprecationWarningBypassesLogger`: Verifies that deprecation warnings appear as plain
  text regardless of log level. Checks both positive (warning present) and negative (no structured
  log markers). Good.

Both tests exercise the change through realistic command execution via the test harness.

### What could be stronger

- There is no test for `--verbose` output. The `printVerbose` path (env var overriding a flag
  setting) is tested only implicitly through `populateFlagsFromEnv`. A test that sets `--verbose`
  and verifies the diagnostic message appears (and that it does *not* appear without `--verbose`)
  would close the loop.

- The commit that deletes a test (`5b4a4a4 Delete test`) has no commit message body explaining what
  was deleted or why. Reviewing the branch, this appears to be cleanup of a temporary repro test,
  but it's not clear from the commit alone.

## Code quality

### Correctness

The changes are correct. The key transformations:

1. **`Fail` callback** (`commands.go:141-149`): Simplified from a nil-check-Logger branch to a
   direct `fmt.Fprintf` to stderr. This is now the single path of execution for error reporting —
   clean and unconditional.

2. **`populateFlagsFromEnv`** (`commands.go:260-295`): The deferred-callback pattern (`logCalls
   []func(*slog.Logger)`) is eliminated in favor of direct `printVerbose` calls. The function
   signature simplifies from `(func(*slog.Logger), error)` to `error`. This works because
   `cctx.Verbose` is now set before `populateFlagsFromEnv` is called (line 455).

3. **Deprecation warnings** (`commands.env.go`, `commands.operator_namespace.go`): Changed from
   `cctx.Logger.Warn(...)` to `fmt.Fprintln(cctx.Options.Stderr, "Warning: ...")`. Correct — these
   are user-facing messages that should always appear.

4. **Workflow exec/view** (`commands.workflow_exec.go:94`, `commands.workflow_view.go:560`): Changed
   from `cctx.Logger.Error(...)` to `fmt.Fprintf(cctx.Options.Stderr, "Warning: ...")`. Note the
   semantic change: these were logged at ERROR level but now say "Warning:" — this is arguably more
   correct since they're not fatal errors (the workflow itself failed; printing the output also
   failed, but execution continues).

### Simplicity

The change is a net reduction of 38 deletions vs 85 insertions — but 55 of those insertions are
tests. The implementation itself is a net simplification: fewer lines, simpler function signatures,
fewer code paths. The `printVerbose` helper is minimal and well-placed.

### Residual concerns

- `Logger` field and `slog` import remain on `CommandContext`. This is correct — the logger is still
  needed for the SDK client (`client.go:46`) and dev server (`commands.server.go:38`). But
  `cctx.Logger` is no longer used for any CLI-level messaging, which is the intent. No issue here.

- The `"log/slog"` import in `commands.go` is retained. It's still referenced by the `Logger` field
  type. Fine.

### Nits

None worth flagging. The change is clean.

## Summary

Well-executed change. The separation of concerns is correct, the tests are meaningful, and the
commit history shows thoughtful iteration. The only gap is a missing test for the `--verbose` flag
itself.
