# Result: Fix UseExisting reporting incorrect type and taskQueue

## State machine transition

**(bug-exists, test-suite-failing)** → **(no-bugs-reported, test-suite-passing-and-adequate)**

## What changed

**`internal/temporalcli/commands.workflow_exec.go`** — in `startWorkflow()`:

1. Added `omitempty` to the `Type` and `TaskQueue` JSON struct tags.
2. When `workflowOpts.IdConflictPolicy.Value == "UseExisting"`, set `Type` and
   `TaskQueue` to empty strings so they are omitted from output. Otherwise,
   populate them from CLI flags as before.

## Why

When `--id-conflict-policy UseExisting` attaches to an already-running workflow,
the CLI was reporting the *caller's requested* type and taskQueue, which may
differ from the actual workflow's values. Since the `ExecuteWorkflow` API doesn't
tell us whether it created a new workflow or attached to an existing one, the
simplest correct fix is to not report values we can't guarantee are accurate.

## Verification

```
go test ./internal/temporalcli/ \
  -run "TestSharedServerSuite/TestWorkflow_Start_UseExisting_OmitsTypeAndTaskQueue" \
  -v -count=1 -timeout 120s
```
