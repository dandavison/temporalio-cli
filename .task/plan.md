# Plan: Fix UseExisting reporting incorrect type and taskQueue

## State machine stage

**(bug-exists, test-suite-failing)** — the failing test
`TestWorkflow_Start_UseExisting_OmitsTypeAndTaskQueue` is committed at `21f0a36`.
This plan transitions us to **(no-bugs-reported, test-suite-passing-and-adequate)**.

## Design

### Problem

`startWorkflow()` in `commands.workflow_exec.go:557-559` unconditionally uses
the caller's CLI flag values (`sharedWorkflowOpts.Type`,
`sharedWorkflowOpts.TaskQueue`) for the output. When `--id-conflict-policy
UseExisting` attaches to an already-running workflow, these values are the
caller's *requested* values, not the actual workflow's values.

### Approach: Omit type and taskQueue when UseExisting

When `workflowOpts.IdConflictPolicy.Value == "UseExisting"`, omit `Type` and
`TaskQueue` from the "Running execution" output. We don't know whether
`ExecuteWorkflow` created a new workflow or attached to an existing one, so the
CLI flag values may be wrong. Rather than making an extra Describe RPC to find
out the truth, simply don't claim anything.

This is the simplest correct fix. A future enhancement could add a Describe call
to report the actual values, but that's not needed now.

### Changes

**File: `internal/temporalcli/commands.workflow_exec.go`**

In `startWorkflow()` (line ~546), the "Running execution" output struct:

1. Add `omitempty` to the `Type` and `TaskQueue` JSON tags.
2. When `workflowOpts.IdConflictPolicy.Value == "UseExisting"`, set `Type` and
   `TaskQueue` to empty strings (i.e. don't populate them). Otherwise, populate
   them from the CLI flags as before.

No other files need changes. The `workflow execute` path uses the same
`startWorkflow()` function, so it's fixed for both commands.

## Verification

### Failing test (must fail before fix, pass after)

```
go test ./internal/temporalcli/ \
  -run "TestSharedServerSuite/TestWorkflow_Start_UseExisting_OmitsTypeAndTaskQueue" \
  -v -count=1 -timeout 120s
```

### Full test suite (must pass after fix)

```
go test ./internal/temporalcli/ -count=1 -timeout 300s
```

### Manual verification

1. Revert the fix (`git stash` or `git revert`), run the failing test, confirm
   it fails.
2. Re-apply the fix, run the failing test, confirm it passes.
3. Run the existing `TestWorkflow_Start_SimpleSuccess` to confirm the
   non-UseExisting path still reports type and taskQueue correctly.
