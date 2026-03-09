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

### Approach: Describe after start when UseExisting

When `workflowOpts.IdConflictPolicy.Value == "UseExisting"`, after
`cl.ExecuteWorkflow()` returns successfully, call
`cl.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())` to obtain the
actual `Type` and `TaskQueue` from the server. Use those in the output instead
of the CLI flag values.

**Why this approach over alternatives:**

- **vs. omitting type/taskQueue entirely**: Omitting is simpler but loses useful
  information. When the workflow *is* newly started, the correct values are still
  reported. When it already existed, the user gets the actual values — strictly
  better UX.
- **vs. raw gRPC call to access `Started` field**: Would require bypassing the
  SDK's `ExecuteWorkflow` and reimplementing its logic (retry, eager dispatch,
  etc). The Describe call is one extra RPC only in the UseExisting path, which
  is acceptable.
- **vs. always calling Describe**: Unnecessary cost in the common case. Only the
  UseExisting path has the ambiguity.

### Changes

**File: `internal/temporalcli/commands.workflow_exec.go`**

In `startWorkflow()` (line ~544), after `cl.ExecuteWorkflow()` succeeds and
before printing the output:

1. Initialise `wfType := sharedWorkflowOpts.Type` and `taskQueue :=
   sharedWorkflowOpts.TaskQueue` (existing behavior, used as defaults).
2. If `workflowOpts.IdConflictPolicy.Value == "UseExisting"`, call
   `cl.DescribeWorkflowExecution(cctx, run.GetID(), run.GetRunID())` and extract
   the actual type and task queue from
   `resp.WorkflowExecutionInfo.GetType().GetName()` and
   `resp.WorkflowExecutionInfo.GetTaskQueue()`. Assign these to `wfType` and
   `taskQueue`.
3. Use `wfType` and `taskQueue` in the output struct instead of the flag values.

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
