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

### Commands

```
go test ./internal/temporalcli/ \
  -run "TestSharedServerSuite/TestWorkflow_Start_UseExisting_OmitsTypeAndTaskQueue" \
  -v -count=1 -timeout 120s
```

### Status: VERIFIED (2026-03-09)

1. **Test passes with fix** (commit `97ec0e5`, HEAD): PASS
2. **Test fails without fix** (commit `21f0a36`, pre-fix): FAIL
   - Output incorrectly contained `"type": "OtherWorkflowType"` and
     `"taskQueue": "other-queue"` (the caller's flags, not the actual workflow's)
3. **No regression on existing test** (`TestWorkflow_Start_SimpleSuccess`): PASS
   - Non-UseExisting path still correctly reports type and taskQueue
