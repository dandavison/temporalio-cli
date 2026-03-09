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

### Approach: Drop type and taskQueue from start output unconditionally

Remove `Type` and `TaskQueue` from the "Running execution" output struct
entirely. These values are redundant (the user just supplied them as flags) and
misleading in the UseExisting case.

### Changes

**File: `internal/temporalcli/commands.workflow_exec.go`**

Remove `Type` and `TaskQueue` from the output struct in `startWorkflow()`.

**File: `internal/temporalcli/commands.workflow_exec_test.go`**

Remove assertions on `type` and `taskQueue` in `TestWorkflow_Start_SimpleSuccess`.
Update `TestWorkflow_Start_UseExisting_OmitsTypeAndTaskQueue` to assert the
fields are absent.

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



