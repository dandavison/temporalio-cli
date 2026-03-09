# Repro: UseExisting reports incorrect type and taskQueue

## Failing test

```
go test ./internal/temporalcli/ \
  -run "TestSharedServerSuite/TestWorkflow_Start_UseExisting_OmitsTypeAndTaskQueue" \
  -v -count=1 -timeout 120s
```

## What the test does

1. Starts a long-running workflow (blocks on signal) with type `DevWorkflow` on the worker's task queue
2. Runs `workflow start` again with `--type OtherWorkflowType --task-queue other-queue --id-conflict-policy UseExisting`
3. Asserts the JSON output does NOT claim `type: "OtherWorkflowType"` or `taskQueue: "other-queue"`

## Observed output (bug)

The second `workflow start` output is:
```json
{
  "workflowId": "...",
  "runId": "<same run ID as first>",
  "type": "OtherWorkflowType",
  "namespace": "default",
  "taskQueue": "other-queue"
}
```

The type and taskQueue are the caller's flags, not the actual workflow's values.

## Root cause

`startWorkflow()` in `commands.workflow_exec.go:557-559` always uses
`sharedWorkflowOpts.Type` and `sharedWorkflowOpts.TaskQueue` (CLI flag values)
for the output, regardless of whether the workflow was newly started or an
existing one was returned via UseExisting.
