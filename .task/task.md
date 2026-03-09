# Bug: `workflow start --id-conflict-policy UseExisting` reports incorrect type and taskQueue

## Description

When `workflow start` is called with `--id-conflict-policy UseExisting` and the workflow
already exists, the CLI attaches to the existing workflow but reports the **caller's** `--type`
and `--task-queue` flags rather than those of the actual running workflow. This is misleading:
the output claims the workflow has a type/taskQueue that it does not.

## Reproduction

```
$ temporal -o json workflow start --workflow-id w --type t1 --task-queue q1
{ "workflowId": "w", "runId": "...", "type": "t1", "taskQueue": "q1", ... }

$ temporal -o json workflow start --workflow-id w --type t2 --task-queue q2 --id-conflict-policy UseExisting
{ "workflowId": "w", "runId": "...", "type": "t2", "taskQueue": "q2", ... }
                                               ^^^            ^^^
                                               WRONG — actual workflow has type t1, taskQueue q1
```

## Root cause

`startWorkflow()` at `commands.workflow_exec.go:557-559` constructs the output using
`sharedWorkflowOpts.Type` and `sharedWorkflowOpts.TaskQueue` — the CLI flag values — rather
than anything derived from the server response. When UseExisting attaches to an existing
workflow, these flags are the *caller's requested* values, not the actual workflow's values.

## Proposed fix

When `--id-conflict-policy UseExisting` is set, omit `type` and `taskQueue` from the CLI output,
since we cannot guarantee their correctness. (The `client.WorkflowRun` returned by the SDK does
not expose the actual workflow's type or task queue.)

Both the `workflow start` and `workflow execute` output paths need this fix.
