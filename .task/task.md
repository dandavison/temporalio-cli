We have a bug affecting workflows. When using UseExisting, if your second workflow "attaches" to an existing workflow, the reported task queue and workflow type are for the second one that never was actually started, and so may be incorrect:
$ temporal -o json workflow start --workflow-id w --type t1 --task-queue q1
{
  "workflowId": "w",
  "runId": "019ca0ea-26e1-789b-ad07-f054994a396c",
  "type": "t1",
  "namespace": "default",
  "taskQueue": "q1"
}
$ temporal -o json workflow start --workflow-id w --type t2 --task-queue q2 --id-conflict-policy UseExisting
{
  "workflowId": "w",
  "runId": "019ca0ea-26e1-789b-ad07-f054994a396c",
  "type": "t2",
  "namespace": "default",
  "taskQueue": "q2"
}

For Standalone Activity let's omit the problematic type and task queue fields in CLI output entirely for now.