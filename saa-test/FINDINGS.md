# SAA CLI test findings

Harness: `saa-test/saa_test.py` (60 tests). Run: `uv run saa-test/saa_test.py --fresh`.
Latest full run with the fixes below applied: **60 passed, 0 failed, 0 errored.**

The server (local branch `fredtzeng/saa-start-delay-pause`, built in via the `go.mod`
replace directive) behaved correctly in every test. Three bugs were found in the **CLI**
(PR #1092), all in the *single standalone activity* code paths of
`internal/temporalcli/commands.activity.go`, where flags were read but not forwarded to the
unary RPC — they were only forwarded on the `--query`/`--workflow-id` batch path. **All
three are now fixed** in this worktree (see diff of `commands.activity.go`); the
corresponding tests now pass.

## Fixes applied
- `update-options` single path now sets `RestoreOriginal: c.RestoreOriginalOptions`.
- `update-options` task-queue field-mask path changed `task_queue_name` → `task_queue.name`.
- `reset` single path now sets `RestoreOriginalOptions: c.RestoreOriginalOptions`.

## Bug 1 — `update-options --restore-original-options` is a no-op for a standalone activity
- Test: `update_options.restore_original`
- Symptom: after `update-options --start-to-close-timeout 10s` then
  `update-options --restore-original-options`, the timeout stays `10s` (expected `60s`),
  exit 0.
- Root cause: the single-execution request in `TemporalActivityUpdateOptionsCommand.run`
  (commands.activity.go ~747-759) never sets `RestoreOriginal`. The flag
  (`c.RestoreOriginalOptions`) is only passed on the batch path (~786).
- Fix: set `RestoreOriginal: c.RestoreOriginalOptions` on the
  `UpdateActivityExecutionOptionsRequest`. (The request also sends a non-nil empty
  `ActivityOptions` with an empty mask; with restore set, validation requires the mask to
  be empty, which it is here, so that is fine.)

## Bug 2 — `update-options --task-queue` is silently dropped
- Test: `update_options.task_queue`
- Symptom: `update-options --task-queue moved-tq` returns exit 0 but the task queue is
  unchanged (`describe` still shows the original).
- Root cause: the field-mask path is built as `task_queue_name`
  (commands.activity.go ~663). The server expects the proto field path `task_queue.name`
  (validator keys off `taskQueue.name` after `util.ParseFieldMask`). The malformed path
  matches no field, so the update is ignored. The timeout paths use correct snake_case
  proto names (`start_to_close_timeout`, etc.), which is why those updates work.
- Fix: change the appended mask path from `task_queue_name` to `task_queue.name`.

## Bug 3 — `reset --restore-original-options` is a no-op for a standalone activity
- Test: `reset.restore_original_scheduled`
- Symptom: after updating `start-to-close-timeout` to `10s`, `reset
  --restore-original-options` leaves it at `10s` (expected `60s`).
- Root cause: the single-execution request in `TemporalActivityResetCommand.run`
  (commands.activity.go ~955-963) sets `KeepPaused` and `ResetHeartbeat` but not
  `RestoreOriginalOptions`. The proto field exists
  (`ResetActivityExecutionRequest.RestoreOriginalOptions`, field 9) and is only set on the
  batch path (~989).
- Fix: set `RestoreOriginalOptions: c.RestoreOriginalOptions` on the
  `ResetActivityExecutionRequest`.

## Coverage gap (not a bug) — `start_delay` is unreachable via the CLI
`activity start` and `activity update-options` expose no `--start-delay` flag, so the
server's start_delay behavior (and the start_delay-related fixes on this server branch)
cannot be exercised through the CLI. If start_delay is meant to be user-facing for SAA,
the CLI needs a `--start-delay` flag on `start` (and likely `update-options`).

## What was verified to work (server + CLI)
start (+ timeout normalization, retry policy, required-flag validation, id-conflict
FAIL/UseExisting, id reuse after terminal); describe; list/count; pause/unpause (SCHEDULED
and STARTED→PAUSE_REQUESTED); cancel (SCHEDULED, PAUSED, STARTED→CANCELED);
terminate; reset (SCHEDULED + restore-original); update-options (timeouts, retry policy,
task queue, restore-original, invalid-value rejection, terminal-state rejection);
complete/fail by id (rejected pre-start, applied when STARTED); full worker lifecycle
(complete, execute, fail-no-retry, retry-then-succeed, retry-exhausted); heartbeat
recording; and all four timeouts (schedule-to-start, schedule-to-close, start-to-close,
heartbeat).

## Kitchen-sink coverage (heartbeating, mixed ops, concurrency, worker crashes)
Heartbeating: structured heartbeat details surfaced in describe; **heartbeat checkpoint
delivered to the retry attempt**; manual complete-by-id on a long-heartbeating attempt;
cancel of a heartbeating attempt reaches CANCELED; pause/unpause, update-options, and reset
applied mid-heartbeat without stalling heartbeats (note: the SDK throttles heartbeat RPCs
to ~0.8× the heartbeat timeout, so tests use a short heartbeat timeout to observe
progress).

Precedence (deterministic): cancel/complete after terminate rejected; double-terminate
absorbing; pause rejected while CANCEL_REQUESTED; update-options accepted while
CANCEL_REQUESTED; terminate overrides PAUSE_REQUESTED.

Concurrency (invariant-based — terminal is absorbing, state always consistent, no
server-internal/crash errors): cancel||terminate race ⇒ ends {CANCELED,TERMINATED} and
stable; six-way op storm (pause/unpause/reset/update/cancel/terminate) on STARTED and on
SCHEDULED both converge to a stable terminal; 10-way pause/unpause flood stays consistent.

Worker crashes & resumes: SIGKILL the worker mid-heartbeat ⇒ after the heartbeat timeout
fires and the activity retries, a restarted worker **resumes with the pre-crash heartbeat
checkpoint** and completes; manual complete-by-id still drives COMPLETED after the worker
dies; retry is redispatched to the restarted worker and completes on a later attempt.

These crash/resume and concurrency tests all pass against the server branch — i.e. SAA
durability and state-machine precedence hold up under a worker crash and under racing
operator requests.

## Server change: unpause/reset honor remaining retry backoff
Implemented on the server branch (commit "Standalone activity: unpause/reset honor remaining
retry backoff"). Previously unpause/reset discarded the pending retry interval and re-dispatched
immediately; now they honor `CompleteTime+CurrentRetryInterval` as a dispatch floor, exactly like
the first dispatch honors `start_delay` (and like the options-update path already did). Companion
fix: update-options recomputes the pending interval while PAUSED (not only SCHEDULED). Server
functional + unit tests updated (PauseWhileRetryNoWait→PauseWhileRetryHonorsBackoff,
InRetryWithLongInterval→InRetryHonorsBackoff, TransitionReset asserts the backoff is honored).
Full standalone server suite passes.

## SDK-client suite (saa_sdk_test.py) — now the primary chaos/kitchen-sink driver
The CLI cannot set `start_delay` (the SDK's `StartActivityOptions` has no such field) nor
`next_retry_delay` (a worker-side failure field), so the chaos suite was ported to drive the
client via the Temporal Python SDK's raw `workflow_service` RPCs while keeping a killable worker
subprocess for crash tests. 22 tests, all passing, including:
- start_delay (SDK-only): defers dispatch; cancel during the delay window; reset honors the
  remaining delay; pause/unpause during the delay window.
- retry-backoff honoring (the server change): cancel during backoff; reset and unpause honor the
  remaining backoff (timing-asserted); explicit `next_retry_delay` is honored.
- heartbeats: details surfaced; checkpoint survives a retry; manual complete on a heartbeating
  attempt.
- precedence: pause rejected / update accepted while CANCEL_REQUESTED; cancel-after-terminate
  rejected.
- chaos (invariant-based, concurrent racing RPCs): cancel‖terminate race; six-way op storm;
  pause/unpause flood — all converge to a stable terminal with no server-internal errors.
- worker crash: SIGKILL mid-heartbeat → restarted worker resumes with the pre-crash checkpoint.
- delete (SDK-only; CLI has no delete command): terminate then delete → describe NotFound.

## Non-bug (investigated and dismissed)
`reset --reset-heartbeats` *does* clear the checkpoint. An earlier harness probe appeared to show
otherwise, but that was a confound: the `hb_checkpoint` activity re-records the same checkpoint on
every attempt-1 run, and reset re-dispatches at attempt 1, so a live worker re-recorded the
checkpoint before the describe read it. Verified against the server's own `HeartbeatReset`
functional test (which polls synchronously) and by migrating to an idle queue before reset.
