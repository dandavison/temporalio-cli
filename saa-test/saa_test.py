#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = ["temporalio"]
# ///
"""
Comprehensive Standalone-Activity (SAA) surface-area test harness.

Drives the SAA surface end-to-end:
  - client/operator + ALL assertions go through the built `temporal` CLI
    (repo-root ./temporal-saa, built against the local server branch)
  - the dev server is the same binary's `server start-dev` (runs our code)
  - an in-process Python-SDK activity worker drives execution where a worker
    is required to reach STARTED / COMPLETED / FAILED / heartbeat / retry

Outputs (all under saa-test/):
  - log.text         append-only run log; RESULT lines drive resume
  - bugs/NN-slug.md  one file per failing/erroring test, with captured I/O

Resume: re-running skips any test whose id already has a terminal RESULT in
log.text. Use --fresh to wipe prior state, --only ID[,ID] to run a subset,
--rerun-failed to re-run only previously failed/errored tests.

Subcommand `worker` runs the SDK worker (the harness spawns it itself).
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import re
import signal
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
CLI_BIN = REPO_ROOT / "temporal-saa"
OUT_DIR = SCRIPT_DIR
LOG_PATH = OUT_DIR / "log.text"
BUGS_DIR = OUT_DIR / "bugs"
SERVER_LOG = OUT_DIR / "server.log"
WORKER_LOG = OUT_DIR / "worker.log"
DB_PATH = OUT_DIR / "devserver.db"

NAMESPACE = "default"
WORKER_TQ = "saa-worker-tq"   # has a worker polling it
IDLE_TQ = "saa-idle-tq"       # no worker: activities stay SCHEDULED

DYNAMIC_CONFIG = {
    "activity.enableStandalone": "true",
    "activity.enableCallbacks": "true",
    "activity.startDelayEnabled": "true",
    "history.enableChasm": "true",
}


# --------------------------------------------------------------------------
# CLI invocation
# --------------------------------------------------------------------------
@dataclass
class CmdResult:
    args: list[str]
    code: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.code == 0

    def json(self) -> Any:
        return json.loads(self.stdout)


class CLI:
    def __init__(self, address: str):
        self.address = address

    def run(self, *args: str, timeout: float = 60.0) -> CmdResult:
        full = [str(CLI_BIN), "--address", self.address, *args]
        try:
            p = subprocess.run(full, capture_output=True, text=True, timeout=timeout)
            return CmdResult(list(args), p.returncode, p.stdout, p.stderr)
        except subprocess.TimeoutExpired as e:
            return CmdResult(list(args), 124, e.stdout or "", (e.stderr or "") + "\n<timeout>")

    def act(self, *args: str, **kw: Any) -> CmdResult:
        return self.run("activity", *args, **kw)

    def parallel(self, cmds: list[list[str]], timeout: float = 60.0) -> list[CmdResult]:
        """Launch all CLI commands concurrently (real racing RPCs), then collect."""
        procs = []
        for args in cmds:
            full = [str(CLI_BIN), "--address", self.address, "activity", *args]
            procs.append((args, subprocess.Popen(full, stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE, text=True)))
        out: list[CmdResult] = []
        for args, p in procs:
            try:
                so, se = p.communicate(timeout=timeout)
                out.append(CmdResult(args, p.returncode, so, se))
            except subprocess.TimeoutExpired:
                p.kill()
                out.append(CmdResult(args, 124, "", "<timeout>"))
        return out

    def spawn(self, *args: str) -> subprocess.Popen:
        """Start a CLI command without waiting (e.g. a blocking `result` long-poll)."""
        full = [str(CLI_BIN), "--address", self.address, "activity", *args]
        return subprocess.Popen(full, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def wait(self, proc: subprocess.Popen, args: list[str], timeout: float = 60.0) -> CmdResult:
        try:
            so, se = proc.communicate(timeout=timeout)
            return CmdResult(args, proc.returncode, so, se)
        except subprocess.TimeoutExpired:
            proc.kill()
            return CmdResult(args, 124, "", "<timeout>")

    def describe(self, activity_id: str, run_id: str | None = None) -> dict:
        a = ["describe", "--activity-id", activity_id, "-o", "json"]
        if run_id:
            a += ["--run-id", run_id]
        r = self.act(*a)
        if not r.ok:
            raise AssertionError(f"describe {activity_id} failed: {r.stderr.strip() or r.stdout.strip()}")
        return r.json()


# --------------------------------------------------------------------------
# Assertions (raise TestFailure -> recorded as bug)
# --------------------------------------------------------------------------
class TestFailure(AssertionError):
    pass


def expect(cond: bool, msg: str) -> None:
    if not cond:
        raise TestFailure(msg)


def expect_eq(actual: Any, expected: Any, what: str) -> None:
    if actual != expected:
        raise TestFailure(f"{what}: expected {expected!r}, got {actual!r}")


def expect_run_state(d: dict, state: str) -> None:
    expect_eq(d.get("runState"), f"PENDING_ACTIVITY_STATE_{state}", "runState")


def expect_status(d: dict, status: str) -> None:
    expect_eq(d.get("status"), f"ACTIVITY_EXECUTION_STATUS_{status}", "status")


def expect_cli_error(r: CmdResult, what: str, needle: str | None = None) -> None:
    if r.ok:
        raise TestFailure(f"{what}: expected CLI error, got success. stdout={r.stdout.strip()!r}")
    if needle and needle.lower() not in (r.stderr + r.stdout).lower():
        raise TestFailure(f"{what}: error did not mention {needle!r}. got: {(r.stderr or r.stdout).strip()!r}")


def poll_until(fn: Callable[[], bool], timeout: float = 20.0, interval: float = 0.5) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return True
        time.sleep(interval)
    return False


TERMINAL = {
    "ACTIVITY_EXECUTION_STATUS_COMPLETED",
    "ACTIVITY_EXECUTION_STATUS_FAILED",
    "ACTIVITY_EXECUTION_STATUS_CANCELED",
    "ACTIVITY_EXECUTION_STATUS_TERMINATED",
    "ACTIVITY_EXECUTION_STATUS_TIMED_OUT",
}
RUN_STATES = {
    "PENDING_ACTIVITY_STATE_SCHEDULED",
    "PENDING_ACTIVITY_STATE_STARTED",
    "PENDING_ACTIVITY_STATE_PAUSED",
    "PENDING_ACTIVITY_STATE_PAUSE_REQUESTED",
    "PENDING_ACTIVITY_STATE_CANCEL_REQUESTED",
    "PENDING_ACTIVITY_STATE_RESET_REQUESTED",
}
# An error is "dirty" (a real defect) if it looks like a crash / server-internal
# fault rather than a legitimate precondition/argument rejection.
DIRTY_ERROR = re.compile(
    r"Internal|panic|nil pointer|runtime error|invalid memory|Unimplemented|"
    r"goroutine|segmentation|index out of range",
    re.IGNORECASE,
)


def assert_clean_errors(results: list[CmdResult], what: str) -> None:
    """Every CLI call must either succeed or fail with a sane (non-crash) error."""
    for r in results:
        if not r.ok and DIRTY_ERROR.search(r.stderr + r.stdout):
            raise TestFailure(f"{what}: dirty error from `{' '.join(r.args)}`: "
                              f"{(r.stderr or r.stdout).strip()[:300]}")


def assert_state_consistent(d: dict, what: str) -> None:
    status = d.get("status")
    if status not in TERMINAL and status != "ACTIVITY_EXECUTION_STATUS_RUNNING":
        raise TestFailure(f"{what}: unknown status {status!r}")
    if status == "ACTIVITY_EXECUTION_STATUS_RUNNING":
        rs = d.get("runState")
        if rs not in RUN_STATES:
            raise TestFailure(f"{what}: RUNNING but unknown runState {rs!r}")


def poll_terminal(cli: CLI, aid: str, timeout: float = 30.0) -> Optional[str]:
    box: dict[str, str] = {}

    def check() -> bool:
        st = cli.describe(aid).get("status")
        if st in TERMINAL:
            box["s"] = st
            return True
        return False

    poll_until(check, timeout=timeout)
    return box.get("s")


def assert_absorbing(cli: CLI, aid: str, what: str, samples: int = 4) -> str:
    """Once terminal, the status must be stable across repeated reads."""
    first = cli.describe(aid).get("status")
    expect(first in TERMINAL, f"{what}: expected terminal, got {first!r}")
    for _ in range(samples):
        time.sleep(0.3)
        cur = cli.describe(aid).get("status")
        if cur != first:
            raise TestFailure(f"{what}: terminal status not absorbing: {first!r} -> {cur!r}")
    return first or ""


# --------------------------------------------------------------------------
# Test registry / context
# --------------------------------------------------------------------------
@dataclass
class TestCtx:
    cli: CLI
    log: "RunLog"
    worker: Optional["WorkerProc"] = None
    _ops: list[str] = field(default_factory=list)

    def aid(self, test_id: str, suffix: str = "") -> str:
        base = test_id.replace(".", "-")
        return f"{base}{('-' + suffix) if suffix else ''}-{int(time.time() * 1000) % 1000000}"

    def record(self, label: str, r: CmdResult) -> CmdResult:
        self._ops.append(f"$ temporal activity {' '.join(r.args)}\n  exit={r.code}\n"
                         f"  stdout: {r.stdout.strip()[:800]}\n  stderr: {r.stderr.strip()[:800]}")
        return r


@dataclass
class Test:
    id: str
    fn: Callable[[TestCtx], None]
    needs_worker: bool
    doc: str


REGISTRY: list[Test] = []


def test(test_id: str, needs_worker: bool = False):
    def deco(fn: Callable[[TestCtx], None]):
        REGISTRY.append(Test(test_id, fn, needs_worker, (fn.__doc__ or "").strip()))
        return fn
    return deco


# --------------------------------------------------------------------------
# Logging + resume
# --------------------------------------------------------------------------
class RunLog:
    def __init__(self, path: Path):
        self.path = path
        self.fh = open(path, "a", buffering=1)

    def line(self, msg: str) -> None:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        self.fh.write(f"[{ts}] {msg}\n")
        print(f"[{ts}] {msg}", flush=True)

    def result(self, test_id: str, status: str, detail: str = "") -> None:
        self.line(f"RESULT {test_id} {status} {detail}".rstrip())

    def close(self) -> None:
        self.fh.close()


def completed_results(path: Path) -> dict[str, str]:
    """Map test_id -> last terminal status (PASS/FAIL/ERROR) from log.text."""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for ln in path.read_text().splitlines():
        m = re.search(r"RESULT (\S+) (PASS|FAIL|ERROR)\b", ln)
        if m:
            out[m.group(1)] = m.group(2)
    return out


def next_bug_number() -> int:
    BUGS_DIR.mkdir(parents=True, exist_ok=True)
    n = 0
    for f in BUGS_DIR.glob("*.md"):
        m = re.match(r"(\d+)-", f.name)
        if m:
            n = max(n, int(m.group(1)))
    return n + 1


def write_bug(test: Test, status: str, err: str, ctx: TestCtx) -> Path:
    num = next_bug_number()
    slug = test.id.replace(".", "-")
    path = BUGS_DIR / f"{num:02d}-{slug}.md"
    body = [
        f"# {test.id} — {status}",
        "",
        f"- when: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- needs_worker: {test.needs_worker}",
        f"- test doc: {test.doc}",
        "",
        "## Failure",
        "```",
        err.strip(),
        "```",
        "",
        "## CLI operations (in order)",
        "```",
        "\n\n".join(ctx._ops) if ctx._ops else "(none captured)",
        "```",
        "",
    ]
    path.write_text("\n".join(body))
    return path


# --------------------------------------------------------------------------
# Server / worker process management
# --------------------------------------------------------------------------
class DevServer:
    def __init__(self, port: int):
        self.port = port
        self.proc: Optional[subprocess.Popen] = None

    @property
    def address(self) -> str:
        return f"localhost:{self.port}"

    def start(self) -> None:
        if DB_PATH.exists():
            DB_PATH.unlink()
        args = [
            str(CLI_BIN), "server", "start-dev",
            "--port", str(self.port),
            "--ui-port", str(self.port + 1000),
            "--http-port", str(self.port + 60),
            "--metrics-port", "0",
            "--db-filename", str(DB_PATH),
            "--log-level", "warn",
            "--headless",
        ]
        for k, v in DYNAMIC_CONFIG.items():
            args += ["--dynamic-config-value", f"{k}={v}"]
        fh = open(SERVER_LOG, "w")
        self.proc = subprocess.Popen(args, stdout=fh, stderr=subprocess.STDOUT)

    def wait_ready(self, cli: CLI, timeout: float = 45.0) -> None:
        if not poll_until(lambda: cli.run("operator", "cluster", "health").ok, timeout=timeout):
            raise RuntimeError(f"dev server not ready; see {SERVER_LOG}")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


class WorkerProc:
    def __init__(self, address: str):
        self.address = address
        self.proc: Optional[subprocess.Popen] = None

    def start(self) -> None:
        args = ["uv", "run", str(SCRIPT_DIR / "saa_test.py"), "worker",
                "--address", self.address, "--task-queue", WORKER_TQ]
        fh = open(WORKER_LOG, "w")
        self.proc = subprocess.Popen(args, stdout=fh, stderr=subprocess.STDOUT)

    def wait_ready(self, cli: CLI, timeout: float = 60.0) -> None:
        # confirm the worker can actually execute by running a probe activity
        probe = f"worker-probe-{int(time.time())}"
        r = cli.act("start", "--activity-id", probe, "--type", "echo",
                    "--task-queue", WORKER_TQ, "--start-to-close-timeout", "30s")
        if not r.ok:
            raise RuntimeError(f"worker probe start failed: {r.stderr}")
        ok = poll_until(
            lambda: cli.describe(probe).get("status") == "ACTIVITY_EXECUTION_STATUS_COMPLETED",
            timeout=timeout)
        if not ok:
            raise RuntimeError(f"worker did not execute probe activity; see {WORKER_LOG}")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def crash(self) -> None:
        """Hard-kill the worker (SIGKILL) to simulate a worker crash."""
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def restart(self, cli: CLI) -> None:
        self.crash()
        self.start()
        self.wait_ready(cli)


# --------------------------------------------------------------------------
# The worker (subcommand `worker`)
# --------------------------------------------------------------------------
def run_worker(address: str, task_queue: str) -> None:
    from temporalio import activity
    from temporalio.client import Client
    from temporalio.exceptions import ApplicationError
    from temporalio.worker import Worker

    @activity.defn(name="echo")
    async def echo(*args: Any) -> Any:
        return args[0] if args else "ok"

    @activity.defn(name="block")
    async def block() -> str:
        # holds STARTED; heartbeats so cancellation can be delivered
        while True:
            activity.heartbeat()
            await asyncio.sleep(1)

    @activity.defn(name="noheartbeat")
    async def noheartbeat() -> str:
        # holds STARTED without heartbeating (for start-to-close / heartbeat timeouts)
        await asyncio.sleep(3600)
        return "done"

    @activity.defn(name="boom")
    async def boom() -> str:
        raise ApplicationError("intentional failure")

    @activity.defn(name="flaky")
    async def flaky() -> str:
        if activity.info().attempt < 2:
            raise ApplicationError("flaky failure")
        return "recovered"

    @activity.defn(name="hb_progress")
    async def hb_progress() -> str:
        # heartbeats an incrementing structured payload; holds STARTED
        i = 0
        while True:
            i += 1
            activity.heartbeat({"progress": i})
            await asyncio.sleep(1)

    @activity.defn(name="hb_checkpoint")
    async def hb_checkpoint() -> Any:
        # attempt 1 records a checkpoint heartbeat then fails; attempt 2 must be
        # resumed with that checkpoint in info.heartbeat_details
        info = activity.info()
        if info.attempt == 1:
            activity.heartbeat("ckpt-value")
            await asyncio.sleep(0.3)
            raise ApplicationError("fail to trigger retry")
        return {"attempt": info.attempt, "resumed_with": list(info.heartbeat_details)}

    @activity.defn(name="crash_resume")
    async def crash_resume() -> Any:
        # attempt 1 records a checkpoint then hangs (so the harness can SIGKILL the
        # worker). After the heartbeat timeout fires and the activity retries, the
        # restarted worker resumes here on a later attempt and returns the checkpoint.
        info = activity.info()
        if info.attempt == 1:
            activity.heartbeat("survived-crash")
            await asyncio.sleep(3600)
        return {"attempt": info.attempt, "resumed_with": list(info.heartbeat_details)}

    async def main() -> None:
        client = await Client.connect(address, namespace=NAMESPACE)
        async with Worker(
            client,
            task_queue=task_queue,
            activities=[echo, block, noheartbeat, boom, flaky, hb_progress,
                        hb_checkpoint, crash_resume],
            max_concurrent_activities=200,
        ):
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for s in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(s, stop.set)
            await stop.wait()

    asyncio.run(main())


# ==========================================================================
# TESTS
# ==========================================================================

# ---- start & validation --------------------------------------------------
@test("start.basic")
def t_start_basic(ctx: TestCtx) -> None:
    """start a basic activity -> SCHEDULED with correct fields."""
    aid = ctx.aid("start.basic")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "MyType",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s"))
    d = ctx.cli.describe(aid)
    expect_status(d, "RUNNING")
    expect_run_state(d, "SCHEDULED")
    expect_eq(d["activityType"]["name"], "MyType", "activityType")
    expect_eq(d["taskQueue"], IDLE_TQ, "taskQueue")
    expect_eq(d["startToCloseTimeout"], "60s", "startToCloseTimeout")
    expect_eq(d["attempt"], 1, "attempt")


@test("start.timeout_normalization")
def t_start_norm(ctx: TestCtx) -> None:
    """schedule-to-start/start-to-close are capped to schedule-to-close."""
    aid = ctx.aid("start.norm")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ,
               "--schedule-to-close-timeout", "30s",
               "--schedule-to-start-timeout", "60s",
               "--start-to-close-timeout", "60s"))
    d = ctx.cli.describe(aid)
    expect_eq(d["scheduleToCloseTimeout"], "30s", "scheduleToCloseTimeout")
    expect_eq(d["scheduleToStartTimeout"], "30s", "scheduleToStartTimeout (capped)")
    expect_eq(d["startToCloseTimeout"], "30s", "startToCloseTimeout (capped)")


@test("start.missing_timeouts")
def t_start_no_timeouts(ctx: TestCtx) -> None:
    """start with neither schedule-to-close nor start-to-close -> error."""
    aid = ctx.aid("start.notimeout")
    r = ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
                   "--task-queue", IDLE_TQ))
    expect_cli_error(r, "start without timeouts")


@test("start.missing_required_flags")
def t_start_required(ctx: TestCtx) -> None:
    """start missing --type or --task-queue -> error."""
    r1 = ctx.record("no-type", ctx.cli.act("start", "--activity-id", ctx.aid("x"),
                    "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s"))
    expect_cli_error(r1, "start without --type")
    r2 = ctx.record("no-tq", ctx.cli.act("start", "--activity-id", ctx.aid("x"),
                    "--type", "T", "--start-to-close-timeout", "60s"))
    expect_cli_error(r2, "start without --task-queue")


@test("start.retry_policy_reflected")
def t_start_retry(ctx: TestCtx) -> None:
    """retry-policy flags are persisted and shown in describe."""
    aid = ctx.aid("start.retry")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s",
               "--retry-initial-interval", "2s",
               "--retry-backoff-coefficient", "3",
               "--retry-maximum-interval", "30s",
               "--retry-maximum-attempts", "7"))
    d = ctx.cli.describe(aid)
    rp = d["retryPolicy"]
    expect_eq(rp.get("initialInterval"), "2s", "retry initialInterval")
    expect_eq(rp.get("backoffCoefficient"), 3, "retry backoffCoefficient")
    expect_eq(rp.get("maximumInterval"), "30s", "retry maximumInterval")
    expect_eq(rp.get("maximumAttempts"), 7, "retry maximumAttempts")


@test("start.duplicate_running_fails")
def t_start_dup(ctx: TestCtx) -> None:
    """default id-conflict-policy FAIL: starting a running id again errors."""
    aid = ctx.aid("start.dup")
    ctx.record("first", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s"))
    r = ctx.record("second", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
                   "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s"))
    expect_cli_error(r, "duplicate running start")


@test("start.id_conflict_use_existing")
def t_start_use_existing(ctx: TestCtx) -> None:
    """id-conflict-policy use-existing: second start attaches to the same run."""
    aid = ctx.aid("start.useexisting")
    r1 = ctx.record("first", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
                    "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s"))
    run1 = ctx.cli.describe(aid)["runId"]
    r2 = ctx.record("second", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
                    "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s",
                    "--id-conflict-policy", "UseExisting"))
    expect(r2.ok, f"use-existing second start should succeed: {r2.stderr}")
    expect_eq(ctx.cli.describe(aid)["runId"], run1, "runId unchanged under use-existing")


@test("start.id_reuse_after_terminal")
def t_start_reuse(ctx: TestCtx) -> None:
    """after terminate, the same id can be started again (new run)."""
    aid = ctx.aid("start.reuse")
    ctx.record("first", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s"))
    run1 = ctx.cli.describe(aid)["runId"]
    ctx.record("terminate", ctx.cli.act("terminate", "--activity-id", aid, "--reason", "cleanup"))
    r = ctx.record("restart", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
                   "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s"))
    expect(r.ok, f"reuse after terminal should succeed: {r.stderr}")
    expect(ctx.cli.describe(aid)["runId"] != run1, "new run id after reuse")


# ---- describe ------------------------------------------------------------
@test("describe.not_found")
def t_describe_nf(ctx: TestCtx) -> None:
    """describe of a non-existent id -> error."""
    r = ctx.record("describe", ctx.cli.act("describe", "--activity-id",
                   "no-such-activity-xyz", "-o", "json"))
    expect_cli_error(r, "describe missing")


# ---- list / count --------------------------------------------------------
@test("list_count.by_type")
def t_list_count(ctx: TestCtx) -> None:
    """list and count agree for a unique ActivityType filter."""
    atype = f"ListType{int(time.time()*1000)%1000000}"
    ids = [ctx.aid("lc", str(i)) for i in range(3)]
    for aid in ids:
        ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", atype,
                   "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    query = f"ActivityType = '{atype}'"
    expect(poll_until(lambda: ctx.cli.act("count", "--query", query, "-o", "json").ok
                      and _count_val(ctx.cli.act("count", "--query", query, "-o", "json")) == 3,
                      timeout=15),
           "count did not reach 3 (visibility lag?)")
    lr = ctx.record("list", ctx.cli.act("list", "--query", query, "-o", "json"))
    expect(lr.ok, f"list failed: {lr.stderr}")
    listed = [x for x in (lr.stdout.strip().splitlines() or []) if x.strip()]
    # list -o json prints a JSON array or jsonl; accept either
    n = _list_len(lr.stdout)
    expect_eq(n, 3, "list count")


def _count_val(r: CmdResult) -> int:
    try:
        return int(r.json().get("count", -1))
    except Exception:
        m = re.search(r"\d+", r.stdout)
        return int(m.group()) if m else -1


def _list_len(out: str) -> int:
    out = out.strip()
    if not out:
        return 0
    try:
        v = json.loads(out)
        return len(v) if isinstance(v, list) else 1
    except Exception:
        return len([ln for ln in out.splitlines() if ln.strip()])


# ---- pause / unpause -----------------------------------------------------
@test("pause.scheduled")
def t_pause_sched(ctx: TestCtx) -> None:
    """pause a SCHEDULED activity -> PAUSED."""
    aid = ctx.aid("pause.sched")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    ctx.record("pause", ctx.cli.act("pause", "--activity-id", aid, "--reason", "test"))
    expect_run_state(ctx.cli.describe(aid), "PAUSED")


@test("pause.unpause_scheduled")
def t_unpause_sched(ctx: TestCtx) -> None:
    """unpause a PAUSED activity -> back to SCHEDULED."""
    aid = ctx.aid("pause.unpause")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    ctx.record("pause", ctx.cli.act("pause", "--activity-id", aid, "--reason", "test"))
    expect_run_state(ctx.cli.describe(aid), "PAUSED")
    ctx.record("unpause", ctx.cli.act("unpause", "--activity-id", aid))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_SCHEDULED", timeout=10),
           "did not return to SCHEDULED after unpause")


@test("pause.cancel_while_paused")
def t_cancel_paused(ctx: TestCtx) -> None:
    """cancel works on a PAUSED activity -> CANCELED."""
    aid = ctx.aid("pause.cancelpaused")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    ctx.record("pause", ctx.cli.act("pause", "--activity-id", aid, "--reason", "test"))
    ctx.record("cancel", ctx.cli.act("cancel", "--activity-id", aid, "--reason", "stop"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_CANCELED", timeout=10),
           "paused activity did not cancel")


# ---- cancel --------------------------------------------------------------
@test("cancel.scheduled")
def t_cancel_sched(ctx: TestCtx) -> None:
    """cancel a SCHEDULED activity -> CANCELED immediately, reason recorded."""
    aid = ctx.aid("cancel.sched")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    ctx.record("cancel", ctx.cli.act("cancel", "--activity-id", aid, "--reason", "myreason"))
    d = ctx.cli.describe(aid)
    expect_status(d, "CANCELED")
    expect_eq(d.get("canceledReason"), "myreason", "canceledReason")


# ---- terminate -----------------------------------------------------------
@test("terminate.scheduled")
def t_terminate_sched(ctx: TestCtx) -> None:
    """terminate a SCHEDULED activity -> TERMINATED."""
    aid = ctx.aid("terminate.sched")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    ctx.record("terminate", ctx.cli.act("terminate", "--activity-id", aid, "--reason", "stop"))
    expect_status(ctx.cli.describe(aid), "TERMINATED")


# ---- reset ---------------------------------------------------------------
@test("reset.scheduled_noop_ok")
def t_reset_sched(ctx: TestCtx) -> None:
    """reset on a SCHEDULED activity succeeds and stays SCHEDULED."""
    aid = ctx.aid("reset.sched")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    r = ctx.record("reset", ctx.cli.act("reset", "--activity-id", aid, "--reset-attempts"))
    expect(r.ok, f"reset failed: {r.stderr}")
    expect_run_state(ctx.cli.describe(aid), "SCHEDULED")


@test("reset.restore_original_scheduled")
def t_reset_restore(ctx: TestCtx) -> None:
    """reset --restore-original-options on a SCHEDULED activity reverts a prior update."""
    aid = ctx.aid("reset.restore")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s"))
    ctx.record("update", ctx.cli.act("update-options", "--activity-id", aid,
               "--start-to-close-timeout", "10s"))
    expect_eq(ctx.cli.describe(aid)["startToCloseTimeout"], "10s", "after update")
    ctx.record("reset", ctx.cli.act("reset", "--activity-id", aid, "--restore-original-options"))
    expect_eq(ctx.cli.describe(aid)["startToCloseTimeout"], "60s", "after reset --restore-original-options")


# ---- complete / fail by id (no worker -> must error) ---------------------
@test("complete.on_scheduled_errors")
def t_complete_sched(ctx: TestCtx) -> None:
    """complete-by-id on a not-yet-started activity -> error."""
    aid = ctx.aid("complete.sched")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    r = ctx.record("complete", ctx.cli.act("complete", "--activity-id", aid, "--result", '"x"'))
    expect_cli_error(r, "complete on scheduled")


@test("fail.on_scheduled_errors")
def t_fail_sched(ctx: TestCtx) -> None:
    """fail-by-id on a not-yet-started activity -> error."""
    aid = ctx.aid("fail.sched")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    r = ctx.record("fail", ctx.cli.act("fail", "--activity-id", aid, "--reason", "boom"))
    expect_cli_error(r, "fail on scheduled")


# ---- update-options ------------------------------------------------------
@test("update_options.timeouts")
def t_update_timeouts(ctx: TestCtx) -> None:
    """update-options changes timeouts; describe reflects new values."""
    aid = ctx.aid("update.timeouts")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s",
               "--heartbeat-timeout", "50s"))
    ctx.record("update", ctx.cli.act("update-options", "--activity-id", aid,
               "--start-to-close-timeout", "30s", "--heartbeat-timeout", "20s"))
    d = ctx.cli.describe(aid)
    expect_eq(d["startToCloseTimeout"], "30s", "updated startToCloseTimeout")
    expect_eq(d["heartbeatTimeout"], "20s", "updated heartbeatTimeout")


@test("update_options.retry_policy")
def t_update_retry(ctx: TestCtx) -> None:
    """update-options changes the retry policy; describe reflects it."""
    aid = ctx.aid("update.retry")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s",
               "--retry-maximum-attempts", "5"))
    ctx.record("update", ctx.cli.act("update-options", "--activity-id", aid,
               "--retry-maximum-attempts", "9", "--retry-initial-interval", "4s"))
    rp = ctx.cli.describe(aid)["retryPolicy"]
    expect_eq(rp.get("maximumAttempts"), 9, "updated maximumAttempts")
    expect_eq(rp.get("initialInterval"), "4s", "updated initialInterval")


@test("update_options.task_queue")
def t_update_tq(ctx: TestCtx) -> None:
    """update-options can change the task queue."""
    aid = ctx.aid("update.tq")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s"))
    ctx.record("update", ctx.cli.act("update-options", "--activity-id", aid,
               "--task-queue", "moved-tq"))
    expect_eq(ctx.cli.describe(aid)["taskQueue"], "moved-tq", "updated taskQueue")


@test("update_options.restore_original")
def t_update_restore(ctx: TestCtx) -> None:
    """restore-original-options reverts a prior update."""
    aid = ctx.aid("update.restore")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s"))
    ctx.record("update", ctx.cli.act("update-options", "--activity-id", aid,
               "--start-to-close-timeout", "10s"))
    expect_eq(ctx.cli.describe(aid)["startToCloseTimeout"], "10s", "after update")
    ctx.record("restore", ctx.cli.act("update-options", "--activity-id", aid,
               "--restore-original-options"))
    expect_eq(ctx.cli.describe(aid)["startToCloseTimeout"], "60s", "after restore")


@test("update_options.invalid_rejected")
def t_update_invalid(ctx: TestCtx) -> None:
    """update-options with an invalid backoff coefficient (<1) -> error."""
    aid = ctx.aid("update.invalid")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s"))
    r = ctx.record("update", ctx.cli.act("update-options", "--activity-id", aid,
                   "--retry-backoff-coefficient", "0.5"))
    expect_cli_error(r, "invalid backoff coefficient")


@test("update_options.on_terminal_rejected")
def t_update_terminal(ctx: TestCtx) -> None:
    """update-options on a TERMINATED activity -> error."""
    aid = ctx.aid("update.terminal")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s"))
    ctx.record("terminate", ctx.cli.act("terminate", "--activity-id", aid, "--reason", "x"))
    r = ctx.record("update", ctx.cli.act("update-options", "--activity-id", aid,
                   "--start-to-close-timeout", "10s"))
    expect_cli_error(r, "update on terminal")


# ---- worker-driven lifecycle ---------------------------------------------
@test("lifecycle.complete", needs_worker=True)
def t_lifecycle_complete(ctx: TestCtx) -> None:
    """worker executes echo -> COMPLETED, result retrievable."""
    aid = ctx.aid("lc.complete")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "echo",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "30s",
               "--input", '"hello"'))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_COMPLETED", timeout=30),
           "did not COMPLETE")
    r = ctx.record("result", ctx.cli.act("result", "--activity-id", aid))
    expect(r.ok, f"result failed: {r.stderr}")
    expect("hello" in r.stdout, f"result missing payload: {r.stdout!r}")


@test("execute.complete", needs_worker=True)
def t_execute(ctx: TestCtx) -> None:
    """execute starts and waits for the result in one call."""
    aid = ctx.aid("execute")
    r = ctx.record("execute", ctx.cli.act("execute", "--activity-id", aid, "--type", "echo",
                   "--task-queue", WORKER_TQ, "--start-to-close-timeout", "30s",
                   "--input", '"yo"', timeout=40))
    expect(r.ok, f"execute failed: {r.stderr}")
    expect("yo" in r.stdout, f"execute output missing payload: {r.stdout!r}")


@test("lifecycle.fail_no_retry", needs_worker=True)
def t_fail_no_retry(ctx: TestCtx) -> None:
    """boom with max-attempts 1 -> FAILED."""
    aid = ctx.aid("lc.failnr")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "boom",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "30s",
               "--retry-maximum-attempts", "1"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_FAILED", timeout=30),
           "did not FAIL")


@test("lifecycle.retry_then_succeed", needs_worker=True)
def t_retry_succeed(ctx: TestCtx) -> None:
    """flaky fails attempt 1 then succeeds -> COMPLETED at attempt 2."""
    aid = ctx.aid("lc.flaky")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "flaky",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "30s",
               "--retry-initial-interval", "1s", "--retry-maximum-attempts", "5"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_COMPLETED", timeout=40),
           "flaky did not COMPLETE")
    expect(ctx.cli.describe(aid)["attempt"] >= 2, "expected attempt >= 2")


@test("lifecycle.retry_exhausted", needs_worker=True)
def t_retry_exhausted(ctx: TestCtx) -> None:
    """boom with max-attempts 2 retries then -> FAILED at attempt 2."""
    aid = ctx.aid("lc.exhaust")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "boom",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "30s",
               "--retry-initial-interval", "1s", "--retry-maximum-attempts", "2"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_FAILED", timeout=40),
           "did not FAIL after exhausting retries")
    expect_eq(ctx.cli.describe(aid)["attempt"], 2, "final attempt")


@test("complete.by_id_started", needs_worker=True)
def t_complete_started(ctx: TestCtx) -> None:
    """complete-by-id on a STARTED activity -> COMPLETED with the result."""
    aid = ctx.aid("complete.started")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "block",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "120s",
               "--heartbeat-timeout", "10s"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_STARTED", timeout=20), "did not reach STARTED")
    r = ctx.record("complete", ctx.cli.act("complete", "--activity-id", aid, "--result", '"manual"'))
    expect(r.ok, f"complete-by-id failed: {r.stderr}")
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_COMPLETED", timeout=15), "did not COMPLETE")


@test("fail.by_id_started", needs_worker=True)
def t_fail_started(ctx: TestCtx) -> None:
    """fail-by-id on a STARTED activity (max-attempts 1) -> FAILED."""
    aid = ctx.aid("fail.started")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "block",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "120s",
               "--heartbeat-timeout", "10s", "--retry-maximum-attempts", "1"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_STARTED", timeout=20), "did not reach STARTED")
    r = ctx.record("fail", ctx.cli.act("fail", "--activity-id", aid, "--reason", "manual-fail"))
    expect(r.ok, f"fail-by-id failed: {r.stderr}")
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_FAILED", timeout=15), "did not FAIL")


@test("pause.started_pause_requested", needs_worker=True)
def t_pause_started(ctx: TestCtx) -> None:
    """pause a STARTED activity -> PAUSE_REQUESTED; unpause -> STARTED."""
    aid = ctx.aid("pause.started")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "block",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "120s",
               "--heartbeat-timeout", "10s"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_STARTED", timeout=20), "did not reach STARTED")
    ctx.record("pause", ctx.cli.act("pause", "--activity-id", aid, "--reason", "p"))
    expect_run_state(ctx.cli.describe(aid), "PAUSE_REQUESTED")
    ctx.record("unpause", ctx.cli.act("unpause", "--activity-id", aid))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_STARTED", timeout=15),
           "did not return to STARTED after unpause")
    ctx.record("cleanup", ctx.cli.act("terminate", "--activity-id", aid, "--reason", "cleanup"))


@test("cancel.started", needs_worker=True)
def t_cancel_started(ctx: TestCtx) -> None:
    """cancel a STARTED activity -> CANCEL_REQUESTED, then CANCELED once the worker observes it."""
    aid = ctx.aid("cancel.started")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "block",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "120s",
               "--heartbeat-timeout", "5s"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_STARTED", timeout=20), "did not reach STARTED")
    ctx.record("cancel", ctx.cli.act("cancel", "--activity-id", aid, "--reason", "stop"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_CANCELED", timeout=30),
           "STARTED activity did not reach CANCELED after cancel (worker cancellation delivery?)")


@test("heartbeat.recorded", needs_worker=True)
def t_heartbeat(ctx: TestCtx) -> None:
    """a heartbeating activity records lastHeartbeatTime / totalHeartbeatCount."""
    aid = ctx.aid("heartbeat")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "block",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "120s",
               "--heartbeat-timeout", "10s"))
    expect(poll_until(lambda: int(ctx.cli.describe(aid).get("totalHeartbeatCount", 0) or 0) > 0,
                      timeout=20), "no heartbeats recorded")
    expect(ctx.cli.describe(aid).get("lastHeartbeatTime") is not None, "lastHeartbeatTime not set")
    ctx.record("cleanup", ctx.cli.act("terminate", "--activity-id", aid, "--reason", "cleanup"))


# ---- timeouts ------------------------------------------------------------
@test("timeout.schedule_to_start", needs_worker=False)
def t_s2s_timeout(ctx: TestCtx) -> None:
    """no worker + short schedule-to-start + max-attempts 1 -> TIMED_OUT."""
    aid = ctx.aid("to.s2s")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--schedule-to-start-timeout", "3s",
               "--start-to-close-timeout", "60s", "--retry-maximum-attempts", "1"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_TIMED_OUT", timeout=20),
           "did not TIME_OUT on schedule-to-start")


@test("timeout.schedule_to_close", needs_worker=False)
def t_s2c_timeout(ctx: TestCtx) -> None:
    """no worker + short schedule-to-close -> TIMED_OUT."""
    aid = ctx.aid("to.s2c")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--schedule-to-close-timeout", "3s",
               "--retry-maximum-attempts", "1"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_TIMED_OUT", timeout=20),
           "did not TIME_OUT on schedule-to-close")


@test("timeout.start_to_close", needs_worker=True)
def t_s2cl_timeout(ctx: TestCtx) -> None:
    """worker holds the attempt past start-to-close (max-attempts 1) -> TIMED_OUT."""
    aid = ctx.aid("to.startclose")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "noheartbeat",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "4s",
               "--retry-maximum-attempts", "1"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_TIMED_OUT", timeout=25),
           "did not TIME_OUT on start-to-close")


@test("timeout.heartbeat", needs_worker=True)
def t_hb_timeout(ctx: TestCtx) -> None:
    """worker attempt that never heartbeats past heartbeat-timeout (max-attempts 1) -> TIMED_OUT."""
    aid = ctx.aid("to.heartbeat")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "noheartbeat",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "120s",
               "--heartbeat-timeout", "4s", "--retry-maximum-attempts", "1"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_TIMED_OUT", timeout=25),
           "did not TIME_OUT on heartbeat")


@test("result.on_failed_nonzero", needs_worker=True)
def t_result_failed(ctx: TestCtx) -> None:
    """result of a FAILED activity exits non-zero and reports the failure."""
    aid = ctx.aid("result.failed")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "boom",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "30s",
               "--retry-maximum-attempts", "1"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_FAILED", timeout=30), "did not FAIL")
    r = ctx.record("result", ctx.cli.act("result", "--activity-id", aid, timeout=20))
    expect(not r.ok, "result of a FAILED activity should exit non-zero")


# ==========================================================================
# KITCHEN SINK: heartbeating, manual completion, mixed/concurrent ops,
# worker crashes & resumes. Where the outcome of a race is non-deterministic,
# assertions are invariant-based (terminal-absorbing, no crash errors, state
# always consistent) rather than asserting one exact state.
# ==========================================================================
def _start_and_await_started(ctx: TestCtx, aid: str, atype: str, *extra: str) -> None:
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", atype,
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "300s", *extra))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_STARTED", timeout=25),
           f"{aid}: did not reach STARTED")


def _terminate(ctx: TestCtx, aid: str) -> None:
    ctx.record("cleanup", ctx.cli.act("terminate", "--activity-id", aid, "--reason", "cleanup"))


# ---- heartbeating details ------------------------------------------------
@test("hb.details_surfaced", needs_worker=True)
def t_hb_details(ctx: TestCtx) -> None:
    """a heartbeating activity's structured details are surfaced in describe."""
    aid = ctx.aid("hb.details")
    _start_and_await_started(ctx, aid, "hb_progress", "--heartbeat-timeout", "30s")
    expect(poll_until(lambda: bool(ctx.cli.describe(aid).get("heartbeatDetails")), timeout=20),
           "no heartbeatDetails surfaced")
    d = ctx.cli.describe(aid)
    details = d.get("heartbeatDetails")
    expect(isinstance(details, list) and details and "progress" in details[0],
           f"unexpected heartbeatDetails shape: {details!r}")
    expect(int(d.get("totalHeartbeatCount", 0) or 0) >= 1, "totalHeartbeatCount not advancing")
    _terminate(ctx, aid)


@test("hb.checkpoint_survives_retry", needs_worker=True)
def t_hb_checkpoint(ctx: TestCtx) -> None:
    """heartbeat checkpoint from a failed attempt is delivered to the retry attempt."""
    aid = ctx.aid("hb.ckpt")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "hb_checkpoint",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "30s",
               "--heartbeat-timeout", "20s", "--retry-initial-interval", "1s",
               "--retry-maximum-attempts", "3"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_COMPLETED", timeout=40), "did not COMPLETE")
    r = ctx.record("result", ctx.cli.act("result", "--activity-id", aid, "-o", "json", timeout=20))
    expect(r.ok, f"result failed: {r.stderr}")
    expect("ckpt-value" in r.stdout, f"checkpoint not delivered to retry: {r.stdout!r}")
    expect_eq(ctx.cli.describe(aid)["attempt"], 2, "completed on attempt 2")


@test("hb.manual_complete_after_heartbeats", needs_worker=True)
def t_hb_manual_complete(ctx: TestCtx) -> None:
    """manual complete-by-id succeeds on a long-heartbeating STARTED activity."""
    aid = ctx.aid("hb.manual")
    # short heartbeat timeout so the SDK's heartbeat-RPC throttle (~0.8x timeout)
    # actually delivers heartbeats to the server within the poll window
    _start_and_await_started(ctx, aid, "hb_progress", "--heartbeat-timeout", "5s")
    expect(poll_until(lambda: int(ctx.cli.describe(aid).get("totalHeartbeatCount", 0) or 0) >= 2,
                      timeout=25), "heartbeats did not accumulate")
    r = ctx.record("complete", ctx.cli.act("complete", "--activity-id", aid, "--result", '"manual-done"'))
    expect(r.ok, f"manual complete failed: {r.stderr}")
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_COMPLETED", timeout=15), "did not COMPLETE")
    res = ctx.record("result", ctx.cli.act("result", "--activity-id", aid))
    expect("manual-done" in res.stdout, f"result missing manual payload: {res.stdout!r}")


@test("hb.cancel_during_heartbeat", needs_worker=True)
def t_hb_cancel(ctx: TestCtx) -> None:
    """cancel of a heartbeating activity is observed by the worker -> CANCELED."""
    aid = ctx.aid("hb.cancel")
    _start_and_await_started(ctx, aid, "hb_progress", "--heartbeat-timeout", "5s")
    ctx.record("cancel", ctx.cli.act("cancel", "--activity-id", aid, "--reason", "stop"))
    st = poll_terminal(ctx.cli, aid, timeout=30)
    expect_eq(st, "ACTIVITY_EXECUTION_STATUS_CANCELED", "terminal after cancel")


# ---- mixed ops during a long heartbeating attempt ------------------------
@test("hb.pause_unpause_during_heartbeat", needs_worker=True)
def t_hb_pause(ctx: TestCtx) -> None:
    """pause/unpause around a heartbeating attempt; heartbeats keep advancing and it stays running."""
    aid = ctx.aid("hb.pause")
    _start_and_await_started(ctx, aid, "hb_progress", "--heartbeat-timeout", "5s")
    n0 = int(ctx.cli.describe(aid).get("totalHeartbeatCount", 0) or 0)
    ctx.record("pause", ctx.cli.act("pause", "--activity-id", aid, "--reason", "p"))
    expect_run_state(ctx.cli.describe(aid), "PAUSE_REQUESTED")
    ctx.record("unpause", ctx.cli.act("unpause", "--activity-id", aid))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_STARTED", timeout=15), "did not return to STARTED")
    expect(poll_until(lambda: int(ctx.cli.describe(aid).get("totalHeartbeatCount", 0) or 0) > n0,
                      timeout=25), "heartbeats stalled across pause/unpause")
    _terminate(ctx, aid)


@test("hb.update_options_during_heartbeat", needs_worker=True)
def t_hb_update(ctx: TestCtx) -> None:
    """update-options applies to a running heartbeating attempt without stalling heartbeats."""
    aid = ctx.aid("hb.update")
    _start_and_await_started(ctx, aid, "hb_progress", "--heartbeat-timeout", "5s")
    n0 = int(ctx.cli.describe(aid).get("totalHeartbeatCount", 0) or 0)
    ctx.record("update", ctx.cli.act("update-options", "--activity-id", aid,
               "--heartbeat-timeout", "8s"))
    expect_eq(ctx.cli.describe(aid)["heartbeatTimeout"], "8s", "updated heartbeatTimeout")
    expect(poll_until(lambda: int(ctx.cli.describe(aid).get("totalHeartbeatCount", 0) or 0) > n0,
                      timeout=25), "heartbeats stalled after update-options")
    _terminate(ctx, aid)


@test("hb.reset_during_heartbeat", needs_worker=True)
def t_hb_reset(ctx: TestCtx) -> None:
    """reset of a heartbeating STARTED attempt leaves it running (deferred); state stays consistent."""
    aid = ctx.aid("hb.reset")
    _start_and_await_started(ctx, aid, "hb_progress", "--heartbeat-timeout", "30s")
    r = ctx.record("reset", ctx.cli.act("reset", "--activity-id", aid, "--reset-heartbeats"))
    expect(r.ok, f"reset failed: {r.stderr}")
    d = ctx.cli.describe(aid)
    assert_state_consistent(d, "reset during heartbeat")
    expect_status(d, "RUNNING")
    _terminate(ctx, aid)


# ---- deterministic precedence (confident assertions) ---------------------
@test("prec.cancel_on_terminated_rejected")
def t_prec_cancel_term(ctx: TestCtx) -> None:
    """cancel after terminate is rejected; stays TERMINATED."""
    aid = ctx.aid("prec.cancelterm")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    ctx.record("terminate", ctx.cli.act("terminate", "--activity-id", aid, "--reason", "x"))
    r = ctx.record("cancel", ctx.cli.act("cancel", "--activity-id", aid, "--reason", "y"))
    expect_cli_error(r, "cancel after terminate")
    assert_clean_errors([r], "cancel after terminate")
    expect_status(ctx.cli.describe(aid), "TERMINATED")


@test("prec.complete_on_terminated_rejected")
def t_prec_complete_term(ctx: TestCtx) -> None:
    """complete-by-id after terminate is rejected."""
    aid = ctx.aid("prec.completeterm")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    ctx.record("terminate", ctx.cli.act("terminate", "--activity-id", aid, "--reason", "x"))
    r = ctx.record("complete", ctx.cli.act("complete", "--activity-id", aid, "--result", '"z"'))
    expect_cli_error(r, "complete after terminate")
    assert_clean_errors([r], "complete after terminate")


@test("prec.double_terminate_absorbing")
def t_prec_double_term(ctx: TestCtx) -> None:
    """terminating twice is harmless (clean) and the state stays TERMINATED."""
    aid = ctx.aid("prec.doubleterm")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    r1 = ctx.record("terminate1", ctx.cli.act("terminate", "--activity-id", aid, "--reason", "a"))
    expect(r1.ok, f"first terminate failed: {r1.stderr}")
    r2 = ctx.record("terminate2", ctx.cli.act("terminate", "--activity-id", aid, "--reason", "b"))
    assert_clean_errors([r2], "second terminate")
    expect_status(ctx.cli.describe(aid), "TERMINATED")


@test("prec.pause_on_cancel_requested_rejected", needs_worker=True)
def t_prec_pause_cancelreq(ctx: TestCtx) -> None:
    """pause is rejected while a STARTED activity is CANCEL_REQUESTED."""
    aid = ctx.aid("prec.pausecancelreq")
    _start_and_await_started(ctx, aid, "block", "--heartbeat-timeout", "60s")
    ctx.record("cancel", ctx.cli.act("cancel", "--activity-id", aid, "--reason", "c"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_CANCEL_REQUESTED", timeout=10),
           "did not reach CANCEL_REQUESTED")
    r = ctx.record("pause", ctx.cli.act("pause", "--activity-id", aid, "--reason", "p"))
    expect_cli_error(r, "pause during cancel-requested")
    assert_clean_errors([r], "pause during cancel-requested")
    _terminate(ctx, aid)


@test("prec.update_on_cancel_requested_applies", needs_worker=True)
def t_prec_update_cancelreq(ctx: TestCtx) -> None:
    """update-options is accepted while a STARTED activity is CANCEL_REQUESTED."""
    aid = ctx.aid("prec.updcancelreq")
    _start_and_await_started(ctx, aid, "block", "--heartbeat-timeout", "60s")
    ctx.record("cancel", ctx.cli.act("cancel", "--activity-id", aid, "--reason", "c"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_CANCEL_REQUESTED", timeout=10),
           "did not reach CANCEL_REQUESTED")
    r = ctx.record("update", ctx.cli.act("update-options", "--activity-id", aid,
                   "--heartbeat-timeout", "15s"))
    expect(r.ok, f"update during cancel-requested should succeed: {r.stderr}")
    expect_eq(ctx.cli.describe(aid)["heartbeatTimeout"], "15s", "updated during cancel-requested")
    _terminate(ctx, aid)


@test("prec.terminate_wins_over_pause_requested", needs_worker=True)
def t_prec_term_pausereq(ctx: TestCtx) -> None:
    """terminate forcefully ends an activity that is PAUSE_REQUESTED."""
    aid = ctx.aid("prec.termpausereq")
    _start_and_await_started(ctx, aid, "block", "--heartbeat-timeout", "60s")
    ctx.record("pause", ctx.cli.act("pause", "--activity-id", aid, "--reason", "p"))
    expect_run_state(ctx.cli.describe(aid), "PAUSE_REQUESTED")
    r = ctx.record("terminate", ctx.cli.act("terminate", "--activity-id", aid, "--reason", "t"))
    expect(r.ok, f"terminate failed: {r.stderr}")
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_TERMINATED", timeout=10), "did not TERMINATE")


# ---- chaos: concurrent racing operations (invariant assertions) ----------
@test("chaos.cancel_terminate_race", needs_worker=True)
def t_chaos_cancel_term(ctx: TestCtx) -> None:
    """fire cancel and terminate concurrently: end terminal in {CANCELED,TERMINATED}, stable, no crash."""
    aid = ctx.aid("chaos.ct")
    _start_and_await_started(ctx, aid, "block", "--heartbeat-timeout", "5s")
    results = ctx.cli.parallel([
        ["cancel", "--activity-id", aid, "--reason", "c"],
        ["terminate", "--activity-id", aid, "--reason", "t"],
    ])
    for r in results:
        ctx._ops.append(f"$ {' '.join(r.args)} -> exit={r.code} {(r.stderr or r.stdout).strip()[:200]}")
    assert_clean_errors(results, "cancel/terminate race")
    st = poll_terminal(ctx.cli, aid, timeout=30)
    expect(st in {"ACTIVITY_EXECUTION_STATUS_CANCELED", "ACTIVITY_EXECUTION_STATUS_TERMINATED"},
           f"unexpected terminal {st!r}")
    assert_absorbing(ctx.cli, aid, "cancel/terminate race")


@test("chaos.op_storm_started", needs_worker=True)
def t_chaos_storm(ctx: TestCtx) -> None:
    """storm a STARTED activity with pause/unpause/reset/update/cancel/terminate at once."""
    aid = ctx.aid("chaos.storm")
    _start_and_await_started(ctx, aid, "block", "--heartbeat-timeout", "5s")
    results = ctx.cli.parallel([
        ["pause", "--activity-id", aid, "--reason", "p"],
        ["unpause", "--activity-id", aid],
        ["reset", "--activity-id", aid, "--reset-heartbeats"],
        ["update-options", "--activity-id", aid, "--heartbeat-timeout", "7s"],
        ["cancel", "--activity-id", aid, "--reason", "c"],
        ["terminate", "--activity-id", aid, "--reason", "t"],
    ])
    for r in results:
        ctx._ops.append(f"$ {' '.join(r.args)} -> exit={r.code} {(r.stderr or r.stdout).strip()[:160]}")
    assert_clean_errors(results, "op storm")
    # terminate/cancel are in the mix, so the activity must converge to terminal
    st = poll_terminal(ctx.cli, aid, timeout=40)
    expect(st in TERMINAL, f"storm did not converge to terminal (got {st!r})")
    assert_absorbing(ctx.cli, aid, "op storm")


@test("chaos.scheduled_op_storm", needs_worker=False)
def t_chaos_storm_sched(ctx: TestCtx) -> None:
    """storm a SCHEDULED (no-worker) activity; converges to terminal, consistent, no crash."""
    aid = ctx.aid("chaos.schedstorm")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "T",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "120s"))
    results = ctx.cli.parallel([
        ["pause", "--activity-id", aid, "--reason", "p"],
        ["unpause", "--activity-id", aid],
        ["reset", "--activity-id", aid, "--reset-attempts"],
        ["update-options", "--activity-id", aid, "--start-to-close-timeout", "90s"],
        ["cancel", "--activity-id", aid, "--reason", "c"],
        ["terminate", "--activity-id", aid, "--reason", "t"],
    ])
    for r in results:
        ctx._ops.append(f"$ {' '.join(r.args)} -> exit={r.code} {(r.stderr or r.stdout).strip()[:160]}")
    assert_clean_errors(results, "scheduled storm")
    st = poll_terminal(ctx.cli, aid, timeout=20)
    expect(st in TERMINAL, f"scheduled storm did not converge to terminal (got {st!r})")
    assert_absorbing(ctx.cli, aid, "scheduled storm")


@test("chaos.pause_unpause_flood", needs_worker=True)
def t_chaos_flood(ctx: TestCtx) -> None:
    """flood with alternating pause/unpause; no crash, state stays consistent, cleans up."""
    aid = ctx.aid("chaos.flood")
    _start_and_await_started(ctx, aid, "block", "--heartbeat-timeout", "30s")
    cmds = []
    for i in range(10):
        cmds.append(["pause", "--activity-id", aid, "--reason", f"p{i}"] if i % 2 == 0
                    else ["unpause", "--activity-id", aid])
    results = ctx.cli.parallel(cmds)
    assert_clean_errors(results, "pause/unpause flood")
    d = ctx.cli.describe(aid)
    assert_state_consistent(d, "pause/unpause flood")
    expect_status(d, "RUNNING")
    _terminate(ctx, aid)
    expect(poll_terminal(ctx.cli, aid, timeout=15) == "ACTIVITY_EXECUTION_STATUS_TERMINATED",
           "flood: terminate did not take")


# ---- worker crashes & resumes --------------------------------------------
@test("crash.heartbeat_checkpoint_survives_worker_crash", needs_worker=True)
def t_crash_checkpoint(ctx: TestCtx) -> None:
    """SIGKILL the worker mid-heartbeat; after timeout+retry, the restarted worker
    resumes with the pre-crash heartbeat checkpoint and completes."""
    assert ctx.worker is not None
    aid = ctx.aid("crash.ckpt")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "crash_resume",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "300s",
               "--heartbeat-timeout", "5s", "--retry-initial-interval", "1s",
               "--retry-maximum-attempts", "5"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_STARTED", timeout=25), "did not reach STARTED")
    expect(poll_until(lambda: bool(ctx.cli.describe(aid).get("heartbeatDetails")), timeout=15),
           "no checkpoint heartbeat before crash")
    ctx.log.line("  crashing worker (SIGKILL)")
    ctx.worker.crash()
    ctx.log.line("  restarting worker")
    ctx.worker.start()
    ctx.worker.wait_ready(ctx.cli)
    # heartbeat timeout (5s) -> retry -> restarted worker resumes on attempt 2+
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_COMPLETED", timeout=60),
           "activity did not complete after worker crash+restart")
    r = ctx.record("result", ctx.cli.act("result", "--activity-id", aid, "-o", "json", timeout=20))
    expect("survived-crash" in r.stdout, f"checkpoint lost across worker crash: {r.stdout!r}")
    expect(ctx.cli.describe(aid)["attempt"] >= 2, "expected a retry attempt after crash")


@test("crash.manual_complete_after_worker_crash", needs_worker=True)
def t_crash_manual(ctx: TestCtx) -> None:
    """after the worker crashes mid-attempt, manual complete-by-id still drives COMPLETED."""
    assert ctx.worker is not None
    aid = ctx.aid("crash.manual")
    # large heartbeat timeout so the attempt stays STARTED after the crash
    _start_and_await_started(ctx, aid, "block", "--heartbeat-timeout", "120s")
    expect(poll_until(lambda: int(ctx.cli.describe(aid).get("totalHeartbeatCount", 0) or 0) >= 1,
                      timeout=15), "no heartbeat before crash")
    ctx.log.line("  crashing worker (SIGKILL)")
    ctx.worker.crash()
    # attempt is still STARTED server-side; manual completion must still work
    r = ctx.record("complete", ctx.cli.act("complete", "--activity-id", aid, "--result", '"manual-after-crash"'))
    expect(r.ok, f"manual complete after crash failed: {r.stderr}")
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_COMPLETED", timeout=15),
           "did not COMPLETE via manual completion after crash")
    ctx.log.line("  restarting worker for subsequent tests")
    ctx.worker.start()
    ctx.worker.wait_ready(ctx.cli)


@test("crash.retry_redispatch_after_crash", needs_worker=True)
def t_crash_redispatch(ctx: TestCtx) -> None:
    """a flaky activity that fails once then blocks: crash during the blocking attempt,
    and the restarted worker picks up the eventual retry and completes it."""
    assert ctx.worker is not None
    aid = ctx.aid("crash.redispatch")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "crash_resume",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "300s",
               "--heartbeat-timeout", "5s", "--retry-initial-interval", "1s",
               "--retry-maximum-attempts", "5"))
    expect(poll_until(lambda: bool(ctx.cli.describe(aid).get("heartbeatDetails")), timeout=25),
           "did not start heartbeating")
    a0 = ctx.cli.describe(aid)["attempt"]
    ctx.worker.crash()
    ctx.worker.start()
    ctx.worker.wait_ready(ctx.cli)
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_COMPLETED", timeout=60),
           "did not complete after redispatch")
    expect(ctx.cli.describe(aid)["attempt"] > a0, "expected attempt to advance after crash")


# ==========================================================================
# AMBITIOUS cross-API combinations: task-queue migration, retry-backoff window
# operations, client long-poll racing terminal transitions, fan-out.
# ==========================================================================
@test("tq.migration_redispatch", needs_worker=True)
def t_tq_migration(ctx: TestCtx) -> None:
    """start on an idle queue, then update-options the task queue to a queue that
    has a worker; the activity must be redispatched there and complete."""
    aid = ctx.aid("tq.migrate")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "echo",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s", "--input", '"moved"'))
    expect_run_state(ctx.cli.describe(aid), "SCHEDULED")
    ctx.record("migrate", ctx.cli.act("update-options", "--activity-id", aid,
               "--task-queue", WORKER_TQ))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_COMPLETED", timeout=30),
           "activity not redispatched/completed after task-queue migration")


@test("backoff.cancel_during_retry", needs_worker=True)
def t_backoff_cancel(ctx: TestCtx) -> None:
    """cancel an activity while it is waiting in retry backoff (SCHEDULED) -> CANCELED."""
    aid = ctx.aid("backoff.cancel")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "boom",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "30s",
               "--retry-initial-interval", "15s", "--retry-maximum-attempts", "5"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_SCHEDULED"
                      and ctx.cli.describe(aid).get("attempt") >= 2, timeout=20),
           "did not enter retry backoff")
    ctx.record("cancel", ctx.cli.act("cancel", "--activity-id", aid, "--reason", "stop"))
    st = poll_terminal(ctx.cli, aid, timeout=15)
    expect_eq(st, "ACTIVITY_EXECUTION_STATUS_CANCELED", "cancel during backoff")


@test("backoff.pause_unpause_during_retry", needs_worker=True)
def t_backoff_pause(ctx: TestCtx) -> None:
    """pause during retry backoff then unpause; the activity resumes and (flaky) completes."""
    aid = ctx.aid("backoff.pause")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "flaky",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "30s",
               "--retry-initial-interval", "20s", "--retry-maximum-attempts", "5"))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("attempt") >= 2
                      and ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_SCHEDULED", timeout=20), "did not enter backoff")
    ctx.record("pause", ctx.cli.act("pause", "--activity-id", aid, "--reason", "p"))
    expect_run_state(ctx.cli.describe(aid), "PAUSED")
    ctx.record("unpause", ctx.cli.act("unpause", "--activity-id", aid))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_COMPLETED", timeout=30),
           "did not complete after pause/unpause during backoff")


@test("longpoll.concurrent_result", needs_worker=True)
def t_concurrent_result(ctx: TestCtx) -> None:
    """three concurrent `result` long-polls all observe the same manual completion."""
    aid = ctx.aid("lp.concurrent")
    _start_and_await_started(ctx, aid, "block", "--heartbeat-timeout", "30s")
    args = ["result", "--activity-id", aid, "-o", "json"]
    pollers = [(args, ctx.cli.spawn(*args)) for _ in range(3)]
    time.sleep(1.0)  # ensure all three are blocked in the long-poll
    r = ctx.record("complete", ctx.cli.act("complete", "--activity-id", aid, "--result", '"shared"'))
    expect(r.ok, f"complete failed: {r.stderr}")
    outs = [ctx.cli.wait(p, a, timeout=20) for a, p in pollers]
    for i, o in enumerate(outs):
        expect(o.ok, f"long-poller {i} failed: {o.stderr}")
        expect("shared" in o.stdout, f"long-poller {i} missing result: {o.stdout!r}")


@test("longpoll.result_observes_cancel", needs_worker=True)
def t_result_cancel(ctx: TestCtx) -> None:
    """a `result` long-poll in flight observes a cancellation as a non-zero exit."""
    aid = ctx.aid("lp.cancel")
    _start_and_await_started(ctx, aid, "block", "--heartbeat-timeout", "5s")
    args = ["result", "--activity-id", aid]
    p = ctx.cli.spawn(*args)
    time.sleep(1.0)
    ctx.record("cancel", ctx.cli.act("cancel", "--activity-id", aid, "--reason", "stop"))
    o = ctx.cli.wait(p, args, timeout=30)
    expect(not o.ok, f"result of a canceled activity should exit non-zero; got {o.stdout!r}")
    expect_eq(ctx.cli.describe(aid).get("status"), "ACTIVITY_EXECUTION_STATUS_CANCELED", "final")


@test("longpoll.result_observes_terminate", needs_worker=True)
def t_result_terminate(ctx: TestCtx) -> None:
    """a `result` long-poll in flight observes a terminate as a non-zero exit."""
    aid = ctx.aid("lp.term")
    _start_and_await_started(ctx, aid, "block", "--heartbeat-timeout", "30s")
    args = ["result", "--activity-id", aid]
    p = ctx.cli.spawn(*args)
    time.sleep(1.0)
    ctx.record("terminate", ctx.cli.act("terminate", "--activity-id", aid, "--reason", "stop"))
    o = ctx.cli.wait(p, args, timeout=20)
    expect(not o.ok, f"result of a terminated activity should exit non-zero; got {o.stdout!r}")
    expect_eq(ctx.cli.describe(aid).get("status"), "ACTIVITY_EXECUTION_STATUS_TERMINATED", "final")


@test("pause.scheduled_then_migrate_then_unpause_runs", needs_worker=True)
def t_pause_migrate_unpause(ctx: TestCtx) -> None:
    """pause a SCHEDULED idle activity, migrate its queue to a worker queue, unpause -> completes."""
    aid = ctx.aid("pause.migrate")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "echo",
               "--task-queue", IDLE_TQ, "--start-to-close-timeout", "60s", "--input", '"go"'))
    ctx.record("pause", ctx.cli.act("pause", "--activity-id", aid, "--reason", "p"))
    expect_run_state(ctx.cli.describe(aid), "PAUSED")
    ctx.record("migrate", ctx.cli.act("update-options", "--activity-id", aid, "--task-queue", WORKER_TQ))
    ctx.record("unpause", ctx.cli.act("unpause", "--activity-id", aid))
    expect(poll_until(lambda: ctx.cli.describe(aid).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_COMPLETED", timeout=30),
           "did not complete after pause+migrate+unpause")


@test("fanout.many_concurrent", needs_worker=True)
def t_fanout(ctx: TestCtx) -> None:
    """start 30 activities concurrently; all complete (by id) and visibility scales."""
    n = 30
    ids = [ctx.aid("fan", str(i)) for i in range(n)]
    ctx.cli.parallel([["start", "--activity-id", a, "--type", "echo", "--task-queue", WORKER_TQ,
                       "--start-to-close-timeout", "30s", "--input", '"x"'] for a in ids])
    expect(poll_until(lambda: all(ctx.cli.describe(a).get("status")
                      == "ACTIVITY_EXECUTION_STATUS_COMPLETED" for a in ids), timeout=60),
           "not all fan-out activities completed")
    # echo is shared with other tests, so the count is a lower-bound sanity check
    expect(poll_until(lambda: _count_val(ctx.cli.act("count", "--query",
                      "ActivityType = 'echo'", "-o", "json")) >= n, timeout=20),
           "visibility count did not reflect the fan-out")


@test("reset.reset_heartbeats_clears_checkpoint", needs_worker=True)
def t_reset_heartbeats_clears(ctx: TestCtx) -> None:
    """reset --reset-heartbeats clears the heartbeat checkpoint. To observe the clear without a
    live worker immediately re-running the (attempt-1) retry and re-recording the same checkpoint,
    migrate the task queue to an idle queue first, then reset, then assert describe shows it
    cleared."""
    aid = ctx.aid("reset.hbclear")
    ctx.record("start", ctx.cli.act("start", "--activity-id", aid, "--type", "hb_checkpoint",
               "--task-queue", WORKER_TQ, "--start-to-close-timeout", "30s",
               "--heartbeat-timeout", "25s", "--retry-initial-interval", "30s",
               "--retry-maximum-attempts", "4"))
    # wait until attempt 1 has failed and we're in backoff with the checkpoint recorded
    expect(poll_until(lambda: bool(ctx.cli.describe(aid).get("heartbeatDetails"))
                      and ctx.cli.describe(aid).get("runState")
                      == "PENDING_ACTIVITY_STATE_SCHEDULED", timeout=20),
           "did not reach backoff with a checkpoint")
    # move to an idle queue so the post-reset re-dispatch is not picked up and re-heartbeated
    ctx.record("migrate", ctx.cli.act("update-options", "--activity-id", aid,
               "--task-queue", IDLE_TQ))
    ctx.record("reset", ctx.cli.act("reset", "--activity-id", aid, "--reset-heartbeats"))
    # describe must now show the checkpoint cleared
    expect(poll_until(lambda: not ctx.cli.describe(aid).get("heartbeatDetails"), timeout=10),
           f"reset --reset-heartbeats did not clear heartbeatDetails: "
           f"{ctx.cli.describe(aid).get('heartbeatDetails')!r}")


# ==========================================================================
# Runner
# ==========================================================================
def run_tests(args: argparse.Namespace) -> int:
    if args.fresh:
        if LOG_PATH.exists():
            LOG_PATH.unlink()
        for f in BUGS_DIR.glob("*.md"):
            f.unlink()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    BUGS_DIR.mkdir(parents=True, exist_ok=True)

    prior = completed_results(LOG_PATH)
    only = set(args.only.split(",")) if args.only else None

    selected: list[Test] = []
    for t in REGISTRY:
        if only and t.id not in only:
            continue
        if args.rerun_failed and prior.get(t.id) == "PASS":
            continue
        if not args.rerun_failed and not only and t.id in prior:
            continue  # resume: skip already-completed
        selected.append(t)

    log = RunLog(LOG_PATH)
    log.line(f"=== SAA test run: {len(selected)} test(s) selected "
             f"({len(REGISTRY)} total, {len(prior)} previously recorded) ===")

    external = args.address is not None
    server = None
    address = args.address or "localhost:7239"
    if not external:
        server = DevServer(7239)
        log.line(f"starting dev server on {address} (db {DB_PATH.name})")
        server.start()

    cli = CLI(address)
    worker = None
    passed = failed = errored = 0
    try:
        if server:
            server.wait_ready(cli)
            log.line("dev server ready")

        need_worker = any(t.needs_worker for t in selected)
        if need_worker:
            log.line("starting SDK worker")
            worker = WorkerProc(address)
            worker.start()
            worker.wait_ready(cli)
            log.line("worker ready")

        for t in selected:
            # a prior crash test may have left the shared worker down; restore it
            if t.needs_worker and worker and not worker.alive():
                log.line("worker not alive; restarting before test")
                worker.start()
                worker.wait_ready(cli)
            ctx = TestCtx(cli=cli, log=log, worker=worker)
            log.line(f"RUN  {t.id}")
            try:
                t.fn(ctx)
            except TestFailure as e:
                failed += 1
                bug = write_bug(t, "FAIL", str(e), ctx)
                log.result(t.id, "FAIL", f"-> {bug.name}")
            except Exception:
                errored += 1
                bug = write_bug(t, "ERROR", traceback.format_exc(), ctx)
                log.result(t.id, "ERROR", f"-> {bug.name}")
            else:
                passed += 1
                log.result(t.id, "PASS")
    finally:
        if worker:
            worker.stop()
        if server:
            server.stop()
        log.line(f"=== done: {passed} passed, {failed} failed, {errored} errored ===")
        log.close()

    return 1 if (failed or errored) else 0


def main() -> int:
    p = argparse.ArgumentParser(description="SAA CLI test harness")
    sub = p.add_subparsers(dest="cmd")

    w = sub.add_parser("worker", help="run the SDK activity worker")
    w.add_argument("--address", required=True)
    w.add_argument("--task-queue", default=WORKER_TQ)

    p.add_argument("--address", help="use an already-running server instead of start-dev")
    p.add_argument("--only", help="comma-separated test ids to run")
    p.add_argument("--fresh", action="store_true", help="wipe prior log/bugs first")
    p.add_argument("--rerun-failed", action="store_true",
                   help="re-run everything except previously-PASSed tests")
    p.add_argument("--list", action="store_true", help="list test ids and exit")

    args = p.parse_args()

    if args.cmd == "worker":
        run_worker(args.address, args.task_queue)
        return 0
    if args.list:
        for t in REGISTRY:
            print(f"{t.id}{'  [worker]' if t.needs_worker else ''}")
        return 0
    if not CLI_BIN.exists():
        print(f"CLI binary not found at {CLI_BIN}; build it first "
              f"(go build -o temporal-saa ./cmd/temporal).", file=sys.stderr)
        return 2
    return run_tests(args)


if __name__ == "__main__":
    sys.exit(main())
