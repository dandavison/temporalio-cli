#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = ["temporalio"]
# ///
"""
Activity state-machine transition coverage for the Standalone-Activity surface.

Drives an activity into every state of the CHASM Activity state machine
(chasm/lib/activity/statemachine.go) and then applies every operator command,
plus the worker- and time-driven events, observing the resulting state via
`describe`. This is the *exploratory* pass requested: it prints what happens for
each transition rather than asserting, so the real behaviour can be eyeballed
before it is frozen into assertions.

Output is two-level:

    From <State>
       To <State>: <trigger> — <one-line description>

State is the observable state from `describe`: `status` collapses the six
"running" sub-states to RUNNING, so the sub-state is read from `runState`
(PENDING_ACTIVITY_STATE_*). Note RESET_REQUESTED has no public runState variant
and surfaces as STARTED; cells that set it up are annotated accordingly.

Reuses the server/worker/CLI plumbing from saa_test.py. Starts its own dev
server + worker unless --address points at an existing one.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import saa_test as h  # noqa: E402

REPORT_PATH = SCRIPT_DIR / "transitions.md"


# --------------------------------------------------------------------------
# Observable state
# --------------------------------------------------------------------------
def observe(cli: h.CLI, aid: str) -> str:
    """Collapse describe() into a single state label, or '<error>' if missing."""
    try:
        d = cli.describe(aid)
    except AssertionError:
        return "<not-found>"
    status = (d.get("status") or "").replace("ACTIVITY_EXECUTION_STATUS_", "")
    if status == "RUNNING":
        return (d.get("runState") or "").replace(
            "PENDING_ACTIVITY_STATE_", ""
        ) or "RUNNING"
    return status or "<unknown>"


def settle(cli: h.CLI, aid: str, before: str, timeout: float) -> str:
    """Return the observed state once it differs from `before`, else after timeout."""
    deadline = time.time() + timeout
    last = observe(cli, aid)
    while time.time() < deadline:
        last = observe(cli, aid)
        if last != before:
            return last
        time.sleep(0.25)
    return last


def err1(r: h.CmdResult) -> str:
    msg = (r.stderr or r.stdout).strip().splitlines()
    line = msg[0] if msg else "(no message)"
    return line.removeprefix("Error: ").strip()[:160]


# --------------------------------------------------------------------------
# State setup: drive a fresh activity into each source state
# --------------------------------------------------------------------------
@dataclass
class Ctx:
    cli: h.CLI
    n: int = 0

    def aid(self, tag: str) -> str:
        self.n += 1
        return f"tm-{tag}-{self.n}-{int(time.time() * 1000) % 1000000}"


def _start(ctx: Ctx, aid: str, atype: str, tq: str, *extra: str) -> h.CmdResult:
    return ctx.cli.act(
        "start", "--activity-id", aid, "--type", atype, "--task-queue", tq, *extra
    )


def _await(ctx: Ctx, aid: str, state: str, timeout: float = 25.0) -> None:
    if not h.poll_until(lambda: observe(ctx.cli, aid) == state, timeout=timeout):
        raise RuntimeError(
            f"{aid}: did not reach {state} (got {observe(ctx.cli, aid)})"
        )


def setup_scheduled(ctx: Ctx) -> str:
    aid = ctx.aid("sched")
    _start(ctx, aid, "T", h.IDLE_TQ, "--start-to-close-timeout", "300s")
    _await(ctx, aid, "SCHEDULED")
    return aid


def setup_started(ctx: Ctx, hb: Optional[str] = None, max_attempts: str = "1") -> str:
    aid = ctx.aid("started")
    extra = [
        "--start-to-close-timeout",
        "300s",
        "--retry-maximum-attempts",
        max_attempts,
    ]
    if hb:
        extra += ["--heartbeat-timeout", hb]
    _start(ctx, aid, "noheartbeat", h.WORKER_TQ, *extra)
    _await(ctx, aid, "STARTED")
    return aid


def setup_cancel_requested(ctx: Ctx) -> str:
    aid = setup_started(ctx)
    ctx.cli.act("cancel", "--activity-id", aid, "--reason", "setup")
    _await(ctx, aid, "CANCEL_REQUESTED")
    return aid


def setup_paused(ctx: Ctx) -> str:
    aid = setup_scheduled(ctx)
    ctx.cli.act("pause", "--activity-id", aid, "--reason", "setup")
    _await(ctx, aid, "PAUSED")
    return aid


def setup_pause_requested(ctx: Ctx) -> str:
    aid = setup_started(ctx)
    ctx.cli.act("pause", "--activity-id", aid, "--reason", "setup")
    _await(ctx, aid, "PAUSE_REQUESTED")
    return aid


def setup_reset_requested(ctx: Ctx) -> str:
    # reset of a STARTED activity defers to RESET_REQUESTED; it surfaces as STARTED.
    aid = setup_started(ctx)
    r = ctx.cli.act("reset", "--activity-id", aid, "--reset-attempts")
    if not r.ok:
        raise RuntimeError(f"{aid}: reset to set up RESET_REQUESTED failed: {err1(r)}")
    _await(ctx, aid, "STARTED")
    return aid


def setup_completed(ctx: Ctx) -> str:
    aid = ctx.aid("completed")
    _start(
        ctx,
        aid,
        "echo",
        h.WORKER_TQ,
        "--start-to-close-timeout",
        "30s",
        "--input",
        '"x"',
    )
    _await(ctx, aid, "COMPLETED")
    return aid


def setup_failed(ctx: Ctx) -> str:
    aid = ctx.aid("failed")
    _start(
        ctx,
        aid,
        "boom",
        h.WORKER_TQ,
        "--start-to-close-timeout",
        "30s",
        "--retry-maximum-attempts",
        "1",
    )
    _await(ctx, aid, "FAILED")
    return aid


def setup_canceled(ctx: Ctx) -> str:
    aid = setup_scheduled(ctx)
    ctx.cli.act("cancel", "--activity-id", aid, "--reason", "setup")
    _await(ctx, aid, "CANCELED")
    return aid


def setup_terminated(ctx: Ctx) -> str:
    aid = setup_scheduled(ctx)
    ctx.cli.act("terminate", "--activity-id", aid, "--reason", "setup")
    _await(ctx, aid, "TERMINATED")
    return aid


def setup_timed_out(ctx: Ctx) -> str:
    aid = ctx.aid("timedout")
    _start(
        ctx,
        aid,
        "T",
        h.IDLE_TQ,
        "--schedule-to-close-timeout",
        "3s",
        "--retry-maximum-attempts",
        "1",
    )
    _await(ctx, aid, "TIMED_OUT")
    return aid


SOURCES: list[tuple[str, Callable[[Ctx], str]]] = [
    ("SCHEDULED", setup_scheduled),
    ("STARTED", lambda c: setup_started(c)),
    ("CANCEL_REQUESTED", setup_cancel_requested),
    ("PAUSED", setup_paused),
    ("PAUSE_REQUESTED", setup_pause_requested),
    ("RESET_REQUESTED (obs STARTED)", setup_reset_requested),
    ("COMPLETED", setup_completed),
    ("FAILED", setup_failed),
    ("CANCELED", setup_canceled),
    ("TERMINATED", setup_terminated),
    ("TIMED_OUT", setup_timed_out),
]


# --------------------------------------------------------------------------
# Operator commands applied at every source state
# --------------------------------------------------------------------------
@dataclass
class Action:
    verb: str
    apply: Callable[[Ctx, str], h.CmdResult]


ACTIONS: list[Action] = [
    Action(
        "cancel", lambda c, a: c.cli.act("cancel", "--activity-id", a, "--reason", "c")
    ),
    Action(
        "terminate",
        lambda c, a: c.cli.act("terminate", "--activity-id", a, "--reason", "t"),
    ),
    Action(
        "pause", lambda c, a: c.cli.act("pause", "--activity-id", a, "--reason", "p")
    ),
    Action("unpause", lambda c, a: c.cli.act("unpause", "--activity-id", a)),
    Action(
        "reset", lambda c, a: c.cli.act("reset", "--activity-id", a, "--reset-attempts")
    ),
    Action(
        "complete",
        lambda c, a: c.cli.act("complete", "--activity-id", a, "--result", '"r"'),
    ),
    Action("fail", lambda c, a: c.cli.act("fail", "--activity-id", a, "--reason", "f")),
    Action(
        "update-options",
        lambda c, a: c.cli.act(
            "update-options", "--activity-id", a, "--start-to-close-timeout", "299s"
        ),
    ),
]


# --------------------------------------------------------------------------
# Worker- and time-driven transitions (not expressible as a CLI verb)
# --------------------------------------------------------------------------
def scenario_lines(ctx: Ctx) -> list[str]:
    out: list[str] = []

    def emit(frm: str, to: str, desc: str) -> None:
        out.append(f"From {frm}\n   To {to}: {desc}")

    # UNSPECIFIED -> SCHEDULED
    aid = ctx.aid("sc-start")
    _start(ctx, aid, "T", h.IDLE_TQ, "--start-to-close-timeout", "300s")
    emit("UNSPECIFIED", observe(ctx.cli, aid), "operator start: initial schedule")

    # SCHEDULED -> STARTED (worker picks up the dispatch task)
    aid = ctx.aid("sc-pickup")
    _start(
        ctx,
        aid,
        "noheartbeat",
        h.WORKER_TQ,
        "--start-to-close-timeout",
        "300s",
        "--retry-maximum-attempts",
        "1",
    )
    emit("SCHEDULED", settle(ctx.cli, aid, "SCHEDULED", 25), "worker accepts the task")

    # STARTED -> COMPLETED (worker returns a result)
    aid = ctx.aid("sc-complete")
    _start(
        ctx,
        aid,
        "echo",
        h.WORKER_TQ,
        "--start-to-close-timeout",
        "30s",
        "--input",
        '"x"',
    )
    emit(
        "STARTED", settle(ctx.cli, aid, "SCHEDULED", 25), "worker completes the attempt"
    )

    # STARTED -> FAILED (worker fails, no retries left)
    aid = ctx.aid("sc-fail")
    _start(
        ctx,
        aid,
        "boom",
        h.WORKER_TQ,
        "--start-to-close-timeout",
        "30s",
        "--retry-maximum-attempts",
        "1",
    )
    emit(
        "STARTED",
        settle(ctx.cli, aid, "SCHEDULED", 25),
        "worker fails, no retries remaining",
    )

    # STARTED -> SCHEDULED (worker fails, retries remain -> reschedule/backoff)
    aid = ctx.aid("sc-reschedule")
    _start(
        ctx,
        aid,
        "boom",
        h.WORKER_TQ,
        "--start-to-close-timeout",
        "30s",
        "--retry-initial-interval",
        "20s",
        "--retry-maximum-attempts",
        "3",
    )
    h.poll_until(lambda: ctx.cli.describe(aid).get("attempt", 1) >= 2, timeout=25)
    emit(
        "STARTED",
        observe(ctx.cli, aid),
        "worker fails, retries remain: reschedule into backoff",
    )

    # CANCEL_REQUESTED -> CANCELED (a heartbeating worker observes the cancel)
    aid = ctx.aid("sc-cancel")
    _start(
        ctx,
        aid,
        "block",
        h.WORKER_TQ,
        "--start-to-close-timeout",
        "120s",
        "--heartbeat-timeout",
        "5s",
    )
    _await(ctx, aid, "STARTED")
    ctx.cli.act("cancel", "--activity-id", aid, "--reason", "c")
    emit(
        "CANCEL_REQUESTED",
        settle(ctx.cli, aid, "CANCEL_REQUESTED", 30),
        "worker observes cancel on heartbeat and acks",
    )

    # PAUSE_REQUESTED -> STARTED (unpause)
    aid = setup_pause_requested(ctx)
    ctx.cli.act("unpause", "--activity-id", aid)
    emit(
        "PAUSE_REQUESTED",
        settle(ctx.cli, aid, "PAUSE_REQUESTED", 15),
        "unpause while worker still holds the attempt",
    )

    # PAUSE_REQUESTED -> PAUSED (heartbeat timeout fires, retries remain)
    aid = setup_started(ctx, hb="6s", max_attempts="3")
    ctx.cli.act("pause", "--activity-id", aid, "--reason", "p")
    _await(ctx, aid, "PAUSE_REQUESTED")
    emit(
        "PAUSE_REQUESTED",
        settle(ctx.cli, aid, "PAUSE_REQUESTED", 30),
        "attempt yields (heartbeat timeout), retries remain: lands paused",
    )

    # RESET_REQUESTED -> SCHEDULED (heartbeat timeout fires, retries remain)
    aid = setup_started(ctx, hb="6s", max_attempts="3")
    ctx.cli.act("reset", "--activity-id", aid, "--reset-attempts")
    _await(ctx, aid, "STARTED")  # reset-requested surfaces as STARTED
    emit(
        "RESET_REQUESTED (obs STARTED)",
        settle(ctx.cli, aid, "STARTED", 30),
        "attempt yields, retries remain: lands scheduled at attempt 1",
    )

    # RESET_REQUESTED -> PAUSED (reset --keep-paused from PAUSE_REQUESTED, then yield)
    aid = setup_started(ctx, hb="6s", max_attempts="3")
    ctx.cli.act("pause", "--activity-id", aid, "--reason", "p")
    _await(ctx, aid, "PAUSE_REQUESTED")
    ctx.cli.act("reset", "--activity-id", aid, "--keep-paused")
    emit(
        "RESET_REQUESTED (keep-paused)",
        settle(ctx.cli, aid, "STARTED", 30),
        "reset with --keep-paused, attempt yields: lands paused",
    )

    # SCHEDULED -> TIMED_OUT (schedule-to-close)
    aid = ctx.aid("sc-s2c")
    _start(
        ctx,
        aid,
        "T",
        h.IDLE_TQ,
        "--schedule-to-close-timeout",
        "3s",
        "--retry-maximum-attempts",
        "1",
    )
    emit(
        "SCHEDULED",
        settle(ctx.cli, aid, "SCHEDULED", 15),
        "schedule-to-close timeout, no retries",
    )

    # STARTED -> TIMED_OUT (start-to-close)
    aid = ctx.aid("sc-s2cl")
    _start(
        ctx,
        aid,
        "noheartbeat",
        h.WORKER_TQ,
        "--start-to-close-timeout",
        "4s",
        "--retry-maximum-attempts",
        "1",
    )
    _await(ctx, aid, "STARTED")
    emit(
        "STARTED",
        settle(ctx.cli, aid, "STARTED", 20),
        "start-to-close timeout, no retries",
    )

    return out


# --------------------------------------------------------------------------
# Runner
# --------------------------------------------------------------------------
def run_matrix(ctx: Ctx) -> list[str]:
    lines: list[str] = []
    for src_label, setup in SOURCES:
        lines.append(f"From {src_label}")
        for act in ACTIONS:
            try:
                aid = setup(ctx)
            except Exception as e:  # noqa: BLE001
                lines.append(f"   To <setup-failed>: {act.verb} — {e}")
                continue
            before = observe(ctx.cli, aid)
            r = act.apply(ctx, aid)
            after = settle(ctx.cli, aid, before, timeout=3.0)
            verdict = "accepted" if r.ok else f"rejected: {err1(r)}"
            lines.append(f"   To {after}: {act.verb} — {verdict}")
    return lines


def render(matrix: list[str], scenarios: list[str]) -> str:
    body = [
        "# Activity state-machine transition coverage",
        "",
        "Observable state from `describe` (`status` + `runState`). "
        "RESET_REQUESTED has no public `runState` and surfaces as STARTED.",
        "",
        "## Operator commands at every source state",
        "",
        "```",
    ]
    body += matrix
    body += ["```", "", "## Worker- and time-driven transitions", "", "```"]
    # scenarios is a list of "From..\n   To.." blocks; group consecutive same-From
    body += scenarios
    body += ["```", ""]
    return "\n".join(body)


def main() -> int:
    p = argparse.ArgumentParser(
        description="Activity transition coverage (exploratory)"
    )
    p.add_argument(
        "--address", help="use an already-running server instead of start-dev"
    )
    args = p.parse_args()

    if not h.CLI_BIN.exists():
        print(f"CLI binary not found at {h.CLI_BIN}; build it first.", file=sys.stderr)
        return 2

    external = args.address is not None
    address = args.address or "localhost:7239"
    server = None
    if not external:
        server = h.DevServer(7239)
        server.start()

    cli = h.CLI(address)
    worker = None
    try:
        if server:
            server.wait_ready(cli)
        worker = h.WorkerProc(address)
        worker.start()
        worker.wait_ready(cli)

        ctx = Ctx(cli=cli)
        matrix = run_matrix(ctx)
        scenarios = scenario_lines(ctx)
    finally:
        if worker:
            worker.stop()
        if server:
            server.stop()

    report = render(matrix, scenarios)
    REPORT_PATH.write_text(report + "\n")
    print(report)
    print(f"\n(report written to {REPORT_PATH})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
