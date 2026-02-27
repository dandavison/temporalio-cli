#!/usr/bin/env bash
# Reproduction script for standalone activity --id-conflict-policy UseExisting
# printing the wrong activity type.
#
# Prerequisites: temporal CLI built and on PATH, or run via `go run ./cmd/temporal`.
# Usage: bash docs/plans/repro-saa-conflict-type.sh
#
# Expected: second start prints Type = DevActivity (the existing activity's type)
# Actual (buggy): second start prints Type = SomeOtherType (the requested type)

set -euo pipefail

ADDR=${TEMPORAL_ADDRESS:-localhost:7233}
TQ="repro-conflict-$$"
ID="repro-conflict-type-test"

cleanup() {
  echo "--- Cleaning up ---"
  temporal activity terminate --activity-id "$ID" --address "$ADDR" --reason cleanup 2>/dev/null || true
  kill "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Start dev server if none is running.
if ! temporal operator cluster health --address "$ADDR" >/dev/null 2>&1; then
  echo "Starting dev server..."
  temporal server start-dev --headless --log-level error &
  SERVER_PID=$!
  sleep 2
fi

echo "=== Step 1: Start a long-running standalone activity ==="
echo "  ID=$ID  Type=DevActivity  TaskQueue=$TQ"
temporal activity start \
  --activity-id "$ID" \
  --type DevActivity \
  --task-queue "$TQ" \
  --start-to-close-timeout 300s \
  --address "$ADDR"

echo ""
echo "=== Step 2: Start with same ID, different type, UseExisting ==="
echo "  ID=$ID  Type=SomeOtherType  --id-conflict-policy UseExisting"
echo ""
echo "The output below should show Type=DevActivity (the EXISTING activity's type)."
echo "If the bug is present, it will instead show Type=SomeOtherType."
echo ""
temporal activity start \
  --activity-id "$ID" \
  --type SomeOtherType \
  --task-queue "$TQ" \
  --start-to-close-timeout 300s \
  --id-conflict-policy UseExisting \
  --address "$ADDR"

echo ""
echo "=== Step 3: Describe the activity for ground truth ==="
temporal activity describe \
  --activity-id "$ID" \
  --address "$ADDR"
