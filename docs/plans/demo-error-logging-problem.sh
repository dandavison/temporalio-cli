#!/usr/bin/env bash
#
# Demonstrates how the CLI conflates error reporting with logging.
#
# A well-behaved CLI prints errors like:
#   Error: failed reaching server: connection refused
#
# Instead, this CLI prints errors as structured log messages, and
# --log-level never silences them entirely.
#
# Prerequisites: Go toolchain. No running Temporal server required
# (the demos use connection-refused errors).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BINARY=$(mktemp /tmp/temporal-demo.XXXXXX)
trap 'rm -f "$BINARY"' EXIT

echo "==> Building CLI from $REPO_ROOT ..."
(cd "$REPO_ROOT" && go build -o "$BINARY" ./cmd/temporal/)
echo

# ---------------------------------------------------------------------------
# Helper: run a command, capture stderr, show it with commentary.
# ---------------------------------------------------------------------------
demo() {
    local label="$1"
    shift
    local expected="$1"
    shift

    echo "--- $label ---"
    echo "  \$ temporal $*"
    # Capture stderr (where errors go); discard stdout.
    local err
    err=$("$BINARY" "$@" 2>&1 >/dev/null) || true
    if [ -z "$err" ]; then
        echo "  stderr: (empty)"
    else
        echo "  stderr: $err"
    fi
    echo "  expected: $expected"
    echo
}

# ---------------------------------------------------------------------------
# 1. Default: errors are log messages
# ---------------------------------------------------------------------------
demo \
    "Problem 1: error is a structured log message (timestamp + level + msg)" \
    "Error: failed reaching server: connection error: ..." \
    workflow list

# ---------------------------------------------------------------------------
# 2. --log-level never: error is silently swallowed
# ---------------------------------------------------------------------------
demo \
    "Problem 2: --log-level never silences the error entirely" \
    "Error: failed reaching server: connection error: ..." \
    --log-level never workflow list

# ---------------------------------------------------------------------------
# 3. --log-format json: error is a JSON log record
# ---------------------------------------------------------------------------
demo \
    "Problem 3: --log-format json wraps the error in JSON" \
    "Error: failed reaching server: connection error: ..." \
    --log-format json workflow list

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
cat <<'EOF'
=== Summary ===

Root cause: the Fail callback in internal/temporalcli/commands.go routes
errors through c.Logger.Error(), so:

  1. Errors inherit log formatting (timestamps, level tags).
  2. --log-level never discards errors via the nop logger.
  3. --log-format json wraps errors in JSON.

Fix: Fail should always write directly to stderr, independent of the logger.
Logging should be disabled by default (except server start-dev).
EOF
