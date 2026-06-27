.PHONY: all gen gen-docs build saa-build saa-test saa-test-cli saa-test-sdk

all: gen build

gen: internal/temporalcli/commands.gen.go cliext/flags.gen.go

internal/temporalcli/commands.gen.go: internal/temporalcli/commands.yaml
	go run ./cmd/gen-commands \
		-input internal/temporalcli/commands.yaml \
		-pkg temporalcli \
		-context "*CommandContext" > $@

cliext/flags.gen.go: cliext/option-sets.yaml
	go run ./cmd/gen-commands \
		-input cliext/option-sets.yaml \
		-pkg cliext > $@

gen-docs: internal/temporalcli/commands.yaml cliext/option-sets.yaml
	go run ./cmd/gen-docs \
		-input internal/temporalcli/commands.yaml \
		-input cliext/option-sets.yaml \
		-output dist/docs

build:
	go build ./cmd/temporal

# --- Standalone Activity (SAA) test harnesses (saa-test/) -------------------
# These require `uv` and build the CLI against the local server branch (via the go.mod
# replace directive) as ./temporal-saa, which both harnesses use to run `server start-dev`
# and as the client.
#
# SAA_ARGS defaults to --fresh (wipe prior results and run the whole suite). Override it to
# run a subset or to resume (each suite skips tests already recorded PASS in its log):
#   make saa-test-sdk SAA_ARGS="--only start_delay.defers_dispatch"
#   make saa-test-cli SAA_ARGS="--rerun-failed"
#   make saa-test-sdk SAA_ARGS=          # resume: run only not-yet-passed tests
#
# By default each run starts a throwaway headless dev server and tears it down. To keep the
# executions and view them in a UI, run your own dev server (with the SAA flags) and point the
# SDK suite at it with --address (the suite leaves it running):
#   ./temporal-saa server start-dev \
#       --dynamic-config-value activity.enableStandalone=true \
#       --dynamic-config-value activity.startDelayEnabled=true \
#       --dynamic-config-value activity.enableCallbacks=true
#   make saa-test-sdk SAA_ARGS="--fresh --address localhost:7233"   # UI at localhost:8233
SAA_ARGS ?= --fresh

saa-build:
	go build -o ./temporal-saa ./cmd/temporal

# CLI-client suite: drives all operations and assertions through the temporal CLI.
saa-test-cli: saa-build
	uv run saa-test/saa_test.py $(SAA_ARGS)

# SDK-client suite: drives the client via the Temporal Python SDK; covers start_delay,
# next_retry_delay, delete, and worker-crash scenarios the CLI cannot reach.
saa-test-sdk: saa-build
	uv run saa-test/saa_sdk_test.py $(SAA_ARGS)

# Run both suites.
saa-test: saa-test-cli saa-test-sdk
