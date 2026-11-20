#!/usr/bin/env bash
# Runs every test against a throwaway Postgres cluster that lives only for this run.
set -euo pipefail
source "$(dirname "$0")/postgres.sh"

export DOWNBEAT_TEST_DATABASE_URL="$DOWNBEAT_PG_URL"
# The end-to-end tests start the console binary, which no test target builds by itself.
cargo build --workspace --bins
cargo test --workspace "$@"
