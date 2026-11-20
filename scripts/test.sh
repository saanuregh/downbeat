#!/usr/bin/env bash
# Runs every test against a throwaway Postgres cluster that lives only for this run.
set -euo pipefail
source "$(dirname "$0")/postgres.sh"

export DOWNBEAT_TEST_DATABASE_URL="$DOWNBEAT_PG_URL"
# The sign-in test against a real identity provider runs when Dex is available.
if [ -z "${DOWNBEAT_TEST_DEX:-}" ] && command -v dex >/dev/null; then
  export DOWNBEAT_TEST_DEX="$(command -v dex)"
fi
# The end-to-end tests start the console binary, which no test target builds by itself.
cargo build --workspace --bins
cargo test --workspace "$@"
