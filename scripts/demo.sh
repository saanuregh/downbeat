#!/usr/bin/env bash
# Runs the example product (scheduler and workers) and the console on this machine, with a
# throwaway Postgres, and no sign-in. Stop it with Ctrl-C; everything it made is removed.
set -euo pipefail
source "$(dirname "$0")/postgres.sh"

if command -v bun >/dev/null; then
  (cd crates/downbeat-console/web && bun install --frozen-lockfile >/dev/null && bun run build >/dev/null)
fi
cargo build -p market -p downbeat-console
# Wherever Cargo puts its build output on this machine.
target=$(cargo metadata --format-version 1 --no-deps | grep -o '"target_directory":"[^"]*"' | cut -d'"' -f4)

export DOWNBEAT_DATABASE_URL="$DOWNBEAT_PG_URL"
export DOWNBEAT_CONSOLE_DATABASE_URL="$DOWNBEAT_PG_URL"
export DOWNBEAT_ENVIRONMENT=dev

"$target/debug/market" scheduler worker --queues default=4,heavy=1 --local &
product=$!
"$target/debug/downbeat-console" --local &
console=$!
trap 'kill $product $console 2>/dev/null; wait; stop_postgres' EXIT INT TERM

echo
echo "The console is at http://127.0.0.1:7401 (the control API at http://127.0.0.1:7400)."
echo "Press Ctrl-C to stop."
wait
