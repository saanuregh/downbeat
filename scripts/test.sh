#!/usr/bin/env bash
# Runs every test against a throwaway Postgres cluster that lives only for this run.
set -euo pipefail

# PG_BIN picks a Postgres installation other than the one on PATH.
bin="${PG_BIN:+$PG_BIN/}"
data="$(mktemp -d)"
trap '"${bin}pg_ctl" -D "$data/db" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$data"' EXIT

"${bin}initdb" -D "$data/db" -U downbeat --auth=trust >/dev/null
"${bin}pg_ctl" -D "$data/db" -o "-k $data -c listen_addresses='' -c max_connections=400 -c fsync=off" -l "$data/log" start >/dev/null

export DOWNBEAT_TEST_DATABASE_URL="postgres://downbeat@localhost/postgres?host=$data"
# The end-to-end tests start the console binary, which no test target builds by itself.
cargo build --workspace --bins
cargo test --workspace "$@"
