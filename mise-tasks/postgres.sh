# shellcheck shell=bash
# Sourced by the tasks that need a database, and not a task itself (it is not executable): starts a throwaway Postgres cluster in a temporary directory that is
# removed on exit, and exports DOWNBEAT_PG_URL for its `postgres` database. PG_BIN picks an
# installation other than the one on PATH.
bin="${PG_BIN:+$PG_BIN/}"
pg_data="$(mktemp -d)"
stop_postgres() {
  "${bin}pg_ctl" -D "$pg_data/db" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$pg_data"
}
trap stop_postgres EXIT
"${bin}initdb" -D "$pg_data/db" -U downbeat --auth=trust >/dev/null
"${bin}pg_ctl" -D "$pg_data/db" -o "-k $pg_data -c listen_addresses='' -c max_connections=400 -c fsync=off" -l "$pg_data/log" start >/dev/null
export DOWNBEAT_PG_URL="postgres://downbeat@localhost/postgres?host=$pg_data"
