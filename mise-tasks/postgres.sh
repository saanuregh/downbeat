# shellcheck shell=bash
# Sourced by the tasks that need a database, and not a task itself (it is not executable): starts a
# throwaway Postgres in a podman container, with its data in memory and its port on 127.0.0.1
# only, removes it on exit, and exports DOWNBEAT_PG_URL for its `postgres` database.
postgres_image=docker.io/library/postgres:18.7
postgres_container="$(podman run --detach --rm --tmpfs /var/lib/postgresql --shm-size 256m \
  --publish 127.0.0.1::5432 --env POSTGRES_USER=downbeat --env POSTGRES_HOST_AUTH_METHOD=trust \
  "$postgres_image" -c max_connections=400 -c fsync=off)"
stop_postgres() {
  podman rm --force --time 0 "$postgres_container" >/dev/null 2>&1 || true
}
trap stop_postgres EXIT
# The image first sets up the cluster with a server that listens on no TCP port, so a TCP check
# passes only once the real server is up.
for _ in $(seq 120); do
  podman exec "$postgres_container" pg_isready --quiet --host 127.0.0.1 --username downbeat && break
  sleep 0.5
done
podman exec "$postgres_container" pg_isready --quiet --host 127.0.0.1 --username downbeat
postgres_port="$(podman port "$postgres_container" 5432/tcp)"
export DOWNBEAT_PG_URL="postgres://downbeat@127.0.0.1:${postgres_port##*:}/postgres"
