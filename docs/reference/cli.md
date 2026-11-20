# Command line reference

Two programs have a command line: your product's binary, through `downbeat::run`
([`crates/downbeat/src/cli.rs`](../../crates/downbeat/src/cli.rs)), and `downbeat-console`
([`crates/downbeat-console/src/main.rs`](../../crates/downbeat-console/src/main.rs)). Both
accept `--help` and `--version`. A flag with an environment variable can be set either way; the
flag wins.

## The product binary

```
<binary> scheduler [worker] [--queues <q>] [--listen <addr>] [--callers <file>] [--local]
                   --database-url <url> [--environment <name>]
<binary> worker [--queues <q>] --database-url <url> [--environment <name>]
<binary> check [--snapshot <file>] [--verify-snapshot <file>]
<binary> run-once <job> [--input <json>] [--environment <name>]
<binary> health --database-url <url> [--environment <name>]
```

A product with its own command line calls `downbeat::child_process(definition)` first, then
passes its own arguments to `downbeat::run_as(args, definition)`.

### `scheduler`

Fires schedules, drives backfills, delivers callbacks, watches for slow and overdue jobs, runs
the reaper, and serves the control API when `--listen` or `--local` is given. Runs until SIGTERM
or Ctrl-C.

| Argument | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `worker` (positional) | | | Also run the worker role in this process. |
| `--queues <name=slots,…>` | | `default=4` | With `worker`: the queues and slots, such as `heavy=1,default=8`. Each queue needs at least one slot. Refused without `worker`. |
| `--listen <address>` | `DOWNBEAT_LISTEN` | Off | Where to serve the control API, such as `0.0.0.0:7400`. Needs `--callers`. |
| `--callers <file>` | `DOWNBEAT_CALLERS` | | The [callers file](configuration.md#the-callers-file). |
| `--local` | | Off | Trust the development key, with audience `local`, and listen on `127.0.0.1:7400` (or `--listen`, which must then be a loopback address). For trying downbeat out. |
| `--database-url <url>` | `DOWNBEAT_DATABASE_URL` | Required | The product's Postgres. The engine's tables are in its `downbeat` schema. |
| `--environment <name>` | `DOWNBEAT_ENVIRONMENT` | None | Which of the definition's environments this is. It selects the schedules. |

### `worker`

Claims and runs jobs from the named queues, and runs the reaper. Runs until SIGTERM or Ctrl-C.

| Argument | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--queues <name=slots,…>` | | `default=4` | The queues and how many slots (child processes) each gets. |
| `--database-url <url>` | `DOWNBEAT_DATABASE_URL` | Required | As above. |
| `--environment <name>` | `DOWNBEAT_ENVIRONMENT` | None | As above. Passed on to child processes. |

### `check`

Builds the definition with every schedule of every environment, prints every pipeline, and exits
with failure when the definition has problems. Warnings go to standard error and do not fail it.
It needs no database.

| Argument | Meaning |
| --- | --- |
| `--snapshot <file>` | Also write the printed text to this file, to commit. |
| `--verify-snapshot <file>` | Fail when this file differs from the printed text. A missing file counts as different. |

### `run-once`

Calls one job's function in this process, in the foreground: no supervisor, no history, no
limits, no database. It prints the `Outcome` and fails when the outcome is `Failed`. For a
command, it starts the program and fails when it exits with a failure status.

| Argument | Environment | Meaning |
| --- | --- | --- |
| `<job>` (positional) | | The job's name. |
| `--input <json>` | | The input as JSON, checked strictly against the job's type. Default `null`, which fits a job with no input. |
| `--environment <name>` | `DOWNBEAT_ENVIRONMENT` | As above. |

`run-once` starts a command's program with the worker's full environment, without its input on
standard input and without the `JOB_*` variables. Use it to check that a program starts, not to
test the program's contract.

### `health`

Exits with success when a worker process on this host (matched by the host name in its worker
id) has refreshed its row within two heartbeats; otherwise with failure. Run it inside a worker's
container as its health check.

| Argument | Environment | Meaning |
| --- | --- | --- |
| `--database-url <url>` | `DOWNBEAT_DATABASE_URL` | Required. |
| `--environment <name>` | `DOWNBEAT_ENVIRONMENT` | As above. |

### Other environment variables

| Variable | Meaning |
| --- | --- |
| `DOWNBEAT_BUILD` | The build label a worker records with each call. Default: the binary's file name and modification time. |
| `DOWNBEAT_LOG_FORMAT` | `json` for JSON log lines. Logs go to standard error. |
| `RUST_LOG` | Log filter. Default `info,sqlx=warn`. |
| `DOWNBEAT_CHILD_FD` | Internal. Set by a worker on its child processes; do not set it yourself. |

### Exit status

`0` on success; `1` on failure (including a definition with problems, a failed `check`, or a
role that stopped with an error); `2` for a command line that does not parse.

## `downbeat-console`

```
downbeat-console --config <file> [--database-url <url>]
downbeat-console --local [--scheduler <url>] [--listen <addr>] [--database-url <url>]
downbeat-console keygen <private-key-file>
```

| Argument | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--config <file>` | `DOWNBEAT_CONSOLE_CONFIG` | | The [console configuration file](configuration.md#the-console-configuration-file). Conflicts with `--local`. |
| `--local` | | Off | No sign-in; one stand-in person who administers everything; the development key; a scheduler on this machine. Must listen on a loopback address. |
| `--scheduler <url>` | | `http://127.0.0.1:7400` | With `--local`: the scheduler's address. Refused without `--local`. |
| `--listen <address>` | | `127.0.0.1:7401` | With `--local`: where to listen. Refused without `--local`; otherwise the configuration file's `listen` applies. |
| `--database-url <url>` | `DOWNBEAT_CONSOLE_DATABASE_URL` | Required | The console's own Postgres database. Its tables are in schema `console`. |

The console runs until SIGTERM or Ctrl-C. `RUST_LOG` sets its log filter (default
`info,sqlx=warn`).

### `keygen`

```sh
downbeat-console keygen <private-key-file>
```

Makes an Ed25519 key pair for a caller of a scheduler. It writes the private key to the file as
PKCS#8 PEM, created with mode `0600`, and refuses to overwrite an existing file. It prints the
public key (base64url) on standard output for the scheduler's callers file. It needs no database.
