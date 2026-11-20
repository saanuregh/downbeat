# Architecture

This document is for contributors. It describes the processes, where each piece of code lives,
the data model, the main flows, and how failures are handled. The rules themselves are in the
[specification](spec.md); the reasons for choices are in [decisions.md](decisions.md).

## Processes

```
                         ┌──────────────────────── product binary ────────────────────────┐
  people ──HTTPS──► downbeat-console ──signed──► scheduler role            worker role     │
                     │   (separate binary)  HTTP  · fires schedules         · claims runs   │
                     │                            · drives backfills        · keeps leases  │
                     ▼                            · delivers callbacks      · supervises    │
              console Postgres                    · control API (/v1)         children ─┐   │
              (schema console)                    · reaper                  · reaper    │   │
                                                         │                      │       │   │
                                                         ▼                      ▼       │   │
                                         product Postgres, schema downbeat ◄────┘       │   │
                                                                                         │   │
                                     child processes (same binary, DOWNBEAT_CHILD_FD=3) ◄┘   │
                                     · run job functions, conditions, link adapters          │
                                     · never touch the engine's tables                       │
                                     command programs (started by the worker)                │
                         └───────────────────────────────────────────────────────────────────┘
```

- **The product binary** embeds the `downbeat` crate and calls `downbeat::run(definition)`. Its
  command line chooses the roles: `scheduler`, `worker`, or `scheduler worker`. Every process
  with a role also runs the reaper.
- **Child processes** are the same binary, started by a worker with `DOWNBEAT_CHILD_FD` set.
  `downbeat::run` sees the variable and runs `child::main` instead of the command line. A child
  talks to its worker over a Unix socket on descriptor 3. Each worker slot owns one child.
- **Command programs** are started by the worker itself, after the child has described them.
- **The console** is a separate binary with its own database. It reaches schedulers only through
  the control API, as a trusted caller.

All coordination goes through Postgres: row locks with `SKIP LOCKED`, unique indexes for the
same-work rules, advisory locks per chain, and the database's clock. There is no leader and no
message broker.

## Crates and modules

### `crates/downbeat` (the engine)

| Module | Owns |
| --- | --- |
| `lib.rs` | The public API: re-exports, `run`, `run_as`, `child_process`, `migrate`, `check_problems`, the prelude. |
| `definition.rs` | `Definition` (zone, environments, dependencies, pipelines, callbacks) and `Settings` (timings), with the shutdown-fit check. |
| `cli.rs` | The product binary's command line; starting roles; signal handling; the `health` command. |
| `check.rs` | `check` (print, snapshot, verify, warnings) and `run-once`. |
| `registry.rs` | Running the pipeline functions and checking the result: the `Registry` every process uses. All `check` problems are found here. |
| `describe.rs` | Jobs and schedules in words, for `check`, the snapshot and the API. |
| `job/mod.rs` | The `Job` trait and `JobSpec`, a job with its types erased; input normalising and hashing. |
| `job/data.rs` | `JobData`, shapes, the canonical identity hash. |
| `job/outcome.rs` | `Result`, `JobError`, `skip`, `not_ready`, `Outcome`. |
| `job/policy.rs` | `Policy`, its defaults, backoff and retry rules, presets. |
| `job/program.rs` | `Program`, `ProgramSpec`, the command environment names. |
| `job/run.rs` | `Run`, `run.start`, the test recorder. |
| `pipeline/mod.rs` | `Pipeline` and `JobBuilder`: schedules, `only_if`, `if_changed`, needs, links. |
| `pipeline/link.rs` | `.after` parents, tuples, `.map`, `Failure`, `Ended`. |
| `pipeline/need.rs` | `done_for`, `done_within`, `or_wait`, `or_fail`. |
| `schedule/mod.rs` | `daily_at`, `every`, the schedule builder, `Occurrence`, `Day`. |
| `schedule/times.rs` | Turning a schedule and a date into occurrences; zone resolution. Pure. |
| `dependencies.rs` | Lazily built, per-process values jobs take by reference. |
| `units.rs` | `30.minutes()`, `8.gb()`, and compact duration text. |
| `reasons.rs` | Every reason sentence, in one place. |
| `events.rs` | `FinishedRun`, `Event`, `EventDelivery`. |
| `starting.rs` | Starting a job on request (API, `run.start`, `InAppClient`): idempotency, rule 1, audit. |
| `client.rs` | `InAppClient`. |
| `protocol.rs` | The messages between a worker and its child, and their framing. |
| `child.rs` | The child process: prepare, call, check a command's output. |
| `worker/mod.rs` | The worker role: slots, claiming, driving a run, deciding how a call ended, recording it, shutdown. |
| `worker/prepare.rs` | Preparing a run: building its input, storing it, checking conditions. |
| `worker/call.rs` | Calling a function or a command under its timeout, memory limit, lease and shutdown. |
| `worker/lease.rs` | Heartbeats, and noticing when a run must stop. |
| `worker/child.rs` | The worker's handle on a child process. |
| `worker/process.rs` | Spawning processes from one thread: process groups, parent-death signal, OOM score, `/proc` memory. The only `unsafe` code. |
| `scheduler/mod.rs` | The scheduler role's periodic tasks. |
| `scheduler/fire.rs` | Registering and firing schedules; creating scheduled runs. |
| `scheduler/plan.rs` | A date's plan: override, day rule or normal times; next occurrence. |
| `scheduler/backfill.rs` | Driving backfills. |
| `scheduler/callbacks.rs` | Delivering `on_finish` and `on_event`. |
| `scheduler/watch.rs` | Slow runs and overdue jobs. |
| `reaper.rs` | Taking back lost runs; ending expired, cancelled, given-up and unregistered runs. |
| `pruning.rs` | Removing old history by chain, and other expired rows. |
| `store/mod.rs` | The connection pool; intervals. |
| `store/migrate.rs` | The engine's migrations. |
| `store/runs.rs` | Creating, claiming and holding runs: every fenced write. |
| `store/ending.rs` | The one ending routine, and creating children. |
| `store/cancel.rs` | Cancelling a run or a chain. |
| `store/history.rs` | Questions about finished runs: last success, needs, expected duration. |
| `store/audit.rs` | Audit rows and engine events. |
| `api/mod.rs` | The control API's configuration (callers file, `--local`) and router. |
| `api/auth.rs` | Verifying every request; scopes and job limits; rate limits. |
| `api/read/` | The GET endpoints, one file per concern: jobs and pipelines, runs, calendar, operations. |
| `api/act/` | The changing endpoints and their previews: starting, runs and chains, pauses, overrides and backfills. |
| `api/views.rs` | Rows into API views, with reasons for unstored states. |
| `api/error.rs` | The API's error shape. |

### Other crates

| Crate | Owns |
| --- | --- |
| `downbeat-macros` | `job.rs` (`#[job]`, `#[command]`: signature checks, the handle, `.test`), `policy.rs` (attribute parsing), `data.rs` (`#[derive(JobData)]`). |
| `downbeat-api` | `token.rs` (claims, signing, verifying, scopes), `keys.rs` (Ed25519 keys, the development key), `types.rs` (every request and response type), `lib.rs` (API version and header names). |
| `downbeat-client` | `client.rs` (builder, signed requests, retries), `error.rs`. |
| `downbeat-console` | `main.rs` (CLI, router, `keygen`), `config.rs`, `session.rs` (OpenID Connect, sessions, CSRF), `access.rs` (grants to scopes), `proxy.rs` (forwarding signed requests), `api.rs` (the console's own endpoints), `store.rs` (its database), `assets.rs` (embedded screens), `error.rs`, and `web/` (the screens). |

## Data model

Every engine table is in schema `downbeat`, created by
[`migrations/0001_initial.sql`](../crates/downbeat/migrations/0001_initial.sql) and tracked in
`downbeat.migration`. All times are compared with the database's `now()`.

| Table | Holds | Written by | Read by |
| --- | --- | --- | --- |
| `chain` | One row per chain: pipeline, first job, who started it, backfill id, cancellation. | `store::runs::create`, `store::cancel` | Ending routine (cancelled?), chain and pipeline views, pruning |
| `run` | Unfinished runs (`queued`, `running`) with their lease, fence, counters, keys, progress, checkpoint, call log. | Every start, the worker, the reaper | Claim, reaper, sweeps, API, backfill |
| `run_history` | Finished runs with status, reasons, output, error chain, stderr tail, callback state. | The ending routine, joined requests, mark as succeeded | Needs, `if_changed`, children's input, API, callbacks, pruning |
| `schedule_state` | Per schedule: watermark, next due time, last registration, times digest. | Scheduler registration and firing, override changes | Firing, job and schedule views |
| `schedule_override` | Dated overrides. Unique per schedule and date. | API | Firing, calendar, previews |
| `pause` | Active pauses of schedules, jobs and queues, and of everything (`kind = 'all'`). | API | Claim, firing, views, overdue check |
| `worker` | One row per worker or scheduler process: role, queues, jobs in its build, build, last seen. | Worker and scheduler heartbeats | `job_not_registered` sweep, queue views, `no_free_worker` reasons, `health` |
| `backfill` | Backfills and their progress. | API, backfill driver | Backfill driver, API |
| `idempotency` | Idempotency keys and the run each produced. | `starting::start` | `starting::start` |
| `used_token` | `jti`s of changing requests, until they expire. | API authentication | API authentication |
| `audit` | What people did, and the engine's events (`actor = 'engine'`, `action = 'event'`). | API, reaper, scheduler, worker | Audit endpoint, event delivery, overdue check |
| `migration` | Applied migrations, with a breaking flag. | `store::migrate` | `store::migrate` |

Indexes and what uses them:

| Index | Serves |
| --- | --- |
| `run_claim` on `run (queue, priority DESC, run_at) WHERE state = 'queued'` | The claim |
| `run_same_work` unique on `run (same_work_key) WHERE starts = 0 AND same_work_key IS NOT NULL` | Rule 1 (the insert's `ON CONFLICT`) |
| `run_turn` unique on `run (turn_key) WHERE starts > 0` | Rule 2 (the function-start write fails on it) |
| `run_job_chain` on `run (job_name, chain_id)` | "Is this child already in the chain?" |
| `run_chain` on `run (chain_id)` | Reading and cancelling a chain; backfill progress |
| `run_history_job` on `(job_name, finished_at DESC)` | A job's runs; last success for needs, `if_changed`, overdue |
| `run_history_finished` on `(finished_at DESC)` | The runs list |
| `run_history_chain` on `(chain_id)` | Reading a chain; parents' endings; pruning |
| `run_history_failed` on `(finished_at DESC) WHERE status = 'failed'` | Failure lists |
| `run_history_schedule` on `(schedule, due_at) WHERE schedule IS NOT NULL` | The calendar |
| `run_history_notify` on `(notify_after) WHERE notify_after IS NOT NULL` | `on_finish` delivery |
| `audit_at` on `audit (at DESC)` | The audit list |
| `audit_notify` on `audit (notify_after) WHERE notify_after IS NOT NULL` | `on_event` delivery |

There is deliberately no index on `lease_expires_at`, `progress` or `checkpoint`, so a heartbeat
changes no indexed column and Postgres can update the row in place. The reaper scans the
`running` rows instead; `run` holds only unfinished work, so it stays small.

The console's tables are in schema `console` of its own database
([`migrations/0001_initial.sql`](../crates/downbeat-console/migrations/0001_initial.sql)):
`person`, `grant`, `session`, `sign_in` (sign-ins in progress), `audit`, `preference`, and
`migration`.

## Main flows

### A scheduled run, end to end

1. **Register.** At start and every minute, `scheduler::fire::register` upserts each schedule's
   `schedule_state` row.
2. **Fire.** Every `poll`, `fire_due` locks the due rows (`FOR UPDATE SKIP LOCKED`), reads
   `now()`, and for each schedule in a savepoint: reads its overrides and pause, computes the
   occurrences since the watermark (`scheduler::plan`, `schedule::times`), and calls `create_run`
   for the latest. `create_run` computes the input in the scheduler process, normalises and
   hashes it, and calls `store::runs::create`, whose `INSERT … ON CONFLICT (same_work_key)`
   either creates the run (and its chain) or joins a queued one. The watermark and next due time
   are updated in the same transaction.
3. **Claim.** A worker slot's `store::runs::claim` takes the most urgent claimable run, raising
   its fence.
4. **Prepare.** `worker::prepare` sends `Prepare` to the slot's child; the child builds the
   input, hash, turn value, condition result and need hashes (`child::prepare`) and answers
   `Prepared`. The worker stores them (`runs::prepared`) and checks needs and `if_changed`
   against `run_history`.
5. **Start.** `runs::function_started` raises `starts`, which takes the turn (or fails on
   `run_turn`, and the run is requeued as `waiting_turn`).
6. **Call.** `worker::call::function` sends `Go`; the child spawns the function and answers
   `Started`, then `Progress`, `Checkpoint` and `Start` messages, and finally `Ended`. Meanwhile
   `worker::lease` heartbeats, and `call` watches the timeout, memory, lease and shutdown.
7. **Decide and record.** `worker::decide` maps the ending to an `Ending` or a `Requeue`.
   `worker::record` writes it: `store::ending::end` for an ending, `runs::requeue` otherwise.
8. **Deliver.** The scheduler's `callbacks::deliver` picks up the new history row and calls
   `on_finish`.

### A linked child

1. In the parent's ending transaction (`store::ending::end_locked`), under the chain's advisory
   lock, `create_children` looks up the parent job's children in the registry. For each, it checks
   that the chain is not cancelled, that the child has no run in the chain, and that every
   parent's latest ending in the chain fits the link. If so it inserts the child with
   `started_by = link`, its parents' run ids, no input and no turn key.
2. When a worker claims the child, `worker::prepare::handed_over` reads the parents' history rows
   and shapes each per link kind. The child process applies the link's adapter (`.map`), and the
   rest is the same as any run. The child's turn key is set when it is prepared.

### A request through the console to a scheduler

1. The browser calls `POST /api/schedulers/production/v1/runs/{id}/retry` with its session cookie
   and `x-csrf-token`.
2. `session::require` finds the session by the cookie's hash, checks idle and absolute expiry and
   the CSRF token.
3. `proxy::forward` loads the person's grants (direct and through groups), and
   `access::access` turns them into a scope and pipelines. Pipelines become job names through the
   scheduler's cached `/v1/pipelines`.
4. The console signs a token (`downbeat_api::sign`) with that scheduler's key: `iss` = its caller
   name, `sub` = the person's email, the scope and jobs, and the method, path and body hash. It
   sends the request, with any `Idempotency-Key`, and relays the answer.
5. The scheduler's `api::auth::verify` checks the token, the caller's limits, the rate limit and
   the `jti`, then `api::act::retry` checks `operate`, takes the chain lock, creates the run, and
   writes an audit row naming `<email> (via console)`.

### Shutdown

1. `cli::wait_for_signal` cancels `stop_claiming` and `stop_scheduler` on SIGTERM or Ctrl-C. The
   scheduler tasks, the reaper and the API's listener stop.
2. `worker::serve` waits up to `drain` for its slots. If some are still busy, it cancels
   `stop_running`. Each busy `call` sends `Cancel` to its child (or SIGTERM to its program), waits
   `kill_grace`, and kills it; the run is requeued with `interruptions + 1`.
3. The worker waits up to `kill_grace + 5 s` for the slots, deletes its `worker` row, and the
   process exits.

## Concurrency and failure handling

- **Fencing.** Every write by a worker carries the fence it claimed with. The reaper raises the
  fence when it takes a run back, so the old worker's later writes change nothing. The ending
  routine deletes the run by fence, so a run ends exactly once.
- **Self-stop before take-back.** A worker stops a call when it has not *sent* a successful
  heartbeat for a whole lease. The reaper waits for the lease plus `kill_grace + 10 s`. So by the
  time another worker can claim the run, the old copy has been killed.
- **One lock per chain.** Ending a run, cancelling, retrying and marking as succeeded all take
  `pg_advisory_xact_lock` on the chain first. Two parents ending together cannot both miss the
  join, and a cancel cannot race a child's creation.
- **Unique indexes, not checks.** Rule 1 and rule 2 are enforced by partial unique indexes, so
  concurrent schedulers, workers and API calls cannot break them.
- **Database clock.** Leases, due times, delays and expiry use `now()`. A process's own clock is
  used only to time its own heartbeats.
- **Bounded waits.** Every wait on a child is bounded by the run's timeout, its lease and shutdown.
  Database calls made for a running function have deadlines, and every connection has a 60-second
  statement timeout.
- **Isolation.** App code runs in children (functions, conditions, adapters, command functions)
  or in the scheduler (schedule inputs, day rules, callbacks). Children never write to the
  engine's tables.
- **Callbacks.** Rows are pushed five minutes ahead and committed before the callback runs, so a
  crash or a failing callback means another delivery, never a lost one.
- **Migrations.** Serialised by an advisory lock; additive; a newer breaking schema stops an
  older engine.

## Where to start reading

1. [`examples/market/src/main.rs`](../examples/market/src/lib.rs): what a product writes.
2. [`crates/downbeat/src/lib.rs`](../crates/downbeat/src/lib.rs) and `cli.rs`: how it starts.
3. `registry.rs`: what a definition becomes.
4. `worker/mod.rs`, `drive` and `decide`: the life of a run.
5. `store/runs.rs` and `store/ending.rs`: every statement that moves a run.
6. `scheduler/fire.rs`: how schedules become runs.
7. `api/auth.rs`, then `api/act/`: the control API.

## Tests

Each layer of tests, and when to use it, is listed in
[CONTRIBUTING.md](../CONTRIBUTING.md#how-the-tests-run). In short: unit tests beside the code for
pure calculation; `crates/downbeat/tests` for the product-facing API and `check`; and
end-to-end tests in [`tests/e2e`](../tests/e2e) that run a test product
([`src/lib.rs`](../tests/e2e/src/lib.rs)) as real scheduler, worker and console processes.

[`scripts/test.sh`](../scripts/test.sh) starts a throwaway Postgres cluster in a temporary
directory (through [`scripts/postgres.sh`](../scripts/postgres.sh)), builds every binary (the
end-to-end tests start the console binary), and runs `cargo test --workspace`. Each end-to-end
test creates its own database in that cluster, so tests run in parallel. The test product uses
short timings (one-second heartbeats, three-second leases).

Behaviour tests that drive real processes are preferred over unit tests; unit tests are for pure
calculation and parsing. How to run them is in [CONTRIBUTING.md](../CONTRIBUTING.md).
