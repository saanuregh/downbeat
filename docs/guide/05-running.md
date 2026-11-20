# 5. Running in production

Your product's binary runs every part of downbeat except the console. You choose which parts each
process runs with the command line. Every flag is in the [CLI reference](../reference/cli.md).

## Roles

| Role | Started with | Does |
| --- | --- | --- |
| Scheduler | `<binary> scheduler` | Fires schedules, drives backfills, delivers `on_finish` and `on_event`, watches for slow and overdue jobs, serves the control API when `--listen` or `--local` is given. |
| Worker | `<binary> worker --queues default=8,heavy=1` | Claims runs from the named queues and runs them in child processes, with that many slots per queue. |
| Both | `<binary> scheduler worker --queues default=4` | Both roles in one process. Good for development and small setups. |

Every process with either role also runs the **reaper**: it takes back runs from workers that
died, ends runs that expired, gave up waiting or belong to no deployed job, and prunes history.
Several reapers are safe at once, so recovery never depends on one process.

You may run any number of schedulers and workers against one database. There is no leader. Run
at least one scheduler if you have schedules, backfills or callbacks, and at least one worker per
queue.

## Environment variables

| Variable | Used by | Meaning |
| --- | --- | --- |
| `DOWNBEAT_DATABASE_URL` | `scheduler`, `worker`, `health` | The product's Postgres. The engine's tables are in its `downbeat` schema. |
| `DOWNBEAT_ENVIRONMENT` | every command | Which of the definition's environments this is. It selects the schedules. Children inherit it. |
| `DOWNBEAT_LISTEN` | `scheduler` | Where to serve the control API, such as `0.0.0.0:7400`. Off when unset. |
| `DOWNBEAT_CALLERS` | `scheduler` | The [callers file](../reference/configuration.md#the-callers-file). Required with `DOWNBEAT_LISTEN`. |
| `DOWNBEAT_BUILD` | `worker` | A build label recorded with each call of a function. Default: the binary's name and modification time. |
| `DOWNBEAT_LOG_FORMAT` | every command | `json` for JSON log lines on standard error. |
| `RUST_LOG` | every command | Log filter. Default `info,sqlx=warn`. |

## Queues and slots

A **queue** is a lane that runs wait in, named by the job's `queue` policy. A **slot** is one
child process that runs one function at a time. `--queues heavy=1,default=8` gives this worker
one slot on `heavy` and eight on `default`. A queue's capacity is the sum of its slots across
all worker processes.

- Within a queue, a worker claims the due run with the highest priority, and the oldest among
  equals. Priority is strict: a steady supply of high-priority runs can starve a low-priority
  run. The remedy is a separate queue.
- Each slot polls its queue once a second (the `poll` setting), so a run starts within about a
  second of being due.
- A worker only claims jobs its own build contains. During a rolling deploy, a new job's runs
  wait for a new worker.
- Put heavy jobs on their own queue, so their slots, memory and machines are sized apart.

### Connections

Each process opens a pool of `4 + 2 × slots` connections to the engine's tables. Each child
process builds its own dependencies, so a worker with eight slots holds eight sets of your pools.
Size them small, or put a connection pooler in front of Postgres. Every engine connection has a
60-second statement timeout.

## Memory limits

There are three layers, and none needs any privilege:

| Layer | What it does | Active |
| --- | --- | --- |
| 1. Protect the worker | Each child is marked as the kernel's preferred victim (`oom_score_adj` 1000), so when the machine or container runs out of memory, the kernel kills a job and not the worker. | Always |
| 2. Sampling | Every two seconds the worker reads the resident memory of each child (or command program) from `/proc`, and kills it above `max_memory`. The run fails as `out_of_memory`. | When the job sets `max_memory` |
| 3. One slot per container | The container's own memory limit is the job's limit, and its CPU is the job's CPU. | When you deploy a queue that way: `--queues heavy=1` in a container sized for the job |

Sampling has gaps. A process that allocates fast can pass its limit between samples, and only
the sampled process is measured, not processes it starts. Use layer 3 when a job needs an exact
limit. downbeat has no CPU limit; a job that needs one runs on a one-slot worker.

## Shutdown and deploy timing

On SIGTERM or Ctrl-C:

1. The worker stops claiming. The scheduler, the reaper and the control API stop at once.
2. The worker waits up to `drain` for running functions to end on their own.
3. It asks the functions still running to stop (`run.is_cancelled()` turns true), waits
   `kill_grace`, then kills their process groups.
4. It releases each remaining run straight back to the queue, so another worker can claim it
   without waiting for the lease. The run's history shows the call as interrupted. This counts
   neither as a failed attempt nor as a lost worker.

All four steps must fit in the time your orchestrator allows between SIGTERM and SIGKILL. Tell
downbeat that time as `stop_within`. Five seconds are kept for step 4, and `check` and every role
refuse to start when `drain + kill_grace + 5 s` is more than `stop_within`.

```rust
Definition::new().zone("UTC").settings(downbeat::Settings {
    stop_within: std::time::Duration::from_secs(120),
    drain: std::time::Duration::from_secs(60),
    ..downbeat::Settings::default()
})
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `heartbeat` | 15 s | How often a worker renews the leases of its runs. |
| `lease` | 3 min | How long a run stays owned without a renewal. |
| `kill_grace` | 10 s | How long a function has to stop after it is asked to, before it is killed. |
| `drain` | 10 s | On shutdown, how long to wait for functions to end on their own. |
| `stop_within` | 30 s | The time allowed between SIGTERM and SIGKILL. |
| `poll` | 1 s | How often schedulers and worker slots look for work. |
| `recycle_child_after` | 500 | A reused child process is replaced after this many runs. |

The settings apply to every queue. A per-queue lease is not implemented
([decision 7](../decisions.md#7-engine-timings-are-set-for-the-whole-definition)).

A long job cannot be drained in a short stop window, so every deploy of its worker interrupts it.
That is cheap when the job saves checkpoints: the run is released, picked up by a new worker, and
continues from `run.last_checkpoint()`. An interruption restarts the run's timeout and never fails
it. `on_event` receives `RunInterrupted` at a run's third interruption and at each one after it,
so a job that never gets to finish is noticed. You can also deploy heavy workers less often than the rest.

If a worker is killed without a clean shutdown, its runs wait for the lease to expire, plus
`kill_grace` and ten seconds, before the reaper takes them back. A run lost this way once goes
back to the queue; lost a second time, it fails as `worker_lost`.

## Health checks

| Check | How |
| --- | --- |
| Scheduler alive | `GET /healthz` on the control API. Needs no token. |
| Scheduler ready for traffic | `GET /readyz`. Answers 503 when the database cannot be reached. |
| Worker alive | `<binary> health`, run inside the worker's container. It succeeds when a worker on this host has refreshed its row within two heartbeats. It needs `DOWNBEAT_DATABASE_URL`. |

Workers serve no HTTP.

## Migrations

Every role brings the `downbeat` schema up to date when it starts, under an advisory lock, so
several processes starting together are safe. Migrations are additive, so an engine one version
older keeps working during a rolling deploy. An engine refuses to start against a schema that has
a newer migration marked as breaking.

To migrate as a separate deploy step, call `downbeat::migrate(&database_url).await` from a small
program of your own. The database role needs to create a schema the first time, and read and
write its own schema after that.

`InAppClient::connect` also migrates when it connects.

## Check and snapshots in CI

`<binary> check` builds the definition as a real start would, with every schedule of every
environment, prints every pipeline, and fails, naming the job and the fix, on:

- two pipelines or two schedules with the same name, or a job named in two pipelines;
- a `#[job]` or `#[command]` compiled into the binary that no pipeline names;
- a dependency a job or a day rule takes that the definition does not provide;
- a link to a job no pipeline names, a parent listed twice, a cycle, or a join whose parents share
  no first job;
- `.if_changed()` on a job with no input, `done_for` on a needed job with no input, a job that
  needs itself;
- a missing or unknown zone, an environment the definition does not list, a time that does not
  parse, an interval under one second, a day of the month outside 1 to 31, a second schedule with
  no name, `daily_at` with no times, `.between` on `daily_at`;
- settings whose shutdown steps do not fit in `stop_within`.

It warns, without failing, about each job that takes turns with `one_at_a_time` or `one_per`.

Commit a snapshot of your schedules and fail CI when it is stale:

```sh
<binary> check --snapshot schedules.snapshot          # write it
<binary> check --verify-snapshot schedules.snapshot   # fail when it differs
```

A pull request that moves a time, changes a policy, renames a job or adds a link then shows it as
a readable diff. The market example's snapshot is
[`examples/market/schedules.snapshot`](../../examples/market/schedules.snapshot).

## Known limits

- **Linux only.** Workers rely on process groups, the parent-death signal and `/proc`.
- **Tasks a function spawns are not tracked.** A function should await what it spawns. A reused
  child is replaced after `recycle_child_after` runs and after any forced kill, and
  `fresh_process` gives a clean process every run
  ([decision 8](../decisions.md#8-tasks-a-function-spawns-are-not-tracked)).
- **A command's background processes** are killed when the program exits only if they stayed in
  its process group. A program that detaches them is not contained; run such a worker in a
  container whose teardown kills everything inside.
- **No sandbox.** Every child runs as the same operating-system user with the same credentials.
- **No CPU limit**, and memory sampling has gaps (see above).
- **Rate limits are in memory, per scheduler.**
- **Typed starts in `downbeat-client` are by name only**; see
  [the API chapter](07-api-and-client.md#downbeat-client).
