# Changelog

All notable changes to downbeat are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Until 1.0, a minor version may break APIs, the
database schema or the control API.

## [0.1.0] - Unreleased

The first version.

### Breaking

Changes to the API of the pre-release crates, made so later versions can add to it without a
major version ([decision 72](docs/decisions.md#72-the-public-api-can-grow-after-01)):

- Request, answer and error types in `downbeat-api` and `downbeat-client`, and the engine's
  output types (`FinishedRun`, `Event`, `Started`, `Failure`, `Ended`, `Outcome`, `Day` and
  others), are `#[non_exhaustive]`: a match needs a `_` arm, and a request is built with
  `new(…)` and setters, such as `PauseRequest::new(note).until_resumed()`.

### Jobs

- `#[job(…)]` turns a typed async function into a job handle; `#[command(…)]` describes a program
  to run in its place.
- `#[derive(JobData)]` for inputs and outputs, with `#[job_data(not_compared)]` fields and a
  canonical identity hash.
- Jobs return `JobResult<T>`; `skip(…)`, `not_ready(…)` and `?` on any printable error.
- Attributes on a job function stay on the function; a `#[cfg]` after `#[job]` removes the job
  and everything generated for it.
- Policies on the job: queue, priority, timeout, attempts, backoff, `retry_on`, `recheck_every`,
  `give_up_after`, `one_at_a_time`, `one_per`, `fresh_process`, `max_memory`, `keep_runs_for`,
  `keep_runs_at_most`, `expect_success_every`, `supports_dry_run`, `name`; presets `frequent`,
  `heavy` and `external`, and product-defined presets.
- `Run`: cancellation, progress, checkpoints, `run.start`, dry runs.
- Dependencies built lazily per process with `Definition::provide`.
- `.test(…)` on every job handle, and `Run::for_test()` with `.due(…)`, `.on_attempt(…)` and
  `.cancelled_already()`.
- `downbeat::testing::Schedules`: a definition's schedules on any date or instant, with no database.
- Each job's input published as a JSON Schema.

### Pipelines

- Links `.after`, `.after_failure` and `.after_any`, joins over tuples, and `.map` adapters, all
  checked by the compiler.
- Chains, with child creation in the same transaction as the parent's ending.
- Conditions `only_if`, `needs` (`done_for`, `done_within`, `or_wait`, `or_fail`) and
  `if_changed`.
- The same-work rules: a request joins queued work for the same input, and the same work never
  runs twice at once.

### Schedules

- `daily_at` and `every`, with windows, weekdays, days of the month, zones, environments,
  `expire_after` and names.
- Day rules (`Day::Normal`, `Off`, `Shifted`) and dated overrides at runtime.
- Missed-occurrence handling, clock-change resolution, and predictable behaviour across deploys.
- Backfills over a date range.

### Runtime

- Scheduler and worker roles in the product's binary; any number of each, with no leader.
- Child processes with process groups, a parent-death signal and out-of-memory protection;
  memory sampling; timeouts; leases, heartbeats and fencing, settable per queue; a reaper for lost
  workers and expired, given-up and unregistered runs; a child that a function left tasks running
  in is replaced.
- Graceful shutdown that releases runs, with timings checked against `stop_within`.
- `on_finish` and `on_event` callbacks delivered at least once from the database; `log_link` and
  `redact`.
- History pruning by chain, batch after batch within a budget, on a clock of its own beside the
  sweeps; a run a sweep cannot handle is logged and passed over, and each sweep walks its runs
  batch after batch in `run_id` order, so runs that keep failing never hide the rest.
- Progress text and the reasons of errors, panics, skips and not-ready are cut at 64 KiB; a child
  that sends a message the worker cannot read is killed and counted as a crash.
- A command whose program, arguments and environment exceed 1 MiB as JSON fails as `bad_input`, naming
  the limit, instead of crashing.
- The `check`, `run-once` and `health` commands, and schedule snapshots for CI.
- `InAppClient` for starting jobs from inside the product. It and `migrate` return a typed
  `downbeat::Error`.
- Linux only.

### Control API

- Versioned HTTP API for every read and action, with a reason on every state and a preview of
  every action, including pausing everything for an outage.
- A database that cannot answer just now (unreachable, a deadlock, a timeout, a read-only server)
  answers 503; any other database failure answers 500, which `downbeat-client` does not retry.
- Per-request Ed25519 tokens bound to the request, a callers file with scopes and job limits,
  replay protection, idempotency keys, and a rate limit per caller and actor.
- `downbeat-api` crate with the wire types and token signing; `downbeat-client` crate for Rust
  callers, starting jobs by name or, through `job_name!`, with a typed input.
- `GET /v1/report` and `Client::report`: each job's recent days (runs, failures, failure rate,
  timing) and yesterday's failed runs, counting runs that pruning has removed.

### Console

- `downbeat-console`: OpenID Connect sign-in, sessions, grants to people and groups by scheduler,
  scope and pipelines, an audit of sign-ins and grant changes, and preferences.
- Signed forwarding of the screens' requests to any number of schedulers.
- Screens built with Bun and embedded in the binary.
- A Report screen: each job's last 7, 14 or 30 days, and yesterday's failed runs.
- `--local` mode and `keygen`.

### Project

- Example product `examples/market`, `mise run demo`, end-to-end tests against throwaway
  Postgres, documentation whose Rust samples are compiled, and a measurement of the hot paths
  (`docs/performance.md`).

[0.1.0]: https://github.com/downbeat-rs/downbeat
