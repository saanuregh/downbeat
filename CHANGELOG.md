# Changelog

All notable changes to downbeat are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Until 1.0, a minor version may break APIs, the
database schema or the control API.

## [0.1.0] - Unreleased

The first version.

### Jobs

- `#[job(…)]` turns a typed async function into a job handle; `#[command(…)]` describes a program
  to run in its place.
- `#[derive(JobData)]` for inputs and outputs, with `#[job_data(not_compared)]` fields and a
  canonical identity hash.
- `skip(…)`, `not_ready(…)` and `?` on any printable error.
- Policies on the job: queue, priority, timeout, attempts, backoff, `retry_on`, `recheck_every`,
  `give_up_after`, `one_at_a_time`, `one_per`, `fresh_process`, `max_memory`, `keep_runs_for`,
  `keep_runs_at_most`, `expect_success_every`, `supports_dry_run`, `name`; presets `frequent`,
  `heavy` and `external`, and product-defined presets.
- `Run`: cancellation, progress, checkpoints, `run.start`, dry runs.
- Dependencies built lazily per process with `Definition::provide`.
- `.test(…)` on every job handle, and `Run::for_test()`.

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
  memory sampling; timeouts; leases, heartbeats and fencing; a reaper for lost workers and
  expired, given-up and unregistered runs.
- Graceful shutdown that releases runs, with timings checked against `stop_within`.
- `on_finish` and `on_event` callbacks delivered at least once from the database; `log_link` and
  `redact`.
- History pruning by chain.
- The `check`, `run-once` and `health` commands, and schedule snapshots for CI.
- `InAppClient` for starting jobs from inside the product.
- Linux only.

### Control API

- Versioned HTTP API for every read and action, with a reason on every state and a preview of
  every action.
- Per-request Ed25519 tokens bound to the request, a callers file with scopes and job limits,
  replay protection, idempotency keys, and a rate limit per caller and actor.
- `downbeat-api` crate with the wire types and token signing; `downbeat-client` crate for Rust
  callers (starts by job name).

### Console

- `downbeat-console`: OpenID Connect sign-in, sessions, grants to people and groups by scheduler,
  scope and pipelines, an audit of sign-ins and grant changes, and preferences.
- Signed forwarding of the screens' requests to any number of schedulers.
- Screens built with Bun and embedded in the binary.
- `--local` mode and `keygen`.

### Project

- Example product `examples/market`, `mise run demo`, end-to-end tests against throwaway
  Postgres, and documentation.

[0.1.0]: https://github.com/downbeat-rs/downbeat
