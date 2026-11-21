# 1. Getting started

This chapter takes you from an empty binary to a job that runs on a schedule on your machine.

A few words used throughout the guide:

- **Product**: your program, the binary that embeds downbeat.
- **Job**: a typed async function marked `#[job]`.
- **Run**: one execution of a job, from the moment it is created until it ends.
- **Pipeline**: a function that names a group of jobs and says what starts each one.
- **Definition**: the complete description of your product's jobs, built by one function.
- **Roles**: the parts of the engine your binary can run. The *scheduler* fires schedules and
  serves the control API. The *worker* runs jobs.

## Requirements

- Linux. The worker relies on process groups, the parent-death signal and `/proc`.
- Rust 1.98 or newer (the crates' minimum supported version); this repository pins its own in
  [`rust-toolchain.toml`](../../rust-toolchain.toml).
- PostgreSQL. downbeat creates and migrates its own tables in a schema named `downbeat`.

## Add the crate

downbeat is not published yet. Depend on the repository:

```toml
[dependencies]
downbeat = { git = "https://github.com/downbeat-rs/downbeat" }
serde = { version = "1", features = ["derive"] }
```

Add `chrono` only if you use `Day::Shifted` (see [schedules](04-schedules.md)), and `tokio` if
your jobs use it directly.

## A first job

```rust
use downbeat::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Report {
    pub date: NaiveDate,
    pub lines: u32,
}

#[job(attempts = 3)]
async fn nightly_report(run: Run, date: NaiveDate) -> JobResult<Report> {
    run.progress("counting");
    let lines = 42; // your work here
    if lines == 0 {
        return skip("nothing happened that day");
    }
    Ok(Report { date, lines })
}
```

- `#[job]` turns the function into a *handle* with the same name. Everything else refers to the
  job through that handle, never through a string.
- The first argument is always `run: Run`. The input comes next, if the job takes one. Values
  your product provides, such as a connection pool, come after that, taken by reference.
- Inputs and outputs are `JobData` types. A struct derives `Serialize`, `Deserialize` and
  `JobData`. Plain types such as `NaiveDate`, `String` or `u32` already are `JobData`.
- `skip(…)` ends the run as skipped, with that reason.

## A pipeline and a definition

```rust
fn reports(pipeline: &mut Pipeline) {
    pipeline
        .job(nightly_report)
        .schedule(daily_at(["02:30"]).input(|occurrence| occurrence.date().pred_opt().unwrap()));
}

fn definition() -> Definition {
    let mut definition = Definition::new().zone("Europe/London").environments(["production", "dev"]);
    definition.pipeline("reports", reports);
    definition
}

fn main() -> std::process::ExitCode {
    downbeat::run(definition)
}
```

- A pipeline names each job once, and says what starts it. Here a schedule starts it every day
  at 02:30 London time, with the previous day's date as input.
- The definition must set a zone. There is no implicit UTC.
- The engine calls `definition()` in every process it starts, including each child process. Keep
  it free of side effects, and let `main` do nothing but call `downbeat::run`.

## Check it

```sh
cargo run -- check
```

`check` builds the definition exactly as a real start would, without connecting to anything. It
prints every pipeline with its jobs, triggers and policies, and fails with a list of problems,
such as a job no pipeline names or a time that does not parse. See
[running in production](05-running.md#check-and-snapshots-in-ci).

## Run it locally

Point the product at a Postgres database and start both roles in one process:

```sh
export DOWNBEAT_DATABASE_URL=postgres://localhost/myproduct
export DOWNBEAT_ENVIRONMENT=dev
cargo run -- scheduler worker --queues default=4 --local
```

- `scheduler worker` runs both roles. In production you usually run them as separate processes.
- `--queues default=4` gives the worker four slots on the `default` queue. A *slot* is one child
  process that runs one function at a time.
- `--local` serves the control API on `127.0.0.1:7400` and trusts a development key. Use it only
  to try things out.
- On start, each role creates or migrates the `downbeat` schema.

To see the runs, start the console in local mode next to it. Build the `downbeat-console`
binary from this repository (`cargo build --release -p downbeat-console`, after building its
screens; see [the console](06-console.md)). It needs a database for its own tables, which may be
the same one:

```sh
DOWNBEAT_CONSOLE_DATABASE_URL=$DOWNBEAT_DATABASE_URL downbeat-console --local
```

Then open <http://127.0.0.1:7401>. See [the console](06-console.md).

To call one job's function in the foreground, with no supervisor, history or limits:

```sh
cargo run -- run-once nightly_report --input '"2026-11-19"'
```

## Next

- [Jobs](02-jobs.md): everything about writing a job.
- [Pipelines](03-pipelines.md): links between jobs, and conditions.
- The [market example](../../examples/market/src/lib.rs) is a complete small product (its
  [`main.rs`](../../examples/market/src/main.rs) is the three-line entry point).
