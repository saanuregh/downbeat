# 2. Jobs

A job is an async function with `#[job(…)]`. Everything about the job itself is written on that
function: what it does, what it needs, and how it runs. What starts it is written in its
[pipeline](03-pipelines.md).

## The job function

```rust
#[job(…policies…)]
async fn name(run: Run, input: Input, dependency: &Dependency, …) -> Result<Output>
```

| Part | Rule |
| --- | --- |
| Name | The function's name is the job's name, stored with every run. Renaming the function renames the job. `#[job(name = "…")]` pins the name. |
| `run: Run` | Always first. See [the `Run` value](#the-run-value). |
| Input | At most one argument, taken by value. Leave it out for a job with no input. A job with several parents takes one tuple. |
| Dependencies | Any further arguments, taken by shared reference (`&T`). The product provides them; see [dependencies](#dependencies). |
| Output | `Result<T>`, where `T` is a `JobData` type or `()`. `Result` is `downbeat::Result`. |

`#[job]` replaces the function with a handle of the same name that implements the `Job` trait.
The macro rejects a function that is not async, is generic, takes `&mut`, or does not take
`run: Run` first.

## Inputs, outputs and `JobData`

`JobData` gives a type three things. It has a stored form (JSON). It has a published shape, which
the API shows so callers know what to send. And it has a way of comparing two values, which
decides whether two requests are "the same work" and whether an input is unchanged.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Prices {
    pub day: TradingDay,
    pub count: u32,
    pub digest: String,
    #[job_data(not_compared)]
    pub fetched_at: String,
}
```

- Derive `Serialize` and `Deserialize` next to `JobData`. A derive macro cannot add other derives
  ([decision 3](../decisions.md#3-jobdata-types-also-derive-serialize-and-deserialize)).
- `#[job_data(not_compared)]` leaves a field out of the comparison: a timestamp, a duration, a
  host name. It follows the field's `#[serde(rename = "…")]`, but not a container's `rename_all`.
- Already `JobData`: `()`, `bool`, `i8` to `i64`, `u8` to `u64`, `usize`, `f64`, `String`,
  `NaiveDate`, `NaiveTime`, `DateTime<Utc>`, `Uuid`, `serde_json::Value`, `Option<T>`, `Vec<T>`,
  and tuples of two to four `JobData` values.
- An input, an output or a checkpoint is at most 64 KiB as JSON. An output over that fails the
  run as `output_too_large`. Put large data in storage and return a pointer to it.
- Change types additively. A stored value with a field this build does not know still decodes,
  so old and new builds can run side by side. An `Option` field that is `None` leaves the
  comparison unchanged, so adding one does not make old values compare differently.
- Input sent through the API or `run.start` is decoded strictly: an unknown field is refused.

An output should say what was produced: counts, ids, a content hash, a pointer to a file. Its
children receive it, and [`if_changed`](03-pipelines.md#only-when-something-changed) compares it.

## How a function ends

| The function | The run | Then |
| --- | --- | --- |
| Returns `Ok(output)` | `succeeded` | Children linked with `.after` start, with the output. |
| Returns `skip("reason")` | `skipped` (`job_skipped`) | Nothing that follows success runs. Use it for "there is nothing to do". |
| Returns `not_ready("reason")` | Back to the queue | Called again after the job's `recheck_every`. Not a failure, and it spends no attempt. Use it for "what I need is not there yet". |
| Returns an error (`?` on any printable error) | Another attempt after the backoff, while attempts remain; then `failed` (`error`) | Children linked with `.after_failure` start once it has failed for good. |
| Panics | `failed` (`panic`) | A panic is never retried. |
| Crashes, goes over `max_memory`, or runs past its timeout | `failed` (`crash`, `out_of_memory`, `timeout`), or another attempt if `retry_on` lists that kind | |

`skip` and `not_ready` return `downbeat::Result`, so they sit beside `?`. For an error without an
error value, use `JobError::msg("…")`.

Use `skip` when something else will ask again, such as a schedule that checks every half hour.
Use `not_ready` when this run is the only one that will ask, such as a job scheduled once a day.

### The contract every job accepts

1. **Safe to run twice.** Runs are at least once. Another attempt, or a lost worker, calls the
   function again.
2. **Safe to kill.** At a timeout, a cancel, a lost lease or a shutdown, `run.is_cancelled()`
   turns true. After a grace period the process is killed.
3. **Nothing to do means `skip`.** Never return `Ok` with an empty output, because children
   follow success.
4. **Outputs are small and honest.** No secrets in inputs, outputs, progress, checkpoints or
   error messages: they are stored and shown.

## Policies

Policies say how a run of the job executes. They are written in `#[job(…)]` and nowhere else.
None is required. Values are ordinary Rust expressions. `30.seconds()`, `5.minutes()`,
`2.hours()` and `30.days()` come from `DurationExt`, and `512.mb()` and `8.gb()` from
`BytesExt`.

```rust
#[job(queue = "heavy", timeout = 3.hours(), attempts = 3, backoff = 1.minutes()..=10.minutes(),
      max_memory = 8.gb())]
```

| Policy | Meaning | Default |
| --- | --- | --- |
| `queue = "heavy"` | The queue the run waits in. A worker serves the queues it is told to. | `"default"` |
| `priority = 10` | Higher is claimed first within a queue (an `i16`). | `0` |
| `timeout = 2.hours()` | The function is asked to stop, then killed, after this long. The clock starts when the function is called. | 5 minutes |
| `attempts = 3` | How many failed calls a run may have before it fails for good. | 1 |
| `backoff = 1.minutes()..=10.minutes()` | The delay before the next attempt. It doubles from the first value up to the second. A single duration is a constant delay. | 30 seconds up to 10 minutes |
| `retry_on = [Error, Timeout]` | Which failures get another attempt: `Error`, `Timeout`, `Crash`, `Memory`. A panic never does. | `[Error]` |
| `recheck_every = 5.minutes()` | How soon a waiting run is looked at again: after `not_ready`, a need with `.or_wait()`, or a command that exits with 75. | 1 minute |
| `give_up_after = 4.hours()` | A run still queued this long after it was created fails as `gave_up`. | 24 hours |
| `one_at_a_time` | No two runs of this job execute at once, whatever their input. | Off |
| `one_per = field` | No two runs with the same value of this input field execute at once. | Off |
| `fresh_process` | A new child process for every run, instead of a reused one. No effect on a command. | Off |
| `max_memory = 8.gb()` | The run is killed when its process's resident memory goes above this. | No limit |
| `keep_runs_for = 30.days()` | How long finished runs stay in the history. For a chain, the first job's setting decides. | 30 days |
| `keep_runs_at_most = 20` | Also keep only the newest this many chains whose first run did not fail. | No limit |
| `expect_success_every = 26.hours()` | Tell `on_event` once when the job has not succeeded for this long. Not while it is paused. | Off |
| `supports_dry_run` | The job checks `run.is_dry_run()` and writes nothing when it is set. Only such a job accepts a dry-run request. | Off |
| `name = "…"` | The job's name, when it should differ from the function's. | The function's name |
| `preset = heavy` | Start from a preset, then apply the other policies. | `Policy::DEFAULT` |

The defaults are `Policy::DEFAULT` in
[`crates/downbeat/src/job/policy.rs`](../../crates/downbeat/src/job/policy.rs). An unknown
policy, or one set twice, fails to compile with the list of valid policies.

### Presets

A preset is a named set of policies for a kind of job that recurs. A job starts from it and sets
only what differs: `#[job(preset = heavy, timeout = 3.hours())]`.

| Preset | Sets | For |
| --- | --- | --- |
| `frequent` | 1-minute timeout, `keep_runs_at_most = 20` | A job on a short interval |
| `heavy` | Queue `heavy`, 2-hour timeout, `fresh_process` | A long, CPU- or memory-heavy job |
| `external` | 10-minute timeout, `attempts = 3`, `retry_on = [Error, Timeout]` | A job that calls an outside service that is sometimes slow or down |

A product defines its own presets as constants of type `Policy`:

```rust
pub const NIGHTLY: downbeat::Policy = downbeat::Policy {
    queue: "batch",
    timeout: std::time::Duration::from_secs(4 * 3600),
    ..downbeat::Policy::DEFAULT
};

#[job(preset = crate::NIGHTLY, attempts = 2)]
async fn rebuild_index(run: Run) -> Result<()> { Ok(()) }
```

A person starting a run through the API may give that one run another timeout (it needs the
`administer` scope). That is the only way a run's policy differs from the function's.

## The `Run` value

| Member | Purpose |
| --- | --- |
| `run.is_cancelled()`, `run.cancelled().await` | True, or complete, once this run has been asked to stop: a timeout, a cancel, a lost lease or a shutdown. |
| `run.progress("41 of 200")` | A line for people, shown with the run. Each call replaces the last. Saved with the next heartbeat. |
| `run.checkpoint(&state)?` | State a later call of the same run reads to continue. At most 64 KiB. Saved with the next heartbeat. |
| `run.checkpoint_now(&state).await?` | The same, stored before it returns. |
| `run.last_checkpoint::<S>()` | What an earlier call of this run last saved, if anything. |
| `run.start(job, input).await?` | Start another job. Calling it again on a later attempt returns the same run. Add `.priority(n)` or `.even_if_queued()` before awaiting. |
| `run.is_dry_run()` | True when a dry run was asked for. |
| `run.due_at()`, `run.attempt()`, `run.id()` | When the run was due; which attempt this is (1 for the first, counting failed calls); its id. |

`run.start` keys each call on the calling run and the call's position within the function. So a
function that calls it twice starts two runs, and a later attempt that reaches the same calls
gets the same two runs back. The started runs begin chains of their own.

## Commands: a job done by a program

A command is a job whose work is done by a program: code that is not Rust, or not ported yet. It
is scheduled, linked, retried and observed like any other job.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Export { pub date: NaiveDate, pub region: String }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct ExportSummary { pub rows: u64, pub file: String }

#[command(preset = heavy)]
fn export_ledger(input: Export) -> Program<ExportSummary> {
    Program::new("./bin/ledger")
        .args(["export", "--date"])
        .arg(input.date)
        .args(["--region"])
        .arg(input.region)
        .pass_env(["DATABASE_URL"])
        .dir("/opt/ledger")
}
```

The function is not async and takes only its input. It does not do the work. It describes the
program to start.

| Rule | Detail |
| --- | --- |
| Fixed words and values | `.args([…])` takes the command's own fixed words and options. `.arg(value)` takes exactly one value, whatever it contains. There is no shell. A value that begins with `-` fails the run (`bad_input`), unless the command calls `.values_may_be_options()`. |
| Environment | The program gets only the variables named in `.pass_env([…])` (copied from the worker when set there) and `.env(name, value)`, plus `JOB_RUN_ID`, `JOB_ATTEMPT`, `JOB_DRY_RUN` (`1` or `0`) and `JOB_OUTPUT_FILE`. |
| Input | The whole input is also written as JSON to its standard input. |
| Ending | Exit status 0 is success. Status 75 (`EXIT_NOT_READY`) means "not ready"; the last line of standard error becomes the reason. Anything else fails the run as `exit_code`, or `signal` when a signal killed it. Such a failure is retried like an `Error`. |
| Output | `Program<T>` declares the output type. JSON the program writes to `JOB_OUTPUT_FILE` becomes the output. An empty file is `null`, which fits `Program<()>`. `{"skip": "reason"}` ends the run as skipped. Output that is not JSON, or does not fit `T`, fails as `bad_input`. Output over 64 KiB fails as `output_too_large`. |
| Standard streams | Standard output passes through to the worker's and is never stored. Standard error passes through too, and the last 16 KiB is stored with a failed run after the definition's `redact` hook has run over it. |
| Stopping | At a timeout or a cancel, the program's process group gets SIGTERM, then SIGKILL after the grace period. When the program exits, anything it left running in its process group is killed. |
| Not available | Progress, checkpoints and `run.start`. Another attempt starts the program again. `fresh_process` has no effect, because the program is always a new process. |

When the program's work is ported later, `#[command]` becomes `#[job]` on a function of the same
name. Its history and its place in every pipeline carry over.

## Dependencies

A job takes values your product provides, such as a database pool or an HTTP client, by
reference. The definition says how to build each one:

```rust
definition.provide(|| async {
    let url = std::env::var("APP_DATABASE_URL")?;
    Ok::<_, anyhow::Error>(sqlx::PgPool::connect(&url).await?)
});
definition.provide(|| async { Ok::<_, std::convert::Infallible>(Exchange::new()) });
```

- The builder is async and may fail. Its error type only needs `Display`.
- It is called lazily, once per process, the first time a job in that process asks for the type.
  Each child process has its own copy, so runs that execute at the same time never share a pool.
- `check` fails when a job takes a type that the definition does not provide.
- A worker's child inherits the worker's environment, so a builder can read credentials from it.
- Size pools small: a child runs one function at a time, and a worker has one child per slot.

## Testing a job

`#[job]` adds a `.test(…)` method to the handle. It calls the function directly, in the test's
process, with the dependencies passed as ordinary arguments. In the market example's module:

```rust
#[tokio::test]
async fn skips_when_nothing_is_published() {
    let run = Run::for_test();
    let day = TradingDay { date: NaiveDate::from_ymd_opt(2026, 11, 20).unwrap() };
    let exchange = Exchange { asked: AtomicU32::new(0) };
    let outcome = fetch_prices.test(run.clone(), day, &exchange).await;
    assert_eq!(outcome, Outcome::Skipped("the exchange has published nothing yet".into()));
    assert_eq!(run.reported(), ["progress: asking the exchange"]);
}
```

- `.test` returns `Outcome::Succeeded(output)`, `Skipped(reason)`, `NotReady(reason)` or
  `Failed(error)`.
- `Run::for_test()` is attempt 1, due now, never cancelled. `.with_checkpoint(&state)` continues
  from a checkpoint, and `.dry()` makes it a dry run.
- `run.started_jobs()` lists what the function started with `run.start`, as job name and input.
  `run.reported()` lists its progress lines and checkpoints, in order.
- For a command, `.test(input)` returns the `ProgramSpec` it would start, with `program`, `args`
  and `env`, so a test can check the arguments.
- `downbeat::check_problems(definition())` returns what `check` would report. An empty list means
  the definition is sound, which makes a good unit test. A test binary holds only the jobs it
  compiles, so a job outside every pipeline is reported there too.

The [authoring tests](../../crates/downbeat/tests/authoring.rs) show these in use.
