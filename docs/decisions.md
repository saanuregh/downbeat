# Decisions made while building

The design (`docs/design/`, copied from the plan) says what downbeat is. This file records each
choice the build had to make that the design left open, or where the build differs from it, with
the reason. Newest last. Each entry is final once written; a later entry may supersede it.

## 1. Dependency versions

The workspace uses widely deployed major versions whose APIs are known and stable: `sqlx` 0.8,
`axum` 0.8, `reqwest` 0.12, `jsonwebtoken` 10, `rand` 0.8, `syn` 2. Newer majors exist for some
(`sqlx` 0.9, `reqwest` 0.13, `syn` 3). `sqlx` 0.8 also matches what the first two adopters already
use, so their binaries carry one copy. Upgrading is a later, separate change.

## 2. The database schema is named `downbeat`

The design named it `jobs`. A public library should not claim a common schema name, and Axon
already has a `jobs` schema for Cadence's tables. Every table lives in schema `downbeat`.

## 3. `JobData` types also derive `Serialize` and `Deserialize`

A derive macro cannot add other derives, so a value type is written
`#[derive(Serialize, Deserialize, JobData)]`. This is the idiomatic pattern (as with `sqlx::FromRow`)
and keeps serde's own attributes available. `#[job_data(not_compared)]` follows a field's
`#[serde(rename = "…")]`; a container-level `rename_all` is not followed.

## 4. Command arguments: fixed text in `.args([…])`, values in `.arg(…)`

The design rejects a value that begins with `-` unless the command allows it. To tell values from
fixed text, `.args([…])` takes the command's own fixed words and options, and `.arg(value)` takes
one value. `.values_may_be_options()` allows a value to begin with `-`.

## 5. A day rule takes the date and one provided value

`.by_calendar(rule)` takes `fn(NaiveDate, &C) -> Day`, where `C` is a value the definition
provides (usually the product's calendar). `check` fails when it is not provided.

## 6. `done_for` compiles on any job; `check` rejects it on a job with no input

Rust cannot express "a job whose input is not `()`" as a bound. `done_within` is still limited to
jobs with no input by the compiler.

## 7. Engine timings are set for the whole definition

`Definition::settings(Settings { … })` sets the heartbeat, lease, grace, drain and stop time for
every queue. The design allowed a lease per queue; that is left for when a product needs it.

## 8. Tasks a function spawns are not tracked

The design asked the engine to cancel what a function spawned when it returns. A tokio task
spawned with `tokio::spawn` cannot be found from outside, and running each function on its own
runtime would break the dependencies the app provides, which must live across runs. Instead a
function should await what it spawns; a reused child is replaced after 500 runs and after any
forced kill, and `fresh_process` gives a clean process every run.

## 9. `InAppClient` connects with a database URL

The design passed the product's own pool. A URL works whatever `sqlx` version the product uses, and
the client's pool is small (four connections).

## 10. A command's output that does not fit its type fails as `bad_input`

The design used `output_too_large` for this. That name is kept for an output over 64 KiB; an output
that does not decode into the command's output type now fails as `bad_input` with a reason that
says so, which is what a person reading the run needs.

## 11. Schedule state remembers a digest of the schedule's times

`schedule_state.times_hash` lets a scheduler see that a schedule's times changed in a deploy, so
the schedule starts again from now and a new time earlier today does not fire (chapter 3,
"Across deploys"). It is the one column the data model gained.

## 12. A cancel request records who asked

`run.cancel_requested` (a boolean) became `run.cancel_requested_by` (text, null when nobody
asked), so a cancelled running run's reason can name the person, as chapter 4 requires.

## 13. `downbeat::migrate(url)` is public

Every role migrates at start. A product that prefers a separate migration step, and the tests, call
`downbeat::migrate` directly.

## 14. Fixed command words go in `.args([…])`, including options

Restating decision 4 for authors: `Program::new("tool").args(["eod", "--date"]).arg(input.date)`.
