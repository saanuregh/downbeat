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

## 15. Request tokens are produced with `ed25519-dalek`, not `jsonwebtoken`

The token is still a standard compact JWS/JWT (RFC 7515, RFC 7519) following RFC 8725. The
`jsonwebtoken` crate's only Ed25519 backend also pulls in the `rsa` crate, which carries an
unfixed timing advisory (RUSTSEC-2023-0071). Signing and checking one fixed format is about a
hundred lines over `ed25519-dalek`, all in `downbeat-api`, shared by the scheduler, the client and
the console. The header names the algorithm `Ed25519` (RFC 9864); `EdDSA` is accepted too.

## 16. `htu` holds the request's path and query

DPoP's `htu` is an absolute URI. A scheduler usually sits behind a proxy or load balancer that
changes the host, and `aud` already pins the token to one scheduler, so the token binds the path
and query only.

## 17. The callers file is TOML

`--callers callers.toml` lists `audience` and one `[[caller]]` table per trusted caller: `name`,
`keys` (base64url Ed25519 public keys), `max_scope` and optional `jobs`. Unknown fields are
rejected so a typo cannot silently widen or narrow a grant.

## 18. The Rust client starts jobs by name

`downbeat-client` does not depend on the engine, so it cannot name a job by its handle. It starts
a job by name with any `Serialize` input; the scheduler checks the input against the job's type and
answers 422 when it does not fit. Typed starts through a shared crate of job handles are left for
when a product asks for them.

## 19. Rate limit per actor

The listener allows each actor 20 requests a second on average with bursts of 100, in memory per
scheduler. It protects against a runaway caller, not a determined one; that is the network's job.

## 20. Only callers marked `acts_for_people` may name another actor

From the first independent review. A caller's token names itself as the actor (`sub` equals
`iss`) unless the callers file sets `acts_for_people = true` for it, as for the console. Rate
limits and idempotency keys are per caller and actor together, so one caller cannot use another's
keys or escape its own limit by changing the actor.

## 21. A caller limited to some jobs sees and changes only those

Every read filters to the jobs the token covers (pipelines, chains, schedules, queues, overrides,
backfills, audit); actions that span jobs (pausing a queue) need a token that covers every job.

## 22. Cancels read under the chain's lock, in one routine

`store::cancel` is the one place that cancels a run or a chain. A run that goes back to the queue
after someone asked for a cancel is ended as cancelled instead, and the claim never takes such a
run.

## 23. Every wait on a child is bounded

Preparing a run, waiting for a command's program and checking its output now watch the job's
timeout, the lease and shutdown, like the function call itself. Heartbeats and database calls made
for a running function have deadlines, and every connection has a 60-second statement timeout.

## 24. A command's leftover background processes are killed when it exits

The program's process group is killed once its main process exits, so a background process cannot
hold its standard error open and keep the run from finishing.

## 25. `check` finds jobs no pipeline names through `inventory`

Every `#[job]` and `#[command]` registers its name with the `inventory` crate, the only way to
list items compiled into a binary without a hand-kept list. A job in a shared crate that one
product does not use must be removed from that product's build, or named in a pipeline.

## 26. The example product and the demo

`examples/market` is a small product written the way the design intends; its schedule snapshot is
committed and CI fails when it is stale. `mise run demo` runs it with the console on this machine
against a throwaway Postgres.
