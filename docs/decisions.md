# Decisions made while building

The specification (`docs/spec.md`) says what downbeat does. This file records each choice the
build made that the original design left open, or where the build differs from that design, with
the reason. Newest last. Each entry is final once written; a later entry may supersede it.

## 1. Dependency versions

The workspace uses widely deployed major versions whose APIs are known and stable: `sqlx` 0.8,
`axum` 0.8, `reqwest` 0.12, `jsonwebtoken` 10, `rand` 0.8, `syn` 2. Newer majors exist for some
(`sqlx` 0.9, `reqwest` 0.13, `syn` 3). `sqlx` 0.8 is also what many existing
services use, so a product's binary is likely to carry one copy. Upgrading is a later, separate change.

## 2. The database schema is named `downbeat`

The design named it `jobs`. A public library should not claim a common schema name that a product
may already use. Every table lives in schema `downbeat`.

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

## 27. A console token carries exactly the scope a request needs

From the console review. A read is signed with `read`; a change first with `operate`, and again
with `administer` only when the scheduler refuses the first. Each token is limited to the pipelines
of every grant that reaches its scope, so a pipeline administration never narrows what a person may
operate elsewhere.

## 28. Only someone who administers a whole scheduler manages its grants

Administering some pipelines does not let a person change grants, so nobody can grant more than
they hold. The grants list shows the schedulers the person administers in full.

## 29. Console sign-in hardening

The identity provider must have verified the person's email (grants and first administrators
follow emails). The return path after sign-in refuses control characters. Signing out needs the
session's CSRF token. `public_url` must be https, or plain http on loopback. In `--local` mode the
console answers only to this machine's own names, so a page elsewhere cannot reach it through DNS
rebinding. The proxy forwards the raw path under `/v1/` only, refusing dot segments and encoded
slashes.

## 30. App code in the scheduler is guarded against panics

From the documentation review. A schedule's input, a day rule and the callbacks are app code that
runs in the scheduler role. A panic in any of them now costs that one call (the schedule is tried
again a minute later, the callback five minutes later), and a scheduler task never dies.

## 31. One rule for "overdue"

A job is overdue when its latest success, or its first run if it has never succeeded, is older
than its `expect_success_every`. The API and `on_event` use the same rule; a job that has never
run is not overdue.

## 32. Refused requests are reported at most once a minute

Every refused control API request is logged; `RequestRefused` carries how many there were since
the last report and is recorded at most once a minute per caller and status, so a flood of bad
requests cannot flood the audit table.

## 33. Lease and heartbeat per queue (supersedes 7)

`Settings::queues` maps a queue to its own `QueueTimings` (heartbeat and lease), as the design
allowed; every other queue uses the definition-wide values. `check` refuses a lease shorter than
two heartbeats.

## 34. A pause of everything

`POST /v1/pause` and `POST /v1/resume` hold and release every run of every job, from the design's
list of things to add during the build. The pause is a row of kind `all`, added by migration 2,
which older engines ignore.

## 35. A test kit with the test's own clock

`downbeat::testing::Schedules` answers, for any date or instant and with no database, what a
definition's schedules would do: occurrences after the day rule, the next one, what an outage would
miss, and each occurrence's input. `Run::for_test()` gained `.due(…)`, `.on_attempt(…)` and
`.cancelled_already()`. Dated overrides live in the database and are not applied by the kit. The
market example became a library with a binary, so its tests can use its definition.

## 36. Each job's input is published as a JSON Schema

`JobView.input_schema` is the input as a JSON Schema (draft 2020-12), built from the shape the
`JobData` derive records. A type it does not recognise accepts any value there; the scheduler still
checks every input against the real type.

## 37. Typed starts through declared job names (supersedes 18)

`downbeat_api::job_name!(pub send_report: ReportRequest)` declares a job's name and input type in
a crate a product's services share, without the engine. `Client::start_job` takes such a name, so
the compiler checks the input. Starting by name stays for callers with no shared crate.

## 38. Pruning runs batch after batch, within a budget

Measured (`docs/performance.md`): one hourly pass of 500 chains kept up with only about 12,000 runs
a day. A pass now deletes batches until a job has nothing left to prune, for up to five minutes. An
index on chains by first job did not make it faster, so none was added.

## 39. The docs' samples are compiled

`tests/guide` is a test package whose build script pulls every Rust sample from the README and
the user guide and compiles it against a small scaffold of the names the samples assume. Rustdoc
doctests were not used: their hidden setup lines would show on GitHub. Samples written with "…"
are syntax templates and are skipped.

## 40. A child that holds leftover tasks is replaced (supersedes 8)

A task spawned with `tokio::spawn` cannot be cancelled from outside, but it can be counted. The
child compares the runtime's alive tasks before and after each function; if the function left any
running, the child says so and the worker replaces it after the run. The leftover tasks die with
the process and never reach the next run.

## 41. Sign-in is tested against a real provider when Dex is available

`tests/e2e/tests/sign_in_dex.rs` runs Dex with an in-memory store and one password user, fills in
its login form, and checks that the console signs the person in and forwards a request for them.
CI does not run it (Dex ships no release binary to download); `mise-tasks/test-rust` runs it whenever
`dex` is on `PATH`.

## 42. The specification was checked rule by rule against the code

An audit read every rule in `docs/spec.md` beside the code. Where the code was wrong it was fixed
and, for the serious rules, tested: "mark as succeeded" now respects `without_children`; inputs
and outputs set by hand are held to 64 KiB; strict decoding finds unknown fields at any depth
(a field sent as `null` is not unknown, because a type may leave a `None` out when it writes
itself); a run started with `ignore_conditions` calls none of its condition code; every API error,
including the framework's own, is JSON; a pipeline's recent chains show only covered jobs; an
output file that is not UTF-8 is `bad_input`; resuming an ended pause is 409; `RunInterrupted`
is recorded only once the requeue is written; each backfill gets its own savepoint;
`keep_runs_at_most` counts chains, not rows; pruning takes the chain lock. Where the code was
right and the wording was not, the specification was corrected.

## 43. A start preview evaluates conditions in the scheduler

A preview of a start with `ignore_conditions` lists the conditions that are false now. That means
calling the job's `only_if` tests and `done_for` closures, which are product code, in the
scheduler process instead of a child. Starting a child per preview would cost a process for a
read, and the tests are meant to be cheap and pure. Each call is guarded, so a panic counts as
false and costs only that call. This is the one exception to "job code never runs in the worker
or scheduler process" (spec 10.1).

## 44. Pruning takes the chain lock without waiting

The end routine creates a chain's children under the chain's advisory lock. Pruning now takes the
same lock with `pg_try_advisory_xact_lock` in its selection, so it never removes a chain while a
run in it is ending, and never waits behind one: a chain that is busy is passed over until the
next pass.

## 45. In `--local` mode the console checks the Host header on every route

The stand-in person administers everything, so a page elsewhere must not reach the console by
pointing its own name at this machine (DNS rebinding). The check used to sit in the session
layer, which left the sign-in routes and the screens' files unchecked; it is now a middleware on
the whole router.

## 46. Strict decoding asks the type's own decoder (supersedes part of 42)

Decision 42 found unknown fields by comparing what a caller sent with what the type writes back.
That refused fields the type knows but leaves out when it writes, such as one with
`skip_serializing_if`. Strict decoding now uses `serde_ignored` (MIT OR Apache-2.0, by the author
of serde, no dependencies beyond serde) to hear every field the decoder skips, at any depth.
Writing the same by hand would mean re-implementing serde's deserializer forwarding. A field sent
as `null` is still accepted.

## 47. Pruning locks first, then checks

Pruning used to test whether a chain may go and take its lock in one statement, so the test saw
the data as it was before the lock. A retry or a "mark as succeeded" committed in between could be
deleted with the chain. Pruning now takes the locks in one statement and deletes, re-checking, in
a second statement of the same transaction, which sees the data as it is once the locks are held.

## 48. A crash or timeout while preparing is a failed attempt

Preparing a run calls product code (input adapters and conditions), so it can crash or hang like
a function. It now counts as a failed call and follows `retry_on` and `attempts`, as the guide
already said, instead of failing the run at once.

## 49. A command that exits cleanly during a shutdown keeps its result

At shutdown a command's program gets SIGTERM and the grace period. If it exits 0 or 75 in that
time, its result is recorded (exit 0's output is checked by the child as usual, and that check is
allowed to finish during the shutdown). Any other exit is taken as a reaction to the signal, and
the run goes back to the queue as interrupted.

## 50. A run marked as succeeded records when (`marked_at`)

`on_finish` drops a delivery that is more than a day old, so a callback that always fails cannot
be retried forever. That rule counted from `finished_at`, so a failure marked as succeeded two
days later was never delivered. Changing `finished_at` would distort how long the run took, so
migration 3 adds `marked_at` beside `marked_by`, set in the same statement, and delivery counts
its day from the later of the two. The column is nullable, so an older engine ignores it.

## 51. CI runs the real sign-in test (supersedes part of 41)

Dex publishes no release binary, so decision 41 left its test to machines that have `dex`. CI now
installs Nix and builds `nixpkgs#dex-oidc`, and the test runs on every pull request. Nix is used
for this one binary only; the rest of CI is unchanged and does not use mise.

## 52. Cancelling a backfill needs every job in its current chain

Cancelling a backfill cancels the chain of the date it is running, and a chain can reach jobs
other than the backfill's own. A token limited to some jobs could therefore cancel runs of jobs it
does not cover. It now needs every job in that chain covered, checked under the chain lock, as
cancelling a chain does; one method on the caller, `require_jobs`, holds that rule for both.

## 53. Keying an input outside a child is guarded

Before a run is created, the scheduler and the API decode its input, hash it and read its
`one_per` field, which calls the product's types. A panic there used to abort the whole batch of
scheduled firings, and on the next tick the same schedule came first again. These three steps are
now one guarded method, `InputOps::keyed`, used by every caller, and an output set by hand is
decoded under the same guard.

## 54. A child cut short while preparing is not reused

When preparing a run times out, is cancelled or loses its lease, the worker kills the child. The
child is now marked spoiled when that happens, and the slot starts a new one, so the next run is
never charged with a crash it did not cause.

## 55. The scheduler removes its own process row

The worker deleted its row from `downbeat.worker` when it stopped, but the scheduler did not, so a
stopped scheduler looked alive for up to a day. Both roles now delete their own rows.

## 56. The screens are checked by rendering, not by comparing pixels

A screenshot comparison in CI breaks on every font, anti-aliasing or browser update, and a person
then approves new images without looking. What matters is that each screen renders its data and
shows no error. `tests/e2e/tests/screens.rs` loads every route of a `--local` console in front of
a real scheduler in headless Chromium, and checks each page's heading, its data and the absence
of an error note. CI runs it with the Google Chrome that GitHub's runners include. How a change
looks is still checked by a person, with screenshots, as CONTRIBUTING.md asks.

## 57. A run interrupted while being prepared goes back at once

A shutdown that caught a run while it was being prepared re-queued it like a failed child, ten
seconds later. A shutdown is usually a deploy, and nothing of the run has happened yet, so it now
goes back at once for the next worker, with nothing counted, as an interrupted call does.

## 58. Every refused sign-in is audited

A sign-in refused because the person was outside the allowed domains and groups was audited, but
one refused because the provider had not verified the email address was not. Both now write a
`sign_in_refused` row with the reason.

## 59. Time decisions use the database's clock

The spec defines "now" as the database's `now()`, so that processes on machines with drifting
clocks agree. Three checks used the process's own clock instead: when a callback is too old to
deliver, whether a pause's end is in the future, and whether a queued run is due later. Each is
now asked of the database, inside a query that already runs where possible (the claim of a
delivery batch, the queue view's worker count), so no request gains a round trip except a pause
with an end time.

## 60. A stopped command's process group is always killed

A command stopped for a timeout, a cancel or a lost lease gets SIGTERM and the grace period. When
its main program exited in that time, the worker returned without killing the group, so anything
the program had started in the background, and that ignored SIGTERM, outlived the run and could
overlap its next attempt. The group is now killed in that case too, as it is when a program ends
by itself.

## 61. Overdue follows the run's own rules, and is decided under a lock

The overdue check now matches how the rest of the engine treats a run. A pause of the job's queue
or of everything quiets it, as it holds back the job's runs (rule 2.5). A dry run is not a
success, because it does no work. A run marked as succeeded counts from when it was marked, as its
`on_finish` does; dating it by its end could put it before the last event and hide the next lapse.
A run started with `not_before` counts from when it may start. The check and the event share a
transaction under an advisory lock per job, so two schedulers that watch at once raise one event.
`health` matches the host whole, from worker rows only, and reads the host name the way the worker
id is built. Found by an audit after section 16 was added; each case has a test that failed before.

## 62. mise manages every development tool and task

mise now installs PostgreSQL's server programs (from conda-forge, prebuilt), actionlint and
shellcheck as well as Bun, all locked for every platform in `mise.lock`. A contributor no longer
needs Postgres from the system or `PG_BIN`, though `PG_BIN` still picks another installation.
PostgreSQL is pinned at 18.4, the newest version conda-forge builds for every platform the lock
covers (18.6 and 18.7 have no macOS Apple silicon build yet). The longer tasks are mise file tasks
in `mise-tasks/`, which are plain bash, so CI runs `mise-tasks/test-rust` without mise and keeps its
own tool setup. `mise run check` now runs every step CI runs (API docs, the example's snapshot),
plus actionlint, which CI leaves out because the runners do not include it. Dex stays optional:
mise could only build it from Go source.

## 63. The tests' Postgres runs in a podman container

`mise-tasks/postgres.sh` now starts the official Postgres image in podman, with its data in memory
and its port on 127.0.0.1, instead of a cluster from local server programs. Locally and in CI the
tests run against the same pinned version; before, CI used Ubuntu's Postgres 16 and mise installed
18.4 from conda-forge, whose dependencies made up most of `mise.lock`. Contributors now need
podman (a podman machine on macOS). The engine does not care how it reaches Postgres; the one test
that looked at connections through the Unix socket now looks at TCP connections to the port. This
replaces the Postgres part of decision 62.

## 64. Rust 1.100

The toolchain is Rust 1.100. The crates still build on 1.98, their declared minimum.

- Cargo 1.100 puts test executables deeper in the target folder, so the end-to-end tests now find
  the console binary in the nearest folder above their own executable that holds it.
- Clippy's new `assert_is_empty` lint is allowed: its suggested rewrite spells out an empty
  array's full element type in every assertion.
- Cargo's new manifest checks found leftovers, now removed: `homepage` repeated `repository`, and
  three unused dependencies. The guide-sample crate declares that it does not inherit the
  workspace's lints, because its samples compile exactly as the docs show them.

## 65. `bun lint` once Bun 1.4.6 ships

Bun's own linter, `bun lint`, is only in Bun canary, which mise cannot install or lock. Until Bun
1.4.6 ships, oxlint stays. `bun lint` reads `.oxlintrc.json` and already passes on the screens.
Its three findings were fixed so that both linters agree:
- `Facts` now takes `Fact` children instead of `[label, value]` pairs, which also removes the
  placeholder `key` props;
- `prefer-tag-over-role` is off, because an SVG drawing needs `role="img"`.

When 1.4.6 ships: pin it in `mise.toml` and CI, replace the `lint` script and the oxlint
dependency with `bun lint --max-warnings=0`, and update CONTRIBUTING.

## 66. Every Rust dependency at its newest version

All direct dependencies are at their newest versions: base64 0.23, ed25519-dalek 3, rand 0.10,
reqwest 0.13, sha2 0.11, sqlx 0.9, syn 3, toml 1 and tower-http 0.7. The lock file holds the newest
compatible versions of the rest. What changed:

- **sqlx 0.9:** a query must be a `&'static str`, or SQL built at run time wrapped in
  `AssertSqlSafe`. The engine's three pruning queries are built from a constant and are wrapped.
  Test helpers that take only fixed SQL now require `&'static str`. Both migration runners are
  hand-written, so sqlx's `migrate` feature is no longer enabled. sqlx's migrator cannot keep the
  engine's rule that an older engine runs on a newer schema unless that newer migration is
  breaking, and one way of migrating is simpler than two, so the console keeps its own runner too.
- **reqwest 0.13:** TLS now checks certificates against the operating system's store, not a
  bundled copy of Mozilla's roots, and uses the aws-lc crypto library. A client given its own
  certificate authority trusts only that authority (`tls_certs_only`).
- **rand 0.10:** API keys come from the operating system's generator (`SysRng`), as before. Session
  ids and token ids come from the thread-local generator, as before.
- **sha2 0.11:** digests no longer format as hex with `{:x}`; one test uses `hex::encode`.
- **Still held back by other crates:** openidconnect 4.1, its newest release, still uses reqwest
  0.12, so the console builds both reqwest versions. axum pins matchit 0.8.4.

## 67. MIT license only

downbeat is licensed under MIT alone, not MIT OR Apache-2.0. One license is simpler for users and
contributors to read, and MIT already allows every use the dual license did.

## 68. The daily report is read on demand, and pruning keeps its counts

People wanted a daily look at how jobs did: failures, failure rate and timing. It is a read
endpoint, `GET /v1/report`, with a console screen, rather than a pushed event: the callbacks
already cover anything that must reach someone, and a report read on demand needs no schedule or
delivery.

Its counts come from the history, but `keep_runs_at_most` prunes a frequent job's successes within
the hour, which would inflate its failure rate. So pruning, in the statement that removes chains,
adds their runs to `downbeat.day_totals` (a count per status and the longest success, per job and
day). That adds no write to a run's ending. Medians and 90th percentiles cannot be added up, so
they come from the runs still in the history. Totals are kept by calendar day in the definition's
zone at pruning time; if the zone changes, older totals stay on the old zone's days.
