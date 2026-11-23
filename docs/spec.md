# downbeat behavioural specification

This document states the rules downbeat follows, precisely enough to test against. "Must" means
the engine does this in every case; a difference between this text and the code is a bug in one
of them. Each section names the modules that implement it, relative to `crates/downbeat/src/`
unless another crate is named. Choices made while building are explained in
[decisions.md](decisions.md); the guide explains the same rules for users.

Terms:

- **Run**: one execution of a job. An unfinished run is a row of `downbeat.run`; a finished run is
  a row of `downbeat.run_history`. A run is in exactly one of the two.
- **Chain**: one starting run and every run linked from it. A row of `downbeat.chain`.
- **Call**: one call of a run's function (or start of a command's program).
- **Fence**: a counter on a run that is raised whenever the run changes hands.
- **Now**: the database's `now()`, unless a rule says otherwise.

## 1. Jobs and the definition

Modules: `job/`, `definition.rs`, `registry.rs`, `check.rs`, `downbeat-macros`.

1. A job's name is its function's name, or the `name` given in its attribute. Names must be unique
   across the definition.
2. A job function must be `async`, non-generic, take `Run` first, then at most one input by value,
   then any number of dependencies by shared reference, and return `JobResult<T>` with `T: JobData`.
   A command function must not be `async`, must take at most its input, and must return
   `Program<T>`. The macros reject anything else at compile time.
3. Every job compiled into the binary must be named in exactly one pipeline. Building the registry
   (which every role, `check`, `run-once`, `InAppClient` and every child process do) must fail on
   any problem listed in [the guide](guide/05-running.md#check-and-snapshots-in-ci), and a role
   must refuse to start.
4. The definition must set a zone; there is no default. When a process is given an environment
   and the definition lists environments, the environment must be one of them.
5. A dependency must be built at most once per process, on first use, by the definition's
   builder. A failed build must fail the call that needed it, as an `error`.
6. A value (input, output, checkpoint) must be at most 64 KiB as JSON (`MAX_VALUE_BYTES`).
   Progress text, and the text of an error, panic, skip or not-ready reason, is cut to 64 KiB in
   the child, ending in ` [cut]`.
7. The identity of a value is its `JobData::identity()`: the serialized value without its
   `not_compared` fields. The *input hash* is the SHA-256 of the identity as canonical JSON:
   object keys sorted, keys whose value is `null` left out. Two inputs are *equal* when their
   hashes are equal.
8. Input from a request (API, `run.start`, `InAppClient`, `run-once`) must be decoded strictly:
   a field the type's decoder does not read, at any depth, is refused and named by its dotted
   path; a field sent as `null` is not refused. A field the type reads but leaves out when it
   writes itself (such as one with `skip_serializing_if`) is known. A stored value must be decoded
   leniently.

## 2. Runs and statuses

Modules: `store/runs.rs`, `store/ending.rs`, `api/views.rs`.

1. An unfinished run's state is `queued` or `running`. A finished run's status is one of
   `succeeded`, `skipped`, `failed`, `cancelled`, `expired`.
2. The API must present both as one `status`: `queued`, `running`, or the finished status.
3. `started_by` is one of `schedule`, `link`, `person`, `program`, `backfill`, `job`. Through the
   API, a token whose actor equals its caller starts runs as `program`; any other actor as
   `person`. `InAppClient` starts runs as `program`; `run.start` as `job`.
4. A finished run must have exactly one reason sentence. A skipped run must have one
   `skipped_because`: `job_skipped`, `condition_false`, `need_not_met`, `input_unchanged` or
   `joined`. A failed run must have one `failed_because`: `error`, `panic`, `crash`,
   `out_of_memory`, `timeout`, `exit_code`, `signal`, `bad_input`, `output_too_large`,
   `need_not_met`, `gave_up`, `worker_lost` or `job_not_registered`.
5. A queued run must report one `queued_because`, decided in this order when it is read:
   1. `paused`, when an active pause covers everything, its job or its queue;
   2. `waiting_turn`, when another run with `starts > 0` holds its turn key;
   3. the stored value (`not_ready`, `need_not_met`, `backing_off`, `scheduled_later`), when the
      run's next claim time is still in the future;
   4. otherwise `no_free_worker`, with a reason saying whether any worker seen in the last minute
      serves the queue.

## 3. The run lifecycle

Modules: `worker/mod.rs`, `worker/prepare.rs`, `worker/call.rs`, `store/runs.rs`.

1. **Claim.** One statement must mark the most urgent claimable run `running`, raise its fence,
   set its lease owner and `lease_expires_at = now() + lease`, and clear `function_started_at`.
   A run is claimable when it is `queued`, on the worker's queue, its `run_at` has passed, no
   cancel was requested, its job is in the worker's build, no active pause covers everything, its job or
   queue, and no other run with `starts > 0` holds its turn key (a null turn key never blocks).
   The most urgent is the highest `priority`, then the earliest `run_at`. Concurrent claims must
   skip locked rows.
2. **Prepare.** While `starts = 0`, the worker must prepare the run before calling it: the child
   builds the input (for a `link` run, from its parents' history rows, through the link's
   adapter), computes the input hash and the turn key, and evaluates conditions (section 6).
   The worker stores the input, hash and turn key, fenced. Preparing is bounded by the run's
   timeout (rule 10.5). A crash or a timeout while preparing is a failed call: it gets another
   attempt when `retry_on` lists it and attempts remain (section 4). A run with `starts > 0` must
   never be prepared again.
3. **Function start.** The worker must record `starts = starts + 1`,
   `function_started_at = now()` and a renewed lease, fenced. On the run's first call this write
   takes the run's turn (section 5). If another run holds the turn, the write must fail, and the
   run must go back to the queue as `waiting_turn` with no delay and nothing counted; it is then
   prepared again on its next claim.
4. **Call.** The function runs in a child process (section 10) under its timeout, memory limit,
   lease and the worker's shutdown.
5. **Decide.** How the call ended decides what is recorded:

   | Call ended | Recorded |
   | --- | --- |
   | Output, within 64 KiB | `succeeded`, with the output |
   | `skip(reason)` | `skipped` (`job_skipped`) |
   | `not_ready(reason)` | Back to the queue as `not_ready`, after `recheck_every`; nothing counted |
   | Error | `error`: another attempt or `failed` (section 4) |
   | Panic | `failed` (`panic`) |
   | Output over 64 KiB | `failed` (`output_too_large`) |
   | Input did not decode | `failed` (`bad_input`) |
   | Child exited or was killed by a signal the worker did not send, or sent a message the worker could not read | `crash`: another attempt or `failed` |
   | Over `max_memory` | `out_of_memory`: another attempt or `failed` |
   | Past the timeout | `timeout`: another attempt or `failed` |
   | Cancel requested | `cancelled`, naming who asked |
   | Lease lost, or no heartbeat succeeded for a whole lease | Nothing; the reaper handles the run |
   | Worker shutting down | Back to the queue (section 11) |

6. Every write by the worker that holds a run must carry the run's id, its fence, the worker's
   id and `state = 'running'`. A write that changes no row means the worker no longer holds the
   run, and the worker must record nothing more for it. The ending is the one exception: it
   matches on id and fence only (see the ending routine), because every change of hands raises
   the fence.
7. A requeue must not happen when a cancel was requested: the run must end as `cancelled`
   instead.
8. When writing its decision fails, a worker must try twice more, a second apart, then leave the
   run to the reaper.

## 4. Attempts and waiting

Modules: `job/policy.rs`, `worker/mod.rs`, `reaper.rs`.

1. A run's attempt number is `failures + 1`, where `failures` counts calls that ended in a
   failure. Waits, lost workers and interruptions must not change it.
2. A failure of kind `Error`, `Timeout`, `Crash` or `Memory` must get another attempt exactly when
   `retry_on` contains the kind and the attempt number is less than `attempts`. Otherwise the run
   fails. `panic`, `bad_input` and `output_too_large` must never get another attempt. A command's
   failure (`exit_code`, `signal`, or a program that could not start) is of kind `Error`.
3. Another attempt must go back to the queue as `backing_off`, with `failures` raised by one,
   after `min(first × 2^(n−2), max)` for attempt `n`, where `first..=max` is the job's backoff.
4. A wait (`not_ready`, a command's exit 75, an unmet need with `.or_wait()`) must go back to the
   queue after `recheck_every`, with nothing counted.
5. A queued run whose `created_at` is more than its job's `give_up_after` ago must end as `failed`
   (`gave_up`), whatever it is waiting for, with a reason that ends with its last stored queued
   reason (without that reason's final full stop), if it has one.
6. "Check now" must make a queued run due at once only when its stored `queued_because` is
   `not_ready`, `need_not_met` or `backing_off`.

## 5. The same-work rules

Modules: `starting.rs`, `store/runs.rs` (`create`, `claim`, `function_started`),
`scheduler/fire.rs`, `store/ending.rs`.

Two runs are the same work when they are of the same job and their input hashes are equal.

**Rule 1: the same work is never queued twice.**

1. A run that rule 1 covers carries `same_work_key = "<job>:<input hash>"`. At most one run with a
   given key and `starts = 0` may exist (a unique index).
2. Creating a covered run whose key already has a run with `starts = 0` must instead *join* that
   run, in the same statement: the existing run's `priority` becomes the greater of the two, its
   `due_at` the later, and its `expires_at` the later, or null when either is null. No run and no
   chain is created.
3. Rule 1 covers: runs created by a schedule; and runs started by a person, a program or
   `run.start`, unless the request is *deliberate*.
4. A request is deliberate when it sets any of `even_if_queued`, `dry_run`, `without_children`,
   `ignore_conditions`, `not_before` or a timeout. Rule 1 must not cover deliberate requests,
   linked children, backfill runs, retries or restarts.
5. A scheduled request that joins must write a history row with status `skipped`,
   `skipped_because = 'joined'`, and `joined_run` set to the run it joined. Such rows must not
   appear in run lists, chains or "latest run" answers, must not be delivered to `on_finish`,
   and must be pruned after two days.
6. A person's or program's request that joins must write an audit row `start_joined`, and the API
   must answer 200 with `joined: true`.

**Rule 2: the same work never runs twice at once.**

7. Every run that has been prepared or created with its input carries a *turn key*:

   | Job policy | Turn key |
   | --- | --- |
   | Default | `<job>:<input hash>` |
   | `one_at_a_time` | `<job>:*` |
   | `one_per = field` | `<job>:<field>=<the field's value as JSON>` |

8. At most one run with a given turn key and `starts > 0` may exist (a unique index). Because
   `starts` never decreases, a run holds its turn from its first call until it leaves
   `downbeat.run`, through every requeue, wait, lost worker and interruption.
9. Rule 2 has no exceptions. A linked run's turn key is null until it is prepared, so it never
   blocks or is blocked before then.
10. Among runs waiting for one turn, no order is promised.

## 6. Conditions

Modules: `worker/prepare.rs`, `child.rs` (`prepare`), `store/history.rs`, `pipeline/need.rs`.

1. Conditions must be evaluated only while the run has not begun (`starts = 0`), and must be
   skipped entirely when the run was started with `ignore_conditions`.
2. With `ignore_conditions`, the child must not call any `only_if` test or `done_for` closure, so
   one that fails cannot fail the run.
3. Order: `only_if` conditions in the order written; then needs in the order written; then
   `if_changed`. The first that stops the run decides.
4. A false `only_if` must end the run as `skipped` (`condition_false`), naming the condition.
5. A `done_for` need is met when the needed job has any `succeeded` history row whose input hash
   equals the hash of the input the need's closure returns. A `done_within(d)` need is met when the
   needed job has a `succeeded` row that finished less than `d` ago.
6. An unmet need must: skip the run (`need_not_met`) by default; with `.or_wait()`, requeue it as
   `need_not_met` after `recheck_every`; with `.or_fail()`, fail it (`need_not_met`).
7. `if_changed` applies only to runs started by `schedule` or `link`. It must skip the run
   (`input_unchanged`) when the job's latest `succeeded` row has the same input hash. With
   `one_per`, only succeeded rows with the same turn key count.

## 7. Links and chains

Modules: `pipeline/`, `store/ending.rs`, `store/cancel.rs`, `api/act/`.

**The ending routine.** Every ending of a run must go through `store::ending::end`, in one
transaction:

1. Take the chain's advisory transaction lock. Everything that changes a chain (ending, cancel,
   retry, mark as succeeded) must take this lock first.
2. Delete the run from `downbeat.run` where its id and fence match. When no row matches, the run
   changed hands: write nothing.
3. Insert the history row with status, reasons, output and `notify_after = now()`.
4. Unless the ended run has `without_children`, create the children whose link is now satisfied.

**Child creation.** For each job linked after the ended run's job, a child must be created when
all of these hold:

| Condition | Rule |
| --- | --- |
| The chain is not cancelled | `chain.cancelled_at` is null |
| The child is not in the chain yet | No unfinished run, and no non-joined history row, of the child's job in this chain |
| Every parent has finished in the chain | No parent job has an unfinished run in the chain |
| Every parent's latest ending fits the link | `.after`: `succeeded`; `.after_failure`: `failed`; `.after_any`: any status |

| Parent's latest ending | `.after` | `.after_failure` | `.after_any` |
| --- | --- | --- | --- |
| `succeeded` | Created (when every parent succeeded) | — | Created |
| `skipped` | — | — | Created |
| `failed` | — | Created | Created |
| `cancelled` | — | — | Created |
| `expired` | — | — | Created |
| No run in this chain | — | — | — |

A created child must: belong to the chain; record its parents' run ids; take its priority and
queue from its own policy; inherit `dry_run`; not inherit `ignore_conditions`; be
`started_by = link`; and never join other work. (A run started `without_children` has no
children, so that flag never reaches one.)

**Child input.** At preparation, the child receives one value per parent, in the link's order:
for `.after`, the parent's output; for `.after_failure`, `Failure { job, run_id, kind, reason }`
where `kind` is the parent's `failed_because`; for `.after_any`, `Ended<T>`, tagged `ended` with
content `value` (the output for `succeeded`, a `Failure` for `failed`, the reason otherwise).
A single parent's value is passed as is; several parents' values form a tuple. A `.map` adapter
is applied last. A value that does not fit must fail the child as `bad_input`.

**Chain actions.**

1. *Retry* must create a new run of the same job, with the same input, parents and `dry_run`, in
   the same chain. A `link` run that ended before it was prepared has no input of its own, so its
   retry is a `link` run again and builds its input from its parents. Retry works under the chain
   lock and must be refused (409) in a cancelled chain. It is not covered by rule 1. Because
   children already in the chain are not created again, only children that have not run follow
   it.
2. *Restart* must start a new chain from the job with the same input, as a deliberate request.
3. *Mark as succeeded* must apply only to a `failed` row: under the chain lock it sets the status
   to `succeeded`, clears `failed_because`, stores the supplied output (checked strictly against
   the output type), the note, `marked_by` and `marked_at = now()`, sets `notify_after = now()`,
   writes an audit row, and creates children as in step 4 of the ending routine.
4. *Cancel a chain* must mark the chain cancelled (once; 409 after), then cancel each unfinished
   run of it as below.
5. *Cancel a run*: a queued run must end at once as `cancelled`; a running run must get
   `cancel_requested_by` set, which its worker sees at its next heartbeat and acts on (section 9).
   A queued run with a cancel requested must never be claimed, and the reaper must end it.

## 8. Schedules and occurrences

Modules: `schedule/`, `scheduler/fire.rs`, `scheduler/plan.rs`, `scheduler/backfill.rs`.

**Occurrences of a date.**

1. A date's plan is, in order of precedence: the dated override for that schedule and date
   (`off`, `times` or `window`); else the day rule's answer (`Off`, `Shifted(delta)` or
   `Normal`); else the normal times.
2. Weekday and day-of-month filters must apply only to normal and shifted plans. A date that fails
   a filter has no occurrences unless an override gives it times.
3. `daily_at`: each listed time on the date (shifted by `delta` if shifted, replaced by the
   override's times if overridden).
4. `every(i)` with a window `start..end`: from `start` on the date, every `i` of elapsed time,
   while at or before `end` (on the next date when `end < start`). Shifted plans move the window;
   a `window` override replaces it. Every occurrence belongs to the window's start date.
5. `every(i)` with no window: from local midnight, every `i`, while before the next local
   midnight. A shift has no effect.
6. Wall-clock times must be resolved in the schedule's zone. A time that does not exist must
   resolve to the first existing minute after it; a time that exists twice, to the earlier
   instant.

**Firing.**

7. Every scheduler, every `poll`, must lock up to 50 of its registered schedules whose
   `next_due_at` has passed, skipping rows another scheduler holds, and read `now()` once.
8. For each locked schedule, inside its own savepoint: if the schedule is paused, create nothing.
   Otherwise take the occurrences after its watermark up to now. Only the latest may produce a run.
   It must be dropped when an expiry applies and `occurrence + expiry < now`; otherwise a run is
   created (section 5, rule 1 applies). The earlier occurrences are dropped. Dropped occurrences
   more than ten seconds old (and an expired latest one) must be reported as one
   `OccurrencesDropped` event.
9. Then the watermark must become `max(watermark, now)` and `next_due_at` the first occurrence
   after now within about a year (null if none).
10. When firing one schedule returns an error, only its savepoint must be rolled back; its
    `next_due_at` must become now + 60 seconds and a `ScheduleFailed` event recorded. Other
    schedules fire.
11. A scheduled run has `due_at` = the occurrence, and `expires_at` = occurrence + expiry, where
    expiry is `expire_after`, or one interval for `every`, or none for `daily_at`.
12. A watermark must never move backwards.

**Registration.** At start and every minute, each scheduler must register its schedules:

13. A schedule with no state row starts with `watermark = now`, `next_due_at = now`.
14. A schedule whose state was last registered more than ten minutes ago gets
    `watermark = max(watermark, now)`.
15. A schedule whose digest of times, weekdays, days of the month and zone changed gets
    `watermark = max(watermark, now − 5 s)` and `next_due_at = now`.

**Overrides.**

16. An override has a schedule, a date, a change, times and a required note. `times` needs at least
    one time and only fits `daily_at`; `window` needs exactly two and only fits
    `every(…).between(…)`. A second override for the same schedule and date replaces the first.
17. Saving or removing an override must set the schedule's `next_due_at = now`.
18. An override must never fire a time before the watermark.

**Backfills.**

19. The scheduler must drive each `running` backfill every two polls, under a row lock. While the
    current date's chain has an unfinished run, it waits. When that chain has a `failed`,
    `expired` or `cancelled` run and `continue_on_failure` is false, the backfill becomes
    `failed`.
20. Otherwise it creates a run for the next date from `next_date` up to `to` that has an
    occurrence under its plan with **no** overrides (the day rule and filters apply), with the
    input computed from that date's first occurrence. The run is `started_by = backfill`, due now,
    expiring at now + expiry, never joined, in a new chain marked with the backfill's id. When no
    date is left, the backfill becomes `done`. An error makes it `failed`.

## 9. Leases, fencing, lost workers and the reaper

Modules: `worker/lease.rs`, `reaper.rs`, `definition.rs` (`Settings`).

1. While it holds a run, a worker must send a heartbeat every `heartbeat` that sets
   `lease_expires_at = now() + lease`, saves pending progress and checkpoint, and reads
   `cancel_requested_by`.
2. A heartbeat that changes no row must stop the call (stop reason *lost*). A cancel requested
   must stop the call (reason *cancel*).
3. When no heartbeat has succeeded for a whole `lease`, timed by the worker's own monotonic clock
   from when the last successful heartbeat was *sent*, the worker must stop the call (reason
   *unreachable*), record nothing, and later record a `DatabaseUnreachable` event.
4. Stopping a call means: ask the function to stop, wait `kill_grace`, then kill the process group.
   A child cut short while it runs other product code (preparing a run, naming a command's
   program, or checking a command's output) is killed at once: that code is synchronous and cannot
   be asked to stop. Such a child is never reused.
5. The reaper runs every five seconds in every process with a role. A `running` run whose lease
   expired more than `kill_grace + 10 s` ago must be handled, skipping locked rows:

   | State | Action |
   | --- | --- |
   | Function not started in this claim | Back to `queued`, fence raised, nothing counted |
   | Function started, `lost_count = 0` | Back to `queued`, fence raised, `lost_count = 1`, a "worker lost" call logged, `WorkerLost` event |
   | Function started, `lost_count ≥ 1` | Ended as `failed` (`worker_lost`) |

6. The reaper must also end: queued runs with a cancel requested (`cancelled`); queued runs with
   `starts = 0` past `expires_at` (`expired`); queued runs past `give_up_after` (`gave_up`); and
   queued runs created more than an hour ago whose job is in the build of no worker seen in the
   last hour, when at least one row of a process in the worker role exists
   (`job_not_registered`). It must delete expired
   used-token records. Every duty runs on every sweep, and a run that cannot be handled is logged
   and passed over, so a failing run holds up neither the other runs of its batch nor the other
   duties. Each duty walks its runs in `run_id` order, batch after batch while a batch comes back
   full, so runs that keep failing never fill every batch and hide the runs after them. Pruning (section 13) runs beside
   the sweeps, so a long pass delays none, and a sweep or a prune that overruns its period delays
   the next rather than making up the missed ones.
7. A run whose function has been called at least once must never be expired.

## 10. Child processes and limits

Modules: `worker/process.rs`, `worker/child.rs`, `worker/call.rs`, `child.rs`, `protocol.rs`.

1. Job code must never run in the worker process. Functions, `only_if` tests, link adapters and
   command functions run in a child: the product's own binary started with `DOWNBEAT_CHILD_FD`.
   Some product code runs in the scheduler or API process instead, each call guarded so that a
   panic costs only that call: a schedule's `.input(…)` function and its day rule (section 8);
   decoding, hashing and reading the `one_per` field of an input before a run is created, and
   decoding an output set by hand; and, for the preview of a start with `ignore_conditions`
   (14.17), the job's `only_if` tests and `done_for` closures, where a panic counts as false;
   and the `on_finish` and `on_event` callbacks (section 12). The definition's `redact` hook runs
   in the worker, guarded too: if it panics, the standard error tail is withheld. When one process
   has both roles, that is also the worker's process.
2. Every process the worker starts must be started from one dedicated thread, in a new process
   group, with a parent-death signal of SIGKILL and `oom_score_adj` 1000. It must exit at once if
   the worker died before the signal was set.
3. A child must report ready within 60 seconds, and runs one function at a time. Messages are
   length-framed JSON over a Unix socket; after the opening `Ready`, each carries its run id, and
   a message for another run must be ignored. A child must not touch the engine's tables. When
   the worker can no longer read the child's messages (it exited, or sent one over 4 MiB), a child
   that has not exited within a second is killed, so the slot never waits on it.
4. A reused child must be replaced after `recycle_child_after` runs whose function it called (a run
   that ends while it is prepared does not count), after any call that ended
   in a panic, crash, timeout, memory kill, stop or shutdown, and after a function that returned
   with tasks it spawned still running (the child counts the runtime's alive tasks before and after
   the call). A `fresh_process` job must get a new child for its run. It has no effect on a
   command, whose program is always a new process.
5. The timeout clock starts when the call starts. The timeout is the run's override when a person
   set one (`0` meaning none), else the job's `timeout`.
6. With `max_memory`, the resident memory of the child (or the command's program) must be sampled
   every two seconds from `/proc`, and the process group killed above the limit.
7. A command's program must get a cleared environment plus its `pass_env` and `env` variables,
   `JOB_RUN_ID`, `JOB_ATTEMPT`, `JOB_DRY_RUN` and `JOB_OUTPUT_FILE`, a file the worker creates
   empty first, readable only by its user (a program that cannot have it fails as not started);
   its input as JSON on standard
   input; and arguments passed directly, without a shell. An argument from `.arg(…)` that begins
   with `-` must fail the run as `bad_input` unless the command allows options. A program whose
   name, arguments, environment, passed variable names and directory together exceed 1 MiB as
   JSON (`MAX_PROGRAM_BYTES`) must fail the run as `bad_input` before it starts. When the program
   exits, its process group must be killed. At a stop, the group gets SIGTERM, then SIGKILL after
   `kill_grace`.
8. A command's exit status 0 means success: an empty output file is `null`, a JSON object with a
   string `skip` ends the run as skipped, a file over 64 KiB fails as `output_too_large`, and a
   file that is not JSON (including one that is not UTF-8) or does not fit the output type fails as
   `bad_input`. Status 75 means not
   ready. Any other ending fails as `exit_code` or `signal`, with the last 16 KiB of standard error
   stored after the definition's `redact` hook. A program that cannot be started fails as `error`,
   with the reason naming why.

## 11. Shutdown

Modules: `cli.rs` (`wait_for_signal`, `serve`), `worker/mod.rs`, `definition.rs`.

1. On SIGTERM or SIGINT, the worker must stop claiming, and the scheduler, reaper and control API
   must stop.
2. The worker must wait up to `drain` for its slots to finish. Then it must stop each running call
   (section 9, step 4). A call that ends by itself during the grace period must be recorded as it
   ended. A command's program is asked to stop with SIGTERM, so it is recorded as ended only when
   it exits 0 (its output is then checked as usual) or 75 within the grace period; any other exit
   counts as stopped. Otherwise the run must go back to the queue at once as `no_free_worker`, with
   `interruptions` raised by one and nothing else counted. A run being prepared goes back to the
   queue at once, as `no_free_worker`, with nothing counted.
3. At a run's third interruption, and at each one after it, a `RunInterrupted` event must be
   recorded.
4. The worker, and the scheduler, must each delete its own `worker` row before it exits.
5. A definition whose `drain + kill_grace + 5 s` exceeds `stop_within` must be rejected.

## 12. Callback delivery

Modules: `scheduler/callbacks.rs`, `events.rs`, `store/audit.rs`.

1. Only processes with the scheduler role deliver callbacks, every `poll`, 20 rows at a time per
   kind, skipping locked rows.
2. Before calling, a row's `notify_after` must be pushed five minutes ahead and committed. No
   transaction may be open while the callback runs.
3. When the callback returns `Ok`, `notify_after` must be set to null. When it returns an error,
   the row comes round again after five minutes.
4. A history row that finished, or was marked as succeeded, more than a day ago (the later of
   the two), or an event recorded more than a day ago, must be dropped (marked delivered) with an
   error log line instead of being called.
5. When the definition has no `on_finish` (or `on_event`), every pending row of that kind must be
   marked delivered.
6. An event row that does not decode into a known `Event` must be marked delivered and skipped.
7. Delivery is at least once. `FinishedRun.run_id` and `EventDelivery.id` are stable keys for
   de-duplication.

## 13. Pruning and retention

Modules: `pruning.rs`.

1. Pruning runs hourly in every process with a role, in batches of 500 chains per statement,
   batch after batch while a batch comes back full, for at most five minutes a pass; what is left
   waits for the next pass.
2. History must be removed a whole chain at a time (deleting the chain cascades to its rows), by
   the policy of the chain's first job:
   - chains started more than `keep_runs_for` ago;
   - with `keep_runs_at_most = n`, chains beyond the newest `n` whose first run did not fail and
     did not join. Each chain counts once, by its first run of that job, and a chain that may not
     be pruned (rule 3) still counts toward the `n`.
3. A chain must not be pruned while it has an unfinished run, a history row waiting for its
   callback, or a row that is its job's latest success, by the rule of 16.4 (a dry run is not a
   success, and a run marked as succeeded counts from when it was marked). Pruning must take the
   chain's lock without waiting, pass over a chain whose lock is held, and check these conditions
   again after the lock is held, in a statement that sees the data as it is then.
4. The statement that removes chains must, in the same statement, add their runs (not dry runs,
   not `joined` rows) to `downbeat.day_totals`: per job and calendar day in the definition's zone,
   the count of each final status and the longest succeeded run. The report reads them with the
   history (section 14), so its counts do not change when runs are pruned. Totals older than 31 days are
   removed.
5. Also removed: `joined` rows after two days; idempotency keys after seven days; overrides 30
   days after their date; delivered audit rows after a year; pauses that have ended; worker rows
   not seen for a day.

## 14. The control API

Modules: `api/mod.rs`, `api/auth.rs`, `api/act/`, `api/read/`, `downbeat-api/src/token.rs`.

**Configuration.**

1. The API must be off unless `--listen` or `--local` is given. `--listen` requires `--callers`.
   `--local` must listen on a loopback address only (default `127.0.0.1:7400`), with audience
   `local` and one caller `local` holding the published development key, `administer` and
   `acts_for_people`.
2. The callers file must reject unknown fields, a caller with no keys, and a caller listed twice.

**Authentication.** For every request under `/v1`:

3. The request must carry `Authorization: Bearer <token>`; the body is read up to 256 KiB (413
   beyond).
4. The token must be a three-part compact JWS whose header has `typ = downbeat-request+jwt`,
   `alg = Ed25519` or `EdDSA`, and a `kid`.
5. Its `iss` must be a configured caller, and the signature must verify strictly against one of
   that caller's keys.
6. `aud` must equal the scheduler's audience; `exp − iat` must be at most 60 seconds; now must be
   at most `exp + 5 s` and at least `iat − 5 s`, by the scheduler process's clock (the token's
   times come from the caller's clock, not the database's).
7. `htm` must equal the request method (case-insensitive), `htu` the path and query as received
   (including `/v1`), and `body_sha256` the base64url SHA-256 of the body, absent for an empty
   body.
8. Failing any of 3–7 must answer 401, except a body over 256 KiB, which is 413. The header is
   checked before the body is read, so a request with no token is 401 whatever its size.
9. `sub` must equal `iss` unless the caller has `acts_for_people`; the token's `scope` must not
   exceed the caller's `max_scope`; for a caller limited to `jobs`, the token must name jobs, all
   within that list. Failing these must answer 403.
10. Each `<caller>/<actor>` pair is limited to 20 requests a second on average with bursts of 100,
    in memory per scheduler process; beyond that, 429.
11. For a method other than GET, the token's `(iss, jti)` must be recorded until `exp + 6 s`. A
    token already recorded must be answered 401.
12. Every request the authentication layer refuses (rules 3–11) must be logged. A
    `RequestRefused` event, carrying the count since the last one, is recorded at most once a
    minute per caller and status. A token that does not verify names no trusted caller, so its
    refusals are reported with no caller. Refusals by an endpoint (403 for scope, 404, 409, 422) are
    answers, not attacks, and are not counted; nor is a 5xx because the database cannot be reached
    or failed.

**Authorization.**

13. GET endpoints need `read`. Start, cancel, retry, restart, check now and cancel chain need
    `operate`. Pauses and resumes, overrides, backfills and mark as succeeded need `administer`,
    and so does a start with `ignore_conditions` or `timeout_seconds`.
14. A token limited to jobs must see and act on only those jobs: lists are filtered, a single
    item of another job is 403, and a chain needs its first job covered to be read (and is left out
    of a pipeline's recent chains otherwise). Cancelling a chain, or a backfill (which cancels the
    chain of the date it is running), needs every job in that chain covered; a queue pause or the
    pause of everything needs a token covering every job.

**Requests.**

15. A start's input must be decoded strictly into the job's input type; failure is 422. A dry run
    of a job without `supports_dry_run` is 422. An unknown job is 404.
16. With an `Idempotency-Key` header, the key must be claimed for `(<caller>/<actor>, job, key)`
    in the same transaction as the run. A repeated key must return the first run with
    `repeated: true` and HTTP 200, creating nothing. A new run answers 201; a joined run 200 with
    `joined: true`.
17. Every changing endpoint must accept `?preview=true`, which must change nothing, after the
    same scope and input checks, and answer `Preview { effects, false_conditions }`.
    `false_conditions` is filled only for a start with `ignore_conditions`.
18. Every real change must write an audit row naming the actor, as `<actor> (via <caller>)` when
    they differ.
19. A pause must have a non-empty note and either a future `until` or `until_resumed = true`.
    A resume of something not paused, or whose pause has already ended, is 409.
20. Errors must be JSON `{ "error": "<code>", "message": "<sentence>" }`, including an unknown
    path, a wrong method and a body that does not parse. Every response must carry
    the `downbeat-api-version` header.
21. The report must count each job's runs by the calendar day, in the definition's zone, on which
    they finished, adding the history and the day totals of pruned runs (section 13), leaving out
    dry runs and `joined` rows. Its days must run from `days − 1` days ago to today by the
    database's clock, with `days` from 1 to 30. Failure rate is failed ÷ (succeeded + failed).

## 15. The console's access model

Modules: `downbeat-console/src/access.rs`, `session.rs`, `proxy.rs`, `api.rs`, `config.rs`.

1. A person may sign in only when the identity token is valid (signature with an asymmetric
   algorithm the provider lists, issuer, audience, expiry, nonce), carries an email the provider
   has verified (`email_verified` is true), and the email's domain is in `allowed_domains` or one
   of the token's groups is in `allowed_groups`. The return path after sign-in must be a path on
   the console: starting with one `/`, with no backslash and no control character.
2. A session id is 32 random bytes, sent as unpadded base64url in the cookie; only the SHA-256 of
   that cookie value is stored. A session must be refused after
   `session_idle_hours` without use or `session_absolute_hours` after it started. Sign-out must
   delete it, and only when the request carries the session's CSRF token.
3. Every `/api` request except a GET, and `POST /auth/logout`, must carry the session's CSRF
   token in `x-csrf-token`. `public_url` must be `https://`, or plain `http://` on a loopback
   address or `localhost`.
4. A person's access to a scheduler for scope `s` is decided from the grants on that scheduler
   given to them or to one of their groups:
   - A first administrator, or anyone in `--local` mode, has every scope on every pipeline.
   - Otherwise the grants with scope ≥ `s` count; with none, the person does not have `s`.
   - A token for `s` carries exactly the scope `s`, limited to the union of the counted grants'
     pipelines, or to none when any counted grant lists no pipelines.
   - A GET is signed for `read`. A change is signed for `operate`; when the scheduler answers 403
     and the person also holds `administer`, it is signed again for `administer`. A request the
     person has no scope for, or that the scheduler refuses at every scope tried, must be answered
     403 and audited as `refused`.
5. Pipelines must be turned into the token's `jobs` from the scheduler's `GET /v1/pipelines`,
   fetched as the console itself and cached for 60 seconds.
6. The forwarded request must be signed with the scheduler entry's key, `iss` = its `caller`,
   `sub` = the person's email, `aud` = its audience; method, raw path, query, body and any
   `Idempotency-Key` pass through unchanged. Only paths under `/v1/` are forwarded; a path with a
   dot segment or an encoded slash must be refused.
7. Adding or removing a grant must need `administer` on every pipeline of that grant's scheduler,
   so nobody can grant more than they hold. Listing grants must show only the schedulers the person
   administers in full; listing people and the console's audit must need that for at least one
   scheduler. A grant to a person needs that person to have signed in before.
8. `--local` must listen on a loopback address only, and must answer only requests whose `Host` is
   `127.0.0.1`, `localhost` or `[::1]` with its port.

## 16. Watching and health

Modules: `scheduler/watch.rs`, `store/history.rs`, `api/read/mod.rs`, `cli.rs` (`health`).

1. Every process with the scheduler role must check, every 30 seconds, for slow runs and overdue
   jobs, by the database's clock.
2. A job's *expected duration* is the median, and its *slow threshold* 1.5 times the 90th
   percentile (at least one second), of the durations of its last 20 `succeeded` runs that were
   not dry runs. A run's duration is from its first start to its end, retries included. A job
   with fewer than five such runs has neither.
3. A running function that has run longer than its job's slow threshold must raise `RunSlow`,
   with how long it has run and the expected duration, once per run.
4. A job with `expect_success_every = d` is *overdue* when more than `d` has passed since its
   latest success, or, when it has never succeeded, since its first run, finished or not. A job
   that has never run is never overdue. A dry run is not a success here. A run marked as
   succeeded is a success from when it was marked. A run that has not started yet counts from when
   it may first start.
5. An overdue job must raise `JobOverdue`, with its latest success if any, once per lapse, however
   many schedulers watch: not again until it succeeds, and not while the job is paused as a run
   would be (rule 2.5: by its job, its queue or everything).
6. `GET /healthz` must answer `Health` with the API version and needs no token. `GET /readyz` must
   answer the same only when the database answers, and 503 (`database`) otherwise. `GET /version`
   must answer the API version and the engine's version.
7. `<binary> health` must exit with success when a worker process on this host has refreshed its
   `worker` row within two heartbeats, and with failure otherwise. The host is the worker id's
   host part, compared whole; scheduler rows do not count.

