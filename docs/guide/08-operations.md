# 8. Operations

This chapter is for the people who keep jobs running: how to read what downbeat says, and what to
do about it. Every action here is available on the console's screens and in the
[HTTP API](../reference/http-api.md), and every action can be previewed first.

## Reading reasons

Every run, and every gap where a run might have been, has a `reason`: one plain sentence that
says what is happening or what happened, and why. Lists return it, not only the detail view, and
`on_finish` receives it. Start there.

A run's `status` is one of:

| Status | Meaning |
| --- | --- |
| `queued` | Waiting to be claimed; `queued_because` says why. |
| `running` | Claimed by a worker: being prepared, or its function is running. |
| `succeeded` | The function returned an output, or a person marked the run as succeeded. |
| `skipped` | It did not do its work; `skipped_because` says why. |
| `failed` | It failed for good; `failed_because` says how. |
| `cancelled` | Someone cancelled it, or its chain or backfill. |
| `expired` | A scheduled or backfill run that was not started before its occurrence expired. |

### `queued_because`

| Value | Meaning | What to do |
| --- | --- | --- |
| `no_free_worker` | Due, and waiting for a slot. The reason says whether every slot on the queue is busy or no running worker serves the queue. Also used for a run that was just released by a shutdown or taken back from a lost worker. | Add slots or workers for the queue, or check that a worker serving it is running. |
| `waiting_turn` | Another run holds its turn: the same work, or the whole job with `one_at_a_time`, or the same value with `one_per`. The reason names the run it waits for. | Usually nothing. If the holder is stuck, look at it. |
| `paused` | Its job or its queue is paused. The reason says by whom, until when, and why. | Resume when ready. |
| `need_not_met` | A need written with `.or_wait()` is not met yet. Checked again every `recheck_every`. | Look at the needed job. "Check now" re-checks at once. |
| `not_ready` | The function returned `not_ready`, or the command exited with 75. The reason gives its message and the next check. | "Check now" re-checks at once. |
| `backing_off` | A call failed and the next attempt waits for its backoff. The reason gives the error and the time of the next attempt. | "Check now" runs it at once. |
| `scheduled_later` | Started with `not_before`, which has not come yet. | Nothing. |

`paused`, `waiting_turn` and `no_free_worker` are worked out when you read the run. The others
were stored when the run was queued, and apply while its next check time is in the future.

### `skipped_because`

| Value | Meaning |
| --- | --- |
| `job_skipped` | The function returned `skip("…")`, or a command wrote `{"skip": "…"}`. |
| `condition_false` | An `only_if` condition was false; the reason names it. |
| `need_not_met` | A need was not met, and the need does not wait. |
| `input_unchanged` | `.if_changed()`: the input equals the input of the job's last success. |
| `joined` | A scheduled occurrence joined a run already queued for the same work. These rows are hidden from run lists and chains, shown on the calendar, and deleted after two days. |

### `failed_because`

| Value | Meaning | Retried? |
| --- | --- | --- |
| `error` | The function returned an error. `error_chain` has the message. | While attempts remain (`Error` is in `retry_on` by default) |
| `panic` | The function panicked. | Never |
| `crash` | The child process died without the worker killing it: an abort, a signal, the kernel's out-of-memory killer. Also a crash while the run was being prepared. | Only with `retry_on = [Crash, …]` |
| `out_of_memory` | The worker killed the process above `max_memory`. | Only with `Memory` in `retry_on` |
| `timeout` | The function ran past its timeout. Also when preparing the run took longer than the timeout. | Only with `Timeout` in `retry_on` |
| `exit_code` | A command's program exited with a status other than 0 or 75, or could not be started. `stderr_tail` has the end of its standard error. | Like `error` |
| `signal` | A command's program was killed by a signal it was not sent by the worker. | Like `error` |
| `bad_input` | The input did not decode into the job's type, a link's data did not fit, a command's argument looked like an option, or a command's output did not fit its type. | Never |
| `output_too_large` | The output was over 64 KiB. | Never |
| `need_not_met` | A need written with `.or_fail()` was not met. | Never |
| `gave_up` | Still queued `give_up_after` after it was created. The reason ends with the last reason it was queued for. | Never |
| `worker_lost` | The worker died while the function ran, for the second time. | Never |
| `job_not_registered` | No running worker has had this job in its build for an hour. Usually a job that was removed or renamed. | Never |

"Attempt 2 of 3" always counts failed calls. A wait, a lost worker or an interrupted deploy is
not an attempt. A run also reports how many times its function was called in all (`calls`), how
often a worker was lost (`lost_count`) and how often a shutdown interrupted it
(`interruptions`), and its detail view lists the last ten calls with the worker and build that
made each.

## Pausing

| Pause | Holds | Needs |
| --- | --- | --- |
| A schedule | The schedule creates no runs. Its position still moves forward, so resuming releases no backlog. The job can still be started by hand. | `administer` on the schedule's job |
| A job | No worker claims any run of the job, however it was started. New runs are still created and wait, and same-work requests join them. | `administer` on the job |
| A queue | No worker claims any run on the queue. | `administer`, with a token covering every job |

- A pause needs a note, and either an end time (`until`) or an explicit `until_resumed: true`.
  An end in the past is refused.
- Runs already executing are not interrupted.
- When a job or queue is resumed, held runs are claimed as usual. Scheduled runs whose occurrence
  expired meanwhile end as `expired`.
- A pause that has ended stops applying at once, and is deleted within the hour.
- The preview of a schedule pause says how many occurrences will not run.

## Dated overrides

Turn a day off, or give it other times or another window, for one or more schedules. See
[schedules](04-schedules.md#a-dated-override-at-runtime). Add them on the Calendar screen or with
`POST /v1/overrides`, always after looking at the preview: it says how many occurrences the day
will have instead, and how many of the new times have already passed and will not run. An
override for the same schedule and date replaces the earlier one.

## Backfills

A backfill runs a scheduled job for each date in a range, as if its schedule had fired on each.

- The input comes from the schedule's `.input`, called with that date's first occurrence. A date
  the schedule's weekdays, days of the month or day rule turn off is passed over. Dated
  overrides do not apply.
- One date at a time, in order. Each date is its own chain. The next date starts when nothing in
  the current chain is queued or running.
- If any run in a date's chain failed, expired or was cancelled, the backfill stops there as
  `failed`, unless it was started with `continue_on_failure`.
- Backfill runs never join other work, and `if_changed` does not apply to them. Each is due when
  it is created, so expiry and `give_up_after` count from then.
- Its progress is stored, so it survives restarts. `GET /v1/backfills` shows the date it has
  reached. Cancelling it cancels the date that is running.
- With several schedules, name the one to take the input from; the default is the job's first.

## Retry, restart, or mark as succeeded

| Action | What it does | Use it when |
| --- | --- | --- |
| **Retry this step** (`/retry`) | A new run of the same job with the same input, in the **same** chain. When it ends, children that have not run yet in this chain start, so the pipeline continues from where it broke. Children that already ran are left alone. Refused in a cancelled chain. | A step failed for a passing reason. |
| **Restart from here** (`/restart`) | A new chain from this job with the same input, so it and everything after it run again. | You fixed something upstream and want everything downstream recomputed. |
| **Mark as succeeded** (`/mark-succeeded`) | Records a failed run as succeeded, with an output you supply (checked against the job's output type) and a required note. Its children start. The run says for ever who recorded the outcome. Needs `administer`. | You fixed the data by hand. |
| **Check now** (`/check-now`) | Makes a run that is `not_ready`, `need_not_met` or `backing_off` due at once. | You know what it waits for is there. |
| **Cancel a run** | A waiting run ends at once as `cancelled`. A running one is asked to stop at its next heartbeat (15 seconds by default) and killed after the grace period. Its `.after` children do not run. | |
| **Cancel a chain** | Everything queued or running in the chain is cancelled, and nothing more is created in it, not even `.after_any` children. | |

Retry and restart need a finished run (restart also accepts an unfinished one whose input is
known). Neither applies `if_changed`, because a person asked for the work.

## Being told what happened

downbeat raises no alerts and exports no metrics itself. Two callbacks on the definition let your
product do that its own way:

```rust
definition.on_finish(|run: FinishedRun| async move {
    if run.failed() {
        alerts::page(&run.job, &run.reason, run.run_id).await?;
    }
    Ok::<_, anyhow::Error>(())
});

definition.on_event(|delivery: EventDelivery| async move {
    metrics::record(&delivery.event);
    Ok::<_, anyhow::Error>(())
});

definition.log_link("https://logs.example.com/search?run={run_id}");
definition.redact(|stderr| stderr.replace(&secret_prefix(), "***"));
```

- `on_finish` is called for every finished run, except `joined` rows. A run marked as succeeded
  is delivered again. `FinishedRun` carries the run id, job, chain, status, `skipped_because`,
  `failed_because`, reason, who started it, the failure count, the output, the finish time and
  the log link.
- `on_event` is called for each `Event`:

  | Event | When |
  | --- | --- |
  | `WorkerLost` | A worker stopped renewing while a run's function executed; the run went back to the queue. |
  | `RunInterrupted` | A run has been interrupted by shutdowns three or more times. |
  | `RunSlow` | A running function has gone past 1.5 times the 90th percentile of the job's last 20 successful durations (once at least five exist). Once per run. |
  | `JobOverdue` | A job with `expect_success_every` has not succeeded for that long. Once per lapse, and not while the job is paused. |
  | `OccurrencesDropped` | A schedule's missed occurrences were dropped, with the count and the first and last. |
  | `ScheduleFailed` | Creating a schedule's run returned an error (for example a day rule's dependency could not be built, or the input does not fit the job). It is tried again a minute later; other schedules are not affected. |
  | `DatabaseUnreachable` | A worker could not renew a lease for a whole lease length and stopped its function. Sent once the database is back. |
  | `RequestRefused` | The control API refused a request, for example with 401, 403, 413 or 429. |

- Both are delivered by the scheduler role, from the database, **at least once**. A callback
  that fails (returns an error) is called again five minutes later; after a day of failures the
  delivery is dropped with a log line. Use `FinishedRun.run_id` or `EventDelivery.id` as a
  de-duplication key in anything that pages someone.
- If the definition sets no `on_finish` (or `on_event`), pending deliveries are discarded.
- `log_link` adds a link to your log system to every run the API returns and every `FinishedRun`.
  Every log line a function writes through `tracing` carries its `run_id`.
- `redact` runs over a failed command's standard error before its tail is stored.

## What to alert on

| Signal | Source | Suggested action |
| --- | --- | --- |
| A run failed | `on_finish` with `status = "failed"` | Page for important jobs; read the reason. |
| A job is overdue | `on_event` `JobOverdue` | Set `expect_success_every` on every job whose absence matters. This catches schedules that stopped, pauses left on, and chains that skip. |
| Occurrences dropped | `on_event` `OccurrencesDropped` | No scheduler was running; check scheduler deployments. |
| A schedule cannot fire | `on_event` `ScheduleFailed` | A bug in the schedule's input or day rule. |
| A worker was lost | `on_event` `WorkerLost` | Look for crashes or kills of worker machines. |
| The database was unreachable | `on_event` `DatabaseUnreachable` | Look at the database and the network. |
| A run is slow | `on_event` `RunSlow` | Warn; it may be about to time out. |
| A run keeps being interrupted | `on_event` `RunInterrupted` | Add checkpoints, or deploy that worker less often. |
| Bursts of refused requests | `on_event` `RequestRefused` | A misconfigured caller, a wrong clock, or someone probing. |
| Runs waiting with `no_free_worker` for long | `GET /v1/queues` (`longest_wait_seconds`) | Add capacity. |
| `job_not_registered` failures | `on_finish` | A job was removed while runs of it waited. |
