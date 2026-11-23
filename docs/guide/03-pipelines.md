# 3. Pipelines

A pipeline is a function that names a group of jobs belonging to one flow, and says for each job
what starts it and on what conditions. It reads from top to bottom as the flow.

```rust
fn market_hours(pipeline: &mut Pipeline) {
    pipeline.job(fetch_prices).schedule(every(30.minutes()).input(|o| TradingDay { date: o.date() }));
    pipeline.job(build_candles).after(fetch_prices).if_changed();
    pipeline.job(export_csv).after(build_candles);
}

definition.pipeline("market_hours", market_hours);
```

Rules that keep it one way:

- Every job is named in exactly one pipeline. A job that only people or programs start is named
  too, as a bare `pipeline.job(name);`. `check` fails on a job that no pipeline names, and on a
  job named twice.
- A pipeline says nothing about how a job runs. Policies live on the job.
- A false condition ends the run as `skipped`, with a sentence that names the condition. So
  there is always a row that says what happened.

| What starts the job, or holds it back | Written as |
| --- | --- |
| A schedule | `.schedule(daily_at([…]))` or `.schedule(every(…))`; see [schedules](04-schedules.md) |
| Another job succeeding | `.after(parent)` |
| Another job failing, or ending in any way | `.after_failure(parent)`, `.after_any(parent)` |
| Another job having done its part | `.needs(job.done_for(…))`, `.needs(job.done_within(…))` |
| A test of the input | `.only_if("name", \|input\| …)` |
| An input that changed | `.if_changed()` |

## Links: data flows from parent to child

```rust
pipeline.job(build_candles).after(fetch_prices);
```

The child's input is the parent's output, and the compiler checks that the types match. A job
has one link. A second `.after` does not compile.

| Link | The child starts when the parent | The child's input |
| --- | --- | --- |
| `.after(parent)` | Succeeded | The parent's output |
| `.after_failure(parent)` | Failed for good, with its attempts used up | `Failure { job, run_id, kind, reason }` |
| `.after_any(parent)` | Ended in any way: succeeded, skipped, failed, cancelled, expired | `Ended<T>`: `Succeeded(T)`, `Skipped(reason)`, `Failed(Failure)`, `Cancelled(reason)`, `Expired(reason)`; a match on it needs a `_` arm |

`kind` in `Failure` is the parent's `failed_because`, such as `error`, `timeout` or `crash`.

### Adapting with `.map`

When the child wants a different shape, the link adapts the parent's output with `.map`. Nothing
is converted silently.

```rust
pipeline.job(send_summary).after(build_candles.map(|c: Candles| Summary { day: c.day }));
pipeline.job(refresh_cache).after(build_candles.map(|_| ()));   // a child with no input
```

### Joins: several parents

A job with several parents lists them as a tuple (up to four) and receives a tuple. It starts
when all of them have succeeded in the same chain.

```rust
#[job]
async fn day_report(run: Run, (prices, candles): (Prices, Candles)) -> JobResult<()> {
    run.progress(format!("{} prices, {} candles", prices.count, candles.rows));
    Ok(())
}

pipeline.job(day_report).after((fetch_prices, build_candles));
pipeline.job(day_digest).after((fetch_prices, build_candles).map(|(p, _)| p.day));
```

`.after_failure` and `.after_any` take one parent.

## Chains

A **chain** is one starting run and every run it leads to through links. A run that a schedule,
a person, a program, a backfill or `run.start` starts begins a new chain. A linked child belongs
to its parent's chain, and so does a step a person retries.

- "Both parents" in a join means both parents *in this chain*, not any two runs of those jobs.
  `check` rejects a join whose parents share no common first job, because they could never end
  in the same chain.
- A link may cross into another pipeline. The child belongs to its own pipeline and to its
  parent's chain.
- When several jobs should start together, give them a common first job. When it does no real
  work, it is one line: `async fn start_of_day(run: Run, day: TradingDay) -> JobResult<TradingDay> { Ok(day) }`.
- A child is created at most once per chain.
- The history of a chain is kept and pruned as a whole, by the first job's `keep_runs_for` and
  `keep_runs_at_most`.

## When a run ends

Every way a run can end goes through one routine, in one database transaction, under a lock on
its chain. It records the ending and creates the children that the ending starts:

| Parent ended as | `.after` children | `.after_failure` children | `.after_any` children |
| --- | --- | --- | --- |
| Succeeded | Start (once every parent has succeeded) | | Start |
| Skipped | | | Start |
| Failed | | Start | Start |
| Cancelled | | | Start |
| Expired | | | Start |

No child is created when:

- the chain was cancelled,
- the run that ended was started "without children",
- the child already has a run in this chain, or
- a parent still has an unfinished run in this chain.

A skip stops a chain: nothing linked with `.after` follows it. That is why a function with
nothing to do returns `skip`, not an empty `Ok`.

A child is created knowing only its parents' run ids. Its input is built in the worker's child
process just before it first runs, through the link's `map`. A child of a dry run is a dry run
too.

## Branching with `only_if`

A branch is a named condition on the child. The child is started, tests its input, and either
does its work or is skipped with the condition's name.

```rust
pipeline
    .job(notify_users)
    .after(refresh_tokens)
    .only_if("some refreshes failed", |report: &RefreshReport| report.failed > 0);

pipeline.job(page_oncall).after_failure(refresh_tokens);
```

The skipped run's reason reads "Skipped: condition 'some refreshes failed' was false." Two
children with opposite conditions make an if and an else. The test runs in the worker's child
process, so it may be any Rust code, but it should be quick and depend only on the input.

For one run per item of a list, call `run.start` once per item. Those runs begin chains of their
own and cannot be joined on. A job that reacts to the failure of many jobs, such as paging
someone, is better written as an [`on_finish` callback](08-operations.md#being-told-what-happened).

## Needs: preconditions

A need says "this job is only worth running if that other job has done its part". It starts
nothing.

```rust
pipeline
    .job(daily_report)
    .schedule(daily_at(["16:00"]).input(|occurrence| TradingDay { date: occurrence.date() }))
    .needs(fetch_prices.done_for(|day: &TradingDay| *day).or_wait());

pipeline.job(sync_accounts).needs(refresh_token.done_within(18.hours()));
```

| Form | For a needed job that | Met when |
| --- | --- | --- |
| `job.done_for(\|input\| …)` | takes an input | It has ever succeeded for exactly the input the closure returns, given this run's input. The compiler checks the types. |
| `job.done_within(duration)` | takes no input | It has succeeded within that time. |

If the need is not met:

| Written | The run |
| --- | --- |
| Nothing more (the default) | Ends as `skipped` (`need_not_met`) |
| `.or_wait()` | Goes back to the queue and is checked again every `recheck_every`, until `give_up_after` |
| `.or_fail()` | Ends as `failed` (`need_not_met`) |

`done_within` compiles only for a job with no input. `done_for` compiles on any job, but `check`
rejects it on a job with no input.

| Situation | Use |
| --- | --- |
| B exists to process what A produced | A link |
| B has its own trigger, but is pointless or wrong unless A has done its part | A need |
| A is outside the product, such as a file someone uploads | Neither: B looks for its input and returns `not_ready` or `skip` |

## Only when something changed

```rust
pipeline.job(build_candles).after(fetch_prices).if_changed();
```

`.if_changed()` skips a run whose input equals the input this job last succeeded on. Since a
child's input is its parent's output, this reads as "only when the parent produced something
new". The run is skipped as `input_unchanged`.

- "Equal" is decided by `JobData`. A field marked `not_compared`, such as a timestamp, does not
  defeat it.
- It compares with the last *success*. An input that changes and changes back is processed again,
  and a job whose last run failed gets another turn on the same input.
- For a job with `one_per = field`, it compares with the last success for the same value of that
  field.
- It applies only to runs started by a schedule or a link. A run that a person or program starts,
  retries, restarts or backfills does the work.
- `check` rejects `.if_changed()` on a job with no input.

## The order conditions are checked in

Conditions are checked once, before the function is first called:

1. `only_if` conditions, in the order written. The first false one skips the run.
2. Needs, in the order written.
3. `if_changed`.

A run started with "ignore conditions" (an API option that needs the `administer` scope) skips all
three. Once the function has been called, the conditions are never checked again for that run, so
a run that has begun is never skipped halfway through.

## The same work is done once

Two runs are **the same work** when they are for the same job with an equal input, as `JobData`
compares it. A job with no input has only one kind of work. Two rules apply to every job, with
nothing to write.

1. **The same work is never queued twice.** A request for work that already has a run which has
   not begun joins that run, and nothing is added. A run has begun once its function has been
   called. The run keeps the higher priority and the later due time of the two.
2. **The same work never runs twice at once.** A run takes its *turn* when its function is first
   called, and keeps it until the run ends, through every attempt and every wait. Another run for
   the same work waits its turn.

Together they give what a request during a run should get: one more run, after the current one.
Requests with different inputs do not affect each other.

| Job | What happens |
| --- | --- |
| A refresh every 30 seconds, no input, whose run is slow | Runs never overlap or pile up. One run waits behind the slow one. |
| A daily job whose input is the date | Each date is its own work. Yesterday's run, still waiting, does not affect today's. |
| A daily job with no input, still waiting when the next day's occurrence arrives | Today's run waits until yesterday's ends. Give the job the date as input to treat days separately. |
| The same report requested twice by mistake | One run if the first has not begun; otherwise one more run after it. Use an idempotency key for an exact guarantee. |

**Rule 1 does not apply** to a deliberate run: a linked child, a backfill run, a retry or restart
by a person, and a request made with `even_if_queued`, `dry_run`, `without_children`,
`ignore_conditions`, `not_before` or a timeout. A scheduled request that joins leaves a `skipped`
(`joined`) row naming the run it joined, so the calendar can show where the occurrence went.

**Rule 2 has no exceptions.** Two policies widen it: `one_at_a_time` makes the whole job one
turn, and `one_per = field` makes each value of that field one turn. A run waiting its turn shows
`queued_because = waiting_turn`. Waiting counts toward `give_up_after`.

A run holds its turn while it waits, so a job that returns `not_ready` and takes turns holds back
its other runs. `check` warns about every job with `one_at_a_time` or `one_per`. Runs waiting
their turn have no promised order.

The exact rules are in the [specification](../spec.md#5-the-same-work-rules).
