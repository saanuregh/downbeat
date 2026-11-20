# 4. Schedules

A schedule starts a job at times on the clock. Two words used here: an **occurrence** is one
moment a schedule fires, and a run's **due time** is the occurrence that created it.

## Writing a schedule

A schedule is a value built from `daily_at` or `every` and passed to `.schedule(…)` in a
pipeline. Everything about *when* is in that value. Everything about *how the job runs* is on the
job.

```rust
pipeline.job(fetch_prices).schedule(
    every(30.minutes())
        .between("09:15", "15:30")
        .on_weekdays([Weekday::Mon, Weekday::Tue, Weekday::Wed, Weekday::Thu, Weekday::Fri])
        .by_calendar(trading_days)
        .input(|occurrence| TradingDay { date: occurrence.date() }),
);
pipeline.job(housekeeping).schedule(daily_at(["02:00"]));
```

| Start with | Fires |
| --- | --- |
| `daily_at(["07:33", "16:00"])` | Every day at those wall-clock times. Times are `HH:MM` or `HH:MM:SS`. |
| `every(30.minutes())` | Every interval, counted from local midnight, or from the start of its window. The shortest interval is one second. |

| Then, optionally | Meaning | Default |
| --- | --- | --- |
| `.between("18:45", "22:30")` | For `every`: a window. It fires at the first time, then every interval, up to and including the second time. A window may cross midnight; it then belongs to the date it started on. | All day |
| `.on_weekdays([Mon, Tue, …])` | Only on these weekdays (`chrono::Weekday`, in the prelude). | Every day |
| `.on_days_of_month([1, 15])` | Only on these days of the month (1 to 31). | Every day |
| `.by_calendar(rule)` | A day rule that can turn a day off or move its times; see [special days](#special-days). | None |
| `.input(\|occurrence\| …)` | The input of each run, computed from the `&Occurrence`. Required when the job takes an input: without it, the code does not compile. A fixed value is `\|_\| value`. | None |
| `.in_zone("UTC")` | A zone for this schedule only. | The definition's zone |
| `.in_environments(["production"])` | The schedule exists only in these environments. | Every environment |
| `.expire_after(1.hours())` | An occurrence is only worth running within this long of its time; see [late and missed occurrences](#late-and-missed-occurrences). | `every`: one interval. `daily_at`: no limit. |
| `.named("morning")` | The schedule's name. A job's second schedule needs one. | The job's name |

`Occurrence` gives `date()` (the local date, or the window's start date), `local()` (the local
date and time) and `at()` (the instant in UTC). When the input is the date itself,
`.input(Occurrence::date)` is enough.

A job can have several schedules, each with its own name:

```rust
pipeline
    .job(fetch_prices)
    .schedule(every(30.minutes()).between("09:15", "15:30").input(|o| TradingDay { date: o.date() }))
    .schedule(daily_at(["18:00"]).named("evening_check").input(|o| TradingDay { date: o.date() }));
```

### Say why, not when

A list of clock times often stands in for something else. Say what you mean:

| What is meant | Write |
| --- | --- |
| "Run once; if it fails, try again" | `daily_at(["07:33"])`, and `attempts = 3` on the job |
| "It may not be ready yet; wait for it" | `daily_at(["17:15"])`, and the job returns `not_ready("…")` until it is |
| "The source may change again; keep checking" | `every(30.minutes()).between(…)`; the job returns `skip` when there is nothing yet; `.if_changed()` on what follows |
| "Run after that other job" | `.after(job)`, not a time a few minutes later |

## Zones and clock changes

The definition must set a zone (`Definition::new().zone("Asia/Kolkata")`), using an IANA name.
There is no implicit UTC. `.in_zone(…)` overrides it for one schedule. `check` fails on a zone it
does not know.

Wall-clock times are resolved in the zone and stored in UTC:

- A local time that does not exist, because clocks moved forward, fires at the first minute that
  exists after it. For example, 02:30 on the day New York moves from 02:00 to 03:00 fires at 03:00.
- A local time that happens twice, because clocks moved back, fires at the first of the two.
- `every` steps by a fixed length of real time from its start, so on a day with a clock change
  the wall-clock times after the change shift by the change.

## Special days

Two layers handle days that differ from the usual.

### A day rule, in code

For what your product's calendar knows. A rule is a function of the date and one value the
definition provides:

```rust
fn trading_days(date: NaiveDate, holidays: &Holidays) -> Day {
    if holidays.0.contains(&date) { Day::Off("exchange holiday".into()) } else { Day::Normal }
}

definition.provide(|| async { Ok::<_, std::convert::Infallible>(Holidays(load_holidays())) });
```

| `Day` | On `daily_at` | On `every(…).between(…)` | On `every(…)` all day |
| --- | --- | --- | --- |
| `Normal` | The usual times | The usual window | The usual times |
| `Off(reason)` | No occurrences | No occurrences | No occurrences |
| `Shifted(delta)` | Every time moves by `delta` | The window moves by `delta` | No effect |

`delta` is a `chrono::TimeDelta`, for example `Day::Shifted(chrono::TimeDelta::minutes(-90))`
for a close moved 90 minutes earlier.

The rule runs in the scheduler, often, and in every scheduler. It must be quick and give the same
answer for the same date everywhere. Read a calendar the product has loaded; never read the clock
or the network. `check` fails when the definition does not provide the rule's value. The reason
of an `Off` day is shown wherever the gap is shown.

### A dated override, at runtime

For a day nobody put in the calendar, a person with the `administer` scope adds a dated override
through the API or the console. It names one or more schedules, a local date in each schedule's
zone, a change and a note:

| Change | For | Effect on that date |
| --- | --- | --- |
| `off` | Any schedule | No occurrences |
| `times` | A `daily_at` schedule | These times instead (at least one) |
| `window` | An `every(…).between(…)` schedule | This window instead (a start and an end) |

For a given schedule and date, the first that exists wins: **a dated override, then the day
rule, then the normal times**. Weekday and day-of-month filters apply to the normal and shifted
times, not to an override's times, so an override can add a session on a day the schedule
usually skips.

- An override changes times only. It cannot change the job, its input, its zone or its policies.
- It never fires the past. A replacement time that has already gone by does not run, and the
  preview says so before the override is saved. An occurrence that already fired is not undone.
- Saving or removing an override makes the scheduler work out the schedule's next time again
  at once.
- Overrides are deleted 30 days after their date. Their audit rows remain.
- Backfills ignore overrides; they follow the day rule.

## Late and missed occurrences

An occurrence can be late for two reasons. No scheduler was running when it was due (a deploy, a
crash, an outage). Or its run was created on time, but no worker started it (workers down or
busy, or the job paused). `.expire_after(…)` covers both: "this occurrence is only worth running
within this long of its time".

| Schedule | Default | Why |
| --- | --- | --- |
| `every(…)` | One interval | A check that is a whole interval late is pointless; the next one is already due. |
| `daily_at(…)` | No limit | Late is better than never. A morning job that must not run at noon says `.expire_after(1.hours())`. |

- When a schedule has missed several occurrences, only the **most recent** one is considered.
  It runs if it has not expired, and is dropped if it has. The earlier ones are dropped too.
  Dropped occurrences are reported to `on_event` as `OccurrencesDropped`, with the count.
- A run that was created but not started by its expiry ends as `expired`, and never runs late.
- A run whose function has been called at least once is never expired.

## Pausing a schedule

A paused schedule creates nothing, and its position still moves forward, so resuming never
releases a backlog. The job can still be started by hand. A pause needs a note, and either an end
time or an explicit "until resumed", because a forgotten pause is a silent outage. Pausing a
*job* is stronger: it holds every run of the job, however it was started. See
[operations](08-operations.md#pausing).

## Environments

`Definition::environments([…])` lists the names your product uses, and each process says which
one it is with `DOWNBEAT_ENVIRONMENT` (or `--environment`).

- A schedule with `.in_environments([…])` exists only in those environments. Elsewhere it is not
  registered at all and never fires.
- `check` fails on an environment name the definition does not list, so a typo cannot silently
  turn a schedule off. When the definition lists environments, a process started with another
  name refuses to start.
- A different schedule for another environment is another `.schedule(…)`, with its own name and
  `.in_environments`.

## Across deploys

Each schedule's state is stored in the database: how far it has fired (its *watermark*) and when
it is next due. That makes deploys predictable:

| Event | Behaviour |
| --- | --- |
| A schedule appears for the first time | It starts from now. Nothing in the past fires. |
| A schedule that no scheduler has registered for ten minutes comes back | It starts from now. |
| Its times, weekdays, days of the month or zone change | It starts again from a few seconds before now, so a new time earlier today does not fire. |
| It is removed | Runs it already created still run. |
| A job is removed | Its waiting runs fail after an hour as `job_not_registered`. |
| A rolling deploy adds a job | Old workers do not claim its runs; new workers pick them up. |
| The database clock steps backwards | A watermark never moves back, so nothing fires twice. |

Any number of schedulers may run at once. Each schedule's state row is locked while it fires, so
an occurrence fires once, without a leader.

The exact rules are in the [specification](../spec.md#8-schedules-and-occurrences).
