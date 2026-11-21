//! Jobs for tests of schedules: registration, firing, overrides and backfills. Schedules here
//! run only in the `rules` environment, so they fire in no other test.
//!
//! Most schedules here have no weekday (`on_weekdays([])`), so they fire only when a test gives
//! them a dated override, which ignores weekday filters.
//!
//! The day rules here read `DayRules` and `BrokenCalendar`, which the definition in `lib.rs`
//! provides; the calendar's provider always fails.

use downbeat::prelude::*;

/// More schedules than one poll handles.
pub const CROWD: usize = 55;

/// The value the working day rules read.
#[derive(Debug)]
pub struct DayRules;

/// A value whose provider always fails, so a day rule that reads it fails.
#[derive(Debug)]
pub struct BrokenCalendar;

fn always_off(_date: NaiveDate, _rules: &DayRules) -> Day {
    Day::Off("closed by the day rule".into())
}

fn panics_on_purpose(_date: NaiveDate, _rules: &DayRules) -> Day {
    panic!("the day rule panics on purpose")
}

fn reads_broken_calendar(_date: NaiveDate, _calendar: &BrokenCalendar) -> Day {
    Day::Normal
}

/// Started by most schedules here.
#[job]
pub async fn on_the_clock(_run: Run) -> JobResult<()> {
    Ok(())
}

/// Takes the date of its occurrence; its schedule's input panics.
#[job]
pub async fn takes_a_day(_run: Run, day: NaiveDate) -> JobResult<NaiveDate> {
    Ok(day)
}

/// Always fails, so a backfill of it stops at its first date.
#[job]
pub async fn fails_on_its_day(_run: Run, day: NaiveDate) -> JobResult<NaiveDate> {
    Err(JobError::msg(format!("{day} fails on purpose")))
}

pub(crate) fn scheduling_rules_jobs(pipeline: &mut Pipeline) {
    let never = || daily_at(["03:00"]).on_weekdays([]).in_environments(["rules"]);
    let mut clock = pipeline
        .job(on_the_clock)
        .schedule(never().named("never_fires"))
        .schedule(
            every(10.minutes())
                .between("03:00", "03:01")
                .on_weekdays([])
                .expire_after(1.hours())
                .in_environments(["rules"])
                .named("missed_window"),
        )
        .schedule(
            every(10.minutes())
                .between("03:00", "03:01")
                .on_weekdays([])
                .expire_after(2.minutes())
                .in_environments(["rules"])
                .named("missed_and_expired"),
        )
        .schedule(every(1.hours()).on_weekdays([]).in_environments(["rules"]).named("all_day_every"))
        // Fires on the 31st only, so the calendar of 2030-01-31 shows its normal window.
        .schedule(
            every(30.minutes())
                .between("09:00", "10:00")
                .on_days_of_month([31])
                .in_environments(["rules"])
                .named("replaced_window"),
        );
    for index in 0..CROWD {
        clock = clock.schedule(never().named(&format!("crowd_{index:02}")));
    }
    clock
        .schedule(daily_at(["12:00"]).by_calendar(always_off).in_environments(["rules"]).named("ruled_off"))
        .schedule(never().by_calendar(panics_on_purpose).named("day_rule_panics"))
        .schedule(
            daily_at(["03:00"]).by_calendar(reads_broken_calendar).in_environments(["rules"]).named("day_rule_fails"),
        );
    pipeline.job(takes_a_day).schedule(
        never().named("input_panics").input(|_: &Occurrence| -> NaiveDate { panic!("the input panics on purpose") }),
    );
    // Fires on the 31st only, so a backfill over a range has few dates.
    pipeline
        .job(fails_on_its_day)
        .schedule(daily_at(["00:00"]).on_days_of_month([31]).input(Occurrence::date).in_environments(["rules"]));
}
