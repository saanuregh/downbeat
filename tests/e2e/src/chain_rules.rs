//! Jobs for tests of chains, backfills, delivery and retention. Schedules here run only in the
//! `rules` environment, so they fire in no other test.

use downbeat::prelude::*;

use crate::Text;
use crate::scheduling_rules::DayRules;

/// Fails when its text starts with `fail`, so a test chooses how it ends.
#[job]
pub async fn may_fail(_run: Run, input: Text) -> Result<Text> {
    if input.text.starts_with("fail") {
        return Err(JobError::msg(format!("{} on purpose", input.text)));
    }
    Ok(input)
}

/// Follows `may_fail` however it ended, and returns what it was handed.
#[job]
pub async fn tells_how_it_ended(_run: Run, ended: Ended<Text>) -> Result<Ended<Text>> {
    Ok(ended)
}

/// Follows `may_fail` when it fails, and returns the failure it was handed.
#[job]
pub async fn takes_the_failure(_run: Run, failure: Failure) -> Result<Failure> {
    Ok(failure)
}

/// Backfilled by date; its schedule fires on the 31st only and gives each run an hour.
#[job]
pub async fn backfilled_day(_run: Run, day: NaiveDate) -> Result<NaiveDate> {
    Ok(day)
}

/// Its schedule's input panics, so a backfill of it cannot start a date.
#[job]
pub async fn backfill_cannot_start(_run: Run, day: NaiveDate) -> Result<NaiveDate> {
    Ok(day)
}

/// Started by `shifted_window`, an hourly schedule its day rule moves by half an hour.
#[job]
pub async fn on_a_shifted_window(_run: Run) -> Result<()> {
    Ok(())
}

/// Keeps at most three chains; tests put its history straight into the tables.
#[job(keep_runs_at_most = 3)]
pub async fn keeps_three(_run: Run) -> Result<()> {
    Ok(())
}

fn half_an_hour_later(_date: NaiveDate, _rules: &DayRules) -> Day {
    Day::Shifted(chrono::TimeDelta::minutes(30))
}

pub(crate) fn chain_rules_jobs(pipeline: &mut Pipeline) {
    pipeline.job(may_fail);
    pipeline.job(tells_how_it_ended).after_any(may_fail);
    pipeline.job(takes_the_failure).after_failure(may_fail);
    pipeline.job(backfilled_day).schedule(
        daily_at(["00:00"])
            .on_days_of_month([31])
            .input(Occurrence::date)
            .expire_after(1.hours())
            .in_environments(["rules"])
            .named("backfilled_on_the_31st"),
    );
    pipeline.job(backfill_cannot_start).schedule(
        daily_at(["00:00"])
            .on_days_of_month([31])
            .input(|_: &Occurrence| -> NaiveDate { panic!("the backfill's input panics on purpose") })
            .in_environments(["rules"])
            .named("backfill_input_panics"),
    );
    pipeline.job(on_a_shifted_window).schedule(
        every(1.hours())
            .between("00:00", "23:00")
            .by_calendar(half_an_hour_later)
            .in_environments(["shifting"])
            .named("shifted_window"),
    );
    pipeline.job(keeps_three);
}
