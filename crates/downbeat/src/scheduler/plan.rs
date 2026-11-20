//! What a schedule does on a date: its dated override, else its day rule, else its normal times.

use std::time::Duration;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use sqlx::PgPool;

use crate::registry::Registry;
use crate::schedule::times::{dates_covering, parse_time};
use crate::schedule::{CompiledSchedule, Day, DayPlan, Occurrence};

/// Where a date's plan came from, for the calendar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PlanSource {
    Normal,
    Rule(String),
    Override { note: String },
}

/// A dated override, as stored.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct OverrideRow {
    pub(crate) override_id: i64,
    pub(crate) schedule: String,
    pub(crate) on_date: NaiveDate,
    pub(crate) change: String,
    pub(crate) times: Vec<NaiveTime>,
    pub(crate) note: String,
    pub(crate) created_by: String,
}

pub(crate) async fn overrides_for(pool: &PgPool, schedule: &str) -> sqlx::Result<Vec<OverrideRow>> {
    sqlx::query_as("SELECT * FROM downbeat.schedule_override WHERE schedule = $1 ORDER BY on_date")
        .bind(schedule)
        .fetch_all(pool)
        .await
}

/// The plan for one date.
pub(crate) async fn plan_for(
    registry: &Registry,
    schedule: &CompiledSchedule,
    overrides: &[OverrideRow],
    date: NaiveDate,
) -> Result<(DayPlan, PlanSource), String> {
    if let Some(row) = overrides.iter().find(|row| row.on_date == date && row.schedule == schedule.name) {
        let plan = match row.change.as_str() {
            "off" => DayPlan::Off(row.note.clone()),
            "window" if row.times.len() == 2 => DayPlan::Window(row.times[0], row.times[1]),
            _ => DayPlan::Times(row.times.clone()),
        };
        return Ok((plan, PlanSource::Override { note: row.note.clone() }));
    }
    if let Some(rule) = &schedule.calendar {
        let what = format!("day rule {}", rule.name);
        let day = crate::guard::guarded_async(&what, (rule.call)(date, std::sync::Arc::clone(&registry.dependencies)))
            .await?;
        let plan = match day {
            Day::Normal => DayPlan::Normal,
            Day::Off(reason) => DayPlan::Off(reason),
            Day::Shifted(by) => DayPlan::Shifted(by),
        };
        let source = if plan == DayPlan::Normal { PlanSource::Normal } else { PlanSource::Rule(rule.name.clone()) };
        return Ok((plan, source));
    }
    Ok((DayPlan::Normal, PlanSource::Normal))
}

/// Every occurrence after `after` and at or before `until`.
pub(crate) async fn occurrences_between(
    registry: &Registry,
    schedule: &CompiledSchedule,
    overrides: &[OverrideRow],
    after: DateTime<Utc>,
    until: DateTime<Utc>,
) -> Result<Vec<Occurrence>, String> {
    let mut found = Vec::new();
    for date in dates_covering(schedule.zone, after, until) {
        let (plan, _) = plan_for(registry, schedule, overrides, date).await?;
        found.extend(schedule.occurrences_on(date, &plan).into_iter().filter(|o| o.at > after && o.at <= until));
    }
    found.sort_by_key(|occurrence| occurrence.at);
    Ok(found)
}

/// The first occurrence after `after`, looking ahead at most a year and a few days.
pub(crate) async fn next_after(
    registry: &Registry,
    schedule: &CompiledSchedule,
    overrides: &[OverrideRow],
    after: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, String> {
    let first = after.with_timezone(&schedule.zone).date_naive().pred_opt().unwrap_or(NaiveDate::MIN);
    for date in first.iter_days().take(370) {
        let (plan, _) = plan_for(registry, schedule, overrides, date).await?;
        if let Some(occurrence) = schedule.occurrences_on(date, &plan).into_iter().find(|o| o.at > after) {
            return Ok(Some(occurrence.at));
        }
    }
    Ok(None)
}

/// A digest of a schedule's times, so a changed schedule starts again from now.
pub(crate) fn times_hash(schedule: &CompiledSchedule) -> String {
    format!("{:?}|{:?}|{:?}|{}", schedule.times, schedule.weekdays, schedule.month_days, schedule.zone)
}

/// Parses override times, for the API.
pub(crate) fn parse_times(texts: &[String]) -> Result<Vec<NaiveTime>, String> {
    texts.iter().map(|text| parse_time(text)).collect()
}

/// How long an occurrence of this schedule stays worth running.
pub(crate) fn expiry_of(schedule: &CompiledSchedule) -> Option<Duration> {
    schedule.expiry()
}
