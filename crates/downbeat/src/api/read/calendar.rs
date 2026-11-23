//! The calendar, schedules and dated overrides.

use axum::Json;
use axum::extract::{Extension, Query, State as Extract};
use chrono::{DateTime, NaiveDate, Utc};
use downbeat_api::Scope;
use downbeat_api::types::{CalendarDay, CalendarEntry, DayOff, OverrideView, ScheduleView};
use serde::Deserialize;

use crate::api::State;
use crate::api::auth::Caller;
use crate::api::error::Answer;
use crate::api::views;
use crate::schedule::DayPlan;
use crate::scheduler::plan::{self, PlanSource};
use crate::store::ending::HistoryRow;

#[derive(Debug, Deserialize)]
pub(crate) struct CalendarQuery {
    date: Option<NaiveDate>,
}

/// Every occurrence of every schedule on a date, in time order, after day rules, overrides and
/// pauses; past occurrences show their run, or why there was none.
pub(crate) async fn calendar(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<CalendarQuery>,
) -> Answer<CalendarDay> {
    caller.require(Scope::Read)?;
    let registry = &state.registry;
    let date = query.date.unwrap_or_else(|| Utc::now().with_timezone(&registry.zone).date_naive());
    let mut entries = Vec::new();
    let mut days_off = Vec::new();
    for schedule in registry.schedules.values() {
        if caller.require_job(schedule.job).is_err() {
            continue;
        }
        let overrides = plan::overrides_for(&state.pool, &schedule.name).await?;
        // A day rule that fails costs only its own schedule (spec 10.1): it is listed as having no
        // occurrences, with why, and every other schedule is shown as usual.
        let (day_plan, source) = match plan::plan_for(registry, schedule, &overrides, date).await {
            Ok(planned) => planned,
            Err(error) => {
                days_off
                    .push(DayOff { schedule: schedule.name.clone(), reason: format!("Its day rule failed: {error}.") });
                continue;
            }
        };
        let paused = views::pause_of(&state.pool, "schedule", &schedule.name).await?.is_some();
        if let DayPlan::Off(reason) = &day_plan {
            let by = match &source {
                PlanSource::Rule(rule) => format!(" (rule {rule})"),
                PlanSource::Override { .. } => " (dated override)".to_owned(),
                PlanSource::Normal => String::new(),
            };
            days_off.push(DayOff { schedule: schedule.name.clone(), reason: format!("{reason}{by}") });
            continue;
        }
        let occurrences = schedule.occurrences_on(date, &day_plan);
        let (Some(first), Some(last)) = (occurrences.first(), occurrences.last()) else { continue };
        let runs: Vec<HistoryRow> = sqlx::query_as(
            "SELECT DISTINCT ON (due_at) * FROM downbeat.run_history
              WHERE schedule = $1 AND due_at BETWEEN $2 AND $3 ORDER BY due_at, finished_at DESC",
        )
        .bind(&schedule.name)
        .bind(first.at)
        .bind(last.at)
        .fetch_all(&state.pool)
        .await?;
        let (source_name, note) = match &source {
            PlanSource::Normal => ("normal", None),
            PlanSource::Rule(rule) => ("rule", Some(format!("moved by rule {rule}"))),
            PlanSource::Override { note } => ("override", Some(note.clone())),
        };
        for occurrence in occurrences {
            entries.push(CalendarEntry {
                schedule: schedule.name.clone(),
                job: schedule.job.to_owned(),
                at: occurrence.at,
                source: source_name.to_owned(),
                note: note.clone(),
                paused,
                run: runs.iter().find(|run| run.due_at == occurrence.at).map(views::summary),
            });
        }
    }
    entries.sort_by_key(|entry| entry.at);
    Ok(Json(CalendarDay { date, entries, days_off }))
}

pub(crate) async fn schedules(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
) -> Answer<Vec<ScheduleView>> {
    caller.require(Scope::Read)?;
    let mut views = Vec::new();
    for schedule in state.registry.schedules.values().filter(|schedule| caller.covers(schedule.job)) {
        let next: Option<DateTime<Utc>> =
            sqlx::query_scalar("SELECT next_due_at FROM downbeat.schedule_state WHERE schedule = $1")
                .bind(&schedule.name)
                .fetch_optional(&state.pool)
                .await?
                .flatten();
        views.push(ScheduleView {
            name: schedule.name.clone(),
            job: schedule.job.to_owned(),
            times: crate::describe::schedule(schedule),
            next_occurrence: next,
            paused: views::pause_of(&state.pool, "schedule", &schedule.name).await?,
        });
    }
    Ok(Json(views))
}

pub(crate) async fn overrides(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
) -> Answer<Vec<OverrideView>> {
    caller.require(Scope::Read)?;
    let visible: Vec<&str> = state
        .registry
        .schedules
        .values()
        .filter(|schedule| caller.covers(schedule.job))
        .map(|schedule| schedule.name.as_str())
        .collect();
    let rows: Vec<plan::OverrideRow> = sqlx::query_as(
        "SELECT * FROM downbeat.schedule_override
          WHERE on_date >= (now() - interval '30 days')::date AND schedule = ANY($1)
          ORDER BY on_date, schedule LIMIT 500",
    )
    .bind(&visible)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(rows.into_iter().map(override_view).collect()))
}

pub(crate) fn override_view(row: plan::OverrideRow) -> OverrideView {
    OverrideView {
        override_id: row.override_id,
        schedule: row.schedule,
        date: row.on_date,
        change: row.change,
        times: row.times.iter().map(|time| time.format("%H:%M").to_string()).collect(),
        note: row.note,
        created_by: row.created_by,
    }
}
