//! Dated overrides of schedules, and backfills.

use super::pauses::known_schedule;
use super::{Acted, PreviewQuery, done, job, preview};
use std::fmt::Write as _;

use axum::Json;
use axum::extract::{Extension, Path, Query, State as Extract};
use chrono::Utc;
use downbeat_api::Scope;
use downbeat_api::types::{BackfillRequest, OverrideRequest};
use serde_json::json;
use uuid::Uuid;

use crate::api::State;
use crate::api::auth::Caller;
use crate::api::error::Failure;
use crate::schedule::Times;
use crate::scheduler::plan;
use crate::store::audit;
use crate::store::cancel::{self};
use crate::store::ending::lock_chain;

/// `POST /overrides`: other times, another window, or nothing, for one date of some schedules.
pub(crate) async fn add_override(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<PreviewQuery>,
    Json(request): Json<OverrideRequest>,
) -> Acted {
    caller.require(Scope::Administer)?;
    if request.note.trim().is_empty() {
        return Err(Failure::bad_request("an override needs a note saying why"));
    }
    if request.schedules.is_empty() {
        return Err(Failure::bad_request("name at least one schedule"));
    }
    let times = plan::parse_times(&request.times).map_err(Failure::bad_request)?;
    let mut effects = Vec::new();
    for name in &request.schedules {
        known_schedule(&state, &caller, name)?;
        let schedule = &state.registry.schedules[name];
        match (request.change.as_str(), &schedule.times) {
            ("off", _) => {}
            ("times", Times::DailyAt(_)) if !times.is_empty() => {}
            ("window", Times::Every { window: Some(_), .. }) if times.len() == 2 => {}
            ("times", _) => {
                return Err(Failure::bad_request(format!(
                    "{name}: other times are for a daily_at schedule, and need at least one time"
                )));
            }
            ("window", _) => {
                return Err(Failure::bad_request(format!(
                    "{name}: another window is for an every(…).between(…) schedule, and needs a start and an end"
                )));
            }
            (other, _) => {
                return Err(Failure::bad_request(format!("'{other}' is not a change; use off, times or window")));
            }
        }
        let overrides = plan::overrides_for(&state.pool, name).await?;
        let (before, _) =
            plan::plan_for(&state.registry, schedule, &overrides, request.date).await.map_err(Failure::bad_request)?;
        let before = schedule.occurrences_on(request.date, &before);
        let after_plan = match request.change.as_str() {
            "off" => crate::schedule::DayPlan::Off(request.note.clone()),
            "window" => crate::schedule::DayPlan::Window(times[0], times[1]),
            _ => crate::schedule::DayPlan::Times(times.clone()),
        };
        let after = schedule.occurrences_on(request.date, &after_plan);
        let past = after.iter().filter(|occurrence| occurrence.at <= Utc::now()).count();
        let mut effect =
            format!("{name} on {}: {} occurrence(s) instead of {}.", request.date, after.len(), before.len());
        if past > 0 {
            let _ = write!(effect, " {past} of the new times have already passed and will not run.");
        }
        effects.push(effect);
    }
    if query.preview {
        return preview(effects, Vec::new());
    }
    let by = caller.audit_name();
    let mut transaction = state.pool.begin().await?;
    let mut last_id = None;
    for name in &request.schedules {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO downbeat.schedule_override (schedule, on_date, change, times, note, created_by) VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (schedule, on_date) DO UPDATE SET change = EXCLUDED.change, times = EXCLUDED.times, note = EXCLUDED.note,
                    created_by = EXCLUDED.created_by, created_at = now()
             RETURNING override_id",
        )
        .bind(name)
        .bind(request.date)
        .bind(&request.change)
        .bind(&times)
        .bind(&request.note)
        .bind(&by)
        .fetch_one(&mut *transaction)
        .await?;
        last_id = Some(id);
        // Saving an override marks the schedule due, so its next time is worked out again.
        sqlx::query("UPDATE downbeat.schedule_state SET next_due_at = now() WHERE schedule = $1")
            .bind(name)
            .execute(&mut *transaction)
            .await?;
        audit::record(
            &mut transaction,
            &by,
            "add_override",
            None,
            None,
            Some(name),
            json!({ "date": request.date, "change": request.change, "times": request.times, "note": request.note }),
        )
        .await?;
    }
    transaction.commit().await?;
    done(effects.join(" "), None, last_id)
}

/// `DELETE /overrides/{id}`.
pub(crate) async fn remove_override(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<i64>,
    Query(query): Query<PreviewQuery>,
) -> Acted {
    caller.require(Scope::Administer)?;
    let row: plan::OverrideRow = sqlx::query_as("SELECT * FROM downbeat.schedule_override WHERE override_id = $1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| Failure::not_found(format!("there is no override {id}")))?;
    known_schedule(&state, &caller, &row.schedule)?;
    if query.preview {
        return preview(vec![format!("Would restore {}'s usual times on {}.", row.schedule, row.on_date)], Vec::new());
    }
    let by = caller.audit_name();
    let mut transaction = state.pool.begin().await?;
    sqlx::query("DELETE FROM downbeat.schedule_override WHERE override_id = $1")
        .bind(id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("UPDATE downbeat.schedule_state SET next_due_at = now() WHERE schedule = $1")
        .bind(&row.schedule)
        .execute(&mut *transaction)
        .await?;
    audit::record(
        &mut transaction,
        &by,
        "remove_override",
        None,
        None,
        Some(&row.schedule),
        json!({ "date": row.on_date }),
    )
    .await?;
    transaction.commit().await?;
    done("The override is removed.", None, Some(id))
}

/// `POST /backfills`: a scheduled job, a date range.
pub(crate) async fn start_backfill(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<PreviewQuery>,
    Json(request): Json<BackfillRequest>,
) -> Acted {
    caller.require(Scope::Administer)?;
    caller.require_job(&request.job)?;
    let found = job(&state, &request.job)?;
    let schedule = request
        .schedule
        .clone()
        .or_else(|| found.schedules.first().cloned())
        .ok_or_else(|| Failure::bad_request(format!("{} has no schedule to take its input from", request.job)))?;
    if !found.schedules.contains(&schedule) {
        return Err(Failure::bad_request(format!("{schedule} is not one of {}'s schedules", request.job)));
    }
    if request.from > request.to {
        return Err(Failure::bad_request("the range ends before it begins"));
    }
    let days = (request.to - request.from).num_days() + 1;
    if query.preview {
        return preview(
            vec![format!(
                "Would run {} for each of {days} date(s) from {} to {}, one date at a time; days its schedule turns off are passed over.",
                request.job, request.from, request.to
            )],
            Vec::new(),
        );
    }
    let by = caller.audit_name();
    let mut transaction = state.pool.begin().await?;
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO downbeat.backfill (job_name, schedule, from_date, to_date, next_date, continue_on_failure, created_by)
         VALUES ($1, $2, $3, $4, $3, $5, $6) RETURNING backfill_id",
    )
    .bind(&request.job)
    .bind(&schedule)
    .bind(request.from)
    .bind(request.to)
    .bind(request.continue_on_failure)
    .bind(&by)
    .fetch_one(&mut *transaction)
    .await?;
    audit::record(
        &mut transaction,
        &by,
        "start_backfill",
        Some(&request.job),
        None,
        Some(&schedule),
        json!({ "from": request.from, "to": request.to }),
    )
    .await?;
    transaction.commit().await?;
    done(format!("Backfilling {days} date(s)."), None, Some(id))
}

/// `POST /backfills/{id}/cancel`.
pub(crate) async fn cancel_backfill(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<i64>,
    Query(query): Query<PreviewQuery>,
) -> Acted {
    caller.require(Scope::Administer)?;
    let job: String = sqlx::query_scalar("SELECT job_name FROM downbeat.backfill WHERE backfill_id = $1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| Failure::not_found(format!("there is no backfill {id}")))?;
    caller.require_job(&job)?;
    if query.preview {
        return preview(vec!["Would stop the backfill and cancel the date it is running now.".to_owned()], Vec::new());
    }
    let by = caller.audit_name();
    let mut transaction = state.pool.begin().await?;
    let chain: Option<Option<Uuid>> = sqlx::query_scalar(
        "UPDATE downbeat.backfill SET state = 'cancelled', reason = $2 WHERE backfill_id = $1 AND state = 'running' RETURNING current_chain",
    )
    .bind(id)
    .bind(format!("Cancelled by {by}."))
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(chain) = chain else { return Err(Failure::conflict("the backfill is not running")) };
    if let Some(chain) = chain {
        lock_chain(&mut transaction, chain).await?;
        cancel::cancel_chain(
            &mut transaction,
            &state.registry,
            chain,
            &format!("Cancelled with its backfill by {by}."),
            &by,
        )
        .await?;
    }
    audit::record(&mut transaction, &by, "cancel_backfill", Some(&job), None, None, json!({ "backfill_id": id }))
        .await?;
    transaction.commit().await?;
    done("The backfill is cancelled.", None, Some(id))
}
