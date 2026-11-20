//! Pausing and resuming jobs, queues and schedules.

use super::{Acted, PreviewQuery, done, job, preview};

use axum::Json;
use axum::extract::{Extension, Path, Query, State as Extract};
use chrono::{DateTime, Utc};
use downbeat_api::Scope;
use downbeat_api::types::PauseRequest;
use serde_json::json;

use crate::api::State;
use crate::api::auth::Caller;
use crate::api::error::Failure;
use crate::scheduler::plan;
use crate::store::audit;

async fn pause(
    state: &State,
    caller: &Caller,
    kind: &str,
    name: &str,
    preview_only: bool,
    request: PauseRequest,
) -> Acted {
    caller.require(Scope::Administer)?;
    if request.note.trim().is_empty() {
        return Err(Failure::bad_request("a pause needs a note saying why"));
    }
    if request.until.is_none() && !request.until_resumed {
        return Err(Failure::bad_request(
            "give the pause an end time with `until`, or set `until_resumed`: a forgotten pause is a silent outage",
        ));
    }
    if let Some(until) = request.until
        && until <= Utc::now()
    {
        return Err(Failure::bad_request("the pause would end in the past"));
    }
    let until_text = request
        .until
        .map_or_else(|| "until resumed".to_owned(), |until| format!("until {}", until.format("%Y-%m-%d %H:%M UTC")));
    if preview_only {
        let effect = match kind {
            "schedule" => {
                let removed = occurrences_until(state, name, request.until).await?;
                format!(
                    "Would stop schedule {name} creating runs {until_text}; {removed} occurrence(s) would not run. The job can still be started by hand."
                )
            }
            "job" => {
                format!("Would hold every run of {name}, however it was started, {until_text}. Running ones continue.")
            }
            "all" => format!(
                "Would hold every run of every job {until_text}. Running ones continue; schedules still create runs, which wait."
            ),
            _ => format!("Would hold every run on queue {name} {until_text}. Running ones continue."),
        };
        return preview(vec![effect], Vec::new());
    }
    let by = caller.audit_name();
    let mut transaction = state.pool.begin().await?;
    sqlx::query(
        "INSERT INTO downbeat.pause (kind, name, until, paused_by, note) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (kind, name) DO UPDATE SET until = EXCLUDED.until, paused_by = EXCLUDED.paused_by, note = EXCLUDED.note, created_at = now()",
    )
    .bind(kind)
    .bind(name)
    .bind(request.until)
    .bind(&by)
    .bind(&request.note)
    .execute(&mut *transaction)
    .await?;
    let (job, schedule) = match kind {
        "job" => (Some(name), None),
        "schedule" => (None, Some(name)),
        _ => (None, None),
    };
    audit::record(
        &mut transaction,
        &by,
        &format!("pause_{kind}"),
        job,
        None,
        schedule,
        json!({ "note": request.note, "until": request.until }),
    )
    .await?;
    transaction.commit().await?;
    done(format!("Paused {until_text}."), None, None)
}

/// How many occurrences of a schedule fall between now and a pause's end (a day ahead if none).
async fn occurrences_until(state: &State, name: &str, until: Option<DateTime<Utc>>) -> Result<usize, Failure> {
    let schedule = state
        .registry
        .schedules
        .get(name)
        .ok_or_else(|| Failure::not_found(format!("there is no schedule named {name}")))?;
    let overrides = plan::overrides_for(&state.pool, name).await?;
    let until = until.unwrap_or_else(|| Utc::now() + chrono::TimeDelta::days(1));
    let found = plan::occurrences_between(&state.registry, schedule, &overrides, Utc::now(), until)
        .await
        .map_err(Failure::bad_request)?;
    Ok(found.len())
}

async fn resume(state: &State, caller: &Caller, kind: &str, name: &str, preview_only: bool) -> Acted {
    caller.require(Scope::Administer)?;
    if preview_only {
        return preview(
            vec![format!(
                "Would resume {kind} {name}; runs it held are claimed as usual, and scheduled runs whose occurrence has expired end as expired."
            )],
            Vec::new(),
        );
    }
    let by = caller.audit_name();
    let mut transaction = state.pool.begin().await?;
    let removed = sqlx::query("DELETE FROM downbeat.pause WHERE kind = $1 AND name = $2")
        .bind(kind)
        .bind(name)
        .execute(&mut *transaction)
        .await?;
    if removed.rows_affected() == 0 {
        return Err(Failure::conflict(format!("{kind} {name} is not paused")));
    }
    audit::record(
        &mut transaction,
        &by,
        &format!("resume_{kind}"),
        (kind == "job").then_some(name),
        None,
        (kind == "schedule").then_some(name),
        json!({}),
    )
    .await?;
    transaction.commit().await?;
    done("Resumed.", None, None)
}

macro_rules! pause_endpoints {
    ($pause:ident, $resume:ident, $kind:literal, $check:ident) => {
        pub(crate) async fn $pause(
            Extract(state): Extract<State>,
            Extension(caller): Extension<Caller>,
            Path(name): Path<String>,
            Query(query): Query<PreviewQuery>,
            Json(request): Json<PauseRequest>,
        ) -> Acted {
            $check(&state, &caller, &name)?;
            pause(&state, &caller, $kind, &name, query.preview, request).await
        }

        pub(crate) async fn $resume(
            Extract(state): Extract<State>,
            Extension(caller): Extension<Caller>,
            Path(name): Path<String>,
            Query(query): Query<PreviewQuery>,
        ) -> Acted {
            $check(&state, &caller, &name)?;
            resume(&state, &caller, $kind, &name, query.preview).await
        }
    };
}

fn known_job(state: &State, caller: &Caller, name: &str) -> Result<(), Failure> {
    caller.require_job(name)?;
    job(state, name).map(|_| ())
}

fn known_queue(state: &State, caller: &Caller, name: &str) -> Result<(), Failure> {
    caller.require_every_job()?;
    let known = state.registry.jobs.values().any(|job| job.spec.policy.queue == name);
    if known { Ok(()) } else { Err(Failure::not_found(format!("no job in this build uses queue {name}"))) }
}

pub(super) fn known_schedule(state: &State, caller: &Caller, name: &str) -> Result<(), Failure> {
    let schedule = state
        .registry
        .schedules
        .get(name)
        .ok_or_else(|| Failure::not_found(format!("there is no schedule named {name}")))?;
    caller.require_job(schedule.job)
}

pause_endpoints!(pause_job, resume_job, "job", known_job);

pause_endpoints!(pause_queue, resume_queue, "queue", known_queue);

pause_endpoints!(pause_schedule, resume_schedule, "schedule", known_schedule);

/// `POST /pause`: hold every run of every job, for an outage or maintenance. Needs a token that
/// covers every job.
pub(crate) async fn pause_everything(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<PreviewQuery>,
    Json(request): Json<PauseRequest>,
) -> Acted {
    caller.require_every_job()?;
    pause(&state, &caller, "all", "all", query.preview, request).await
}

/// `POST /resume`: end the pause of everything.
pub(crate) async fn resume_everything(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<PreviewQuery>,
) -> Acted {
    caller.require_every_job()?;
    resume(&state, &caller, "all", "all", query.preview).await
}
