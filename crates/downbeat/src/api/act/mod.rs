//! The changing endpoints. Each accepts `?preview=true`, which changes nothing and says what the
//! action would do; each real action writes an audit row naming the caller and the actor.

mod pauses;
mod run;
mod schedules;
mod start;

pub(crate) use pauses::{pause_job, pause_queue, pause_schedule, resume_job, resume_queue, resume_schedule};
pub(crate) use run::{cancel_chain, cancel_run, check_now, mark_succeeded, restart, retry};
pub(crate) use schedules::{add_override, cancel_backfill, remove_override, start_backfill};
pub(crate) use start::start;

use axum::Json;
use axum::response::{IntoResponse, Response};
use downbeat_api::types::{Done, Preview};
use serde::Deserialize;
use uuid::Uuid;

use crate::api::State;
use crate::api::auth::Caller;
use crate::api::error::Failure;
use crate::registry::RegisteredJob;
use crate::store::ending::HistoryRow;
use crate::store::runs::{self, RunRow, StartedBy};

#[derive(Debug, Default, Deserialize)]
pub(crate) struct PreviewQuery {
    #[serde(default)]
    preview: bool,
}

type Acted = Result<Response, Failure>;

#[allow(clippy::unnecessary_wraps)] // Each handler ends with one of these; returning `Acted` keeps them uniform.
fn preview(effects: Vec<String>, false_conditions: Vec<String>) -> Acted {
    Ok(Json(Preview { effects, false_conditions }).into_response())
}

#[allow(clippy::unnecessary_wraps)]
fn done(message: impl Into<String>, run_id: Option<Uuid>, id: Option<i64>) -> Acted {
    Ok(Json(Done { message: message.into(), run_id, id }).into_response())
}

/// A service acting for itself is a program; anyone a caller vouches for is a person.
fn started_by(caller: &Caller) -> StartedBy {
    if caller.actor == caller.name { StartedBy::Program } else { StartedBy::Person }
}

fn job<'a>(state: &'a State, name: &str) -> Result<&'a RegisteredJob, Failure> {
    state.registry.job(name).ok_or_else(|| Failure::not_found(format!("there is no job named {name}")))
}

/// The run, unfinished or finished.
enum Found {
    Unfinished(RunRow),
    Finished(HistoryRow),
}

async fn find_run(state: &State, caller: &Caller, id: Uuid) -> Result<Found, Failure> {
    let mut connection = state.pool.acquire().await?;
    if let Some(row) = runs::get(&mut connection, id).await? {
        caller.require_job(&row.job_name)?;
        return Ok(Found::Unfinished(row));
    }
    let row: HistoryRow = sqlx::query_as("SELECT * FROM downbeat.run_history WHERE run_id = $1 AND joined_run IS NULL")
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or_else(|| Failure::not_found(format!("there is no run {id}")))?;
    caller.require_job(&row.job_name)?;
    Ok(Found::Finished(row))
}
