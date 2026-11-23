//! The read endpoints. Every list is bounded and paged in the database.

mod calendar;
mod jobs;
mod operations;
mod report;
mod runs;

pub(crate) use calendar::{calendar, overrides, schedules};
pub(crate) use jobs::{chain, job, jobs, pipeline, pipelines};
pub(crate) use operations::{audit, backfills, queues};
pub(crate) use report::report;
pub(crate) use runs::{run, runs};

use axum::Json;
use axum::extract::State as Extract;
use downbeat_api::API_VERSION;
use downbeat_api::types::{Health, Version};
use downbeat_api::with_defaults;

use crate::api::State;
use crate::api::error::Failure;

/// The most items one page returns.
const MAX_PAGE: i64 = 200;

pub(crate) async fn healthz() -> Json<Health> {
    Json(with_defaults!(Health { ok: true, api: API_VERSION }))
}

pub(crate) async fn readyz(Extract(state): Extract<State>) -> Result<Json<Health>, Failure> {
    sqlx::query("SELECT 1").execute(&state.pool).await?;
    Ok(Json(with_defaults!(Health { ok: true, api: API_VERSION })))
}

pub(crate) async fn version() -> Json<Version> {
    Json(with_defaults!(Version { api: API_VERSION, engine: env!("CARGO_PKG_VERSION").to_owned() }))
}

/// `GET /pause`: the pause of everything, if there is one.
pub(crate) async fn pause_of_everything(
    axum::extract::State(state): axum::extract::State<crate::api::State>,
    axum::extract::Extension(caller): axum::extract::Extension<crate::api::auth::Caller>,
) -> crate::api::error::Answer<Option<downbeat_api::types::PauseView>> {
    caller.require(downbeat_api::Scope::Read)?;
    Ok(axum::Json(crate::api::views::pause_of(&state.pool, "all", "all").await?))
}
