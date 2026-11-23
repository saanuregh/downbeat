//! Queues, backfills and the audit trail.

use super::MAX_PAGE;
use std::collections::BTreeMap;

use axum::Json;
use axum::extract::{Extension, Query, State as Extract};
use chrono::{DateTime, Utc};
use downbeat_api::Scope;
use downbeat_api::types::{AuditEntry, BackfillState, BackfillView, Page, QueueView, WorkerView};
use downbeat_api::with_defaults;
use serde::Deserialize;
use uuid::Uuid;

use crate::api::State;
use crate::api::auth::Caller;
use crate::api::error::Answer;
use crate::api::views;
use crate::scheduler::backfill::BackfillRow;
use crate::store::runs::RunRow;

/// An audit row, as stored.
#[derive(sqlx::FromRow)]
struct AuditRow {
    audit_id: i64,
    at: DateTime<Utc>,
    actor: String,
    action: String,
    job_name: Option<String>,
    run_id: Option<Uuid>,
    schedule: Option<String>,
    detail: serde_json::Value,
}

pub(crate) async fn queues(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
) -> Answer<Vec<QueueView>> {
    caller.require(Scope::Read)?;
    // A caller limited to some jobs sees only their queues, and only their runs in them.
    let mut names: Vec<String> = state
        .registry
        .jobs
        .values()
        .filter(|job| caller.covers(job.spec.name))
        .map(|job| job.spec.policy.queue.to_owned())
        .collect();
    let covered: Option<Vec<String>> = caller.jobs.clone();
    names.sort();
    names.dedup();
    let workers: Vec<(String, serde_json::Value, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT worker_id, queues, build, last_seen_at FROM downbeat.worker WHERE role = 'worker' AND last_seen_at > now() - interval '1 minute'",
    )
    .fetch_all(&state.pool)
    .await?;
    let mut views = Vec::new();
    for name in names {
        let queued: Vec<RunRow> = sqlx::query_as(
            "SELECT * FROM downbeat.run WHERE queue = $1 AND state = 'queued' AND ($2::text[] IS NULL OR job_name = ANY($2))
              ORDER BY run_at LIMIT 500",
        )
        .bind(&name)
        .bind(&covered)
        .fetch_all(&state.pool)
        .await?;
        let running: i64 =
            sqlx::query_scalar("SELECT count(*) FROM downbeat.run WHERE queue = $1 AND state = 'running' AND ($2::text[] IS NULL OR job_name = ANY($2))")
                .bind(&name)
                .bind(&covered)
                .fetch_one(&state.pool)
                .await?;
        let longest = queued.iter().map(|run| run.run_at).min().map(|at| (Utc::now() - at).num_seconds().max(0));
        let mut because: BTreeMap<String, i64> = BTreeMap::new();
        let count = i64::try_from(queued.len()).unwrap_or(i64::MAX);
        for view in views::unfinished(&state.pool, &state.registry, queued).await? {
            *because.entry(view.queued_because.map_or_else(String::new, |because| because.to_string())).or_default() +=
                1;
        }
        let serving = workers
            .iter()
            .filter_map(|(id, queues, build, seen)| {
                queues.get(&name).and_then(serde_json::Value::as_i64).map(|slots| {
                    with_defaults!(WorkerView {
                        worker_id: id.clone(),
                        slots,
                        build: build.clone(),
                        last_seen_at: *seen,
                    })
                })
            })
            .collect();
        views.push(with_defaults!(QueueView {
            paused: views::pause_of(&state.pool, "queue", &name).await?,
            name,
            queued: count,
            running,
            queued_because: because,
            longest_wait_seconds: longest,
            workers: serving,
        }));
    }
    Ok(Json(views))
}

pub(crate) async fn backfills(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
) -> Answer<Vec<BackfillView>> {
    caller.require(Scope::Read)?;
    let rows: Vec<BackfillRow> = sqlx::query_as(
        "SELECT * FROM downbeat.backfill WHERE ($1::text[] IS NULL OR job_name = ANY($1))
          ORDER BY created_at DESC LIMIT 200",
    )
    .bind(&caller.jobs)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(rows.into_iter().map(backfill_view).collect()))
}

pub(crate) fn backfill_view(row: BackfillRow) -> BackfillView {
    with_defaults!(BackfillView {
        backfill_id: row.backfill_id,
        job: row.job_name,
        schedule: row.schedule,
        from: row.from_date,
        to: row.to_date,
        reached: row.next_date,
        state: BackfillState::from(row.state.as_str()),
        reason: row.reason,
        continue_on_failure: row.continue_on_failure,
        created_by: row.created_by,
        created_at: row.created_at,
    })
}

#[derive(Debug, Deserialize)]
pub(crate) struct AuditQuery {
    limit: Option<i64>,
    /// Entries before this id, for the next page.
    before: Option<i64>,
}

pub(crate) async fn audit(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<AuditQuery>,
) -> Answer<Page<AuditEntry>> {
    caller.require(Scope::Read)?;
    let limit = query.limit.unwrap_or(50).clamp(1, MAX_PAGE);
    let rows: Vec<AuditRow> = sqlx::query_as(
        "SELECT audit_id, at, actor, action, job_name, run_id, schedule, detail FROM downbeat.audit
          WHERE ($1::bigint IS NULL OR audit_id < $1) AND ($3::text[] IS NULL OR job_name = ANY($3))
          ORDER BY audit_id DESC LIMIT $2",
    )
    .bind(query.before)
    .bind(limit + 1)
    .bind(&caller.jobs)
    .fetch_all(&state.pool)
    .await?;
    let more = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
    let items: Vec<AuditEntry> = rows
        .into_iter()
        .take(usize::try_from(limit).unwrap_or(0))
        .map(|row| {
            with_defaults!(AuditEntry {
                audit_id: row.audit_id,
                at: row.at,
                actor: row.actor,
                action: row.action,
                job: row.job_name,
                run_id: row.run_id,
                schedule: row.schedule,
                detail: row.detail,
            })
        })
        .collect();
    let next_cursor = more.then(|| items.last().map(|entry| entry.audit_id.to_string())).flatten();
    Ok(Json(with_defaults!(Page { items, next_cursor })))
}
