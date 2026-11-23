//! Queues, backfills and the audit trail.

use super::{id_cursor, page_of, page_size};
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BackfillsQuery {
    /// Only this job's backfills.
    job: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
}

/// `GET /backfills`: newest first, by id. The primary key serves the order and the cursor; the job
/// filter is checked on the way, and backfills are made by people, so there are few to pass over.
pub(crate) async fn backfills(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<BackfillsQuery>,
) -> Answer<Page<BackfillView>> {
    caller.require(Scope::Read)?;
    let mut jobs = caller.jobs.clone();
    if let Some(job) = &query.job {
        caller.require_job(job)?;
        jobs = Some(vec![job.clone()]);
    }
    let size = page_size(query.limit);
    let before = query.cursor.as_deref().map(id_cursor).transpose()?;
    let rows: Vec<BackfillRow> = sqlx::query_as(
        "SELECT * FROM downbeat.backfill
          WHERE ($1::text[] IS NULL OR job_name = ANY($1)) AND ($2::bigint IS NULL OR backfill_id < $2)
          ORDER BY backfill_id DESC LIMIT $3",
    )
    .bind(&jobs)
    .bind(before)
    .bind(size + 1)
    .fetch_all(&state.pool)
    .await?;
    let items = rows.into_iter().map(backfill_view).collect();
    Ok(Json(page_of(items, size, |backfill| backfill.backfill_id.to_string())))
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
    let size = page_size(query.limit);
    let rows: Vec<AuditRow> = sqlx::query_as(
        "SELECT audit_id, at, actor, action, job_name, run_id, schedule, detail FROM downbeat.audit
          WHERE ($1::bigint IS NULL OR audit_id < $1) AND ($3::text[] IS NULL OR job_name = ANY($3))
          ORDER BY audit_id DESC LIMIT $2",
    )
    .bind(query.before)
    .bind(size + 1)
    .bind(&caller.jobs)
    .fetch_all(&state.pool)
    .await?;
    let items: Vec<AuditEntry> = rows
        .into_iter()
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
    Ok(Json(page_of(items, size, |entry| entry.audit_id.to_string())))
}
