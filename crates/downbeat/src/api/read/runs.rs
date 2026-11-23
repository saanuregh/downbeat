//! The one list of runs, and a single run.

use super::MAX_PAGE;

use axum::Json;
use axum::extract::{Extension, Path, Query, State as Extract};
use chrono::{DateTime, Utc};
use downbeat_api::Scope;
use downbeat_api::types::{Page, QueuedBecause, RunView};
use downbeat_api::with_defaults;
use serde::Deserialize;
use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

use crate::api::State;
use crate::api::auth::Caller;
use crate::api::error::{Answer, Failure};
use crate::api::views;
use crate::store::ending::HistoryRow;
use crate::store::runs::RunRow;

/// `GET /runs` filters.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunsQuery {
    job: Option<String>,
    pipeline: Option<String>,
    status: Option<String>,
    queued_because: Option<String>,
    skipped_because: Option<String>,
    failed_because: Option<String>,
    started_by: Option<String>,
    queue: Option<String>,
    /// Runs created at or after this time.
    from: Option<DateTime<Utc>>,
    /// Runs created before this time.
    to: Option<DateTime<Utc>>,
    /// A field of the input: `day=2026-11-08`.
    input: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
}

/// Where a page of runs continues: in the unfinished runs, or in the history.
enum Cursor {
    Unfinished(DateTime<Utc>, Uuid),
    Finished(DateTime<Utc>, Uuid),
}

impl Cursor {
    fn parse(text: &str) -> Result<Cursor, Failure> {
        let bad = || Failure::bad_request("the cursor is not one this API returned");
        let mut parts = text.splitn(3, '_');
        let (Some(kind), Some(micros), Some(id)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(bad());
        };
        let at = DateTime::from_timestamp_micros(micros.parse().map_err(|_| bad())?).ok_or_else(bad)?;
        let id = id.parse().map_err(|_| bad())?;
        match kind {
            "u" => Ok(Cursor::Unfinished(at, id)),
            "f" => Ok(Cursor::Finished(at, id)),
            _ => Err(bad()),
        }
    }

    fn text(&self) -> String {
        match self {
            Cursor::Unfinished(at, id) => format!("u_{}_{id}", at.timestamp_micros()),
            Cursor::Finished(at, id) => format!("f_{}_{id}", at.timestamp_micros()),
        }
    }
}

/// One list of runs: unfinished ones first, newest first, then finished ones, newest first.
pub(crate) async fn runs(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<RunsQuery>,
) -> Answer<Page<RunView>> {
    caller.require(Scope::Read)?;
    let limit = query.limit.unwrap_or(50).clamp(1, MAX_PAGE);
    let cursor = query.cursor.as_deref().map(Cursor::parse).transpose()?;
    let jobs = jobs_filter(&state, &caller, &query)?;
    let input = query
        .input
        .as_deref()
        .map(|filter| {
            filter
                .split_once('=')
                .map(|(field, value)| (field.to_owned(), value.to_owned()))
                .ok_or_else(|| Failure::bad_request("filter by input as field=value"))
        })
        .transpose()?;
    let wants_unfinished = query.status.as_deref().is_none_or(|status| status == "queued" || status == "running")
        && query.skipped_because.is_none()
        && query.failed_because.is_none();
    let wants_finished = query.status.as_deref().is_none_or(|status| status != "queued" && status != "running")
        && query.queued_because.is_none();

    let mut items = Vec::new();
    let mut next = None;
    if wants_unfinished && !matches!(cursor, Some(Cursor::Finished(..))) {
        let mut sql = QueryBuilder::<Postgres>::new("SELECT * FROM downbeat.run WHERE true");
        common_filters(&mut sql, &query, jobs.as_ref(), input.as_ref(), "created_at");
        if let Some(status) = &query.status {
            sql.push(" AND state = ").push_bind(status.clone());
        }
        if let Some(Cursor::Unfinished(at, id)) = &cursor {
            sql.push(" AND (created_at, run_id) < (").push_bind(*at).push(", ").push_bind(*id).push(")");
        }
        sql.push(" ORDER BY created_at DESC, run_id DESC LIMIT ").push_bind(limit + 1);
        let rows: Vec<RunRow> = sql.build_query_as().fetch_all(&state.pool).await?;
        let examined = usize::try_from(limit).unwrap_or(usize::MAX);
        // The cursor marks the last row examined, kept or not, so a filtered-out page still pages on.
        if rows.len() > examined {
            let last = &rows[examined - 1];
            next = Some(Cursor::Unfinished(last.created_at, last.run_id).text());
        }
        let rows: Vec<RunRow> = rows.into_iter().take(examined).collect();
        for view in views::unfinished(&state.pool, &state.registry, rows).await? {
            if query
                .queued_because
                .as_deref()
                .is_none_or(|because| view.queued_because.map(QueuedBecause::as_str) == Some(because))
            {
                items.push(view);
            }
        }
    }
    if next.is_none() && wants_finished && (i64::try_from(items.len()).unwrap_or(i64::MAX)) < limit {
        let room = limit - i64::try_from(items.len()).unwrap_or(i64::MAX);
        let mut sql = QueryBuilder::<Postgres>::new("SELECT * FROM downbeat.run_history WHERE joined_run IS NULL");
        common_filters(&mut sql, &query, jobs.as_ref(), input.as_ref(), "finished_at");
        for (column, value) in [
            ("status", &query.status),
            ("skipped_because", &query.skipped_because),
            ("failed_because", &query.failed_because),
        ] {
            if let Some(value) = value {
                sql.push(format!(" AND {column} = ")).push_bind(value.clone());
            }
        }
        if let Some(Cursor::Finished(at, id)) = &cursor {
            sql.push(" AND (finished_at, run_id) < (").push_bind(*at).push(", ").push_bind(*id).push(")");
        }
        sql.push(" ORDER BY finished_at DESC, run_id DESC LIMIT ").push_bind(room + 1);
        let rows: Vec<HistoryRow> = sql.build_query_as().fetch_all(&state.pool).await?;
        let more = i64::try_from(rows.len()).unwrap_or(i64::MAX) > room;
        for row in rows.into_iter().take(usize::try_from(room).unwrap_or(0)) {
            items.push(views::finished(&state.registry, row, None));
        }
        if more && let Some(last) = items.last() {
            next = last.finished_at.map(|at| Cursor::Finished(at, last.run_id).text());
        }
    }
    Ok(Json(with_defaults!(Page { items, next_cursor: next })))
}

/// The jobs a list may show: those the token covers, narrowed by `job` and `pipeline`.
fn jobs_filter(state: &State, caller: &Caller, query: &RunsQuery) -> Result<Option<Vec<String>>, Failure> {
    let mut jobs: Option<Vec<String>> = caller.jobs.clone();
    if let Some(job) = &query.job {
        caller.require_job(job)?;
        jobs = Some(vec![job.clone()]);
    }
    if let Some(pipeline) = &query.pipeline {
        let in_pipeline: Vec<String> = state
            .registry
            .pipelines
            .get(pipeline)
            .ok_or_else(|| Failure::not_found(format!("there is no pipeline named {pipeline}")))?
            .iter()
            .map(|job| (*job).to_owned())
            .collect();
        jobs = Some(match jobs {
            Some(jobs) => jobs.into_iter().filter(|job| in_pipeline.contains(job)).collect(),
            None => in_pipeline,
        });
    }
    Ok(jobs)
}

fn common_filters(
    sql: &mut QueryBuilder<Postgres>,
    query: &RunsQuery,
    jobs: Option<&Vec<String>>,
    input: Option<&(String, String)>,
    time: &str,
) {
    if let Some(jobs) = jobs {
        sql.push(" AND job_name = ANY(").push_bind(jobs.clone()).push(")");
    }
    if let Some(started_by) = &query.started_by {
        sql.push(" AND started_by = ").push_bind(started_by.clone());
    }
    if let Some(queue) = &query.queue {
        sql.push(" AND queue = ").push_bind(queue.clone());
    }
    if let Some(from) = query.from {
        sql.push(format!(" AND {time} >= ")).push_bind(from);
    }
    if let Some(to) = query.to {
        sql.push(format!(" AND {time} < ")).push_bind(to);
    }
    if let Some((field, value)) = input {
        sql.push(" AND input ->> ").push_bind(field.clone()).push(" = ").push_bind(value.clone());
    }
}

pub(crate) async fn run(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<Uuid>,
) -> Answer<RunView> {
    caller.require(Scope::Read)?;
    let mut connection = state.pool.acquire().await?;
    if let Some(row) = crate::store::runs::get(&mut connection, id).await? {
        caller.require_job(&row.job_name)?;
        let detail = views::detail(&state.pool, id, row.parents.clone(), &row.call_log, None, None).await?;
        let views = views::unfinished(&state.pool, &state.registry, vec![row]).await?;
        let mut view = views.into_iter().next().expect("one view for one row");
        view.detail = Some(detail);
        return Ok(Json(view));
    }
    let row: HistoryRow = sqlx::query_as("SELECT * FROM downbeat.run_history WHERE run_id = $1")
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or_else(|| Failure::not_found(format!("there is no run {id}")))?;
    caller.require_job(&row.job_name)?;
    let detail = views::detail(
        &state.pool,
        id,
        row.parents.clone(),
        &row.call_log,
        row.error_chain.clone(),
        row.stderr_tail.clone(),
    )
    .await?;
    Ok(Json(views::finished(&state.registry, row, Some(detail))))
}
