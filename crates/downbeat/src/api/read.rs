//! The read endpoints. Every list is bounded and paged in the database.

use std::collections::{BTreeMap, VecDeque};

use axum::Json;
use axum::extract::{Extension, Path, Query, State as Extract};
use chrono::{DateTime, NaiveDate, Utc};
use downbeat_api::types::{
    AuditEntry, BackfillView, CalendarDay, CalendarEntry, ChainNode, ChainSummary, ChainView, DayOff, Health, JobView,
    OverrideView, Page, PipelineNode, PipelineView, QueueView, RunSummary, RunView, ScheduleView, Version, WorkerView,
};
use downbeat_api::{API_VERSION, Scope};
use serde::Deserialize;
use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

use super::State;
use super::auth::Caller;
use super::error::{Answer, Failure};
use super::views;
use crate::pipeline::LinkKind;
use crate::registry::RegisteredJob;
use crate::schedule::DayPlan;
use crate::scheduler::backfill::BackfillRow;
use crate::scheduler::plan::{self, PlanSource};
use crate::store::ending::HistoryRow;
use crate::store::history;
use crate::store::runs::RunRow;

/// The most items one page returns.
const MAX_PAGE: i64 = 200;

/// A chain, as stored.
#[derive(sqlx::FromRow)]
struct ChainRow {
    chain_id: Uuid,
    pipeline: String,
    first_job: String,
    started_at: DateTime<Utc>,
    cancelled_at: Option<DateTime<Utc>>,
    cancelled_by: Option<String>,
}

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

pub(crate) async fn healthz() -> Json<Health> {
    Json(Health { ok: true, api: API_VERSION })
}

pub(crate) async fn readyz(Extract(state): Extract<State>) -> Result<Json<Health>, Failure> {
    sqlx::query("SELECT 1").execute(&state.pool).await?;
    Ok(Json(Health { ok: true, api: API_VERSION }))
}

pub(crate) async fn version() -> Json<Version> {
    Json(Version { api: API_VERSION, engine: env!("CARGO_PKG_VERSION").to_owned() })
}

pub(crate) async fn jobs(Extract(state): Extract<State>, Extension(caller): Extension<Caller>) -> Answer<Vec<JobView>> {
    caller.require(Scope::Read)?;
    let mut views = Vec::new();
    for (name, job) in &state.registry.jobs {
        if caller.require_job(name).is_ok() {
            views.push(job_view(&state, job).await?);
        }
    }
    Ok(Json(views))
}

pub(crate) async fn job(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(name): Path<String>,
) -> Answer<JobView> {
    caller.require(Scope::Read)?;
    caller.require_job(&name)?;
    let job = state.registry.job(&name).ok_or_else(|| Failure::not_found(format!("there is no job named {name}")))?;
    Ok(Json(job_view(&state, job).await?))
}

async fn job_view(state: &State, job: &RegisteredJob) -> Result<JobView, Failure> {
    let name = job.spec.name;
    let last: Option<HistoryRow> = sqlx::query_as(
        "SELECT * FROM downbeat.run_history WHERE job_name = $1 AND joined_run IS NULL ORDER BY finished_at DESC LIMIT 1",
    )
    .bind(name)
    .fetch_optional(&state.pool)
    .await?;
    let next: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT min(next_due_at) FROM downbeat.schedule_state WHERE schedule = ANY($1)")
            .bind(&job.schedules)
            .fetch_one(&state.pool)
            .await?;
    let expected = history::expected_duration(&state.pool, name).await?;
    let overdue = match job.spec.policy.expect_success_every {
        Some(every) => {
            let last_success = history::last_success(&state.pool, name, None).await?.map(|(_, at)| at);
            last_success.is_some_and(|at| {
                Utc::now() - at > chrono::TimeDelta::from_std(every).unwrap_or(chrono::TimeDelta::MAX)
            })
        }
        None => false,
    };
    Ok(JobView {
        name: name.to_owned(),
        pipeline: job.pipeline.clone(),
        kind: if job.spec.is_command() { "command" } else { "function" }.to_owned(),
        queue: job.spec.policy.queue.to_owned(),
        triggers: crate::describe::triggers_and_conditions(&state.registry, job),
        policies: job.spec.policy.describe(),
        input: serde_json::to_value(&job.spec.input).unwrap_or_default(),
        output: serde_json::to_value(&job.spec.output).unwrap_or_default(),
        supports_dry_run: job.spec.policy.supports_dry_run,
        last_run: last.as_ref().map(views::summary),
        next_occurrence: next,
        expected_seconds: expected.map(|(median, _)| i64::try_from(median.as_secs()).unwrap_or(i64::MAX)),
        paused: views::pause_of(&state.pool, "job", name).await?,
        overdue,
    })
}

pub(crate) async fn pipelines(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
) -> Answer<Vec<PipelineView>> {
    caller.require(Scope::Read)?;
    let names: Vec<String> = state.registry.pipelines.keys().cloned().collect();
    let views =
        names.iter().map(|name| pipeline_view(&state, &caller, name)).filter(|view| !view.jobs.is_empty()).collect();
    Ok(Json(views))
}

pub(crate) async fn pipeline(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(name): Path<String>,
) -> Answer<PipelineView> {
    caller.require(Scope::Read)?;
    if !state.registry.pipelines.contains_key(&name) {
        return Err(Failure::not_found(format!("there is no pipeline named {name}")));
    }
    let mut view = pipeline_view(&state, &caller, &name);
    if view.jobs.is_empty() {
        return Err(Failure::forbidden("this token covers no job in that pipeline"));
    }
    let chains: Vec<ChainRow> =
        sqlx::query_as("SELECT * FROM downbeat.chain WHERE pipeline = $1 ORDER BY started_at DESC LIMIT 20")
            .bind(&name)
            .fetch_all(&state.pool)
            .await?;
    for ChainRow { chain_id, first_job, started_at, cancelled_at, .. } in chains {
        let statuses: Vec<(String, String)> = sqlx::query_as(
            "SELECT DISTINCT ON (job_name) job_name, status FROM (
                SELECT job_name, state AS status, created_at AS at FROM downbeat.run WHERE chain_id = $1
                UNION ALL
                SELECT job_name, status, finished_at FROM downbeat.run_history WHERE chain_id = $1 AND joined_run IS NULL) runs
             ORDER BY job_name, at DESC",
        )
        .bind(chain_id)
        .fetch_all(&state.pool)
        .await?;
        view.recent_chains.push(ChainSummary {
            chain_id,
            first_job,
            started_at,
            statuses: statuses.into_iter().collect(),
            cancelled: cancelled_at.is_some(),
        });
    }
    Ok(Json(view))
}

/// A pipeline as the caller may see it: only the jobs its token covers.
fn pipeline_view(state: &State, caller: &Caller, name: &str) -> PipelineView {
    let jobs = state.registry.pipelines.get(name).cloned().unwrap_or_default();
    let nodes = jobs
        .iter()
        .filter(|job| caller.covers(job))
        .filter_map(|job| state.registry.job(job))
        .map(|job| PipelineNode {
            job: job.spec.name.to_owned(),
            link: job.link.as_ref().map(|link| link_name(link.kind).to_owned()),
            parents: job
                .link
                .as_ref()
                .map(|link| link.parents.iter().map(|parent| (*parent).to_owned()).collect())
                .unwrap_or_default(),
            schedules: job.schedules.clone(),
        })
        .collect();
    PipelineView { name: name.to_owned(), jobs: nodes, recent_chains: Vec::new() }
}

fn link_name(kind: LinkKind) -> &'static str {
    match kind {
        LinkKind::After => "after",
        LinkKind::AfterFailure => "after_failure",
        LinkKind::AfterAny => "after_any",
    }
}

pub(crate) async fn chain(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<Uuid>,
) -> Answer<ChainView> {
    caller.require(Scope::Read)?;
    let chain: Option<ChainRow> =
        sqlx::query_as("SELECT * FROM downbeat.chain WHERE chain_id = $1").bind(id).fetch_optional(&state.pool).await?;
    let ChainRow { pipeline, first_job, started_at, cancelled_at, cancelled_by, .. } =
        chain.ok_or_else(|| Failure::not_found(format!("there is no chain {id}")))?;
    caller.require_job(&first_job)?;
    let ended: Vec<HistoryRow> = sqlx::query_as(
        "SELECT * FROM downbeat.run_history WHERE chain_id = $1 AND joined_run IS NULL ORDER BY finished_at DESC",
    )
    .bind(id)
    .fetch_all(&state.pool)
    .await?;
    let unfinished: Vec<RunRow> =
        sqlx::query_as("SELECT * FROM downbeat.run WHERE chain_id = $1").bind(id).fetch_all(&state.pool).await?;

    // Every job the chain could reach, in order from its first job.
    let mut order = Vec::new();
    let mut queue = VecDeque::from([first_job.clone()]);
    while let Some(job) = queue.pop_front() {
        if order.contains(&job) {
            continue;
        }
        if let Some(children) = state.registry.children.get(job.as_str()) {
            queue.extend(children.iter().map(|child| (*child).to_owned()));
        }
        order.push(job);
    }
    let mut nodes = Vec::new();
    for job in order.iter().filter(|job| caller.covers(job)) {
        let mut runs: Vec<RunSummary> = unfinished
            .iter()
            .filter(|run| &run.job_name == job)
            .map(|run| RunSummary {
                run_id: run.run_id,
                status: run.state.clone(),
                reason: run.queued_reason.clone().unwrap_or_default(),
                at: run.created_at,
            })
            .collect();
        runs.extend(ended.iter().filter(|row| &row.job_name == job).map(views::summary));
        let parents: Vec<String> = state
            .registry
            .job(job)
            .and_then(|j| j.link.as_ref())
            .map(|link| link.parents.iter().map(|p| (*p).to_owned()).collect())
            .unwrap_or_default();
        let why_not =
            runs.is_empty().then(|| why_not(&state, job, &parents, &ended, &unfinished, cancelled_at.is_some()));
        nodes.push(ChainNode { job: job.clone(), parents, runs, why_not });
    }
    Ok(Json(ChainView { chain_id: id, pipeline, first_job, started_at, cancelled_at, cancelled_by, nodes }))
}

/// Why a job the chain could reach has no run in it.
fn why_not(
    state: &State,
    job: &str,
    parents: &[String],
    ended: &[HistoryRow],
    unfinished: &[RunRow],
    cancelled: bool,
) -> String {
    if cancelled {
        return "Did not run: the chain was cancelled.".to_owned();
    }
    let kind = state.registry.job(job).and_then(|job| job.link.as_ref()).map_or(LinkKind::After, |link| link.kind);
    for parent in parents {
        if unfinished.iter().any(|run| &run.job_name == parent) {
            return format!("Waiting: {parent} has not finished in this chain.");
        }
        match ended.iter().find(|row| &row.job_name == parent) {
            None => return format!("Waiting: {parent} has not run in this chain."),
            Some(row) => {
                let satisfied = match kind {
                    LinkKind::After => row.status == "succeeded",
                    LinkKind::AfterFailure => row.status == "failed",
                    LinkKind::AfterAny => true,
                };
                if !satisfied {
                    return format!("Did not run: {parent} {} in this chain.", row.status);
                }
            }
        }
    }
    "Did not run.".to_owned()
}

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
        for row in rows {
            if i64::try_from(items.len()).unwrap_or(i64::MAX) == limit {
                let last: &RunView = items.last().expect("a full page");
                next = Some(Cursor::Unfinished(last.created_at, last.run_id).text());
                break;
            }
            let view = views::unfinished(&state.pool, &state.registry, row, None).await?;
            if query.queued_because.as_deref().is_none_or(|because| view.queued_because.as_deref() == Some(because)) {
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
    Ok(Json(Page { items, next_cursor: next }))
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
    sql: &mut QueryBuilder<'_, Postgres>,
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
        return Ok(Json(views::unfinished(&state.pool, &state.registry, row, Some(detail)).await?));
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
        let (day_plan, source) =
            plan::plan_for(registry, schedule, &overrides, date).await.map_err(Failure::bad_request)?;
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
        for row in queued {
            let view = views::unfinished(&state.pool, &state.registry, row, None).await?;
            *because.entry(view.queued_because.unwrap_or_default()).or_default() += 1;
        }
        let serving = workers
            .iter()
            .filter_map(|(id, queues, build, seen)| {
                queues.get(&name).and_then(serde_json::Value::as_i64).map(|slots| WorkerView {
                    worker_id: id.clone(),
                    slots,
                    build: build.clone(),
                    last_seen_at: *seen,
                })
            })
            .collect();
        views.push(QueueView {
            paused: views::pause_of(&state.pool, "queue", &name).await?,
            name,
            queued: count,
            running,
            queued_because: because,
            longest_wait_seconds: longest,
            workers: serving,
        });
    }
    Ok(Json(views))
}

pub(crate) async fn overrides(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
) -> Answer<Vec<OverrideView>> {
    caller.require(Scope::Read)?;
    let rows: Vec<plan::OverrideRow> = sqlx::query_as("SELECT * FROM downbeat.schedule_override WHERE on_date >= (now() - interval '30 days')::date ORDER BY on_date, schedule LIMIT 500")
        .fetch_all(&state.pool)
        .await?;
    let visible = |row: &plan::OverrideRow| {
        state.registry.schedules.get(&row.schedule).is_some_and(|schedule| caller.covers(schedule.job))
    };
    Ok(Json(rows.into_iter().filter(|row| visible(row)).map(override_view).collect()))
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

pub(crate) async fn backfills(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
) -> Answer<Vec<BackfillView>> {
    caller.require(Scope::Read)?;
    let rows: Vec<BackfillRow> = sqlx::query_as("SELECT * FROM downbeat.backfill ORDER BY created_at DESC LIMIT 200")
        .fetch_all(&state.pool)
        .await?;
    Ok(Json(rows.into_iter().filter(|row| caller.covers(&row.job_name)).map(backfill_view).collect()))
}

pub(crate) fn backfill_view(row: BackfillRow) -> BackfillView {
    BackfillView {
        backfill_id: row.backfill_id,
        job: row.job_name,
        schedule: row.schedule,
        from: row.from_date,
        to: row.to_date,
        reached: row.next_date,
        state: row.state,
        reason: row.reason,
        continue_on_failure: row.continue_on_failure,
        created_by: row.created_by,
        created_at: row.created_at,
    }
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
        .map(|row| AuditEntry {
            audit_id: row.audit_id,
            at: row.at,
            actor: row.actor,
            action: row.action,
            job: row.job_name,
            run_id: row.run_id,
            schedule: row.schedule,
            detail: row.detail,
        })
        .collect();
    let next_cursor = more.then(|| items.last().map(|entry| entry.audit_id.to_string())).flatten();
    Ok(Json(Page { items, next_cursor }))
}
