//! Jobs, pipelines and chains.

use std::collections::{HashMap, VecDeque};

use axum::Json;
use axum::extract::{Extension, Path, State as Extract};
use chrono::{DateTime, Utc};
use downbeat_api::Scope;
use downbeat_api::types::{ChainNode, ChainSummary, ChainView, JobView, PipelineNode, PipelineView, RunSummary};
use uuid::Uuid;

use crate::api::State;
use crate::api::auth::Caller;
use crate::api::error::{Answer, Failure};
use crate::api::views;
use crate::pipeline::LinkKind;
use crate::registry::RegisteredJob;
use crate::store::ending::HistoryRow;
use crate::store::history;
use crate::store::runs::RunRow;

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
        Some(every) => history::wait_for_success(&state.pool, name, every).await?.is_some_and(|wait| wait.overdue),
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
        input_schema: job.spec.input.json_schema(),
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
    let mut views: Vec<PipelineView> = state
        .registry
        .pipelines
        .keys()
        .map(|name| pipeline_view(&state, &caller, name))
        .filter(|view| !view.jobs.is_empty())
        .collect();
    let names: Vec<String> = views.iter().map(|view| view.name.clone()).collect();
    let mut chains = recent_chains(&state, &caller, &names, PIPELINES_LIST_CHAINS).await?;
    for view in &mut views {
        view.recent_chains = chains.remove(&view.name).unwrap_or_default();
    }
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
    let names = [name];
    view.recent_chains =
        recent_chains(&state, &caller, &names, PIPELINE_CHAINS).await?.remove(&names[0]).unwrap_or_default();
    Ok(Json(view))
}

/// Recent chains shown per pipeline in the list of pipelines, and on one pipeline's page.
const PIPELINES_LIST_CHAINS: i64 = 5;
const PIPELINE_CHAINS: i64 = 20;

/// The newest chains of each named pipeline, newest first, with each job's latest status: two
/// queries in all. A chain is readable only when its first job is covered (spec 14.14), so others
/// are left out, and so are the steps of jobs the token does not cover.
async fn recent_chains(
    state: &State,
    caller: &Caller,
    pipelines: &[String],
    per_pipeline: i64,
) -> Result<HashMap<String, Vec<ChainSummary>>, Failure> {
    let chains: Vec<ChainRow> = sqlx::query_as(
        "SELECT chain_id, pipeline, first_job, started_at, cancelled_at, cancelled_by FROM (
            SELECT c.*, row_number() OVER (PARTITION BY pipeline ORDER BY started_at DESC) AS newest
              FROM downbeat.chain c
             WHERE pipeline = ANY($1) AND ($2::text[] IS NULL OR first_job = ANY($2))) ranked
          WHERE newest <= $3
          ORDER BY pipeline, started_at DESC",
    )
    .bind(pipelines)
    .bind(caller.jobs.as_deref())
    .bind(per_pipeline)
    .fetch_all(&state.pool)
    .await?;
    let chain_ids: Vec<Uuid> = chains.iter().map(|chain| chain.chain_id).collect();
    let statuses: Vec<(Uuid, String, String)> = sqlx::query_as(
        "SELECT DISTINCT ON (chain_id, job_name) chain_id, job_name, status FROM (
            SELECT chain_id, job_name, state AS status, created_at AS at FROM downbeat.run WHERE chain_id = ANY($1)
            UNION ALL
            SELECT chain_id, job_name, status, finished_at FROM downbeat.run_history
             WHERE chain_id = ANY($1) AND joined_run IS NULL) runs
         ORDER BY chain_id, job_name, at DESC",
    )
    .bind(&chain_ids)
    .fetch_all(&state.pool)
    .await?;
    let mut by_pipeline: HashMap<String, Vec<ChainSummary>> = HashMap::new();
    for ChainRow { chain_id, pipeline, first_job, started_at, cancelled_at, .. } in chains {
        let statuses = statuses
            .iter()
            .filter(|(id, job, _)| *id == chain_id && caller.covers(job))
            .map(|(_, job, status)| (job.clone(), status.clone()))
            .collect();
        by_pipeline.entry(pipeline).or_default().push(ChainSummary {
            chain_id,
            first_job,
            started_at,
            statuses,
            cancelled: cancelled_at.is_some(),
        });
    }
    Ok(by_pipeline)
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
