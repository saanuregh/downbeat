//! The changing endpoints. Each accepts `?preview=true`, which changes nothing and says what the
//! action would do; each real action writes an audit row naming the caller and the actor.

use std::fmt::Write as _;
use std::time::Duration;

use axum::Json;
use axum::extract::{Extension, Path, Query, State as Extract};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use downbeat_api::types::{
    BackfillRequest, Done, MarkSucceededRequest, OverrideRequest, PauseRequest, Preview, StartRequest, StartResponse,
};
use downbeat_api::{IDEMPOTENCY_HEADER, Scope};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::State;
use super::auth::Caller;
use super::error::Failure;
use crate::job::Started;
use crate::reasons;
use crate::registry::RegisteredJob;
use crate::schedule::Times;
use crate::scheduler::plan;
use crate::starting::{self, StartError, StartJob, StartOptions};
use crate::store::cancel::{self, Cancelled};
use crate::store::ending::{self, HistoryRow, lock_chain};
use crate::store::runs::{self, NewRun, Place, RunRow, StartedBy, Timeout};
use crate::store::{audit, history};

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

/// `POST /jobs/{name}/runs`: start a job.
pub(crate) async fn start(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(name): Path<String>,
    Query(query): Query<PreviewQuery>,
    headers: HeaderMap,
    Json(request): Json<StartRequest>,
) -> Acted {
    caller.require(Scope::Operate)?;
    caller.require_job(&name)?;
    if request.ignore_conditions || request.timeout_seconds.is_some() {
        caller.require(Scope::Administer)?;
    }
    let found = job(&state, &name)?;
    let ops = &found.spec.input_ops;
    let input = (ops.normalize)(request.input.clone(), true)
        .map_err(|error| Failure::unprocessable(format!("the input does not match {name}'s input type: {error}")))?;
    let input_hash = (ops.hash)(&input).map_err(Failure::unprocessable)?;
    let turn_value = ops.turn_value.map(|value_of| value_of(&input)).transpose().map_err(Failure::unprocessable)?;
    if request.dry_run && !found.spec.policy.supports_dry_run {
        return Err(Failure::unprocessable(format!("{name} does not support dry runs")));
    }
    let options = StartOptions {
        priority: request.priority,
        not_before: request.not_before,
        dry_run: request.dry_run,
        without_children: request.without_children,
        even_if_queued: request.even_if_queued,
        ignore_conditions: request.ignore_conditions,
        timeout: request
            .timeout_seconds
            .map(|seconds| if seconds == 0 { Timeout::Never } else { Timeout::After(Duration::from_secs(seconds)) }),
    };
    if query.preview {
        return preview_start(&state, found, &input, &input_hash, &options).await;
    }
    let key = headers.get(IDEMPOTENCY_HEADER).and_then(|value| value.to_str().ok()).map(str::to_owned);
    let request = StartJob {
        job: name.clone(),
        input,
        input_hash,
        turn_value,
        options,
        started_by: started_by(&caller),
        actor: Some(caller.audit_name()),
        idempotency: key.map(|key| (format!("{}/{}", caller.name, caller.actor), key)),
    };
    let result = starting::start(&state.pool, &state.registry, request).await.map_err(|error| match error {
        StartError::Database(error) => Failure::from(error),
        other => Failure::unprocessable(other.to_string()),
    })?;
    let (status, joined) = match result.started {
        Started::New(_) if !result.repeated => (StatusCode::CREATED, false),
        Started::New(_) => (StatusCode::OK, false),
        Started::Joined(_) => (StatusCode::OK, true),
    };
    let response = StartResponse {
        run_id: result.started.run_id(),
        status: "queued".to_owned(),
        joined,
        repeated: result.repeated,
    };
    Ok((status, Json(response)).into_response())
}

async fn preview_start(
    state: &State,
    found: &RegisteredJob,
    input: &Value,
    input_hash: &str,
    options: &StartOptions,
) -> Acted {
    let name = found.spec.name;
    let mut effects = Vec::new();
    let queued: Option<Uuid> =
        sqlx::query_scalar("SELECT run_id FROM downbeat.run WHERE same_work_key = $1 AND starts = 0")
            .bind(runs::same_work_key(name, Some(input_hash)))
            .fetch_optional(&state.pool)
            .await?;
    match queued {
        Some(run_id)
            if !options.even_if_queued
                && !options.dry_run
                && !options.without_children
                && !options.ignore_conditions =>
        {
            effects
                .push(format!("Would join run {}, which is already queued for the same work.", reasons::short(run_id)));
        }
        _ => effects.push(format!("Would create a run of {name} on queue {}.", found.spec.policy.queue)),
    }
    let children = state.registry.children.get(name).cloned().unwrap_or_default();
    if options.without_children || children.is_empty() {
        effects.push("Nothing would follow it.".to_owned());
    } else {
        effects.push(format!("When it ends, it can start: {}.", children.join(", ")));
    }
    let false_conditions =
        if options.ignore_conditions { false_conditions(state, found, input, input_hash).await? } else { Vec::new() };
    preview(effects, false_conditions)
}

/// The conditions of a job that are false right now, for a start that would ignore them.
async fn false_conditions(
    state: &State,
    found: &RegisteredJob,
    input: &Value,
    input_hash: &str,
) -> Result<Vec<String>, Failure> {
    let mut false_now = Vec::new();
    for condition in &found.conditions {
        if !(condition.test)(input).unwrap_or(false) {
            false_now.push(format!("only if '{}'", condition.name));
        }
    }
    for need in &found.needs {
        let met = match &need.test {
            crate::pipeline::NeedTest::DoneFor(hash_of) => match hash_of(input) {
                Ok(hash) => history::succeeded_for(&state.pool, need.job, &hash).await?,
                Err(_) => false,
            },
            crate::pipeline::NeedTest::DoneWithin(within) => {
                history::succeeded_within(&state.pool, need.job, *within).await?
            }
        };
        if !met {
            false_now.push(format!("needs {}", need.job));
        }
    }
    if found.if_changed
        && let Some((Some(hash), _)) = history::last_success(&state.pool, found.spec.name, None).await?
        && hash == input_hash
    {
        false_now.push("if its input changed".to_owned());
    }
    Ok(false_now)
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

/// `POST /runs/{id}/cancel`.
pub(crate) async fn cancel_run(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<Uuid>,
    Query(query): Query<PreviewQuery>,
) -> Acted {
    caller.require(Scope::Operate)?;
    let Found::Unfinished(run) = find_run(&state, &caller, id).await? else {
        return Err(Failure::conflict("the run has already finished"));
    };
    if query.preview {
        let effect = match run.function_started_at {
            Some(started) => {
                let checkpoint = if run.checkpoint.is_some() {
                    "it has a checkpoint, which a restart does not use"
                } else {
                    "it has no checkpoint"
                };
                let ran_for = crate::units::format_duration((Utc::now() - started).to_std().unwrap_or_default());
                format!("Would stop {} after {ran_for} of work; {checkpoint}.", run.job_name)
            }
            None => format!("Would cancel the waiting run of {}.", run.job_name),
        };
        return preview(vec![effect, "Its .after children would not run.".to_owned()], Vec::new());
    }
    let by = caller.audit_name();
    let mut transaction = state.pool.begin().await?;
    lock_chain(&mut transaction, run.chain_id).await?;
    let cancelled =
        cancel::cancel_run(&mut transaction, &state.registry, run.run_id, &reasons::cancelled(&by), &by).await?;
    audit::record(&mut transaction, &by, "cancel_run", Some(&run.job_name), Some(run.run_id), None, json!({})).await?;
    transaction.commit().await?;
    match cancelled {
        Cancelled::Ended => done("Cancelled.", Some(run.run_id), None),
        Cancelled::Asked => {
            done("Asked the worker to stop the run; it will within a heartbeat.", Some(run.run_id), None)
        }
        Cancelled::Gone => Err(Failure::conflict("the run finished before it could be cancelled")),
    }
}

/// `POST /runs/{id}/retry`: the same job and input, in the same chain, which then continues.
pub(crate) async fn retry(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<Uuid>,
    Query(query): Query<PreviewQuery>,
) -> Acted {
    caller.require(Scope::Operate)?;
    let Found::Finished(row) = find_run(&state, &caller, id).await? else {
        return Err(Failure::conflict("the run has not finished; cancel it first or wait"));
    };
    let found = job(&state, &row.job_name)?;
    if query.preview {
        return preview(
            vec![format!(
                "Would run {} again with the same input in the same chain; children that have not run yet would then start.",
                row.job_name
            )],
            Vec::new(),
        );
    }
    let by = caller.audit_name();
    let policy = &found.spec.policy;
    let run = NewRun {
        job: row.job_name.clone(),
        queue: policy.queue.to_owned(),
        priority: policy.priority,
        turn_key: row.turn_key.clone(),
        input: row.input.clone(),
        input_hash: row.input_hash.clone(),
        joins_queued_work: false,
        due_at: None,
        expires_at: None,
        not_before: None,
        started_by: started_by(&caller),
        schedule: None,
        place: Place::Chain { chain_id: row.chain_id, parents: row.parents.clone() },
        dry_run: row.dry_run,
        ignore_conditions: false,
        without_children: false,
        timeout_override: None,
        actor: Some(by.clone()),
        queued_reason: None,
    };
    let mut transaction = state.pool.begin().await?;
    lock_chain(&mut transaction, row.chain_id).await?;
    let cancelled: bool = sqlx::query_scalar("SELECT cancelled_at IS NOT NULL FROM downbeat.chain WHERE chain_id = $1")
        .bind(row.chain_id)
        .fetch_one(&mut *transaction)
        .await?;
    if cancelled {
        return Err(Failure::conflict("the chain was cancelled; restart from here to begin a new chain"));
    }
    let created = runs::create(&mut transaction, &run).await?;
    audit::record(
        &mut transaction,
        &by,
        "retry",
        Some(&row.job_name),
        Some(created.run_id()),
        None,
        json!({ "retried": row.run_id }),
    )
    .await?;
    transaction.commit().await?;
    done("Retrying this step in the same chain.", Some(created.run_id()), None)
}

/// `POST /runs/{id}/restart`: a new chain from this job with the same input.
pub(crate) async fn restart(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<Uuid>,
    Query(query): Query<PreviewQuery>,
) -> Acted {
    caller.require(Scope::Operate)?;
    let (job_name, input) = match find_run(&state, &caller, id).await? {
        Found::Finished(row) => (row.job_name, row.input),
        Found::Unfinished(row) => (row.job_name, row.input),
    };
    let found = job(&state, &job_name)?;
    let input =
        input.ok_or_else(|| Failure::conflict("the run's input is not known yet, because it has not been prepared"))?;
    if query.preview {
        let children = state.registry.children.get(job_name.as_str()).cloned().unwrap_or_default();
        return preview(
            vec![format!(
                "Would start a new chain from {job_name} with the same input; everything downstream ({}) would run again.",
                children.join(", ")
            )],
            Vec::new(),
        );
    }
    let ops = &found.spec.input_ops;
    let input_hash = (ops.hash)(&input).map_err(Failure::unprocessable)?;
    let turn_value = ops.turn_value.map(|value_of| value_of(&input)).transpose().map_err(Failure::unprocessable)?;
    let request = StartJob {
        job: job_name.clone(),
        input,
        input_hash,
        turn_value,
        options: StartOptions { even_if_queued: true, ..StartOptions::default() },
        started_by: started_by(&caller),
        actor: Some(caller.audit_name()),
        idempotency: None,
    };
    let result = starting::start(&state.pool, &state.registry, request).await.map_err(|error| match error {
        StartError::Database(error) => Failure::from(error),
        other => Failure::unprocessable(other.to_string()),
    })?;
    done("Restarted from here in a new chain.", Some(result.started.run_id()), None)
}

/// `POST /runs/{id}/mark-succeeded`: after fixing data by hand, record a failed run as succeeded.
pub(crate) async fn mark_succeeded(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<Uuid>,
    Query(query): Query<PreviewQuery>,
    Json(request): Json<MarkSucceededRequest>,
) -> Acted {
    caller.require(Scope::Administer)?;
    if request.note.trim().is_empty() {
        return Err(Failure::bad_request("marking a run as succeeded needs a note saying why"));
    }
    let Found::Finished(row) = find_run(&state, &caller, id).await? else {
        return Err(Failure::conflict("only a failed run can be marked as succeeded"));
    };
    if row.status != "failed" {
        return Err(Failure::conflict(format!("the run {}; only a failed run can be marked as succeeded", row.status)));
    }
    let found = job(&state, &row.job_name)?;
    let output = (found.spec.normalize_output)(request.output, true).map_err(|error| {
        Failure::unprocessable(format!("the output does not match {}'s output type: {error}", row.job_name))
    })?;
    if query.preview {
        let children = state.registry.children.get(row.job_name.as_str()).cloned().unwrap_or_default();
        return preview(
            vec![format!(
                "Would record run {} as succeeded, marked by you, and start what follows it: {}.",
                reasons::short(id),
                children.join(", ")
            )],
            Vec::new(),
        );
    }
    let by = caller.audit_name();
    let mut transaction = state.pool.begin().await?;
    lock_chain(&mut transaction, row.chain_id).await?;
    let updated: Option<HistoryRow> = sqlx::query_as(
        "UPDATE downbeat.run_history SET status = 'succeeded', failed_because = NULL, output = $2, reason = $3, marked_by = $4,
                note = $5, notify_after = now()
          WHERE run_id = $1 AND status = 'failed' RETURNING *",
    )
    .bind(id)
    .bind(&output)
    .bind(reasons::marked_succeeded(&by, &request.note))
    .bind(&by)
    .bind(&request.note)
    .fetch_optional(&mut *transaction)
    .await?;
    let updated = updated.ok_or_else(|| Failure::conflict("the run is no longer failed"))?;
    ending::create_children(&mut transaction, &state.registry, &updated).await?;
    audit::record(
        &mut transaction,
        &by,
        "mark_succeeded",
        Some(&row.job_name),
        Some(id),
        None,
        json!({ "note": request.note }),
    )
    .await?;
    transaction.commit().await?;
    done("Marked as succeeded; what follows it has started.", Some(id), None)
}

/// `POST /runs/{id}/check-now`: a run that is waiting is looked at again at once.
pub(crate) async fn check_now(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<Uuid>,
    Query(query): Query<PreviewQuery>,
) -> Acted {
    caller.require(Scope::Operate)?;
    let Found::Unfinished(run) = find_run(&state, &caller, id).await? else {
        return Err(Failure::conflict("the run has already finished"));
    };
    if query.preview {
        return preview(vec![format!("Would make the waiting run of {} due now.", run.job_name)], Vec::new());
    }
    if !runs::check_now(&state.pool, id).await? {
        return Err(Failure::conflict(
            "the run is not waiting on a check: it is running, or queued for another reason",
        ));
    }
    let mut connection = state.pool.acquire().await?;
    audit::record(&mut connection, &caller.audit_name(), "check_now", Some(&run.job_name), Some(id), None, json!({}))
        .await?;
    done("The run is due now.", Some(id), None)
}

/// `POST /chains/{id}/cancel`: everything queued or running in it stops; nothing more is created.
pub(crate) async fn cancel_chain(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<Uuid>,
    Query(query): Query<PreviewQuery>,
) -> Acted {
    caller.require(Scope::Operate)?;
    let by = caller.audit_name();
    let mut transaction = state.pool.begin().await?;
    lock_chain(&mut transaction, id).await?;
    let jobs = cancel::jobs_in_chain(&mut transaction, id).await?;
    if jobs.is_empty() {
        return Err(Failure::not_found(format!("there is no chain {id}")));
    }
    for job in &jobs {
        caller.require_job(job)?;
    }
    if query.preview {
        let unfinished: Vec<String> = sqlx::query_scalar("SELECT job_name FROM downbeat.run WHERE chain_id = $1")
            .bind(id)
            .fetch_all(&mut *transaction)
            .await?;
        return preview(
            vec![format!(
                "Would cancel {} unfinished run(s) ({}) and create nothing more in the chain.",
                unfinished.len(),
                unfinished.join(", ")
            )],
            Vec::new(),
        );
    }
    if !cancel::cancel_chain(&mut transaction, &state.registry, id, &format!("Cancelled with its chain by {by}."), &by)
        .await?
    {
        return Err(Failure::conflict("the chain was already cancelled"));
    }
    audit::record(&mut transaction, &by, "cancel_chain", None, None, None, json!({ "chain_id": id })).await?;
    transaction.commit().await?;
    done("The chain is cancelled.", None, None)
}

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

fn known_schedule(state: &State, caller: &Caller, name: &str) -> Result<(), Failure> {
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
