//! Acting on runs and chains: cancel, retry this step, restart from here, mark as succeeded,
//! check now.

use super::{Acted, Found, PreviewQuery, done, find_run, job, preview, started_by};

use axum::Json;
use axum::extract::{Extension, Path, Query, State as Extract};
use chrono::Utc;
use downbeat_api::Scope;
use downbeat_api::types::MarkSucceededRequest;
use serde_json::json;
use uuid::Uuid;

use crate::api::State;
use crate::api::auth::Caller;
use crate::api::error::Failure;
use crate::reasons;
use crate::starting::{self, StartError, StartJob, StartOptions};
use crate::store::audit;
use crate::store::cancel::{self, Cancelled};
use crate::store::ending::{self, HistoryRow, lock_chain};
use crate::store::runs::{self, NewRun, Place};

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
