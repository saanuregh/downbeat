//! Starting a job.

use super::{Acted, PreviewQuery, job, preview, started_by};
use std::time::Duration;

use axum::Json;
use axum::extract::{Extension, Path, Query, State as Extract};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use downbeat_api::types::{RunStatus, StartRequest, StartResponse};
use downbeat_api::with_defaults;
use downbeat_api::{IDEMPOTENCY_HEADER, Scope};
use serde_json::Value;
use uuid::Uuid;

use crate::api::State;
use crate::api::auth::Caller;
use crate::api::error::Failure;
use crate::job::KeyedInput;
use crate::job::Started;
use crate::reasons;
use crate::registry::RegisteredJob;
use crate::starting::{self, StartError, StartJob, StartOptions};
use crate::store::history;
use crate::store::runs::{self, Timeout};

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
    let KeyedInput { input, hash: input_hash, turn_value } =
        found.spec.input_ops.keyed(&name, request.input.clone(), true).map_err(|error| {
            Failure::unprocessable(format!("the input does not match {name}'s input type: {error}"))
        })?;
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
    let response = with_defaults!(StartResponse {
        run_id: result.started.run_id(),
        status: RunStatus::Queued,
        joined,
        repeated: result.repeated,
    });
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
        Some(run_id) if !options.deliberate() => {
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
        if options.ignore_conditions { false_conditions(state, found, input).await? } else { Vec::new() };
    preview(effects, false_conditions)
}

/// The conditions of a job that are false right now, for a start that would ignore them. These
/// call the product's code, so each call is guarded: a panic marks that condition as false.
async fn false_conditions(state: &State, found: &RegisteredJob, input: &Value) -> Result<Vec<String>, Failure> {
    let mut false_now = Vec::new();
    for condition in &found.conditions {
        let what = format!("the condition '{}' of {}", condition.name, found.spec.name);
        if !crate::guard::guarded(&what, || (condition.test)(input)).unwrap_or(false) {
            false_now.push(format!("only if '{}'", condition.name));
        }
    }
    for need in &found.needs {
        let met = match &need.test {
            crate::pipeline::NeedTest::DoneFor(hash_of) => {
                match crate::guard::guarded(&format!("the input a need on {} looks for", need.job), || hash_of(input)) {
                    Ok(hash) => history::succeeded_for(&state.pool, need.job, &hash).await?,
                    Err(_) => false,
                }
            }
            crate::pipeline::NeedTest::DoneWithin(within) => {
                history::succeeded_within(&state.pool, need.job, *within).await?
            }
        };
        if !met {
            false_now.push(format!("needs {}", need.job));
        }
    }
    // `if_changed` is not listed: it applies only to runs a schedule or a link starts.
    Ok(false_now)
}
