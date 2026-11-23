//! Forwarding the screens' requests to a scheduler, signed for the signed-in person.
//!
//! The browser never talks to a scheduler and never holds a scheduler token. The console checks the
//! person's grants, then signs the request with exactly the scope and jobs they hold, naming them
//! as the actor; the scheduler checks it again.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Extension, OriginalUri, Path, State as Extract};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use downbeat_api::types::PipelineView;
use downbeat_api::{Claims, IDEMPOTENCY_HEADER, RequestBinding, Scope, sign};
use serde_json::json;

use crate::State;
use crate::access::access;
use crate::config::Scheduler;
use crate::error::Failure;
use crate::session::Session;
use crate::store;

/// How long a scheduler's pipelines are remembered, to turn pipeline grants into job lists.
const PIPELINES_FOR: Duration = Duration::from_secs(60);

/// Jobs by pipeline name.
type JobsByPipeline = BTreeMap<String, Vec<String>>;

/// Each scheduler's jobs by pipeline, as last fetched.
#[derive(Default)]
pub(crate) struct PipelineCache(Mutex<HashMap<String, (Instant, JobsByPipeline)>>);

/// `ANY /api/schedulers/{name}/v1/{*rest}`.
pub(crate) async fn forward(
    Extract(state): Extract<State>,
    Extension(session): Extension<Arc<Session>>,
    Path((name, _rest)): Path<(String, String)>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Failure> {
    let scheduler = state
        .config
        .scheduler(&name)
        .ok_or_else(|| Failure::not_found(format!("there is no scheduler named {name}")))?;
    let target =
        target_path(&uri, &name).ok_or_else(|| Failure::not_found("only the scheduler's /v1 API is forwarded"))?;

    // A read is signed with `read`; a change first with `operate`, then, if the scheduler says the
    // action needs more, with `administer`. Each carries the pipelines of the grants that reach it.
    let scopes: &[Scope] = if method == Method::GET { &[Scope::Read] } else { &[Scope::Operate, Scope::Administer] };
    let grants = store::grants_of(&state.pool, &session.person).await?;
    let mut response = None;
    for scope in scopes {
        let Some(granted) = access(&state.config, &session.person, &grants, &name, *scope) else { continue };
        let jobs = match granted.pipelines {
            None => None,
            Some(pipelines) => Some(jobs_of(&state, scheduler, &pipelines).await?),
        };
        let answer = send(
            &state,
            scheduler,
            &session.person.email,
            granted.scope,
            jobs,
            method.clone(),
            &target,
            &body,
            headers.get(IDEMPOTENCY_HEADER),
        )
        .await?;
        let refused = answer.status() == StatusCode::FORBIDDEN;
        response = Some(answer);
        if !refused {
            break;
        }
    }
    let refused = response.as_ref().is_none_or(|answer| answer.status() == StatusCode::FORBIDDEN);
    if refused {
        store::audit(
            &state.pool,
            &session.person.email,
            "refused",
            json!({ "scheduler": name, "method": method.as_str(), "path": target }),
        )
        .await?;
    }
    response
        .ok_or_else(|| Failure::forbidden(format!("you have no grant that lets you do this on {}", scheduler.label)))
}

/// The scheduler path and query a console path forwards to: exactly as the browser sent it, after
/// `/api/schedulers/{name}`, and only under `/v1/`. Dot segments are refused, so a path cannot be
/// normalised out of the API, and so is an encoded slash, which would change the route.
fn target_path(uri: &Uri, scheduler: &str) -> Option<String> {
    let prefix = format!("/api/schedulers/{scheduler}");
    let path = uri.path().strip_prefix(&prefix)?;
    let lowered = path.to_ascii_lowercase();
    let dot_segment = path.split('/').any(|segment| segment == "." || segment == "..") || lowered.contains("%2e");
    if !path.starts_with("/v1/") || dot_segment || lowered.contains("%2f") || lowered.contains("%5c") {
        return None;
    }
    Some(match uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    })
}

/// Sends one signed request to a scheduler and relays its answer.
#[allow(clippy::too_many_arguments)]
async fn send(
    state: &State,
    scheduler: &Scheduler,
    actor: &str,
    scope: Scope,
    jobs: Option<Vec<String>>,
    method: Method,
    path_and_query: &str,
    body: &[u8],
    idempotency_key: Option<&header::HeaderValue>,
) -> Result<Response, Failure> {
    let binding = RequestBinding { method: method.as_str(), path_and_query, body };
    let claims = Claims::for_request(
        &scheduler.caller,
        actor,
        &scheduler.audience,
        scope,
        jobs,
        binding,
        chrono::Utc::now().timestamp(),
    );
    let token = sign(&scheduler.key, &claims);
    let mut request = state.http.request(method, format!("{}{path_and_query}", scheduler.url)).bearer_auth(token);
    if !body.is_empty() {
        request = request.header(header::CONTENT_TYPE, "application/json").body(body.to_vec());
    }
    if let Some(key) = idempotency_key {
        request = request.header(IDEMPOTENCY_HEADER, key.clone());
    }
    let answer = request
        .send()
        .await
        .map_err(|error| Failure::unavailable(format!("{} could not be reached: {error}", scheduler.label)))?;
    let status = StatusCode::from_u16(answer.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let bytes = answer.bytes().await.map_err(|error| Failure::unavailable(error.to_string()))?;
    Ok((status, [(header::CONTENT_TYPE, "application/json")], bytes).into_response())
}

/// The jobs in some pipelines of a scheduler, from its pipelines list (fetched as the console).
async fn jobs_of(state: &State, scheduler: &Scheduler, pipelines: &[String]) -> Result<Vec<String>, Failure> {
    let cached = {
        let cache = state.pipelines.0.lock().expect("pipeline cache");
        cache.get(&scheduler.name).filter(|(at, _)| at.elapsed() < PIPELINES_FOR).map(|(_, map)| map.clone())
    };
    let map = if let Some(map) = cached {
        map
    } else {
        {
            let response =
                send(state, scheduler, &scheduler.caller, Scope::Read, None, Method::GET, "/v1/pipelines", &[], None)
                    .await?;
            let status = response.status();
            if !status.is_success() {
                return Err(Failure::unavailable(format!(
                    "{} answered {status} when asked for its pipelines",
                    scheduler.label
                )));
            }
            let bytes = axum::body::to_bytes(response.into_body(), 4 << 20)
                .await
                .map_err(|error| Failure::unavailable(error.to_string()))?;
            let views: Vec<PipelineView> = serde_json::from_slice(&bytes).map_err(|error| {
                Failure::unavailable(format!("the scheduler's pipelines could not be read: {error}"))
            })?;
            let map: JobsByPipeline = views
                .into_iter()
                .map(|view| (view.name, view.jobs.into_iter().map(|node| node.job).collect()))
                .collect();
            state
                .pipelines
                .0
                .lock()
                .expect("pipeline cache")
                .insert(scheduler.name.clone(), (Instant::now(), map.clone()));
            map
        }
    };
    let mut jobs: Vec<String> = pipelines.iter().filter_map(|pipeline| map.get(pipeline)).flatten().cloned().collect();
    jobs.sort();
    jobs.dedup();
    Ok(jobs)
}

/// Whether a scheduler answers, and which API version it speaks.
pub(crate) async fn version_of(state: &State, scheduler: &Scheduler) -> Option<u32> {
    let answer =
        state.http.get(format!("{}/version", scheduler.url)).timeout(Duration::from_secs(3)).send().await.ok()?;
    let version: downbeat_api::types::Version = answer.json().await.ok()?;
    Some(version.api)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(path: &str) -> Option<String> {
        target_path(&path.parse().unwrap(), "prod")
    }

    #[test]
    fn forwards_the_raw_path_under_v1_only() {
        assert_eq!(
            target("/api/schedulers/prod/v1/jobs/send%20report/runs?preview=true").as_deref(),
            Some("/v1/jobs/send%20report/runs?preview=true")
        );
        assert_eq!(target("/api/schedulers/prod/healthz"), None);
        assert_eq!(target("/api/schedulers/prod/v1/../healthz"), None);
        assert_eq!(target("/api/schedulers/prod/v1/%2e%2e/healthz"), None);
        assert_eq!(target("/api/schedulers/prod/v1/jobs/a%2Fb"), None);
        assert_eq!(target("/api/schedulers/other/v1/jobs"), None);
    }
}
