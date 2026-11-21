//! Verifying every request: the token's signature against the issuing caller's keys, its audience,
//! its lifetime, the request it was made for, that a changing request's token is used once, and
//! that the caller is within what this scheduler grants it.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::{Request, State as Extract};
use axum::http::{HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use downbeat_api::{API_VERSION, RequestBinding, Scope, VERSION_HEADER, verify as verify_token};

use super::State;
use super::error::Failure;
use crate::events::Event;
use crate::store::audit;

/// The largest request body the API reads.
const MAX_BODY: usize = 256 * 1024;
/// Requests per second an actor may make, on average, and in a burst.
const RATE: f64 = 20.0;
const BURST: f64 = 100.0;

/// Who is asking, as verified from the token.
#[derive(Debug, Clone)]
pub(crate) struct Caller {
    /// The trusted caller that signed the token.
    pub(crate) name: String,
    /// The person or service acting.
    pub(crate) actor: String,
    pub(crate) scope: Scope,
    pub(crate) jobs: Option<Vec<String>>,
}

impl Caller {
    /// Refuses unless the token's scope covers this.
    pub(crate) fn require(&self, scope: Scope) -> Result<(), Failure> {
        if self.scope >= scope {
            Ok(())
        } else {
            Err(Failure::forbidden(format!("this needs the {scope} scope; the token has {}", self.scope)))
        }
    }

    /// Refuses unless the token covers every job: for actions on things that span jobs.
    pub(crate) fn require_every_job(&self) -> Result<(), Failure> {
        match self.jobs {
            Some(_) => Err(Failure::forbidden("this needs a token that covers every job")),
            None => Ok(()),
        }
    }

    /// Whether the token covers this job.
    pub(crate) fn covers(&self, job: &str) -> bool {
        self.jobs.as_ref().is_none_or(|jobs| jobs.iter().any(|allowed| allowed == job))
    }

    /// Refuses unless the token covers this job.
    pub(crate) fn require_job(&self, job: &str) -> Result<(), Failure> {
        match &self.jobs {
            Some(jobs) if !jobs.iter().any(|allowed| allowed == job) => {
                Err(Failure::forbidden(format!("this caller, or this token, does not cover the job {job}")))
            }
            _ => Ok(()),
        }
    }

    /// Refuses unless the token covers every one of these jobs, as acting on a whole chain needs.
    pub(crate) fn require_jobs(&self, jobs: &[String]) -> Result<(), Failure> {
        jobs.iter().try_for_each(|job| self.require_job(job))
    }

    /// How the audit names who did something: the actor, and the caller that vouched for them.
    pub(crate) fn audit_name(&self) -> String {
        if self.actor == self.name { self.actor.clone() } else { format!("{} (via {})", self.actor, self.name) }
    }
}

/// Every response says which API version the scheduler speaks.
pub(crate) async fn version_header(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(VERSION_HEADER, HeaderValue::from(API_VERSION));
    response
}

pub(crate) async fn verify(Extract(state): Extract<State>, request: Request, next: Next) -> Response {
    match check(&state, request).await {
        Ok(request) => next.run(request).await,
        Err((failure, caller)) => {
            // A database that cannot be reached is not a refused request (spec 14.12).
            if failure.status() != StatusCode::SERVICE_UNAVAILABLE {
                refused(&state, caller, &failure).await;
            }
            failure.into_response()
        }
    }
}

async fn check(state: &State, request: Request) -> Result<Request, (Failure, Option<String>)> {
    let unauthorized = |message: String| (Failure::new(StatusCode::UNAUTHORIZED, "unauthorized", message), None);
    let (parts, body) = request.into_parts();
    let token = parts
        .headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| {
            unauthorized("every request needs `Authorization: Bearer <token>` signed by a trusted caller".into())
        })?
        .to_owned();
    let body: Bytes = axum::body::to_bytes(body, MAX_BODY).await.map_err(|_| {
        (Failure::new(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "the request body is over 256 KiB"), None)
    })?;
    // Nested under /v1, the router sees the path without its prefix; the token binds the full one.
    let path_and_query = parts.extensions.get::<axum::extract::OriginalUri>().map_or_else(
        || parts.uri.path_and_query().map_or_default(ToString::to_string),
        |original| original.0.path_and_query().map_or_default(ToString::to_string),
    );
    let binding = RequestBinding { method: parts.method.as_str(), path_and_query: &path_and_query, body: &body };
    let now = chrono::Utc::now().timestamp();
    let config = &state.config;
    let claims = verify_token(
        &token,
        |issuer| config.callers.get(issuer).map(|caller| caller.keys.as_slice()),
        &config.audience,
        binding,
        now,
    )
    .map_err(|error| unauthorized(error.to_string()))?;
    let trusted = &config.callers[&claims.iss];
    let refuse = |message: String| (Failure::forbidden(message), Some(claims.iss.clone()));
    if claims.sub != claims.iss && !trusted.acts_for_people {
        return Err(refuse(format!(
            "caller {} may only act as itself; its tokens must name it as the actor",
            claims.iss
        )));
    }
    if claims.scope > trusted.max_scope {
        return Err(refuse(format!(
            "caller {} may be granted at most {}; the token asks for {}",
            claims.iss, trusted.max_scope, claims.scope
        )));
    }
    if let (Some(allowed), asked) = (&trusted.jobs, &claims.jobs) {
        let within = asked.as_ref().is_some_and(|asked| asked.iter().all(|job| allowed.contains(job)));
        if !within {
            return Err(refuse(format!("caller {} is limited to some jobs; its token must name them", claims.iss)));
        }
    }
    if !state.rate_limits.allow(&format!("{}/{}", claims.iss, claims.sub)) {
        return Err((
            Failure::new(StatusCode::TOO_MANY_REQUESTS, "rate_limited", "too many requests from this actor; slow down"),
            Some(claims.iss.clone()),
        ));
    }
    if parts.method != Method::GET {
        let first_use: bool = sqlx::query_scalar(
            "INSERT INTO downbeat.used_token (caller, jti, expires_at) VALUES ($1, $2, to_timestamp($3::bigint + $4::bigint + 1))
             ON CONFLICT DO NOTHING RETURNING true",
        )
        .bind(&claims.iss)
        .bind(&claims.jti)
        .bind(claims.exp)
        .bind(downbeat_api::LEEWAY_SECONDS)
        .fetch_optional(&state.pool)
        .await
        .map_err(|error| (Failure::from(error), None))?
        .unwrap_or(false);
        if !first_use {
            // The token verified, so its caller is known and is named in the report.
            let message = "this token has been used already; sign a new one for each request";
            return Err((Failure::new(StatusCode::UNAUTHORIZED, "unauthorized", message), Some(claims.iss.clone())));
        }
    }
    let caller = Caller { name: claims.iss, actor: claims.sub, scope: claims.scope, jobs: claims.jobs };
    let mut request = Request::from_parts(parts, Body::from(body));
    request.extensions_mut().insert(caller);
    Ok(request)
}

/// Refused requests are counted and reported, since a burst of them is worth knowing about.
///
/// Each is logged; `on_event` hears at most once a minute per caller and status, with how many
/// there were, so a flood of bad requests cannot flood the database.
async fn refused(state: &State, caller: Option<String>, failure: &Failure) {
    tracing::warn!(?caller, ?failure, "a control API request was refused");
    let status = failure.status().as_u16();
    let Some(count) = state.refusals.count(caller.clone(), status) else { return };
    if let Ok(mut connection) = state.pool.acquire().await {
        let event = Event::RequestRefused { caller, status, count, reason: failure.message().to_owned() };
        let _ = audit::event(&mut connection, &event, None, None, None).await;
    }
}

/// Who was refused (the caller, if the token named a trusted one) and with which status.
type RefusalKey = (Option<String>, u16);

/// Refused requests per caller and status since each was last reported, and when that was.
#[derive(Debug, Default)]
pub(crate) struct Refusals(Mutex<HashMap<RefusalKey, (i64, Option<Instant>)>>);

impl Refusals {
    /// Counts one refusal; returns how many to report when a minute has passed since the last
    /// report (or for the first).
    fn count(&self, caller: Option<String>, status: u16) -> Option<i64> {
        let mut counts = self.0.lock().expect("refusal counts");
        let now = Instant::now();
        let (count, reported) = counts.entry((caller, status)).or_insert((0, None));
        *count += 1;
        if reported.is_some_and(|at| at.elapsed() < std::time::Duration::from_secs(60)) {
            return None;
        }
        let total = *count;
        *count = 0;
        *reported = Some(now);
        Some(total)
    }
}

/// A token bucket per actor.
#[derive(Debug, Default)]
pub(crate) struct RateLimits(Mutex<HashMap<String, (f64, Instant)>>);

impl RateLimits {
    fn allow(&self, actor: &str) -> bool {
        let mut buckets = self.0.lock().expect("rate limits");
        let now = Instant::now();
        let (tokens, last) = buckets.entry(actor.to_owned()).or_insert((BURST, now));
        *tokens = (*tokens + now.duration_since(*last).as_secs_f64() * RATE).min(BURST);
        *last = now;
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            true
        } else {
            false
        }
    }
}
