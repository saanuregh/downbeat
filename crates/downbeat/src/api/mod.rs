//! The control API: the one HTTP interface on the scheduler. Programs use it to start jobs, and the
//! console uses it for everything people see and do. Every request is verified against a token
//! signed by a trusted caller; the scheduler holds only public keys.

mod act;
mod auth;
mod error;
mod read;
mod views;

use crate::process_error::ProcessError;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::routing::{delete, get, post};
use downbeat_api::{DEVELOPMENT_AUDIENCE, PublicKey, Scope, development_key};
use serde::Deserialize;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::registry::Registry;
use auth::RateLimits;

/// The address `--local` listens on when no other is given.
const LOCAL_LISTEN: &str = "127.0.0.1:7400";

/// Who may call this scheduler, and where it listens.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) listen: SocketAddr,
    /// The name tokens must carry in `aud` to be for this scheduler.
    pub(crate) audience: String,
    pub(crate) callers: BTreeMap<String, TrustedCaller>,
}

/// A caller the scheduler trusts, and the most it may be granted.
#[derive(Debug, Clone)]
pub(crate) struct TrustedCaller {
    pub(crate) keys: Vec<PublicKey>,
    pub(crate) max_scope: Scope,
    pub(crate) jobs: Option<Vec<String>>,
    /// Whether its tokens may name someone else as the actor, as a console does for people.
    pub(crate) acts_for_people: bool,
}

/// The callers file, as written.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CallersFile {
    audience: String,
    #[serde(default, rename = "caller")]
    callers: Vec<CallerEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CallerEntry {
    name: String,
    keys: Vec<String>,
    max_scope: Scope,
    #[serde(default)]
    jobs: Option<Vec<String>>,
    #[serde(default)]
    acts_for_people: bool,
}

impl Config {
    /// The API's configuration from the command line, or `None` when the API is off.
    pub(crate) fn load(
        listen: Option<SocketAddr>,
        callers: Option<&Path>,
        local: bool,
    ) -> Result<Option<Config>, ProcessError> {
        if local {
            let listen = listen.unwrap_or_else(|| LOCAL_LISTEN.parse().expect("a valid address"));
            if !listen.ip().is_loopback() {
                return Err(ProcessError::Refused(format!(
                    "--local listens on this machine only; {listen} is not a loopback address"
                )));
            }
            let caller = TrustedCaller {
                keys: vec![development_key().public()],
                max_scope: Scope::Administer,
                jobs: None,
                acts_for_people: true,
            };
            return Ok(Some(Config {
                listen,
                audience: DEVELOPMENT_AUDIENCE.to_owned(),
                callers: BTreeMap::from([("local".to_owned(), caller)]),
            }));
        }
        let Some(listen) = listen else { return Ok(None) };
        let path = callers.ok_or_else(|| {
            ProcessError::Refused(
                "--listen needs --callers, the file of trusted callers (or --local to try it out)".to_owned(),
            )
        })?;
        let text = std::fs::read_to_string(path)
            .map_err(|source| ProcessError::io(format!("reading {}", path.display()), source))?;
        let file: CallersFile =
            toml::from_str(&text).map_err(|source| ProcessError::CallersFile { path: path.to_path_buf(), source })?;
        let mut callers = BTreeMap::new();
        for entry in file.callers {
            let keys = entry.keys.iter().map(|key| PublicKey::from_base64(key)).collect::<Result<Vec<_>, _>>()?;
            if keys.is_empty() {
                return Err(ProcessError::Refused(format!("caller {} has no keys", entry.name)));
            }
            let caller = TrustedCaller {
                keys,
                max_scope: entry.max_scope,
                jobs: entry.jobs,
                acts_for_people: entry.acts_for_people,
            };
            if callers.insert(entry.name.clone(), caller).is_some() {
                return Err(ProcessError::Refused(format!("caller {} is listed twice", entry.name)));
            }
        }
        Ok(Some(Config { listen, audience: file.audience, callers }))
    }
}

/// What every handler shares.
pub(crate) struct ApiState {
    pub(crate) pool: PgPool,
    pub(crate) registry: Arc<Registry>,
    pub(crate) config: Config,
    rate_limits: RateLimits,
    refusals: auth::Refusals,
}

pub(crate) type State = Arc<ApiState>;

pub(crate) async fn serve(
    pool: PgPool,
    registry: Arc<Registry>,
    config: Config,
    stop: CancellationToken,
) -> Result<(), ProcessError> {
    let listen = config.listen;
    let state = Arc::new(ApiState {
        pool,
        registry,
        config,
        rate_limits: RateLimits::default(),
        refusals: auth::Refusals::default(),
    });
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|source| ProcessError::io(format!("listening on {listen}"), source))?;
    tracing::info!(%listen, "control API listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move { stop.cancelled().await })
        .await
        .map_err(|source| ProcessError::io(format!("serving on {listen}"), source))?;
    Ok(())
}

pub(crate) fn router(state: State) -> Router {
    let api = Router::new()
        .route("/jobs", get(read::jobs))
        .route("/jobs/{name}", get(read::job))
        .route("/jobs/{name}/runs", post(act::start))
        .route("/jobs/{name}/pause", post(act::pause_job))
        .route("/jobs/{name}/resume", post(act::resume_job))
        .route("/pipelines", get(read::pipelines))
        .route("/pipelines/{name}", get(read::pipeline))
        .route("/chains/{id}", get(read::chain))
        .route("/chains/{id}/cancel", post(act::cancel_chain))
        .route("/runs", get(read::runs))
        .route("/runs/{id}", get(read::run))
        .route("/runs/{id}/cancel", post(act::cancel_run))
        .route("/runs/{id}/retry", post(act::retry))
        .route("/runs/{id}/restart", post(act::restart))
        .route("/runs/{id}/mark-succeeded", post(act::mark_succeeded))
        .route("/runs/{id}/check-now", post(act::check_now))
        .route("/calendar", get(read::calendar))
        .route("/schedules", get(read::schedules))
        .route("/schedules/{name}/pause", post(act::pause_schedule))
        .route("/schedules/{name}/resume", post(act::resume_schedule))
        .route("/queues", get(read::queues))
        .route("/queues/{name}/pause", post(act::pause_queue))
        .route("/queues/{name}/resume", post(act::resume_queue))
        .route("/pause", get(read::pause_of_everything).post(act::pause_everything))
        .route("/resume", post(act::resume_everything))
        .route("/overrides", get(read::overrides).post(act::add_override))
        .route("/overrides/{id}", delete(act::remove_override))
        .route("/backfills", get(read::backfills).post(act::start_backfill))
        .route("/backfills/{id}/cancel", post(act::cancel_backfill))
        .route("/audit", get(read::audit))
        .route("/report", get(read::report))
        // Unknown paths under /v1 go through the token check too, so a request without one is
        // answered 401 and counted, like any other (spec 14.3).
        .fallback(|| async { error::Failure::not_found("there is no such endpoint; see the HTTP API reference") })
        .layer(axum::middleware::from_fn_with_state(Arc::clone(&state), auth::verify));
    Router::new()
        .nest("/v1", api)
        .route("/healthz", get(read::healthz))
        .route("/readyz", get(read::readyz))
        .route("/version", get(read::version))
        .fallback(|| async { error::Failure::not_found("there is no such endpoint; see the HTTP API reference") })
        .layer(axum::middleware::from_fn(error::as_json))
        .layer(axum::middleware::from_fn(auth::version_header))
        .with_state(state)
}
