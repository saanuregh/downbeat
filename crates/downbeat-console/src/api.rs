//! The console's own API: who is signed in, grants, people, its audit and preferences.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path, Query, State as Extract};
use downbeat_api::{API_VERSION, Scope};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::State;
use crate::access::{administers_fully, highest};
use crate::error::Failure;
use crate::proxy::version_of;
use crate::session::Session;
use crate::store::{self, Grant};

#[derive(Debug, Serialize)]
pub(crate) struct Me {
    person: PersonView,
    csrf_token: String,
    local: bool,
    schedulers: Vec<SchedulerView>,
}

#[derive(Debug, Serialize)]
struct PersonView {
    email: String,
    name: String,
}

#[derive(Debug, Serialize)]
struct SchedulerView {
    name: String,
    label: String,
    scope: Option<Scope>,
    reachable: bool,
    api_version: Option<u32>,
    /// Whether this console understands the scheduler's API version.
    supported: bool,
}

/// `GET /api/me`.
pub(crate) async fn me(
    Extract(state): Extract<State>,
    Extension(session): Extension<Arc<Session>>,
) -> Result<Json<Me>, Failure> {
    let grants = store::grants_of(&state.pool, &session.person).await?;
    // Every scheduler is asked at once, so one that is down costs one timeout, not one each.
    let versions =
        futures::future::join_all(state.config.schedulers.iter().map(|scheduler| version_of(&state, scheduler))).await;
    let mut schedulers = Vec::new();
    for (scheduler, version) in state.config.schedulers.iter().zip(versions) {
        schedulers.push(SchedulerView {
            name: scheduler.name.clone(),
            label: scheduler.label.clone(),
            scope: highest(&state.config, &session.person, &grants, &scheduler.name),
            reachable: version.is_some(),
            supported: version
                .is_none_or(|version| version == API_VERSION || version.checked_add(1) == Some(API_VERSION)),
            api_version: version,
        });
    }
    Ok(Json(Me {
        person: PersonView { email: session.person.email.clone(), name: session.person.name.clone() },
        csrf_token: session.csrf_token.clone(),
        local: state.config.local,
        schedulers,
    }))
}

/// Refuses unless the person administers every pipeline of this scheduler, so nobody can grant
/// more than they hold.
async fn require_administer(state: &State, session: &Session, scheduler: &str) -> Result<(), Failure> {
    let grants = store::grants_of(&state.pool, &session.person).await?;
    if administers_fully(&state.config, &session.person, &grants, scheduler) {
        Ok(())
    } else {
        Err(Failure::forbidden(format!(
            "only people who administer all of {scheduler} can change who may do what on it"
        )))
    }
}

/// `GET /api/grants`: every grant, for people who administer something.
pub(crate) async fn grants(
    Extract(state): Extract<State>,
    Extension(session): Extension<Arc<Session>>,
) -> Result<Json<Vec<Grant>>, Failure> {
    let administered = administered_schedulers(&state, &session).await?;
    let grants = store::grants(&state.pool).await?;
    Ok(Json(grants.into_iter().filter(|grant| administered.contains(&grant.scheduler)).collect()))
}

/// The schedulers this person administers in full.
async fn administered_schedulers(state: &State, session: &Session) -> Result<Vec<String>, Failure> {
    let grants = store::grants_of(&state.pool, &session.person).await?;
    let administered: Vec<String> = state
        .config
        .schedulers
        .iter()
        .filter(|scheduler| administers_fully(&state.config, &session.person, &grants, &scheduler.name))
        .map(|scheduler| scheduler.name.clone())
        .collect();
    if administered.is_empty() {
        return Err(Failure::forbidden("this is for people who administer a scheduler"));
    }
    Ok(administered)
}

async fn administers_something(state: &State, session: &Session) -> Result<(), Failure> {
    administered_schedulers(state, session).await.map(|_| ())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NewGrant {
    to_person: Option<String>,
    to_group: Option<String>,
    scheduler: String,
    scope: Scope,
    pipelines: Option<Vec<String>>,
}

/// `POST /api/grants`.
pub(crate) async fn add_grant(
    Extract(state): Extract<State>,
    Extension(session): Extension<Arc<Session>>,
    Json(grant): Json<NewGrant>,
) -> Result<Json<Grant>, Failure> {
    if state.config.scheduler(&grant.scheduler).is_none() {
        return Err(Failure::not_found(format!("there is no scheduler named {}", grant.scheduler)));
    }
    require_administer(&state, &session, &grant.scheduler).await?;
    let person_id: Option<i64> = match (&grant.to_person, &grant.to_group) {
        (Some(email), None) => Some(
            sqlx::query_scalar("SELECT person_id FROM console.person WHERE lower(email) = lower($1)")
                .bind(email)
                .fetch_optional(&state.pool)
                .await?
                .ok_or_else(|| Failure::not_found(format!("{email} has not signed in yet; a person can be granted access after their first sign-in, or grant their group")))?,
        ),
        (None, Some(_)) => None,
        _ => return Err(Failure::bad_request("give exactly one of to_person and to_group")),
    };
    let grant_id: i64 = sqlx::query_scalar(
        "INSERT INTO console.grant (to_person, to_group, scheduler, scope, pipelines, created_by) VALUES ($1, $2, $3, $4, $5, $6) RETURNING grant_id",
    )
    .bind(person_id)
    .bind(&grant.to_group)
    .bind(&grant.scheduler)
    .bind(grant.scope.to_string())
    .bind(&grant.pipelines)
    .bind(&session.person.email)
    .fetch_one(&state.pool)
    .await?;
    store::audit(&state.pool, &session.person.email, "grant", json!({ "grant_id": grant_id, "to_person": grant.to_person, "to_group": grant.to_group, "scheduler": grant.scheduler, "scope": grant.scope, "pipelines": grant.pipelines })).await?;
    let added = store::grant(&state.pool, grant_id).await?.ok_or_else(|| Failure::internal("the grant vanished"))?;
    Ok(Json(added))
}

/// `DELETE /api/grants/{id}`.
pub(crate) async fn remove_grant(
    Extract(state): Extract<State>,
    Extension(session): Extension<Arc<Session>>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, Failure> {
    let scheduler: String = sqlx::query_scalar("SELECT scheduler FROM console.grant WHERE grant_id = $1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| Failure::not_found(format!("there is no grant {id}")))?;
    require_administer(&state, &session, &scheduler).await?;
    sqlx::query("DELETE FROM console.grant WHERE grant_id = $1").bind(id).execute(&state.pool).await?;
    store::audit(&state.pool, &session.person.email, "revoke", json!({ "grant_id": id, "scheduler": scheduler }))
        .await?;
    Ok(Json(json!({ "message": "The grant is removed." })))
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub(crate) struct PersonRow {
    email: String,
    name: String,
    groups: Vec<String>,
    last_seen_at: chrono::DateTime<chrono::Utc>,
}

/// `GET /api/people`.
pub(crate) async fn people(
    Extract(state): Extract<State>,
    Extension(session): Extension<Arc<Session>>,
) -> Result<Json<Vec<PersonRow>>, Failure> {
    administers_something(&state, &session).await?;
    let people =
        sqlx::query_as("SELECT email, name, groups, last_seen_at FROM console.person ORDER BY lower(email) LIMIT 1000")
            .fetch_all(&state.pool)
            .await?;
    Ok(Json(people))
}

#[derive(Debug, Deserialize)]
pub(crate) struct AuditQuery {
    before: Option<i64>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub(crate) struct AuditRow {
    audit_id: i64,
    at: chrono::DateTime<chrono::Utc>,
    actor: String,
    action: String,
    detail: Value,
}

/// `GET /api/audit`: sign-ins, sign-outs, grant changes and refused actions.
pub(crate) async fn audit(
    Extract(state): Extract<State>,
    Extension(session): Extension<Arc<Session>>,
    Query(query): Query<AuditQuery>,
) -> Result<Json<Value>, Failure> {
    administers_something(&state, &session).await?;
    let mut rows: Vec<AuditRow> = sqlx::query_as(
        "SELECT audit_id, at, actor, action, detail FROM console.audit WHERE ($1::bigint IS NULL OR audit_id < $1) ORDER BY audit_id DESC LIMIT 51",
    )
    .bind(query.before)
    .fetch_all(&state.pool)
    .await?;
    let more = rows.len() > 50;
    rows.truncate(50);
    let next = more.then(|| rows.last().map(|row| row.audit_id.to_string())).flatten();
    Ok(Json(json!({ "items": rows, "next_cursor": next })))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Preferences {
    #[serde(default)]
    time_zone: Option<String>,
    #[serde(default)]
    filters: Value,
}

/// Preferences as stored. Lenient, unlike the request type, so after a rollback the fields this
/// console knows survive a row written by a newer one instead of resetting to defaults.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct StoredPreferences {
    #[serde(default)]
    time_zone: Option<String>,
    #[serde(default)]
    filters: Value,
}

/// `GET /api/preferences`.
pub(crate) async fn preferences(
    Extract(state): Extract<State>,
    Extension(session): Extension<Arc<Session>>,
) -> Result<Json<StoredPreferences>, Failure> {
    let data: Option<Value> = sqlx::query_scalar("SELECT data FROM console.preference WHERE person_id = $1")
        .bind(session.person.person_id)
        .fetch_optional(&state.pool)
        .await?;
    let stored = data.and_then(|data| {
        serde_json::from_value(data)
            .inspect_err(|error| {
                tracing::warn!(person = %session.person.email, %error, "stored preferences could not be read; showing defaults");
            })
            .ok()
    });
    Ok(Json(stored.unwrap_or_default()))
}

/// `PUT /api/preferences`.
pub(crate) async fn save_preferences(
    Extract(state): Extract<State>,
    Extension(session): Extension<Arc<Session>>,
    Json(preferences): Json<Preferences>,
) -> Result<Json<Preferences>, Failure> {
    if let Some(zone) = &preferences.time_zone
        && zone.len() > 64
    {
        return Err(Failure::bad_request("that is not a time zone name"));
    }
    let data = serde_json::to_value(&preferences).map_err(|error| Failure::internal(error.to_string()))?;
    if data.to_string().len() > 16 * 1024 {
        return Err(Failure::bad_request("preferences are limited to 16 KiB"));
    }
    sqlx::query("INSERT INTO console.preference (person_id, data) VALUES ($1, $2) ON CONFLICT (person_id) DO UPDATE SET data = EXCLUDED.data")
        .bind(session.person.person_id)
        .bind(&data)
        .execute(&state.pool)
        .await?;
    Ok(Json(preferences))
}
