//! The audit trail, and the engine's events, which are delivered from it.

use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::events::Event;

/// Records what someone did.
pub(crate) async fn record(
    connection: &mut PgConnection,
    actor: &str,
    action: &str,
    job: Option<&str>,
    run_id: Option<Uuid>,
    schedule: Option<&str>,
    detail: Value,
) -> sqlx::Result<()> {
    sqlx::query("INSERT INTO downbeat.audit (actor, action, job_name, run_id, schedule, detail) VALUES ($1, $2, $3, $4, $5, $6)")
        .bind(actor)
        .bind(action)
        .bind(job)
        .bind(run_id)
        .bind(schedule)
        .bind(detail)
        .execute(connection)
        .await?;
    Ok(())
}

/// Records an event of the engine's own, to be delivered to `on_event`.
pub(crate) async fn event(
    connection: &mut PgConnection,
    event: &Event,
    job: Option<&str>,
    run_id: Option<Uuid>,
    schedule: Option<&str>,
) -> sqlx::Result<()> {
    let detail = serde_json::to_value(event).unwrap_or(Value::Null);
    sqlx::query(
        "INSERT INTO downbeat.audit (actor, action, job_name, run_id, schedule, detail, notify_after)
         VALUES ('engine', 'event', $1, $2, $3, $4, now())",
    )
    .bind(job)
    .bind(run_id)
    .bind(schedule)
    .bind(detail)
    .execute(connection)
    .await?;
    Ok(())
}
