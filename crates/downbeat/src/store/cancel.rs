//! Cancelling, in one place: every cancel reads the runs under the chain's lock, so it never acts
//! on a state that has already changed.

use sqlx::PgConnection;
use uuid::Uuid;

use super::ending::{Ending, Status, end_locked};
use super::runs::{RunRow, request_cancel};
use crate::registry::Registry;

/// What cancelling one run did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cancelled {
    /// It was waiting, and is now ended as cancelled.
    Ended,
    /// It is running; its worker stops it within a heartbeat.
    Asked,
    /// It had already finished.
    Gone,
}

/// Cancels one run. The caller holds the run's chain lock.
pub(crate) async fn cancel_run(
    connection: &mut PgConnection,
    registry: &Registry,
    run_id: Uuid,
    reason: &str,
    by: &str,
) -> sqlx::Result<Cancelled> {
    let row: Option<RunRow> = sqlx::query_as("SELECT * FROM downbeat.run WHERE run_id = $1 FOR UPDATE")
        .bind(run_id)
        .fetch_optional(&mut *connection)
        .await?;
    let Some(row) = row else { return Ok(Cancelled::Gone) };
    if row.state == "queued" {
        let ending = Ending::new(Status::Cancelled, reason);
        if end_locked(connection, registry, row.run_id, row.fence, &ending).await?.is_some() {
            return Ok(Cancelled::Ended);
        }
    }
    request_cancel(connection, run_id, by).await?;
    Ok(Cancelled::Asked)
}

/// Cancels a chain: marks it, so nothing more is created in it, and cancels each of its unfinished
/// runs. The caller holds the chain's lock. False when it was already cancelled.
pub(crate) async fn cancel_chain(
    connection: &mut PgConnection,
    registry: &Registry,
    chain_id: Uuid,
    reason: &str,
    by: &str,
) -> sqlx::Result<bool> {
    let marked = sqlx::query("UPDATE downbeat.chain SET cancelled_at = now(), cancelled_by = $2 WHERE chain_id = $1 AND cancelled_at IS NULL")
        .bind(chain_id)
        .bind(by)
        .execute(&mut *connection)
        .await?;
    if marked.rows_affected() == 0 {
        return Ok(false);
    }
    let unfinished: Vec<Uuid> = sqlx::query_scalar("SELECT run_id FROM downbeat.run WHERE chain_id = $1")
        .bind(chain_id)
        .fetch_all(&mut *connection)
        .await?;
    for run_id in unfinished {
        cancel_run(connection, registry, run_id, reason, by).await?;
    }
    Ok(true)
}

/// Every job with a run in a chain, finished or not.
pub(crate) async fn jobs_in_chain(connection: &mut PgConnection, chain_id: Uuid) -> sqlx::Result<Vec<String>> {
    sqlx::query_scalar(
        "SELECT job_name FROM downbeat.run WHERE chain_id = $1
         UNION SELECT job_name FROM downbeat.run_history WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(connection)
    .await
}
