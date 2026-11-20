//! Delivering `on_finish` and `on_event`, from the database, at least once. A row is pushed five
//! minutes ahead before its callback runs, so a crash or a failed callback means it comes round
//! again; after a day of failures it is dropped with a log line. No transaction is held open while
//! the app's code runs.

use sqlx::PgPool;

use crate::events::{Event, EventDelivery, FinishedRun};
use crate::registry::Registry;
use crate::store::ending::HistoryRow;

const BATCH: i64 = 20;

pub(crate) async fn deliver(pool: &PgPool, registry: &Registry) -> sqlx::Result<()> {
    deliver_finished(pool, registry).await?;
    deliver_events(pool, registry).await
}

async fn deliver_finished(pool: &PgPool, registry: &Registry) -> sqlx::Result<()> {
    let Some(on_finish) = &registry.on_finish else {
        sqlx::query("UPDATE downbeat.run_history SET notify_after = NULL WHERE notify_after IS NOT NULL")
            .execute(pool)
            .await?;
        return Ok(());
    };
    // A row is stale a day after the run last changed (its end, or its marking as succeeded), by
    // the database's clock, which every process shares.
    let rows: Vec<ClaimedRun> = sqlx::query_as(
        "UPDATE downbeat.run_history SET notify_after = now() + interval '5 minutes'
          WHERE run_id IN (SELECT run_id FROM downbeat.run_history WHERE notify_after <= now()
                            ORDER BY notify_after LIMIT $1 FOR UPDATE SKIP LOCKED)
        RETURNING *, greatest(finished_at, marked_at) < now() - interval '1 day' AS stale",
    )
    .bind(BATCH)
    .fetch_all(pool)
    .await?;
    for ClaimedRun { row, stale } in rows {
        let run_id = row.run_id;
        let delivered = if stale {
            tracing::error!(%run_id, "on_finish failed for a day; this run's notification is dropped");
            true
        } else {
            match crate::guard::guarded_async("on_finish", on_finish(finished_run(registry, row))).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!(%run_id, %error, "on_finish failed; it will be tried again in five minutes");
                    false
                }
            }
        };
        if delivered {
            sqlx::query("UPDATE downbeat.run_history SET notify_after = NULL WHERE run_id = $1")
                .bind(run_id)
                .execute(pool)
                .await?;
        }
    }
    Ok(())
}

/// A history row claimed for delivery, and whether it is too old to deliver.
#[derive(sqlx::FromRow)]
struct ClaimedRun {
    #[sqlx(flatten)]
    row: HistoryRow,
    stale: bool,
}

pub(crate) fn finished_run(registry: &Registry, row: HistoryRow) -> FinishedRun {
    FinishedRun {
        log_link: registry.log_link_for(row.run_id),
        run_id: row.run_id,
        job: row.job_name,
        chain_id: row.chain_id,
        status: row.status,
        skipped_because: row.skipped_because,
        failed_because: row.failed_because,
        reason: row.reason,
        started_by: row.started_by,
        actor: row.actor,
        failures: i32::from(row.failures),
        output: row.output,
        finished_at: row.finished_at,
    }
}

#[derive(sqlx::FromRow)]
struct EventRow {
    audit_id: i64,
    at: chrono::DateTime<chrono::Utc>,
    detail: serde_json::Value,
    /// Recorded more than a day ago, by the database's clock.
    stale: bool,
}

async fn deliver_events(pool: &PgPool, registry: &Registry) -> sqlx::Result<()> {
    let Some(on_event) = &registry.on_event else {
        sqlx::query("UPDATE downbeat.audit SET notify_after = NULL WHERE notify_after IS NOT NULL")
            .execute(pool)
            .await?;
        return Ok(());
    };
    let rows: Vec<EventRow> = sqlx::query_as(
        "UPDATE downbeat.audit SET notify_after = now() + interval '5 minutes'
          WHERE audit_id IN (SELECT audit_id FROM downbeat.audit WHERE notify_after <= now()
                              ORDER BY notify_after LIMIT $1 FOR UPDATE SKIP LOCKED)
        RETURNING audit_id, at, detail, at < now() - interval '1 day' AS stale",
    )
    .bind(BATCH)
    .fetch_all(pool)
    .await?;
    for row in rows {
        let delivered = match serde_json::from_value::<Event>(row.detail) {
            Err(error) => {
                tracing::warn!(audit_id = row.audit_id, %error, "an event this build does not know is skipped");
                true
            }
            Ok(_) if row.stale => {
                tracing::error!(audit_id = row.audit_id, "on_event failed for a day; this event is dropped");
                true
            }
            Ok(event) => match crate::guard::guarded_async(
                "on_event",
                on_event(EventDelivery { id: row.audit_id, at: row.at, event }),
            )
            .await
            {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!(audit_id = row.audit_id, %error, "on_event failed; it will be tried again in five minutes");
                    false
                }
            },
        };
        if delivered {
            sqlx::query("UPDATE downbeat.audit SET notify_after = NULL WHERE audit_id = $1")
                .bind(row.audit_id)
                .execute(pool)
                .await?;
        }
    }
    Ok(())
}
