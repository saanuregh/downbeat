//! Pruning: history is removed a chain at a time, by the retention of the chain's first job, so a
//! chain is either whole or gone.

use std::time::Duration;

use sqlx::PgPool;

use crate::registry::Registry;
use crate::store::interval;

/// Chains removed per statement.
const BATCH: i64 = 500;

pub(crate) async fn prune(pool: &PgPool, registry: &Registry) -> sqlx::Result<()> {
    for (name, job) in &registry.jobs {
        let policy = &job.spec.policy;
        prune_older_than(pool, name, policy.keep_runs_for).await?;
        if let Some(count) = policy.keep_runs_at_most {
            prune_beyond_count(pool, name, i64::from(count)).await?;
        }
    }
    sqlx::query("DELETE FROM downbeat.run_history WHERE joined_run IS NOT NULL AND finished_at < now() - interval '2 days'").execute(pool).await?;
    sqlx::query("DELETE FROM downbeat.idempotency WHERE created_at < now() - interval '7 days'").execute(pool).await?;
    sqlx::query("DELETE FROM downbeat.schedule_override WHERE on_date < (now() - interval '30 days')::date").execute(pool).await?;
    sqlx::query("DELETE FROM downbeat.audit WHERE at < now() - interval '1 year' AND notify_after IS NULL").execute(pool).await?;
    sqlx::query("DELETE FROM downbeat.pause WHERE until < now()").execute(pool).await?;
    sqlx::query("DELETE FROM downbeat.worker WHERE last_seen_at < now() - interval '1 day'").execute(pool).await?;
    Ok(())
}

/// The chains a job started that may go: nothing in them is unfinished, nothing in them is waiting
/// for a callback, and none holds a job's latest success.
const PRUNABLE: &str = "
    NOT EXISTS (SELECT 1 FROM downbeat.run r WHERE r.chain_id = c.chain_id)
    AND NOT EXISTS (SELECT 1 FROM downbeat.run_history h WHERE h.chain_id = c.chain_id AND h.notify_after IS NOT NULL)
    AND NOT EXISTS (
        SELECT 1 FROM downbeat.run_history h
         WHERE h.chain_id = c.chain_id AND h.status = 'succeeded'
           AND h.finished_at = (SELECT max(l.finished_at) FROM downbeat.run_history l
                                 WHERE l.job_name = h.job_name AND l.status = 'succeeded'))";

async fn prune_older_than(pool: &PgPool, job: &str, keep_for: Duration) -> sqlx::Result<()> {
    let sql = format!(
        "DELETE FROM downbeat.chain WHERE chain_id IN (
            SELECT c.chain_id FROM downbeat.chain c
             WHERE c.first_job = $1 AND c.started_at < now() - $2 AND {PRUNABLE}
             LIMIT $3)"
    );
    sqlx::query(&sql).bind(job).bind(interval(keep_for)).bind(BATCH).execute(pool).await?;
    Ok(())
}

async fn prune_beyond_count(pool: &PgPool, job: &str, keep: i64) -> sqlx::Result<()> {
    let sql = format!(
        "DELETE FROM downbeat.chain WHERE chain_id IN (
            SELECT c.chain_id FROM downbeat.chain c
              JOIN downbeat.run_history h ON h.chain_id = c.chain_id AND h.job_name = c.first_job AND h.joined_run IS NULL
             WHERE c.first_job = $1 AND h.status <> 'failed' AND {PRUNABLE}
             ORDER BY h.finished_at DESC
             OFFSET $2 LIMIT $3)"
    );
    sqlx::query(&sql).bind(job).bind(keep).bind(BATCH).execute(pool).await?;
    Ok(())
}
