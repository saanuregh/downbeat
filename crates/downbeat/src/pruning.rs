//! Pruning: history is removed a chain at a time, by the retention of the chain's first job, so a
//! chain is either whole or gone.

use std::time::Duration;

use sqlx::PgPool;

use crate::registry::Registry;
use crate::store::interval;

/// Chains removed per statement, so no statement holds its locks for long.
const BATCH: i64 = 500;
/// How long one pass may keep deleting. Measured at about two thousand chains a second, a pass
/// keeps up with far more than the tens of thousands of runs a day the engine is sized for, and
/// whatever is left waits for the next pass.
const PASS_BUDGET: Duration = Duration::from_secs(300);

pub(crate) async fn prune(pool: &PgPool, registry: &Registry) -> sqlx::Result<()> {
    let started = std::time::Instant::now();
    for (name, job) in &registry.jobs {
        let policy = &job.spec.policy;
        while prune_older_than(pool, name, policy.keep_runs_for).await? == BATCH && started.elapsed() < PASS_BUDGET {}
        if let Some(count) = policy.keep_runs_at_most {
            while prune_beyond_count(pool, name, i64::from(count)).await? == BATCH && started.elapsed() < PASS_BUDGET {}
        }
    }
    sqlx::query(
        "DELETE FROM downbeat.run_history WHERE joined_run IS NOT NULL AND finished_at < now() - interval '2 days'",
    )
    .execute(pool)
    .await?;
    sqlx::query("DELETE FROM downbeat.idempotency WHERE created_at < now() - interval '7 days'").execute(pool).await?;
    sqlx::query("DELETE FROM downbeat.schedule_override WHERE on_date < (now() - interval '30 days')::date")
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM downbeat.audit WHERE at < now() - interval '1 year' AND notify_after IS NULL")
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM downbeat.pause WHERE until < now()").execute(pool).await?;
    sqlx::query("DELETE FROM downbeat.worker WHERE last_seen_at < now() - interval '1 day'").execute(pool).await?;
    Ok(())
}

/// The chains a job started that may go: nothing in them is unfinished, nothing in them is waiting
/// for a callback, and none holds a job's latest success. The chain's lock is taken (without
/// waiting) as the end routine does, so a chain is never removed while a run in it is ending; each
/// statement is its own transaction, so the lock lasts until the chain is gone.
const PRUNABLE: &str = "
    NOT EXISTS (SELECT 1 FROM downbeat.run r WHERE r.chain_id = c.chain_id)
    AND NOT EXISTS (SELECT 1 FROM downbeat.run_history h WHERE h.chain_id = c.chain_id AND h.notify_after IS NOT NULL)
    AND NOT EXISTS (
        SELECT 1 FROM downbeat.run_history h
         WHERE h.chain_id = c.chain_id AND h.status = 'succeeded'
           AND h.finished_at = (SELECT max(l.finished_at) FROM downbeat.run_history l
                                 WHERE l.job_name = h.job_name AND l.status = 'succeeded'))
    AND pg_try_advisory_xact_lock(hashtextextended(c.chain_id::text, 0))";

/// Removes one batch of a job's chains older than its retention; returns how many went.
async fn prune_older_than(pool: &PgPool, job: &str, keep_for: Duration) -> sqlx::Result<i64> {
    let sql = format!(
        "DELETE FROM downbeat.chain WHERE chain_id IN (
            SELECT c.chain_id FROM downbeat.chain c
             WHERE c.first_job = $1 AND c.started_at < now() - $2 AND {PRUNABLE}
             LIMIT $3)"
    );
    let done = sqlx::query(&sql).bind(job).bind(interval(keep_for)).bind(BATCH).execute(pool).await?;
    Ok(i64::try_from(done.rows_affected()).unwrap_or(i64::MAX))
}

/// Removes one batch of a job's chains beyond the newest `keep` whose first run did not fail;
/// returns how many went. Chains are counted once each, by their first run.
async fn prune_beyond_count(pool: &PgPool, job: &str, keep: i64) -> sqlx::Result<i64> {
    let sql = format!(
        "DELETE FROM downbeat.chain WHERE chain_id IN (
            SELECT c.chain_id FROM (
                SELECT chain_id FROM (
                    SELECT DISTINCT ON (h.chain_id) h.chain_id, h.status, h.finished_at
                      FROM downbeat.run_history h JOIN downbeat.chain c ON c.chain_id = h.chain_id
                     WHERE c.first_job = $1 AND h.job_name = $1 AND h.joined_run IS NULL
                     ORDER BY h.chain_id, h.finished_at) first_runs
                 WHERE status <> 'failed'
                 ORDER BY finished_at DESC
                OFFSET $2) beyond
              JOIN downbeat.chain c ON c.chain_id = beyond.chain_id
             WHERE {PRUNABLE}
             LIMIT $3)"
    );
    let done = sqlx::query(&sql).bind(job).bind(keep).bind(BATCH).execute(pool).await?;
    Ok(i64::try_from(done.rows_affected()).unwrap_or(i64::MAX))
}
