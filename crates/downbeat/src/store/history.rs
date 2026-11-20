//! Questions about finished runs that conditions and expectations ask.

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::ending::HistoryRow;
use super::interval;

/// The job's latest success: its input hash and when it finished. With a turn key, only runs that
/// held that turn count (for `one_per`, "the same value").
pub(crate) async fn last_success(
    pool: &PgPool,
    job: &str,
    turn_key: Option<&str>,
) -> sqlx::Result<Option<(Option<String>, DateTime<Utc>)>> {
    sqlx::query_as(
        "SELECT input_hash, finished_at FROM downbeat.run_history
          WHERE job_name = $1 AND status = 'succeeded' AND ($2::text IS NULL OR turn_key = $2)
          ORDER BY finished_at DESC LIMIT 1",
    )
    .bind(job)
    .bind(turn_key)
    .fetch_optional(pool)
    .await
}

/// Whether the job has succeeded for exactly this input.
pub(crate) async fn succeeded_for(pool: &PgPool, job: &str, input_hash: &str) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE job_name = $1 AND status = 'succeeded' AND input_hash = $2)",
    )
    .bind(job)
    .bind(input_hash)
    .fetch_one(pool)
    .await
}

/// Whether the job has succeeded within this long.
pub(crate) async fn succeeded_within(pool: &PgPool, job: &str, within: Duration) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run_history
                         WHERE job_name = $1 AND status = 'succeeded' AND finished_at > now() - $2)",
    )
    .bind(job)
    .bind(interval(within))
    .fetch_one(pool)
    .await
}

/// Finished runs by id, in the order asked for.
pub(crate) async fn by_ids(pool: &PgPool, ids: &[Uuid]) -> sqlx::Result<Vec<HistoryRow>> {
    let rows: Vec<HistoryRow> =
        sqlx::query_as("SELECT * FROM downbeat.run_history WHERE run_id = ANY($1)").bind(ids).fetch_all(pool).await?;
    Ok(ids.iter().filter_map(|id| rows.iter().find(|row| row.run_id == *id).cloned()).collect())
}

/// A job's expected duration (the median of its last twenty successes) and the "slow" threshold
/// (one and a half times their 90th percentile). Needs at least five successes.
pub(crate) async fn expected_duration(pool: &PgPool, job: &str) -> sqlx::Result<Option<(Duration, Duration)>> {
    let row: Option<(i64, Option<f64>, Option<f64>)> = sqlx::query_as(
        "SELECT count(*), percentile_cont(0.5) WITHIN GROUP (ORDER BY seconds), percentile_cont(0.9) WITHIN GROUP (ORDER BY seconds)
           FROM (SELECT EXTRACT(EPOCH FROM finished_at - started_at)::float8 AS seconds
                   FROM downbeat.run_history
                  WHERE job_name = $1 AND status = 'succeeded' AND started_at IS NOT NULL
                  ORDER BY finished_at DESC LIMIT 20) recent",
    )
    .bind(job)
    .fetch_optional(pool)
    .await?;
    Ok(match row {
        Some((count, Some(median), Some(p90))) if count >= 5 => {
            Some((Duration::from_secs_f64(median.max(0.0)), Duration::from_secs_f64((p90 * 1.5).max(1.0))))
        }
        _ => None,
    })
}

/// Since when a job has been waiting for a success: its latest success, or, if it has never
/// succeeded, its first run. `None` for a job that has never run. The one rule `expect_success_every`
/// is measured by, for the API and for `on_event` alike.
pub(crate) async fn waiting_for_success_since(pool: &PgPool, job: &str) -> sqlx::Result<Option<DateTime<Utc>>> {
    if let Some((_, at)) = last_success(pool, job, None).await? {
        return Ok(Some(at));
    }
    sqlx::query_scalar("SELECT min(created_at) FROM downbeat.run_history WHERE job_name = $1 AND joined_run IS NULL")
        .bind(job)
        .fetch_one(pool)
        .await
}

/// Whether a job has gone longer than `every` without a success.
pub(crate) async fn overdue(pool: &PgPool, job: &str, every: Duration) -> sqlx::Result<Option<DateTime<Utc>>> {
    let since = waiting_for_success_since(pool, job).await?;
    Ok(since.filter(|since| Utc::now() - *since > chrono::TimeDelta::from_std(every).unwrap_or(chrono::TimeDelta::MAX)))
}
