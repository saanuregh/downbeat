//! Questions about finished runs that conditions and expectations ask.

use std::collections::HashMap;
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
/// (one and a half times their 90th percentile). Needs at least five successes. A success lasts
/// from its first start to its end, retries included; dry runs are left out.
pub(crate) async fn expected_duration(pool: &PgPool, job: &str) -> sqlx::Result<Option<(Duration, Duration)>> {
    Ok(expected_durations(pool, &[job.to_owned()]).await?.remove(job))
}

/// [`expected_duration`] for several jobs in one query; a job without one is left out.
pub(crate) async fn expected_durations(
    pool: &PgPool,
    jobs: &[String],
) -> sqlx::Result<HashMap<String, (Duration, Duration)>> {
    let rows: Vec<(String, i64, Option<f64>, Option<f64>)> = sqlx::query_as(
        "SELECT job.name, count(*), percentile_cont(0.5) WITHIN GROUP (ORDER BY seconds),
                percentile_cont(0.9) WITHIN GROUP (ORDER BY seconds)
           FROM (SELECT DISTINCT name FROM unnest($1::text[]) AS name) job
          CROSS JOIN LATERAL (SELECT EXTRACT(EPOCH FROM finished_at - started_at)::float8 AS seconds
                                FROM downbeat.run_history
                               WHERE job_name = job.name AND status = 'succeeded' AND NOT dry_run AND started_at IS NOT NULL
                               ORDER BY finished_at DESC LIMIT 20) recent
          GROUP BY job.name",
    )
    .bind(jobs)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(job, count, median, p90)| match (median, p90) {
            (Some(median), Some(p90)) if count >= 5 => {
                Some((job, (Duration::from_secs_f64(median.max(0.0)), Duration::from_secs_f64((p90 * 1.5).max(1.0)))))
            }
            _ => None,
        })
        .collect())
}

/// How long a job has gone without a success, for `expect_success_every`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct Wait {
    /// When the wait began: the latest success, or, if there is none, the first run.
    pub(crate) since: DateTime<Utc>,
    pub(crate) last_success: Option<DateTime<Utc>>,
    /// Whether the wait is longer than the job allows, by the database's clock.
    pub(crate) overdue: bool,
}

/// The one rule `expect_success_every` is measured by, for the API and for `on_event` alike.
/// `None` for a job that has never run.
///
/// A dry run does no work, so it is not a success here. A run a person marked as succeeded counts
/// from when it was marked. A run still waiting to start counts from when it may first start, so a
/// run started with `not_before` is not overdue before it was due, and one still queued or running
/// counts at all, so a job whose runs never finish is overdue too.
pub(crate) async fn wait_for_success(pool: &PgPool, job: &str, every: Duration) -> sqlx::Result<Option<Wait>> {
    sqlx::query_as(
        "WITH success AS (
              SELECT max(GREATEST(finished_at, marked_at)) AS at FROM downbeat.run_history
               WHERE job_name = $1 AND status = 'succeeded' AND NOT dry_run),
         waiting AS (
              SELECT COALESCE((SELECT at FROM success), (
                       SELECT min(began) FROM (
                         SELECT created_at AS began FROM downbeat.run_history WHERE job_name = $1 AND joined_run IS NULL
                         UNION ALL
                         SELECT CASE WHEN starts = 0 THEN run_at ELSE created_at END FROM downbeat.run WHERE job_name = $1
                       ) runs)) AS since)
         SELECT since, (SELECT at FROM success) AS last_success, since < now() - $2 AS overdue
           FROM waiting WHERE since IS NOT NULL",
    )
    .bind(job)
    .bind(interval(every))
    .fetch_optional(pool)
    .await
}
