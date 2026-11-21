//! Watching for runs that are slow and jobs that are overdue, and telling `on_event` once.

use sqlx::PgPool;
use uuid::Uuid;

use crate::events::Event;
use crate::registry::Registry;
use crate::store::{audit, history, runs};

pub(crate) async fn check(pool: &PgPool, registry: &Registry) -> sqlx::Result<()> {
    slow_runs(pool).await?;
    overdue_jobs(pool, registry).await
}

async fn slow_runs(pool: &PgPool) -> sqlx::Result<()> {
    // How long each has run is measured by the database's clock, like every time rule.
    let running: Vec<(Uuid, String, f64)> = sqlx::query_as(
        "SELECT run_id, job_name, EXTRACT(EPOCH FROM now() - function_started_at)::float8 FROM downbeat.run
          WHERE state = 'running' AND function_started_at IS NOT NULL AND NOT slow_reported",
    )
    .fetch_all(pool)
    .await?;
    for (run_id, job, seconds) in running {
        let Some((expected, slow_after)) = history::expected_duration(pool, &job).await? else { continue };
        let running_for = std::time::Duration::from_secs_f64(seconds.max(0.0));
        if running_for > slow_after && runs::mark_slow(pool, run_id).await? {
            let event = Event::RunSlow {
                run_id,
                job: job.clone(),
                running_seconds: i64::try_from(running_for.as_secs()).unwrap_or(i64::MAX),
                expected_seconds: i64::try_from(expected.as_secs()).unwrap_or(i64::MAX),
            };
            let mut connection = pool.acquire().await?;
            audit::event(&mut connection, &event, Some(&job), Some(run_id), None).await?;
        }
    }
    Ok(())
}

async fn overdue_jobs(pool: &PgPool, registry: &Registry) -> sqlx::Result<()> {
    for (name, job) in &registry.jobs {
        let Some(every) = job.spec.policy.expect_success_every else { continue };
        let Some(wait) = history::wait_for_success(pool, name, every).await? else { continue };
        if wait.overdue {
            report_overdue(pool, name, job.spec.policy.queue, wait).await?;
        }
    }
    Ok(())
}

/// Raises `JobOverdue` once per lapse. The check and the event share a transaction under a lock per
/// job, so two schedulers watching at once raise it once between them.
async fn report_overdue(pool: &PgPool, job: &str, queue: &str, wait: history::Wait) -> sqlx::Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('job_overdue:' || $1, 0))")
        .bind(job)
        .execute(&mut *transaction)
        .await?;
    // Paused as a run would be (rule 2.5): by its job, its queue or everything.
    let quiet: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM downbeat.pause
                         WHERE ((kind = 'job' AND name = $1) OR (kind = 'queue' AND name = $2) OR kind = 'all')
                           AND (until IS NULL OR until > now()))
             OR EXISTS (SELECT 1 FROM downbeat.audit WHERE action = 'event' AND job_name = $1
                         AND detail->>'event' = 'job_overdue' AND at > $3)",
    )
    .bind(job)
    .bind(queue)
    .bind(wait.since)
    .fetch_one(&mut *transaction)
    .await?;
    if !quiet {
        let event = Event::JobOverdue { job: job.to_owned(), last_success: wait.last_success };
        audit::event(&mut transaction, &event, Some(job), None, None).await?;
    }
    transaction.commit().await
}
