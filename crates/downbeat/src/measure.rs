//! Entry points for measuring the engine's hottest statements against a real database. Used by
//! the measurement in `tests/e2e/tests/measure.rs` and by tests of single engine steps; not part of
//! the public API.

use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::definition::Definition;
use crate::registry::Registry;
use crate::store::runs::{self, Held};

/// Claims one run from a queue, as a worker slot does; returns its id and fence.
pub async fn claim(pool: &PgPool, queue: &str, jobs: &[String]) -> sqlx::Result<Option<(Uuid, i64)>> {
    Ok(runs::claim(pool, queue, "measure", Duration::from_secs(180), jobs).await?.map(|run| (run.run_id, run.fence)))
}

/// One heartbeat of a claimed run, as a worker's lease keeper sends it.
pub async fn heartbeat(pool: &PgPool, run_id: Uuid, fence: i64) -> sqlx::Result<bool> {
    let held = Held { run_id, fence, worker: "measure" };
    Ok(runs::heartbeat(pool, held, Duration::from_secs(180), Some("measuring"), None).await?.is_some())
}

/// One pass of pruning, as the hourly task runs it.
pub async fn prune(pool: &PgPool, definition: Definition) -> sqlx::Result<()> {
    let registry = registry(definition, None)?;
    crate::pruning::prune(pool, &registry).await
}

/// One pass of callback delivery, as the scheduler runs it every `poll`.
pub async fn deliver_callbacks(pool: &PgPool, definition: Definition) -> sqlx::Result<()> {
    let registry = registry(definition, None)?;
    crate::scheduler::callbacks::deliver(pool, &registry).await
}

/// Prunes one batch of a job's old chains with `between` awaited between pruning's two steps (taking
/// the locks, then removing what may still go), so a test can change the data in that window.
pub async fn prune_old_chains_with_a_pause<F: Future<Output = ()>>(
    pool: &PgPool,
    definition: Definition,
    job: &str,
    between: impl FnOnce() -> F,
) -> sqlx::Result<i64> {
    let registry = registry(definition, None)?;
    let keep_for = registry.job(job).map_or(Duration::from_secs(0), |found| found.spec.policy.keep_runs_for);
    let mut transaction = pool.begin().await?;
    let locked = crate::pruning::lock_older_than(&mut transaction, job, keep_for).await?;
    between().await;
    crate::pruning::remove_locked(transaction, &locked).await
}

/// Registers the schedules of this environment, as each scheduler does at start and every minute.
pub async fn register_schedules(pool: &PgPool, definition: Definition, environment: &str) -> sqlx::Result<()> {
    let registry = registry(definition, Some(environment))?;
    crate::scheduler::fire::register(pool, &registry).await
}

/// One pass of firing the due schedules of this environment, as the scheduler runs it every `poll`.
pub async fn fire_due_schedules(pool: &PgPool, definition: Definition, environment: &str) -> sqlx::Result<()> {
    let registry = registry(definition, Some(environment))?;
    crate::scheduler::fire::fire_due(pool, &registry).await
}

/// The registry a role would build from this definition, in this environment, or the definition's
/// problems as an error.
fn registry(definition: Definition, environment: Option<&str>) -> sqlx::Result<Registry> {
    Registry::build(definition, environment).map_err(|problems| sqlx::Error::Protocol(format!("{problems:?}")))
}

/// One pass of the watch for slow runs and overdue jobs, as the scheduler runs it every 30 s.
pub async fn watch_once(pool: &PgPool, definition: Definition) -> sqlx::Result<()> {
    crate::scheduler::watch::check(pool, &registry(definition, None)?).await
}
