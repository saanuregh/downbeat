//! Entry points for measuring the engine's hottest statements against a real database. Used by
//! the measurement in `tests/e2e/tests/measure.rs`; not part of the public API.

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
    let registry =
        Registry::build(definition, None).map_err(|problems| sqlx::Error::Protocol(format!("{problems:?}")))?;
    crate::pruning::prune(pool, &registry).await
}
