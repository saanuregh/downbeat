//! The scheduler role: fires schedules, drives backfills, delivers callbacks, watches for slow and
//! overdue jobs, and serves the control API.

pub(crate) mod backfill;
pub(crate) mod callbacks;
pub(crate) mod fire;
pub(crate) mod plan;
mod watch;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::registry::Registry;

/// How often schedules are registered again, which keeps them from being treated as gone.
const REGISTER_EVERY: Duration = Duration::from_secs(60);

pub(crate) async fn serve(pool: PgPool, registry: Arc<Registry>, stop: CancellationToken) -> anyhow::Result<()> {
    fire::register(&pool, &registry).await?;
    let id = format!("scheduler-{}", crate::worker::worker_id());
    tracing::info!(schedules = registry.schedules.len(), environment = ?registry.environment, "scheduler started");

    let tick = registry.settings.poll;
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(every(REGISTER_EVERY, stop.clone(), {
        let (pool, registry) = (pool.clone(), Arc::clone(&registry));
        move || {
            let (pool, registry) = (pool.clone(), Arc::clone(&registry));
            async move { fire::register(&pool, &registry).await.map_err(anyhow::Error::from) }
        }
    }));
    tasks.spawn(every(tick, stop.clone(), {
        let (pool, registry) = (pool.clone(), Arc::clone(&registry));
        move || {
            let (pool, registry) = (pool.clone(), Arc::clone(&registry));
            async move { fire::fire_due(&pool, &registry).await.map_err(anyhow::Error::from) }
        }
    }));
    tasks.spawn(every(tick, stop.clone(), {
        let (pool, registry) = (pool.clone(), Arc::clone(&registry));
        move || {
            let (pool, registry) = (pool.clone(), Arc::clone(&registry));
            async move { callbacks::deliver(&pool, &registry).await.map_err(anyhow::Error::from) }
        }
    }));
    tasks.spawn(every(tick * 2, stop.clone(), {
        let (pool, registry) = (pool.clone(), Arc::clone(&registry));
        move || {
            let (pool, registry) = (pool.clone(), Arc::clone(&registry));
            async move { backfill::drive(&pool, &registry).await.map_err(anyhow::Error::from) }
        }
    }));
    tasks.spawn(every(Duration::from_secs(30), stop.clone(), {
        let (pool, registry) = (pool.clone(), Arc::clone(&registry));
        move || {
            let (pool, registry) = (pool.clone(), Arc::clone(&registry));
            async move { watch::check(&pool, &registry).await.map_err(anyhow::Error::from) }
        }
    }));
    tasks.spawn(every(registry.settings.heartbeat, stop.clone(), {
        let pool = pool.clone();
        move || {
            let (pool, id) = (pool.clone(), id.clone());
            async move {
                sqlx::query(
                    "INSERT INTO downbeat.worker (worker_id, role, build) VALUES ($1, 'scheduler', $1)
                     ON CONFLICT (worker_id) DO UPDATE SET last_seen_at = now()",
                )
                .bind(&id)
                .execute(&pool)
                .await
                .map(|_| ())
                .map_err(anyhow::Error::from)
            }
        }
    }));
    while tasks.join_next().await.is_some() {}
    Ok(())
}

/// Runs a task on an interval until stopped. A failure is logged and the task runs again next time.
async fn every<F, Fut>(period: Duration, stop: CancellationToken, mut task: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = tick.tick() => {
                // A scheduler task never dies: a panic anywhere in it is logged and the task runs again.
                let ran = crate::guard::guarded_async("a scheduler task", async { task().await.map_err(|error| format!("{error:#}")) }).await;
                if let Err(error) = ran {
                    tracing::warn!(%error, "a scheduler task failed; it will run again");
                }
            }
        }
    }
}
