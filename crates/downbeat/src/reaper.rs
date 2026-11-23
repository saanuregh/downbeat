//! The reaper and sweeps: recovery that runs in every engine process, safe in several at once.
//!
//! It takes back runs whose worker stopped renewing, ends runs that expired, gave up waiting or
//! whose job no running worker knows, and prunes the history on a clock of its own.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{Acquire, PgConnection, PgPool};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::events::Event;
use crate::reasons;
use crate::registry::Registry;
use crate::store::ending::{self, Ending, Status};
use crate::store::{audit, interval};

/// How often the sweeps run.
const SWEEP_EVERY: Duration = Duration::from_secs(5);
/// How often history is pruned.
const PRUNE_EVERY: Duration = Duration::from_secs(3600);
/// Rows handled per statement, so no sweep holds locks for long.
const BATCH: i64 = 100;

pub(crate) async fn serve(pool: PgPool, registry: Arc<Registry>, stop: CancellationToken) {
    let sweep = || async {
        for error in sweep_once(&pool, &registry).await {
            tracing::warn!(%error, "part of a sweep failed");
        }
    };
    let prune = || async {
        if let Err(error) = crate::pruning::prune(&pool, &registry).await {
            tracing::warn!(%error, "pruning failed");
        }
    };
    on_two_clocks(sweep, prune, stop).await;
}

/// Calls `sweep` every five seconds and `prune` every hour, each first at once, until stopped. The
/// two run side by side, so a long prune never holds up recovery.
async fn on_two_clocks<S, P>(sweep: impl FnMut() -> S, prune: impl FnMut() -> P, stop: CancellationToken)
where
    S: Future<Output = ()>,
    P: Future<Output = ()>,
{
    tokio::select! {
        () = stop.cancelled() => {}
        () = every(SWEEP_EVERY, sweep) => {}
        () = every(PRUNE_EVERY, prune) => {}
    }
}

/// Calls `work` every `period`, first at once. A call that overruns pushes the next one back
/// rather than making up for the ticks it missed.
async fn every<F: Future<Output = ()>>(period: Duration, mut work: impl FnMut() -> F) {
    let mut ticks = tokio::time::interval(period);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        work().await;
    }
}

/// Something one sweep could not do, with the error that stopped it. The sweep still did the rest.
#[derive(Debug, thiserror::Error)]
#[error("{what}: {source}")]
pub(crate) struct SweepError {
    what: String,
    #[source]
    source: sqlx::Error,
}

/// One pass of the sweeps: every step runs, and within a step a run that cannot be handled is
/// passed over, so one row that keeps failing holds up no other duty. Returns what failed.
pub(crate) async fn sweep_once(pool: &PgPool, registry: &Registry) -> Vec<SweepError> {
    let mut sweep = Sweep { pool, registry, failed: Vec::new() };
    let result = take_back_lost(&mut sweep).await;
    sweep.note("taking back runs of lost workers", result);
    let result = cancel_requested(&mut sweep).await;
    sweep.note("ending queued runs with a cancel requested", result);
    let result = expire(&mut sweep).await;
    sweep.note("expiring queued runs", result);
    let result = give_up(&mut sweep).await;
    sweep.note("giving up on queued runs", result);
    let result = unregistered(&mut sweep).await;
    sweep.note("ending queued runs of unregistered jobs", result);
    let result = sqlx::query("DELETE FROM downbeat.used_token WHERE expires_at < now()").execute(pool).await;
    sweep.note("deleting expired used tokens", result.map(drop));
    sweep.failed
}

/// What every step of a sweep needs, and what has failed so far.
struct Sweep<'a> {
    pool: &'a PgPool,
    registry: &'a Registry,
    failed: Vec<SweepError>,
}

impl Sweep<'_> {
    fn note(&mut self, what: &str, result: sqlx::Result<()>) {
        if let Err(source) = result {
            self.failed.push(SweepError { what: what.to_owned(), source });
        }
    }

    /// Ends one run; a failure is noted and the step goes on with its other runs.
    async fn end(&mut self, run_id: Uuid, chain_id: Uuid, fence: i64, ending: Ending) {
        if let Err(source) = ending::end(self.pool, self.registry, run_id, chain_id, fence, ending).await {
            self.failed.push(SweepError { what: format!("ending run {run_id}"), source });
        }
    }
}

#[derive(sqlx::FromRow)]
struct LostRow {
    run_id: Uuid,
    job_name: String,
    chain_id: Uuid,
    fence: i64,
    lease_owner: Option<String>,
    lost_count: i16,
    function_started_at: Option<DateTime<Utc>>,
    cancel_requested_by: Option<String>,
}

/// Runs whose lease expired longer ago than the margin: back to the queue, or failed when the
/// worker was lost while the function ran for the second time.
async fn take_back_lost(sweep: &mut Sweep<'_>) -> sqlx::Result<()> {
    let margin = interval(sweep.registry.settings.reaper_margin());
    let mut transaction = sweep.pool.begin().await?;
    let rows: Vec<LostRow> = sqlx::query_as(
        "SELECT run_id, job_name, chain_id, fence, lease_owner, lost_count, function_started_at, cancel_requested_by
           FROM downbeat.run
          WHERE state = 'running' AND lease_expires_at < now() - $1
          LIMIT $2 FOR UPDATE SKIP LOCKED",
    )
    .bind(margin)
    .bind(BATCH)
    .fetch_all(&mut *transaction)
    .await?;
    let mut to_end = Vec::new();
    for row in rows {
        let worker = row.lease_owner.clone().unwrap_or_else(|| "unknown".to_owned());
        // Someone asked for a cancel: the run is not requeued (spec 3.7), it ends as cancelled.
        if let Some(by) = &row.cancel_requested_by {
            let ending = Ending::new(Status::Cancelled, reasons::cancelled(by));
            to_end.push((row, ending));
            continue;
        }
        if row.function_started_at.is_some() && row.lost_count > 0 {
            let ending = Ending::failed("worker_lost", reasons::worker_lost_twice(&worker));
            to_end.push((row, ending));
            continue;
        }
        // A savepoint per run, so one that fails leaves the others to be taken back.
        let mut savepoint = transaction.begin().await?;
        match requeue(&mut savepoint, &row, worker).await {
            Ok(()) => savepoint.commit().await?,
            Err(source) => {
                savepoint.rollback().await?;
                sweep.failed.push(SweepError { what: format!("taking back run {}", row.run_id), source });
            }
        }
    }
    transaction.commit().await?;
    for (row, ending) in to_end {
        sweep.end(row.run_id, row.chain_id, row.fence, ending).await;
    }
    Ok(())
}

/// Puts a lost run back in the queue. One whose function had started counts the loss, logs the
/// call and records a `WorkerLost` event.
async fn requeue(connection: &mut PgConnection, row: &LostRow, worker: String) -> sqlx::Result<()> {
    let Some(started) = row.function_started_at else {
        sqlx::query(
            "UPDATE downbeat.run SET state = 'queued', fence = fence + 1, lease_owner = NULL, lease_expires_at = NULL,
                    queued_because = 'no_free_worker', queued_reason = $2
              WHERE run_id = $1",
        )
        .bind(row.run_id)
        .bind(format!("Re-queued: worker {worker} stopped responding before the function started."))
        .execute(&mut *connection)
        .await?;
        return Ok(());
    };
    let ran_for = (Utc::now() - started).to_std().unwrap_or_default();
    sqlx::query(
        "UPDATE downbeat.run SET state = 'queued', fence = fence + 1, lease_owner = NULL, lease_expires_at = NULL,
                lost_count = lost_count + 1, function_started_at = NULL, queued_because = 'no_free_worker',
                queued_reason = $2,
                call_log = downbeat.append_call(call_log, jsonb_build_object(
                    'worker', $3::text, 'build', '', 'started_at', $4::timestamptz, 'ended_at', now(), 'ended', 'worker lost'))
          WHERE run_id = $1",
    )
    .bind(row.run_id)
    .bind(reasons::worker_lost(&worker, ran_for))
    .bind(&worker)
    .bind(started)
    .execute(&mut *connection)
    .await?;
    let event = Event::WorkerLost { run_id: row.run_id, job: row.job_name.clone(), worker };
    audit::event(connection, &event, Some(&row.job_name), Some(row.run_id), None).await
}

#[derive(sqlx::FromRow)]
struct QueuedRow {
    run_id: Uuid,
    chain_id: Uuid,
    fence: i64,
    due_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    queued_reason: Option<String>,
    queued_because: Option<String>,
}

/// Queued runs that someone asked to cancel while they were held: they end as cancelled.
async fn cancel_requested(sweep: &mut Sweep<'_>) -> sqlx::Result<()> {
    let rows: Vec<(Uuid, Uuid, i64, String)> = sqlx::query_as(
        "SELECT run_id, chain_id, fence, cancel_requested_by FROM downbeat.run
          WHERE state = 'queued' AND cancel_requested_by IS NOT NULL LIMIT $1",
    )
    .bind(BATCH)
    .fetch_all(sweep.pool)
    .await?;
    for (run_id, chain_id, fence, by) in rows {
        sweep.end(run_id, chain_id, fence, Ending::new(Status::Cancelled, reasons::cancelled(&by))).await;
    }
    Ok(())
}

/// Queued runs never started whose occurrence has expired.
async fn expire(sweep: &mut Sweep<'_>) -> sqlx::Result<()> {
    let rows: Vec<QueuedRow> = sqlx::query_as(
        "SELECT run_id, chain_id, fence, due_at, expires_at, queued_reason, queued_because FROM downbeat.run
          WHERE state = 'queued' AND starts = 0 AND expires_at < now() LIMIT $1",
    )
    .bind(BATCH)
    .fetch_all(sweep.pool)
    .await?;
    for row in rows {
        let why = match row.queued_because.as_deref() {
            Some("paused") => "its job or queue was paused",
            Some("waiting_turn") => "it was waiting its turn",
            _ => "no worker started it in time",
        };
        let expires_at = row.expires_at.unwrap_or(row.due_at);
        let ending = Ending::new(Status::Expired, reasons::expired(row.due_at, expires_at, sweep.registry.zone, why));
        sweep.end(row.run_id, row.chain_id, row.fence, ending).await;
    }
    Ok(())
}

/// Queued runs still waiting past their job's `give_up_after`.
async fn give_up(sweep: &mut Sweep<'_>) -> sqlx::Result<()> {
    let mut by_limit: BTreeMap<Duration, Vec<String>> = BTreeMap::new();
    for (name, job) in &sweep.registry.jobs {
        by_limit.entry(job.spec.policy.give_up_after).or_default().push((*name).to_owned());
    }
    for (limit, jobs) in by_limit {
        let rows: Vec<QueuedRow> = sqlx::query_as(
            "SELECT run_id, chain_id, fence, due_at, expires_at, queued_reason, queued_because FROM downbeat.run
              WHERE state = 'queued' AND job_name = ANY($1) AND created_at < now() - $2 LIMIT $3",
        )
        .bind(&jobs)
        .bind(interval(limit))
        .bind(BATCH)
        .fetch_all(sweep.pool)
        .await?;
        for row in rows {
            let ending = Ending::failed("gave_up", reasons::gave_up(limit, row.queued_reason.as_deref()));
            sweep.end(row.run_id, row.chain_id, row.fence, ending).await;
        }
    }
    Ok(())
}

/// Queued runs of a job that no worker seen in the last hour has in its build.
async fn unregistered(sweep: &mut Sweep<'_>) -> sqlx::Result<()> {
    let rows: Vec<QueuedRow> = sqlx::query_as(
        "SELECT r.run_id, r.chain_id, r.fence, r.due_at, r.expires_at, r.queued_reason, r.queued_because
           FROM downbeat.run r
          WHERE r.state = 'queued' AND r.created_at < now() - interval '1 hour'
            AND EXISTS (SELECT 1 FROM downbeat.worker w WHERE w.role = 'worker')
            AND NOT EXISTS (SELECT 1 FROM downbeat.worker w
                             WHERE w.role = 'worker' AND w.last_seen_at > now() - interval '1 hour' AND r.job_name = ANY(w.jobs))
          LIMIT $1",
    )
    .bind(BATCH)
    .fetch_all(sweep.pool)
    .await?;
    for row in rows {
        let ending = Ending::failed("job_not_registered", reasons::not_registered());
        sweep.end(row.run_id, row.chain_id, row.fence, ending).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn sweeps_run_every_five_seconds_and_pruning_every_hour() {
        let (sweeps, prunes) = (AtomicU32::new(0), AtomicU32::new(0));
        let stop = CancellationToken::new();
        let clocks = on_two_clocks(
            || async {
                sweeps.fetch_add(1, Ordering::SeqCst);
            },
            || async {
                prunes.fetch_add(1, Ordering::SeqCst);
            },
            stop.clone(),
        );
        let two_hours = async {
            tokio::time::sleep(Duration::from_hours(2) + Duration::from_millis(1)).await;
            stop.cancel();
        };
        tokio::join!(clocks, two_hours);
        assert_eq!(prunes.load(Ordering::SeqCst), 3, "at start, after one hour and after two");
        assert_eq!(sweeps.load(Ordering::SeqCst), 2 * 3600 / 5 + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_long_prune_holds_up_no_sweep_and_a_slow_sweep_causes_no_burst() {
        let (sweeps, prunes) = (AtomicU32::new(0), AtomicU32::new(0));
        let stop = CancellationToken::new();
        let clocks = on_two_clocks(
            || async {
                // The second sweep takes twelve seconds, past two of its ticks.
                if sweeps.fetch_add(1, Ordering::SeqCst) == 1 {
                    tokio::time::sleep(Duration::from_secs(12)).await;
                }
            },
            || async {
                prunes.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_mins(5)).await;
            },
            stop.clone(),
        );
        let observe = async {
            tokio::time::sleep(Duration::from_secs(18)).await;
            let after_slow_sweep = sweeps.load(Ordering::SeqCst);
            tokio::time::sleep(Duration::from_mins(4)).await;
            stop.cancel();
            after_slow_sweep
        };
        let ((), after_slow_sweep) = tokio::join!(clocks, observe);
        assert_eq!(prunes.load(Ordering::SeqCst), 1, "the first prune is still running");
        // Sweeps at 0 s and 5 s, which ends at 17 s; one more at once for the ticks it missed, then
        // every five seconds from there: 22 s, 27 s, and so on up to 257 s.
        assert_eq!(after_slow_sweep, 3, "no sweep bursts to catch up");
        assert_eq!(sweeps.load(Ordering::SeqCst), 2 + 49, "sweeps went on during the prune");
    }
}
