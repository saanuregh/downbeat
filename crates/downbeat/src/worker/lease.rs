//! Keeping a run's lease: a heartbeat every few seconds, carrying the latest progress and
//! checkpoint, and noticing when the run should stop.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use sqlx::PgPool;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use uuid::Uuid;

use crate::store::runs::{self, Held};

/// Why a held run must stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StopReason {
    /// A person asked; their name.
    CancelRequested(String),
    /// The fence moved: another worker or the reaper has the run now.
    Lost,
    /// No heartbeat has succeeded for a whole lease; the database may think the run is lost.
    Unreachable,
}

#[derive(Default)]
struct Pending {
    progress: Option<String>,
    checkpoint: Option<Value>,
}

/// The lease keeper for one run.
pub(crate) struct Lease {
    pending: Arc<Mutex<Pending>>,
    pub(crate) stop: watch::Receiver<Option<StopReason>>,
    task: JoinHandle<()>,
    pool: PgPool,
    held: (Uuid, i64, String),
    length: Duration,
}

impl Lease {
    pub(crate) fn keep(pool: &PgPool, held: Held<'_>, heartbeat: Duration, length: Duration) -> Lease {
        let pending: Arc<Mutex<Pending>> = Arc::default();
        let (signal, stop) = watch::channel(None);
        let owned = (held.run_id, held.fence, held.worker.to_owned());
        let task = tokio::spawn(beat(pool.clone(), owned.clone(), heartbeat, length, Arc::clone(&pending), signal));
        Lease { pending, stop, task, pool: pool.clone(), held: owned, length }
    }

    /// Saved with the next heartbeat.
    pub(crate) fn progress(&self, text: String) {
        self.pending.lock().expect("lease state").progress = Some(text);
    }

    /// Saved with the next heartbeat.
    pub(crate) fn checkpoint(&self, state: Value) {
        self.pending.lock().expect("lease state").checkpoint = Some(state);
    }

    /// Saves a checkpoint now. False when the run is no longer held.
    pub(crate) async fn checkpoint_now(&self, state: Value) -> sqlx::Result<bool> {
        let (run_id, fence, worker) = &self.held;
        let held = Held { run_id: *run_id, fence: *fence, worker };
        Ok(runs::heartbeat(&self.pool, held, self.length, None, Some(&state)).await?.is_some())
    }

    pub(crate) fn stopped(&self) -> Option<StopReason> {
        self.stop.borrow().clone()
    }

    pub(crate) fn release(self) {
        self.task.abort();
    }
}

impl Drop for Lease {
    /// A lease is never renewed by a slot that has gone, whatever way it went.
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn beat(
    pool: PgPool,
    (run_id, fence, worker): (Uuid, i64, String),
    heartbeat: Duration,
    length: Duration,
    pending: Arc<Mutex<Pending>>,
    signal: watch::Sender<Option<StopReason>>,
) {
    // Timed from when a heartbeat that succeeded was sent, not from its reply, so the worker never
    // believes it holds the run for longer than the database does.
    let mut last_good_send = Instant::now();
    let mut next_beat = Instant::now() + heartbeat;
    loop {
        // Wake for the next beat, or when the lease would run out without one, whichever is first,
        // so any `heartbeat`, including one under a second, is kept exactly.
        tokio::time::sleep_until(next_beat.min(last_good_send + length)).await;
        if last_good_send.elapsed() >= length {
            let _ = signal.send(Some(StopReason::Unreachable));
            return;
        }
        if Instant::now() < next_beat {
            continue;
        }
        next_beat = Instant::now() + heartbeat;
        let (progress, checkpoint) = {
            let mut pending = pending.lock().expect("lease state");
            (pending.progress.take(), pending.checkpoint.take())
        };
        let sent = Instant::now();
        let held = Held { run_id, fence, worker: &worker };
        // A heartbeat that hangs (a half-open connection) must not outlast the lease.
        let within = length.saturating_sub(last_good_send.elapsed());
        let sent_heartbeat = tokio::time::timeout(
            within,
            runs::heartbeat(&pool, held, length, progress.as_deref(), checkpoint.as_ref()),
        )
        .await;
        let failed = match sent_heartbeat {
            Ok(Ok(Some(cancel))) => {
                last_good_send = sent;
                if let Some(by) = cancel {
                    let _ = signal.send(Some(StopReason::CancelRequested(by)));
                }
                false
            }
            Ok(Ok(None)) => {
                let _ = signal.send(Some(StopReason::Lost));
                return;
            }
            Ok(Err(error)) => {
                tracing::warn!(%run_id, %error, "a heartbeat failed; trying again");
                true
            }
            Err(_) => {
                tracing::warn!(%run_id, within = ?within, "a heartbeat got no answer within the lease; trying again");
                true
            }
        };
        if failed {
            let mut pending = pending.lock().expect("lease state");
            pending.progress = pending.progress.take().or(progress);
            pending.checkpoint = pending.checkpoint.take().or(checkpoint);
        }
    }
}
