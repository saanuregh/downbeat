//! What the engine tells a product: every finished run, and events worth knowing about.
//!
//! Both are delivered from the database by the scheduler role, at least once. Each delivery
//! carries an id that a callback which pages someone should pass on as its de-duplication key.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// A finished run, as `on_finish` receives it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FinishedRun {
    /// The run's id; also the delivery's de-duplication key.
    pub run_id: Uuid,
    pub job: String,
    pub chain_id: Uuid,
    /// `succeeded`, `skipped`, `failed`, `cancelled` or `expired`.
    pub status: String,
    /// For a skipped run, why: `job_skipped`, `condition_false`, `need_not_met`, `input_unchanged`
    /// or `joined`.
    pub skipped_because: Option<String>,
    /// For a failed run, how: `error`, `panic`, `crash`, `timeout` and so on.
    pub failed_because: Option<String>,
    /// One plain sentence: what happened and why.
    pub reason: String,
    pub started_by: String,
    pub actor: Option<String>,
    pub failures: i32,
    pub output: Option<Value>,
    pub finished_at: DateTime<Utc>,
    /// The link to the run's logs, when the definition has a `log_link`.
    pub log_link: Option<String>,
}

impl FinishedRun {
    pub fn failed(&self) -> bool {
        self.status == "failed"
    }
}

/// Something worth knowing about that is not a finished run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// A worker stopped responding while a run's function was executing.
    WorkerLost { run_id: Uuid, job: String, worker: String },
    /// A run has been interrupted by deploys this many times.
    RunInterrupted { run_id: Uuid, job: String, times: i32 },
    /// A run has gone much longer than usual.
    RunSlow { run_id: Uuid, job: String, running_seconds: i64, expected_seconds: i64 },
    /// A job has not succeeded within its `expect_success_every`.
    JobOverdue { job: String, last_success: Option<DateTime<Utc>> },
    /// Occurrences of a schedule were dropped because no scheduler ran in time.
    OccurrencesDropped { schedule: String, count: i64, first: DateTime<Utc>, last: DateTime<Utc> },
    /// Creating a schedule's run failed; it is tried again a minute later.
    ScheduleFailed { schedule: String, error: String },
    /// The database could not be reached for longer than a lease; running functions were stopped.
    DatabaseUnreachable { worker: String },
    /// Requests to the control API were refused: this many since the last report, at most one a
    /// minute per caller and status.
    RequestRefused { caller: Option<String>, status: u16, count: i64, reason: String },
}

/// One delivery of an event: the event and its de-duplication key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventDelivery {
    pub id: i64,
    pub at: DateTime<Utc>,
    pub event: Event,
}
