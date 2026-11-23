//! What the control API's endpoints take and return.
//!
//! Every type is `#[non_exhaustive]`, so a later version can add a field without breaking a caller.
//! A response can still be built, for a test or a fake scheduler, from `Default` by setting its
//! fields; a request is built with its `new(…)` and setters.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

mod codes;
mod requests;

pub use codes::{
    BackfillState, CallEnded, ErrorCode, FailedBecause, JobKind, QueuedBecause, RunStatus, SkippedBecause,
};
pub use requests::{BackfillRequest, MarkSucceededRequest, OverrideRequest, PauseRequest, StartRequest};

/// One page of a list, and the cursor for the next.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Page<T> {
    pub items: Vec<T>,
    /// Pass as `cursor` for the next page; absent on the last.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

impl<T> Default for Page<T> {
    fn default() -> Self {
        Page { items: Vec::new(), next_cursor: None }
    }
}

/// Which page of a list to ask for: the first, or the one after a page's `next_cursor`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PageRequest {
    /// How many items, at most; the scheduler's default (50) when not set, and never over 200.
    pub limit: Option<u32>,
    /// The `next_cursor` of the page before.
    pub cursor: Option<String>,
}

impl PageRequest {
    /// The first page, of the default size.
    pub fn new() -> Self {
        PageRequest::default()
    }

    /// At most this many items.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// The page after the one whose `next_cursor` this is.
    #[must_use]
    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }
}

/// An error, with a sentence that says what to do.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ApiError {
    pub error: ErrorCode,
    pub message: String,
}

/// The versions a scheduler speaks.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Version {
    pub api: u32,
    pub engine: String,
}

/// One run, queued, running or finished.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RunView {
    pub run_id: Uuid,
    pub job: String,
    pub pipeline: Option<String>,
    pub chain_id: Uuid,
    pub status: RunStatus,
    pub queued_because: Option<QueuedBecause>,
    pub skipped_because: Option<SkippedBecause>,
    pub failed_because: Option<FailedBecause>,
    /// One plain sentence: what is happening or happened, and why.
    pub reason: String,
    pub started_by: String,
    pub actor: Option<String>,
    pub schedule: Option<String>,
    pub queue: String,
    pub priority: i16,
    pub due_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    /// The attempt number, counting failed calls.
    pub attempt: i32,
    pub attempts: i32,
    /// How many times the function has been called in all.
    pub calls: i32,
    pub lost_count: i32,
    pub interruptions: i32,
    pub dry_run: bool,
    pub progress: Option<String>,
    pub has_checkpoint: bool,
    pub slow: bool,
    pub expected_seconds: Option<i64>,
    pub input: Option<Value>,
    pub output: Option<Value>,
    pub log_link: Option<String>,
    /// Set when a person, not the job, recorded the outcome.
    pub marked_by: Option<String>,
    /// When they did.
    #[serde(default)]
    pub marked_at: Option<DateTime<Utc>>,
    pub note: Option<String>,
    /// Only in the detail view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<RunDetail>,
}

/// What only the single-run view returns.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RunDetail {
    pub error_chain: Vec<String>,
    pub stderr_tail: Option<String>,
    pub call_log: Vec<CallView>,
    pub parents: Vec<Uuid>,
    pub children: Vec<Uuid>,
}

/// One call of a run's function.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CallView {
    pub worker: String,
    pub build: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub ended: CallEnded,
}

/// A job: what it is, what starts it, how it runs, and how it has been doing.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobView {
    pub name: String,
    pub pipeline: String,
    pub kind: JobKind,
    pub queue: String,
    /// What starts it and on what conditions, in words.
    pub triggers: Vec<String>,
    /// Its policies that differ from the defaults, in words.
    pub policies: Vec<String>,
    pub input: Value,
    pub output: Value,
    /// The input as a JSON Schema, for forms and callers in other languages.
    pub input_schema: Value,
    pub supports_dry_run: bool,
    pub last_run: Option<RunSummary>,
    pub next_occurrence: Option<DateTime<Utc>>,
    pub expected_seconds: Option<i64>,
    pub paused: Option<PauseView>,
    pub overdue: bool,
}

/// A run in a few fields, for lists inside other views.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RunSummary {
    pub run_id: Uuid,
    pub status: RunStatus,
    pub reason: String,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PauseView {
    pub by: String,
    pub note: String,
    pub until: Option<DateTime<Utc>>,
    pub since: DateTime<Utc>,
}

/// A pipeline: its jobs and links, and its recent chains.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PipelineView {
    pub name: String,
    pub jobs: Vec<PipelineNode>,
    #[serde(default)]
    pub recent_chains: Vec<ChainSummary>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PipelineNode {
    pub job: String,
    /// `after`, `after_failure` or `after_any`, when the job is linked.
    pub link: Option<String>,
    pub parents: Vec<String>,
    pub schedules: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ChainSummary {
    pub chain_id: Uuid,
    pub first_job: String,
    pub started_at: DateTime<Utc>,
    /// Status of each job that ran, by job name.
    pub statuses: BTreeMap<String, RunStatus>,
    pub cancelled: bool,
}

/// One chain: every job it could reach, with its run, or why it did not run.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ChainView {
    pub chain_id: Uuid,
    pub pipeline: String,
    pub first_job: String,
    pub started_at: DateTime<Utc>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub cancelled_by: Option<String>,
    pub nodes: Vec<ChainNode>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ChainNode {
    pub job: String,
    pub parents: Vec<String>,
    /// Its runs in this chain, newest first (more than one after a retry).
    pub runs: Vec<RunSummary>,
    /// When it has no run: why.
    pub why_not: Option<String>,
}

/// A schedule: its times in words, its next occurrence, its pause.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ScheduleView {
    pub name: String,
    pub job: String,
    pub times: String,
    pub next_occurrence: Option<DateTime<Utc>>,
    pub paused: Option<PauseView>,
}

/// Every occurrence of every schedule on one date.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CalendarDay {
    pub date: NaiveDate,
    pub entries: Vec<CalendarEntry>,
    /// Schedules with no occurrences that day, and why.
    pub days_off: Vec<DayOff>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CalendarEntry {
    pub schedule: String,
    pub job: String,
    pub at: DateTime<Utc>,
    /// `normal`, `rule` or `override`.
    pub source: String,
    pub note: Option<String>,
    pub paused: bool,
    /// For a past occurrence: its run, if it had one.
    pub run: Option<RunSummary>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DayOff {
    pub schedule: String,
    pub reason: String,
}

/// A queue: what is waiting and why, and who serves it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueueView {
    pub name: String,
    pub queued: i64,
    pub running: i64,
    pub queued_because: BTreeMap<String, i64>,
    pub longest_wait_seconds: Option<i64>,
    pub workers: Vec<WorkerView>,
    pub paused: Option<PauseView>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct WorkerView {
    pub worker_id: String,
    pub slots: i64,
    pub build: String,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct BackfillView {
    pub backfill_id: i64,
    pub job: String,
    pub schedule: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    /// The next date to run, or the one running now.
    pub reached: NaiveDate,
    pub state: BackfillState,
    pub reason: Option<String>,
    pub continue_on_failure: bool,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AuditEntry {
    pub audit_id: i64,
    pub at: DateTime<Utc>,
    pub actor: String,
    pub action: String,
    pub job: Option<String>,
    pub run_id: Option<Uuid>,
    pub schedule: Option<String>,
    pub detail: Value,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OverrideView {
    pub override_id: i64,
    pub schedule: String,
    pub date: NaiveDate,
    pub change: String,
    pub times: Vec<String>,
    pub note: String,
    pub created_by: String,
}

/// What a start did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StartResponse {
    pub run_id: Uuid,
    pub status: RunStatus,
    /// The same work was already queued; this is its run.
    #[serde(default)]
    pub joined: bool,
    /// The idempotency key was used before; this is the run it produced.
    #[serde(default)]
    pub repeated: bool,
}

/// What an action would do, from `?preview=true`. Nothing is changed.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Preview {
    /// One sentence per effect.
    pub effects: Vec<String>,
    /// Conditions that are false right now, for a start that ignores conditions.
    #[serde(default)]
    pub false_conditions: Vec<String>,
}

/// What a changing action did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Done {
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
}

/// `/healthz` and `/readyz`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Health {
    pub ok: bool,
    pub api: u32,
}

/// `GET /report`: each job's recent days, and yesterday's failures. Days are calendar days in the
/// definition's zone.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Report {
    pub zone: String,
    /// Oldest first; the last is today, so far.
    pub days: Vec<NaiveDate>,
    /// Jobs that failed yesterday first, most failures first, then the rest by name.
    pub jobs: Vec<JobReport>,
    /// Newest first, at most 50; only runs still in the history, so a failure pruning has removed
    /// since is counted in `jobs` but not listed here.
    pub failed_yesterday: Vec<FailedRun>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobReport {
    pub job: String,
    /// One per day of [`Report::days`], in the same order.
    pub days: Vec<DayReport>,
}

/// One job's runs that finished on one day. Dry runs and joined requests are not counted.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DayReport {
    pub date: NaiveDate,
    pub succeeded: i64,
    pub failed: i64,
    pub skipped: i64,
    pub cancelled: i64,
    pub expired: i64,
    /// Failed ÷ (succeeded + failed); absent when neither happened.
    pub failure_rate: Option<f64>,
    /// From start to finish, over succeeded runs still in the history.
    pub median_seconds: Option<f64>,
    pub p90_seconds: Option<f64>,
    /// Over every succeeded run, including those pruned since.
    pub longest_seconds: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FailedRun {
    pub run_id: Uuid,
    pub job: String,
    pub failed_because: Option<FailedBecause>,
    pub reason: String,
    pub finished_at: DateTime<Utc>,
    pub log_link: Option<String>,
}
