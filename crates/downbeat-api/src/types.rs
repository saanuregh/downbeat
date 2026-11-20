//! What the control API's endpoints take and return.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// One page of a list, and the cursor for the next.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    /// Pass as `cursor` for the next page; absent on the last.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// An error, with a sentence that says what to do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    pub error: String,
    pub message: String,
}

/// The versions a scheduler speaks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub api: u32,
    pub engine: String,
}

/// One run, queued, running or finished.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunView {
    pub run_id: Uuid,
    pub job: String,
    pub pipeline: Option<String>,
    pub chain_id: Uuid,
    /// `queued`, `running`, `succeeded`, `skipped`, `failed`, `cancelled` or `expired`.
    pub status: String,
    pub queued_because: Option<String>,
    pub skipped_because: Option<String>,
    pub failed_because: Option<String>,
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
    pub note: Option<String>,
    /// Only in the detail view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<RunDetail>,
}

/// What only the single-run view returns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunDetail {
    pub error_chain: Vec<String>,
    pub stderr_tail: Option<String>,
    pub call_log: Vec<CallView>,
    pub parents: Vec<Uuid>,
    pub children: Vec<Uuid>,
}

/// One call of a run's function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallView {
    pub worker: String,
    pub build: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub ended: String,
}

/// A job: what it is, what starts it, how it runs, and how it has been doing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobView {
    pub name: String,
    pub pipeline: String,
    /// `function` or `command`.
    pub kind: String,
    pub queue: String,
    /// What starts it and on what conditions, in words.
    pub triggers: Vec<String>,
    /// Its policies that differ from the defaults, in words.
    pub policies: Vec<String>,
    pub input: Value,
    pub output: Value,
    pub supports_dry_run: bool,
    pub last_run: Option<RunSummary>,
    pub next_occurrence: Option<DateTime<Utc>>,
    pub expected_seconds: Option<i64>,
    pub paused: Option<PauseView>,
    pub overdue: bool,
}

/// A run in a few fields, for lists inside other views.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSummary {
    pub run_id: Uuid,
    pub status: String,
    pub reason: String,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PauseView {
    pub by: String,
    pub note: String,
    pub until: Option<DateTime<Utc>>,
    pub since: DateTime<Utc>,
}

/// A pipeline: its jobs and links, and its recent chains.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineView {
    pub name: String,
    pub jobs: Vec<PipelineNode>,
    #[serde(default)]
    pub recent_chains: Vec<ChainSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineNode {
    pub job: String,
    /// `after`, `after_failure` or `after_any`, when the job is linked.
    pub link: Option<String>,
    pub parents: Vec<String>,
    pub schedules: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainSummary {
    pub chain_id: Uuid,
    pub first_job: String,
    pub started_at: DateTime<Utc>,
    /// Status of each job that ran, by job name.
    pub statuses: BTreeMap<String, String>,
    pub cancelled: bool,
}

/// One chain: every job it could reach, with its run, or why it did not run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChainView {
    pub chain_id: Uuid,
    pub pipeline: String,
    pub first_job: String,
    pub started_at: DateTime<Utc>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub cancelled_by: Option<String>,
    pub nodes: Vec<ChainNode>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChainNode {
    pub job: String,
    pub parents: Vec<String>,
    /// Its runs in this chain, newest first (more than one after a retry).
    pub runs: Vec<RunSummary>,
    /// When it has no run: why.
    pub why_not: Option<String>,
}

/// A schedule: its times in words, its next occurrence, its pause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleView {
    pub name: String,
    pub job: String,
    pub times: String,
    pub next_occurrence: Option<DateTime<Utc>>,
    pub paused: Option<PauseView>,
}

/// Every occurrence of every schedule on one date.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalendarDay {
    pub date: NaiveDate,
    pub entries: Vec<CalendarEntry>,
    /// Schedules with no occurrences that day, and why.
    pub days_off: Vec<DayOff>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DayOff {
    pub schedule: String,
    pub reason: String,
}

/// A queue: what is waiting and why, and who serves it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueView {
    pub name: String,
    pub queued: i64,
    pub running: i64,
    pub queued_because: BTreeMap<String, i64>,
    pub longest_wait_seconds: Option<i64>,
    pub workers: Vec<WorkerView>,
    pub paused: Option<PauseView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerView {
    pub worker_id: String,
    pub slots: i64,
    pub build: String,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillView {
    pub backfill_id: i64,
    pub job: String,
    pub schedule: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    /// The next date to run, or the one running now.
    pub reached: NaiveDate,
    pub state: String,
    pub reason: Option<String>,
    pub continue_on_failure: bool,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverrideView {
    pub override_id: i64,
    pub schedule: String,
    pub date: NaiveDate,
    pub change: String,
    pub times: Vec<String>,
    pub note: String,
    pub created_by: String,
}

/// `POST /jobs/{name}/runs`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
pub struct StartRequest {
    #[serde(default)]
    pub input: Value,
    #[serde(default)]
    pub priority: Option<i16>,
    #[serde(default)]
    pub not_before: Option<DateTime<Utc>>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub without_children: bool,
    #[serde(default)]
    pub even_if_queued: bool,
    #[serde(default)]
    pub ignore_conditions: bool,
    /// A timeout for this run only, in seconds; 0 for none. Needs `administer`.
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
}

/// What a start did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartResponse {
    pub run_id: Uuid,
    pub status: String,
    /// The same work was already queued; this is its run.
    #[serde(default)]
    pub joined: bool,
    /// The idempotency key was used before; this is the run it produced.
    #[serde(default)]
    pub repeated: bool,
}

/// What an action would do, from `?preview=true`. Nothing is changed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Preview {
    /// One sentence per effect.
    pub effects: Vec<String>,
    /// Conditions that are false right now, for a start that ignores conditions.
    #[serde(default)]
    pub false_conditions: Vec<String>,
}

/// `POST …/pause`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PauseRequest {
    pub note: String,
    /// When the pause ends. Required unless `until_resumed`.
    #[serde(default)]
    pub until: Option<DateTime<Utc>>,
    #[serde(default)]
    pub until_resumed: bool,
}

/// `POST /runs/{id}/mark-succeeded`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarkSucceededRequest {
    #[serde(default)]
    pub output: Value,
    pub note: String,
}

/// `POST /overrides`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverrideRequest {
    pub schedules: Vec<String>,
    pub date: NaiveDate,
    /// `off`, `times` or `window`.
    pub change: String,
    /// For `times`: the day's times; for `window`: its start and end. `HH:MM`.
    #[serde(default)]
    pub times: Vec<String>,
    pub note: String,
}

/// `POST /backfills`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillRequest {
    pub job: String,
    /// Which of the job's schedules supplies the input; default the job's own.
    #[serde(default)]
    pub schedule: Option<String>,
    pub from: NaiveDate,
    pub to: NaiveDate,
    #[serde(default)]
    pub continue_on_failure: bool,
}

/// What a changing action did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Done {
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
}

/// `/healthz` and `/readyz`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Health {
    pub ok: bool,
    pub api: u32,
}
