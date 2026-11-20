//! Ending a run: the one routine every ending goes through. In one transaction it moves the run to
//! the history and creates the children that its ending starts.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{FromRow, PgConnection, PgPool};
use uuid::Uuid;

use super::runs::{Call, NewRun, Place, RunRow, StartedBy, create};
use crate::pipeline::LinkKind;
use crate::registry::Registry;

/// How a run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    Succeeded,
    Skipped,
    Failed,
    Cancelled,
    Expired,
}

impl Status {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Status::Succeeded => "succeeded",
            Status::Skipped => "skipped",
            Status::Failed => "failed",
            Status::Cancelled => "cancelled",
            Status::Expired => "expired",
        }
    }
}

/// Everything recorded about a run's ending.
#[derive(Debug, Clone)]
pub(crate) struct Ending {
    pub(crate) status: Status,
    pub(crate) skipped_because: Option<&'static str>,
    pub(crate) failed_because: Option<&'static str>,
    pub(crate) reason: String,
    pub(crate) error_chain: Option<Vec<String>>,
    pub(crate) output: Option<Value>,
    pub(crate) stderr_tail: Option<String>,
    pub(crate) call: Option<Call>,
    /// The ending is a failed call of the function, which counts toward its attempts.
    pub(crate) failed_call: bool,
}

impl Ending {
    pub(crate) fn new(status: Status, reason: impl Into<String>) -> Self {
        Ending {
            status,
            skipped_because: None,
            failed_because: None,
            reason: reason.into(),
            error_chain: None,
            output: None,
            stderr_tail: None,
            call: None,
            failed_call: false,
        }
    }

    pub(crate) fn skipped(because: &'static str, reason: impl Into<String>) -> Self {
        Ending { skipped_because: Some(because), ..Ending::new(Status::Skipped, reason) }
    }

    pub(crate) fn failed(because: &'static str, reason: impl Into<String>) -> Self {
        Ending { failed_because: Some(because), ..Ending::new(Status::Failed, reason) }
    }

    #[must_use]
    pub(crate) fn with_call(mut self, call: Option<Call>) -> Self {
        self.call = call;
        self
    }
}

/// A finished run, as stored.
#[derive(Debug, Clone, FromRow)]
pub(crate) struct HistoryRow {
    pub(crate) run_id: Uuid,
    pub(crate) job_name: String,
    pub(crate) queue: String,
    pub(crate) priority: i16,
    pub(crate) due_at: DateTime<Utc>,
    pub(crate) started_by: String,
    pub(crate) schedule: Option<String>,
    pub(crate) chain_id: Uuid,
    pub(crate) parents: Vec<Uuid>,
    pub(crate) input: Option<Value>,
    pub(crate) input_hash: Option<String>,
    pub(crate) turn_key: Option<String>,
    pub(crate) dry_run: bool,
    pub(crate) ignore_conditions: bool,
    pub(crate) without_children: bool,
    pub(crate) starts: i32,
    pub(crate) failures: i16,
    pub(crate) lost_count: i16,
    pub(crate) interruptions: i16,
    pub(crate) call_log: Value,
    pub(crate) actor: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) status: String,
    pub(crate) skipped_because: Option<String>,
    pub(crate) failed_because: Option<String>,
    pub(crate) reason: String,
    pub(crate) error_chain: Option<Vec<String>>,
    pub(crate) output: Option<Value>,
    pub(crate) stderr_tail: Option<String>,
    pub(crate) started_at: Option<DateTime<Utc>>,
    pub(crate) finished_at: DateTime<Utc>,
    pub(crate) marked_by: Option<String>,
    pub(crate) note: Option<String>,
    pub(crate) joined_run: Option<Uuid>,
    pub(crate) notify_after: Option<DateTime<Utc>>,
}

/// Takes the chain's lock. Everything that changes a chain takes it first, so two parents that end
/// at the same instant never miss each other.
pub(crate) async fn lock_chain(connection: &mut PgConnection, chain_id: Uuid) -> sqlx::Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))").bind(chain_id).execute(connection).await?;
    Ok(())
}

/// Ends a run, if it still has this fence. Returns the history row, or `None` when the run had
/// changed hands and nothing was written.
pub(crate) async fn end(pool: &PgPool, registry: &Registry, run_id: Uuid, chain_id: Uuid, fence: i64, ending: Ending) -> sqlx::Result<Option<HistoryRow>> {
    let mut transaction = pool.begin().await?;
    lock_chain(&mut transaction, chain_id).await?;
    let ended = end_locked(&mut transaction, registry, run_id, fence, &ending).await?;
    transaction.commit().await?;
    Ok(ended)
}

/// The same as [`end`], inside a transaction that already holds the chain's lock.
pub(crate) async fn end_locked(
    connection: &mut PgConnection,
    registry: &Registry,
    run_id: Uuid,
    fence: i64,
    ending: &Ending,
) -> sqlx::Result<Option<HistoryRow>> {
    let Some(run): Option<RunRow> = sqlx::query_as("DELETE FROM downbeat.run WHERE run_id = $1 AND fence = $2 RETURNING *")
        .bind(run_id)
        .bind(fence)
        .fetch_optional(&mut *connection)
        .await?
    else {
        return Ok(None);
    };
    let call = ending.call.as_ref().map(|call| serde_json::to_value(call).unwrap_or(Value::Null));
    let row: HistoryRow = sqlx::query_as(
        "INSERT INTO downbeat.run_history (
            run_id, job_name, queue, priority, due_at, started_by, schedule, chain_id, parents, input, input_hash,
            turn_key, dry_run, ignore_conditions, without_children, starts, failures, lost_count, interruptions,
            call_log, actor, created_at, status, skipped_because, failed_because, reason, error_chain, output,
            stderr_tail, started_at, notify_after)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
                 CASE WHEN $20::jsonb IS NULL THEN $21 ELSE downbeat.append_call($21, $20::jsonb) END,
                 $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, now())
         RETURNING *",
    )
    .bind(run.run_id)
    .bind(&run.job_name)
    .bind(&run.queue)
    .bind(run.priority)
    .bind(run.due_at)
    .bind(&run.started_by)
    .bind(&run.schedule)
    .bind(run.chain_id)
    .bind(&run.parents)
    .bind(&run.input)
    .bind(&run.input_hash)
    .bind(&run.turn_key)
    .bind(run.dry_run)
    .bind(run.ignore_conditions)
    .bind(run.without_children)
    .bind(run.starts)
    .bind(run.failures + i16::from(ending.failed_call))
    .bind(run.lost_count)
    .bind(run.interruptions)
    .bind(call)
    .bind(&run.call_log)
    .bind(&run.actor)
    .bind(run.created_at)
    .bind(ending.status.as_str())
    .bind(ending.skipped_because)
    .bind(ending.failed_because)
    .bind(&ending.reason)
    .bind(&ending.error_chain)
    .bind(&ending.output)
    .bind(&ending.stderr_tail)
    .bind(run.first_started_at)
    .fetch_one(&mut *connection)
    .await?;
    if !run.without_children {
        create_children(connection, registry, &row).await?;
    }
    Ok(Some(row))
}

/// Creates each child whose parents have now all ended the way its link asks, in this chain.
pub(crate) async fn create_children(connection: &mut PgConnection, registry: &Registry, ended: &HistoryRow) -> sqlx::Result<()> {
    let cancelled: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT cancelled_at FROM downbeat.chain WHERE chain_id = $1")
        .bind(ended.chain_id)
        .fetch_optional(&mut *connection)
        .await?
        .flatten();
    if cancelled.is_some() {
        return Ok(());
    }
    let Some(children) = registry.children.get(ended.job_name.as_str()) else { return Ok(()) };
    for child_name in children {
        let Some(child) = registry.job(child_name) else { continue };
        let Some(link) = &child.link else { continue };
        if already_in_chain(connection, child_name, ended.chain_id).await? {
            continue;
        }
        let mut parents = Vec::new();
        for parent in &link.parents {
            match latest_ending(connection, parent, ended.chain_id).await? {
                Some((run_id, status)) if satisfies(link.kind, &status) => parents.push(run_id),
                _ => break,
            }
        }
        if parents.len() != link.parents.len() {
            continue;
        }
        let policy = &child.spec.policy;
        let run = NewRun {
            job: (*child_name).to_owned(),
            queue: policy.queue.to_owned(),
            priority: policy.priority,
            input: None,
            input_hash: None,
            turn_key: None,
            joins_queued_work: false,
            due_at: None,
            expires_at: None,
            not_before: None,
            started_by: StartedBy::Link,
            schedule: None,
            place: Place::Chain { chain_id: ended.chain_id, parents },
            dry_run: ended.dry_run,
            ignore_conditions: false,
            without_children: false,
            timeout_override: None,
            actor: None,
            queued_reason: None,
        };
        create(connection, &run).await?;
    }
    Ok(())
}

fn satisfies(kind: LinkKind, status: &str) -> bool {
    match kind {
        LinkKind::After => status == "succeeded",
        LinkKind::AfterFailure => status == "failed",
        LinkKind::AfterAny => true,
    }
}

async fn already_in_chain(connection: &mut PgConnection, job: &str, chain_id: Uuid) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE job_name = $1 AND chain_id = $2)
             OR EXISTS (SELECT 1 FROM downbeat.run_history WHERE job_name = $1 AND chain_id = $2 AND joined_run IS NULL)",
    )
    .bind(job)
    .bind(chain_id)
    .fetch_one(connection)
    .await
}

/// How a job last ended in a chain, unless it has a run there that has not ended.
async fn latest_ending(connection: &mut PgConnection, job: &str, chain_id: Uuid) -> sqlx::Result<Option<(Uuid, String)>> {
    let unfinished: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE job_name = $1 AND chain_id = $2)")
        .bind(job)
        .bind(chain_id)
        .fetch_one(&mut *connection)
        .await?;
    if unfinished {
        return Ok(None);
    }
    sqlx::query_as(
        "SELECT run_id, status FROM downbeat.run_history
          WHERE job_name = $1 AND chain_id = $2 AND joined_run IS NULL
          ORDER BY finished_at DESC LIMIT 1",
    )
    .bind(job)
    .bind(chain_id)
    .fetch_optional(connection)
    .await
}
